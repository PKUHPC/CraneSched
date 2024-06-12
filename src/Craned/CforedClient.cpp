#include "CforedClient.h"

#include <utility>

#include "crane/String.h"
namespace Craned {

CforedClient::CforedClient() : m_stopped_(false){};

CforedClient::~CforedClient() {
  CRANE_TRACE("CforedClient to {} is being destructed.", m_cfored_name_);
  m_stopped_ = true;
  if (m_fwd_thread_.joinable()) m_fwd_thread_.join();
  m_cq_.Shutdown();
  CRANE_TRACE("CforedClient to {} was destructed.", m_cfored_name_);
};

void CforedClient::InitChannelAndStub(const std::string& cfored_name) {
  m_cfored_name_ = cfored_name;
  grpc::ChannelArguments channel_args;
  std::string cfored_address = fmt::format("{}:{}", cfored_name, "10012");
  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (g_config.ListenConf.UseTls) {
    grpc::SslCredentialsOptions ssl_opts;
    // pem_root_certs is actually the certificate of server side rather than
    // CA certificate. CA certificate is not needed.
    // Since we use the same cert/key pair for both cranectld/craned,
    // pem_root_certs is set to the same certificate.
    ssl_opts.pem_root_certs = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_cert_chain = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_private_key = g_config.ListenConf.ServerKeyContent;

    m_cfored_channel_ = grpc::CreateCustomChannel(
        cfored_address, grpc::SslCredentials(ssl_opts), channel_args);
  } else {
    m_cfored_channel_ = grpc::CreateCustomChannel(
        cfored_address, grpc::InsecureChannelCredentials(), channel_args);
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CraneForeD::NewStub(m_cfored_channel_);

  m_fwd_thread_ = std::thread([this] { AsyncSendRecvThread_(); });
}

void CforedClient::CleanOutputQueueAndWriteToStreamThread_(
    ClientAsyncReaderWriter<StreamCforedTaskIORequest, StreamCforedTaskIOReply>*
        stream,
    std::atomic<bool>* write_pending) {
  CRANE_TRACE("CleanOutputQueueThread started.");
  std::pair<task_id_t, std::string> output;
  bool ok = m_output_queue_.try_dequeue(output);

  // Make sure before exit all output has been drained.
  while (!m_stopped_ || ok) {
    if (!ok) {
      std::this_thread::sleep_for(std::chrono::milliseconds(75));
      ok = m_output_queue_.try_dequeue(output);
      continue;
    }

    StreamCforedTaskIORequest request;
    request.set_type(StreamCforedTaskIORequest::CRANED_TASK_OUTPUT);

    auto* payload = request.mutable_payload_task_output_req();
    payload->set_msg(output.second), payload->set_task_id(output.first);

    while (write_pending->load(std::memory_order::acquire))
      std::this_thread::sleep_for(std::chrono::milliseconds(25));

    CRANE_TRACE("Writing output...");
    write_pending->store(true, std::memory_order::release);
    stream->Write(request, (void*)Tag::Write);

    ok = m_output_queue_.try_dequeue(output);
  }

  CRANE_TRACE("CleanOutputQueueThread exited.");
}

void CforedClient::AsyncSendRecvThread_() {
  enum class State : int {
    Registering = 0,
    WaitRegisterAck = 1,
    Forwarding = 2,
    Unregistering = 3,
    End = 4,
  };

  std::thread output_clean_thread;
  std::atomic<bool> write_pending;

  bool ok;
  Tag tag;
  grpc::ClientContext context;
  StreamCforedTaskIORequest request;
  StreamCforedTaskIOReply reply;
  grpc::CompletionQueue::NextStatus next_status;

  auto stream =
      m_stub_->AsyncTaskIOStream(&context, &m_cq_, (void*)Tag::Prepare);

  CRANE_TRACE("Preparing TaskIOStream...");

  State state = State::Registering;
  while (true) {
    auto ddl = std::chrono::system_clock::now() + std::chrono::milliseconds(50);
    next_status = m_cq_.AsyncNext((void**)&tag, &ok, ddl);
    // CRANE_TRACE("NextStatus: {}, ok: {}, Tag received: {}, state: {}",
    //             int(next_status), ok, intptr_t(tag), int(state));

    if (next_status == CompletionQueue::SHUTDOWN) break;

    // TIMEOUT is like the Idle event in libuv and
    // thus a context switch of state machine.
    if (next_status == grpc::CompletionQueue::TIMEOUT) {
      if (m_stopped_) {
        // No need to switch to Unregistering state if already switched.
        if (state == State::Unregistering) continue;
        // Wait for forwarding thread to drain output queue and stop.
        if (output_clean_thread.joinable()) output_clean_thread.join();
        // If some writes are pending, let state machine clean them up.
        if (write_pending.load(std::memory_order::acquire)) continue;

        // Cfored received stopping signal. Unregistering...
        CRANE_TRACE("Unregistering on cfored {}.", m_cfored_name_);

        request.Clear();
        request.set_type(StreamCforedTaskIORequest::CRANED_UNREGISTER);
        request.mutable_payload_unregister_req()->set_craned_id(
            g_config.CranedIdOfThisNode);

        stream->WriteLast(request, grpc::WriteOptions(), (void*)Tag::Write);

        // There's no need to issue a read request here,
        // since every state ends with a read request issuing.

        state = State::Unregistering;
      }

      continue;
    }

    CRANE_ASSERT(next_status == grpc::CompletionQueue::GOT_EVENT);

    // All failures of Write or Read cause the end of state machine.
    // But, for Prepare tag which indicates the stream is ready,
    // ok is false, since there's no message to read.
    if (!ok && tag != Tag::Prepare) {
      CRANE_ERROR("Cfored connection failed.");
      state = State::End;
    }

    switch (state) {
    case State::Registering:
      // Stream is ready. Start registering.
      CRANE_TRACE("Registering new stream on cfored {}", m_cfored_name_);

      CRANE_ASSERT_MSG_VA(tag == Tag::Prepare, "Tag: {}", int(tag));

      request.set_type(StreamCforedTaskIORequest::CRANED_REGISTER);
      request.mutable_payload_register_req()->set_craned_id(
          g_config.CranedIdOfThisNode);

      write_pending.store(true, std::memory_order::release);
      stream->Write(request, (void*)Tag::Write);

      state = State::WaitRegisterAck;
      break;

    case State::WaitRegisterAck: {
      CRANE_TRACE("WaitRegisterAck");

      if (tag == Tag::Write) {
        write_pending.store(false, std::memory_order::release);
        CRANE_TRACE("Cfored Registration was sent. Reading Ack...");

        reply.Clear();
        stream->Read(&reply, (void*)Tag::Read);
      } else if (tag == Tag::Read) {
        CRANE_TRACE("Cfored RegisterAck Read. Start Forwarding..");
        state = State::Forwarding;

        // Issue initial read request
        reply.Clear();
        stream->Read(&reply, (void*)Tag::Read);

        // Start output forwarding thread
        output_clean_thread =
            std::thread(&CforedClient::CleanOutputQueueAndWriteToStreamThread_,
                        this, stream.get(), &write_pending);
      }
    } break;

    case State::Forwarding: {
      CRANE_TRACE("Forwarding State");
      // Do nothing for acknowledgements of successful writes in Forward State.
      if (tag == Tag::Write) {
        write_pending.store(false, std::memory_order::release);
        break;
      }

      CRANE_ASSERT(tag == Tag::Read);
      if (reply.type() != StreamCforedTaskIOReply::CRANED_TASK_INPUT) {
        CRANE_ERROR("Expect TASK_INPUT, but got {}", reply.type());
        break;
      }

      task_id_t task_id = reply.payload_task_input_req().task_id();
      const std::string& msg = reply.payload_task_input_req().msg();

      m_mtx_.Lock();
      if (m_task_fwd_meta_map_.contains(task_id)) {
        m_task_fwd_meta_map_[task_id].input_cb(msg);
      } else {
        CRANE_ERROR("Cfored {} trying to send msg to unknown task #{}",
                    m_cfored_name_, task_id);
      }
      m_mtx_.Unlock();

      reply.Clear();
      stream->Read(&reply, (void*)Tag::Read);
    } break;

    case State::Unregistering:
      if (tag == Tag::Write) {
        CRANE_TRACE("UNREGISTER msg was sent. waiting for reply...");
        break;
      }
      CRANE_ASSERT(tag == Tag::Read);
      CRANE_TRACE("UNREGISTER_REPLY msg received.");

      if (reply.type() != StreamCforedTaskIOReply::CRANED_UNREGISTER_REPLY) {
        CRANE_TRACE("Expect UNREGISTER_REPLY, but got {}. Ignoring it.",
                    reply.type());
        break;
      }

      state = State::End;
      [[fallthrough]];

    case State::End:
      break;
    }

    CRANE_TRACE("Next state: {}", int(state));
    if (state == State::End) break;
  }
}

void CforedClient::InitTaskFwdAndSetInputCb(
    task_id_t task_id, std::function<void(const std::string&)> task_input_cb) {
  absl::MutexLock lock(&m_mtx_);
  m_task_fwd_meta_map_[task_id].input_cb = std::move(task_input_cb);
}

bool CforedClient::TaskOutputFinish(task_id_t task_id) {
  absl::MutexLock lock(&m_mtx_);
  auto& task_fwd_meta = m_task_fwd_meta_map_.at(task_id);
  task_fwd_meta.output_stopped = true;
  return task_fwd_meta.output_stopped && task_fwd_meta.proc_stopped;
};

bool CforedClient::TaskProcessStop(task_id_t task_id) {
  absl::MutexLock lock(&m_mtx_);
  auto& task_fwd_meta = m_task_fwd_meta_map_.at(task_id);
  task_fwd_meta.proc_stopped = true;
  return task_fwd_meta.output_stopped && task_fwd_meta.proc_stopped;
};

void CforedClient::TaskOutPutForward(task_id_t task_id,
                                     const std::string& msg) {
  CRANE_TRACE("Receive TaskOutputForward for task #{}: {}", task_id, msg);
  m_output_queue_.enqueue({task_id, msg});
}

bool CforedManager::Init() {
  m_loop_ = uvw::loop::create();

  m_register_handle_ = m_loop_->resource<uvw::async_handle>();
  m_register_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) { RegisterCb_(); });

  m_task_stop_handle_ = m_loop_->resource<uvw::async_handle>();
  m_task_stop_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) { TaskStopCb_(); });

  m_unregister_handle_ = m_loop_->resource<uvw::async_handle>();
  m_unregister_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) { UnregisterCb_(); });

  m_ev_loop_thread_ = std::thread([=, this]() { EvLoopThread_(m_loop_); });

  return true;
}

CforedManager::~CforedManager() {
  CRANE_TRACE("CforedManager destructor called.");
  m_stopped_ = true;
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
}

void CforedManager::EvLoopThread_(const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("CforedMgrThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      m_loop_->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_stopped_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in CforedManager EvLoop.");
  }

  m_loop_->run();
}

void CforedManager::RegisterIOForward(std::string const& cfored,
                                      task_id_t task_id, int fd) {
  RegisterElem elem{.cfored = cfored, .task_id = task_id, .fd = fd};
  std::promise<bool> done;
  std::future<bool> done_fut = done.get_future();

  m_register_queue_.enqueue(std::make_pair(std::move(elem), std::move(done)));
  m_register_handle_->send();
  done_fut.wait();
}

void CforedManager::RegisterCb_() {
  std::pair<RegisterElem, std::promise<bool>> p;
  while (m_register_queue_.try_dequeue(p)) {
    RegisterElem& elem = p.first;
    if (m_cfored_client_map_.contains(elem.cfored)) {
      m_cfored_client_ref_count_map_[elem.cfored]++;
    } else {
      auto cfored_client = std::make_shared<CforedClient>();
      cfored_client->InitChannelAndStub(elem.cfored);

      m_cfored_client_map_[elem.cfored] = std::move(cfored_client);
      m_cfored_client_ref_count_map_[elem.cfored] = 1;
    }

    m_cfored_client_map_[elem.cfored]->InitTaskFwdAndSetInputCb(
        elem.task_id, [fd = elem.fd](const std::string& msg) {
          ssize_t sz_sent = 0;
          while (sz_sent != msg.size())
            sz_sent += write(fd, msg.c_str() + sz_sent, msg.size() - sz_sent);
        });

    CRANE_TRACE("Registering fd {} for outputs of task #{}", elem.fd,
                elem.task_id);
    auto poll_handle = m_loop_->resource<uvw::poll_handle>(elem.fd);
    poll_handle->on<uvw::poll_event>([this, elem = std::move(elem)](
                                         const uvw::poll_event&,
                                         uvw::poll_handle& h) {
      CRANE_TRACE("Detect task #{} output.", elem.task_id);

      constexpr int MAX_BUF_SIZE = 4096;
      char buf[MAX_BUF_SIZE];

      auto ret = read(elem.fd, buf, MAX_BUF_SIZE);
      if (ret == 0) {
        CRANE_TRACE("Task #{} to cfored {} finished its output.", elem.task_id,
                    elem.cfored);
        h.close();

        bool ok_to_free =
            m_cfored_client_map_[elem.cfored]->TaskOutputFinish(elem.task_id);
        if (ok_to_free) {
          CRANE_TRACE("It's ok to unregister task #{} on {}", elem.task_id,
                      elem.cfored);
          UnregisterIOForward_(elem.cfored, elem.task_id);
        }
        return;
      }

      if (ret == -1)
        CRANE_ERROR("Error when reading task #{} output", elem.task_id);

      std::string output(buf, ret);
      CRANE_TRACE("Fwd to task #{}: {}", elem.task_id, output);
      m_cfored_client_map_[elem.cfored]->TaskOutPutForward(elem.task_id,
                                                           output);
    });
    int ret = poll_handle->start(uvw::poll_handle::poll_event_flags::READABLE);
    if (ret < 0)
      CRANE_ERROR("poll_handle->start() error: {}", uv_strerror(ret));

    p.second.set_value(true);
  }
}

void CforedManager::TaskProcOnCforedStopped(std::string const& cfored,
                                            task_id_t task_id) {
  TaskStopElem elem{.cfored = cfored, .task_id = task_id};
  m_task_stop_queue_.enqueue(std::move(elem));
  m_task_stop_handle_->send();
}

void CforedManager::TaskStopCb_() {
  TaskStopElem elem;
  while (m_task_stop_queue_.try_dequeue(elem)) {
    const std::string& cfored = elem.cfored;
    task_id_t task_id = elem.task_id;

    CRANE_TRACE("Task #{} to cfored {} just stopped its process.", elem.task_id,
                elem.cfored);
    bool ok_to_free =
        m_cfored_client_map_[elem.cfored]->TaskProcessStop(elem.task_id);
    if (ok_to_free) {
      CRANE_TRACE("It's ok to unregister task #{} on {}", elem.task_id,
                  elem.cfored);
      UnregisterIOForward_(elem.cfored, elem.task_id);
    }
  }
}

void CforedManager::UnregisterIOForward_(const std::string& cfored,
                                         task_id_t task_id) {
  UnregisterElem elem{.cfored = cfored, .task_id = task_id};
  m_unregister_queue_.enqueue(std::move(elem));
  m_unregister_handle_->send();
}

void CforedManager::UnregisterCb_() {
  UnregisterElem elem;
  while (m_unregister_queue_.try_dequeue(elem)) {
    const std::string& cfored = elem.cfored;
    task_id_t task_id = elem.task_id;

    auto count = m_cfored_client_ref_count_map_[cfored];
    if (count == 1) {
      m_cfored_client_ref_count_map_.erase(cfored);
      m_cfored_client_map_.erase(cfored);
    } else {
      --m_cfored_client_ref_count_map_[cfored];
    }

    g_task_mgr->TaskStopAndDoStatusChangeAsync(task_id);
  }
}

}  // namespace Craned
