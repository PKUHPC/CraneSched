#include "CforedClient.h"

#include <utility>

#include "crane/String.h"
namespace Craned {

CforedClient::CforedClient() : m_stopped_(false){};

CforedClient::~CforedClient() {
  CRANE_TRACE("CforedClient to {} is being destructed.", m_cfored_name_);
  m_stopped_ = true;
  if (m_async_read_write_thread_.joinable()) m_async_read_write_thread_.join();
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

  m_async_read_write_thread_ = std::thread([this] { AsyncSendRecvThread_(); });
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
      if (m_task_input_cb_map_.contains(task_id)) {
        m_task_input_cb_map_[task_id](msg);
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

void CforedClient::SetTaskInputForwardCb(
    task_id_t task_id, std::function<void(const std::string&)> task_input_cb) {
  absl::MutexLock lock(&m_mtx_);
  m_task_input_cb_map_[task_id] = std::move(task_input_cb);
}

void CforedClient::UnsetTaskInputForwardCb(task_id_t task_id) {
  absl::MutexLock lock(&m_mtx_);
  m_task_input_cb_map_.erase(task_id);
};

void CforedClient::TaskOutPutForward(task_id_t task_id,
                                     const std::string& msg) {
  CRANE_TRACE("Receive TaskOutputForward for task #{}: {}", task_id, msg);
  m_output_queue_.enqueue({task_id, msg});
}

bool CforedManager::Init() {
  m_loop_ = uvw::loop::create();
  m_ev_loop_thread_ = std::thread([=, this]() { EvLoopThread_(m_loop_); });

  return true;
}

CforedManager::~CforedManager() {
  CRANE_TRACE("CforedManager destructor called.");
  m_stopped_ = true;
  m_loop_->stop();

  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();

  m_cfored_client_map_.clear();
}

void CforedManager::EvLoopThread_(const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("CforedMgrThr");

  while (!m_stopped_) {
    // TODO: Improve high frequency of looping!
    if (m_stopped_temp_) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      continue;
    }

    m_loop_->run();
  }
}

void CforedManager::RegisterIOForward(std::string const& cfored,
                                      task_id_t task_id, int fd) {
  // TODO: Use async way to register handle rather than locking!
  absl::MutexLock lock(&m_mtx);
  m_stopped_temp_ = true;
  m_loop_->stop();

  auto poll_handle = m_loop_->resource<uvw::poll_handle>(fd);
  poll_handle->on<uvw::poll_event>(
      [this, task_id, cfored](const uvw::poll_event&, uvw::poll_handle& h) {
        int fd = h.fd();
        constexpr int MAX_BUF_SIZE = 4096;

        char buf[MAX_BUF_SIZE];
        auto ret = read(fd, buf, MAX_BUF_SIZE);
        if (ret == 0) return;
        if (ret == -1)
          CRANE_ERROR("Error when reading task #{} output", task_id);

        std::string output(buf, ret);

        absl::MutexLock lock(&this->m_mtx);
        this->m_cfored_client_map_[cfored]->TaskOutPutForward(task_id, output);
      });

  poll_handle->start(uvw::poll_handle::poll_event_flags::READABLE);
  m_stopped_temp_ = false;

  if (m_cfored_client_map_.contains(cfored)) {
    m_cfored_client_ref_count_map_[cfored]++;
  } else {
    auto cfored_client = std::make_shared<CforedClient>();

    // FIXME: No error handling here.
    // TODO: time-consuming operation! Move it to uvw worker thread pool
    //  and notify by async event.
    cfored_client->InitChannelAndStub(cfored);

    m_cfored_client_map_[cfored] = std::move(cfored_client);
    m_cfored_client_ref_count_map_[cfored] = 1;
  }

  m_task_id_handle_map_[task_id] = poll_handle;
  m_cfored_client_map_[cfored]->SetTaskInputForwardCb(
      task_id, [fd](const std::string& msg) {
        ssize_t sz_sent = 0;
        while (sz_sent != msg.size())
          sz_sent += write(fd, msg.c_str() + sz_sent, msg.size() - sz_sent);
      });
}

void CforedManager::UnregisterIOForward(std::string const& cfored,
                                        task_id_t task_id) {
  // TODO: Too large critical area!
  absl::MutexLock lock(&m_mtx);
  auto count = m_cfored_client_ref_count_map_[cfored];
  if (count == 1) {
    m_cfored_client_ref_count_map_.erase(cfored);
    m_cfored_client_map_.erase(cfored);
  } else {
    --m_cfored_client_ref_count_map_[cfored];
    m_cfored_client_map_[cfored]->UnsetTaskInputForwardCb(task_id);
  }

  m_stopped_temp_ = true;
  m_loop_->stop();

  m_task_id_handle_map_[task_id]->stop();
  m_task_id_handle_map_[task_id]->close();
  m_task_id_handle_map_.erase(task_id);

  m_stopped_temp_ = false;
}

}  // namespace Craned
