#include "CforedClient.h"

#include <utility>

#include "crane/String.h"
namespace Craned {

CforedClient::CforedClient() : m_stopped(false){};

CforedClient::~CforedClient() {
  // Todo: remove this log
  CRANE_TRACE("Free cfored client {}", m_cfored_name_);
  m_stopped = true;

  m_async_read_write_thread_.join();
  // Todo: remove this log
  CRANE_TRACE("Free cfored client {} complete", m_cfored_name_);
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

void CforedClient::AsyncSendRecvThread_() {
  using crane::grpc::StreamCfroedTaskIOReply;
  using crane::grpc::StreamCfroedTaskIORequest;
  grpc::ClientContext context;
  auto read_writer = m_stub_->TaskIOStream(&context);
  enum class State : int {
    Registering = 0,
    Forwarding = 1,
    Unregistering = 2,
    Failing = 3
  };
  auto state = State::Registering;
  StreamCfroedTaskIORequest request;
  StreamCfroedTaskIOReply reply;
  while (true) {
    switch (state) {
      case State::Registering: {
        request.set_type(
            crane::grpc::
                StreamCfroedTaskIORequest_CranedRequestType_CRANED_REGISTER);
        request.mutable_payload_register_req()->set_craned_id(
            g_config.CranedIdOfThisNode);
        CRANE_TRACE("Register this node to {}", m_cfored_name_);
        auto ok = read_writer->Write(request);
        if (!ok) {
          CRANE_ERROR(
              "Connection error when trying to register this craned to cfored "
              "{}",
              m_cfored_name_);
          state = State::Failing;
          break;
        }

        ok = read_writer->Read(&reply);
        if (!ok) {
          CRANE_ERROR(
              "Error when reading for CranedRegisterReply form cfored {}",
              m_cfored_name_);
          state = State::Failing;
          break;
        }

        if (reply.type() !=
            crane::grpc::
                StreamCfroedTaskIOReply_CranedReplyType_CRANED_REGISTER_REPLY) {
          CRANE_ERROR(
              "expect type CRANED_REGISTER_REPLY but receive {} from cfored "
              "{}",
              reply.type(), m_cfored_name_);
          state = State::Failing;
          break;
        }
        state = State::Forwarding;
        CRANE_TRACE("This node register to {}", m_cfored_name_);
        break;

      }
      case State::Forwarding: {
        absl::Mutex state_mtx;
        CRANE_TRACE("Forwarding ready for {}", m_cfored_name_);

        std::thread read_thread([&]() {
          state_mtx.Lock();
          while (
              (state == State::Forwarding || state == State::Unregistering) &&
              !m_stopped) {
            state_mtx.Unlock();
            auto ok = read_writer->Read(&reply);
            if (!ok) {
              absl::MutexLock lock(&state_mtx);
              CRANE_ERROR(
                  "Error when reading for CranedRegisterReply form cfored {}",
                  m_cfored_name_);
              state = State::Failing;
              return;
            }
            CRANE_TRACE("Read reply from {} of type {}", m_cfored_name_,
                        reply.type());
            if (reply.type() ==
                crane::grpc::
                    StreamCfroedTaskIOReply_CranedReplyType_CRANED_TASK_INPUT) {
              const auto task_id = reply.payload_task_input_req().task_id();
              const auto& msg = reply.payload_task_input_req().msg();
              m_mtx_.Lock();
              if (m_task_input_cb_map_.contains(task_id)) {
                m_task_input_cb_map_[task_id](msg);
              } else {
                CRANE_ERROR("Cfored {} trying to send msg to unknown task #{}",
                            m_cfored_name_, task_id);
              }
              m_mtx_.Unlock();
            } else if (
                reply.type() ==
                crane::grpc::
                    StreamCfroedTaskIOReply_CranedReplyType_CRANED_UNREGISTER_REPLY) {
              CRANE_TRACE("Unregister this node to {} successful",
                          m_cfored_name_);
              return;
            }

            state_mtx.Lock();
          }
          state_mtx.Unlock();
        });

        std::thread write_thread([&]() {
          state_mtx.Lock();
          while (state == State::Forwarding && !m_stopped) {
            state_mtx.Unlock(); /* reading msg from tasks*/
            auto approximate_size = m_msg_queue_to_cfored_.size_approx();
            std::vector<std::pair<task_id_t, std::string>> args;
            args.resize(approximate_size);
            m_msg_queue_to_cfored_.try_dequeue_bulk(args.begin(),
                                                    approximate_size);
            for (const auto& [task_id, msg] : args) {
              request.clear_payload();
              request.clear_type();
              request.set_type(
                  crane::grpc::
                      StreamCfroedTaskIORequest_CranedRequestType_CRANED_TASK_OUTPUT);
              auto* req = request.mutable_payload_task_output_req();
              req->set_msg(msg);
              req->set_task_id(task_id);
              auto ok = read_writer->Write(request);
              if (!ok) {
                absl::MutexLock lock(&state_mtx);
                CRANE_ERROR(
                    "Connection error when trying to unregister this craned to "
                    "cfored {}",
                    m_cfored_name_);
                state = State::Failing;
                return;
              }
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            state_mtx.Lock();
          }
          state = State::Unregistering;
          state_mtx.Unlock();
          request.clear_payload();
          request.clear_type();
          request.set_type(
              crane::grpc::
                  StreamCfroedTaskIORequest_CranedRequestType_CRANED_UNREGISTER);
          request.mutable_payload_unregister_req()->set_craned_id(
              g_config.CranedIdOfThisNode);
          CRANE_TRACE("Unregister this node to {}", m_cfored_name_);
          auto ok = read_writer->Write(request);
          if (!ok) {
            CRANE_ERROR(
                "Connection error when trying to unregister this craned to "
                "cfored {}",
                m_cfored_name_);
            state = State::Failing;
          }
          read_writer->WritesDone();
        });

        write_thread.join();
        read_thread.join();
        break;
      }
      case State::Unregistering: {
        return;
      }
      case State::Failing: {
        return;
      }
    }
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
  m_msg_queue_to_cfored_.enqueue({task_id, msg});
}

CforedManager::CforedManager()
    : m_stopped_(false), m_stopped_temp_(false), m_loop(uvw::loop::create()) {
  m_ev_loop_thread_ = std::thread([this]() {
    util::SetCurrentThreadName("TaskMSGReadThr");
    while (!m_stopped_) {
      if (m_stopped_temp_) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        continue;
      }

      m_loop->run();
    }
  });
}

CforedManager::~CforedManager() {
  CRANE_TRACE("free cfored manager");
  m_stopped_ = true;
  m_loop->stop();
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
  m_cfored_client_map_.clear();
}

void CforedManager::RegisterIOForward(TaskInstance* instance) {
  auto* meta = dynamic_cast<InteractiveMetaInTaskInstance*>(instance->meta.get());
  int fd = meta->msg_forward_fd;
  task_id_t task_id = instance->task.task_id();
  const std::string& cfored_name =
      instance->task.interactive_meta().cfored_name();

  absl::MutexLock lock(&m_mtx);
  m_stopped_temp_ = true;
  m_loop->stop();
  auto poll_handle = m_loop->resource<uvw::poll_handle>(fd);
  poll_handle->on<uvw::poll_event>(
      [fd, this](const uvw::poll_event&, uvw::poll_handle&) {
        int MAX_BUF_SIZE = 4096;
        absl::MutexLock lock(&this->m_mtx);
        if (!this->m_fd_task_instance_map_.contains(fd)) return;
        auto* instance = this->m_fd_task_instance_map_[fd];
        task_id_t task_id = instance->task.task_id();
        const std::string& cfored_name =
            instance->task.interactive_meta().cfored_name();
        char buf[MAX_BUF_SIZE];
        auto ret = read(fd, buf, MAX_BUF_SIZE);
        if (ret == 0) {
          return;
        } else if (ret == -1) {
          CRANE_ERROR("Error when reading from task #{} output", task_id);
        }
        std::string output(buf, ret);
        this->m_cfored_client_map_[cfored_name]->TaskOutPutForward(
            task_id, output);
      });
  poll_handle->start(uvw::poll_handle::poll_event_flags::READABLE);
  m_stopped_temp_ = false;

  if (m_cfored_client_map_.contains(cfored_name)) {
    m_cfored_client_ref_count_map_[cfored_name]++;
  } else {
    auto cfored_client = std::make_shared<CforedClient>();
    cfored_client->InitChannelAndStub(cfored_name);
    m_cfored_client_map_[cfored_name] = std::move(cfored_client);
    m_cfored_client_ref_count_map_[cfored_name] = 1;
  }
  m_task_id_handle_map_[task_id] = poll_handle;
  m_fd_task_instance_map_[fd] = instance;
  m_cfored_client_map_[cfored_name]->SetTaskInputForwardCb(
      task_id, [fd](const std::string& msg) {
        CRANE_TRACE("forwarding msg {} to task", msg);
        auto size = write(fd, msg.c_str(), msg.size());
        while (size != msg.size()) {
          size += write(fd, msg.c_str() + size, msg.size() - size);
        }
      });
}

void CforedManager::UnregisterIOForward(TaskInstance* instance) {
  auto* meta = dynamic_cast<InteractiveMetaInTaskInstance*>(instance->meta.get());
  int fd = meta->msg_forward_fd;
  task_id_t task_id = instance->task.task_id();
  const std::string& cfored_name =
      instance->task.interactive_meta().cfored_name();
  absl::MutexLock lock(&m_mtx);
  auto count = m_cfored_client_ref_count_map_[cfored_name];
  if (count == 1) {
    m_cfored_client_ref_count_map_.erase(cfored_name);
    m_cfored_client_map_.erase(cfored_name);
  } else {
    --m_cfored_client_ref_count_map_[cfored_name];
    m_cfored_client_map_[cfored_name]->UnsetTaskInputForwardCb(task_id);
  }
  m_stopped_temp_ = true;
  m_loop->stop();
  m_task_id_handle_map_[task_id]->stop();
  m_task_id_handle_map_[task_id]->close();
  m_task_id_handle_map_.erase(task_id);
  m_stopped_temp_ = false;
  m_fd_task_instance_map_.erase(fd);
}

}  // namespace Craned
