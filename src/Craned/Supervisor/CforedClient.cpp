/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "CforedClient.h"

#include <cerrno>

#include "crane/String.h"
namespace Supervisor {

using crane::grpc::StreamCforedTaskIOReply;
using crane::grpc::StreamCforedTaskIORequest;

CforedClient::CforedClient() : m_stopped_(false) {};

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
  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (g_config.CforedListenConf.UseTls) {
    m_cfored_channel_ = CreateTcpTlsChannelByHostname(
        cfored_name, kCforedDefaultPort, g_config.CforedListenConf.TlsCerts);
  } else {
    m_cfored_channel_ =
        CreateTcpInsecureChannel(cfored_name, kCforedDefaultPort);
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::CraneForeD::NewStub(m_cfored_channel_);

  m_fwd_thread_ = std::thread([this] { AsyncSendRecvThread_(); });
}

void CforedClient::CleanOutputQueueAndWriteToStreamThread_(
    grpc::ClientAsyncReaderWriter<StreamCforedTaskIORequest,
                                  StreamCforedTaskIOReply>* stream,
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

    if (next_status == grpc::CompletionQueue::SHUTDOWN) break;

    // TIMEOUT is like the Idle event in libuv and
    // thus a context switch of state machine.
    if (next_status == grpc::CompletionQueue::TIMEOUT) {
      if (m_stopped_) {
        CRANE_TRACE("TIMEOUT with m_stopped_=true.");

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
        CRANE_ERROR("Expect TASK_INPUT, but got {}", (int)reply.type());
        break;
      }

      task_id_t task_id = reply.payload_task_input_req().task_id();
      const std::string& msg = reply.payload_task_input_req().msg();

      m_mtx_.Lock();

      if (!m_task_fwd_meta_.input_stopped)
        m_task_fwd_meta_.input_stopped = !m_task_fwd_meta_.input_cb(msg);

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
                    (int)reply.type());
        reply.Clear();
        stream->Read(&reply, (void*)Tag::Read);
        break;
      }

      state = State::End;
      [[fallthrough]];

    case State::End:
      m_stopped_ = true;
      if (output_clean_thread.joinable()) output_clean_thread.join();
      break;
    }

    CRANE_TRACE("Next state: {}", int(state));
    if (state == State::End) break;
  }
}

void CforedClient::InitTaskFwdAndSetInputCb(
    std::function<bool(const std::string&)> task_input_cb) {
  absl::MutexLock lock(&m_mtx_);
  m_task_fwd_meta_.input_cb = std::move(task_input_cb);
}

bool CforedClient::TaskOutputFinish() {
  absl::MutexLock lock(&m_mtx_);
  m_task_fwd_meta_.output_stopped = true;
  return m_task_fwd_meta_.proc_stopped;
};

bool CforedClient::TaskProcessStop() {
  absl::MutexLock lock(&m_mtx_);
  m_task_fwd_meta_.proc_stopped = true;
  return m_task_fwd_meta_.output_stopped;
};

void CforedClient::TaskOutPutForward(const std::string& msg) {
  CRANE_TRACE("Receive TaskOutputForward for task #{}: {}", g_task_mgr->task_id,
              msg);
  m_output_queue_.enqueue(msg);
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

void CforedManager::RegisterIOForward(std::string const& cfored, int fd,
                                      bool pty) {
  RegisterElem elem{.cfored = cfored, .fd = fd, .pty = pty};
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
    m_cfored_client_ = std::make_shared<CforedClient>();
    m_cfored_client_->InitChannelAndStub(elem.cfored);

    m_cfored_client_->InitTaskFwdAndSetInputCb(
        [fd = elem.fd](const std::string& msg) -> bool {
          ssize_t sz_sent = 0, sz_written;
          while (sz_sent != msg.size()) {
            sz_written = write(fd, msg.c_str() + sz_sent, msg.size() - sz_sent);
            if (sz_written < 0) {
              CRANE_ERROR("Pipe to Crun task was broken.");
              return false;
            }

            sz_sent += sz_written;
          }
          return true;
        });

    CRANE_TRACE("Registering fd {} for outputs of task #{}", elem.fd,
                g_task_mgr->task_id);
    auto poll_handle = m_loop_->resource<uvw::poll_handle>(elem.fd);
    poll_handle->on<uvw::poll_event>(
        [this, elem = std::move(elem)](const uvw::poll_event&,
                                       uvw::poll_handle& h) {
          CRANE_TRACE("Detect task #{} output.", g_task_mgr->task_id);

          constexpr int MAX_BUF_SIZE = 4096;
          char buf[MAX_BUF_SIZE];

          auto ret = read(elem.fd, buf, MAX_BUF_SIZE);
          bool read_finished{false};

          if (ret == 0) {
            if (!elem.pty) {
              read_finished = true;
            } else {
              // For pty,do nothing, process exit on return -1 and error set to
              // EIO
              CRANE_TRACE("Read EOF from pty task #{} on cfored {}",
                          g_task_mgr->task_id, elem.cfored);
            }
          }

          if (ret == -1) {
            if (!elem.pty) {
              CRANE_ERROR("Error when reading task #{} output, error {}",
                          g_task_mgr->task_id, std::strerror(errno));
              return;
            }

            if (errno == EIO) {
              // For pty output, the read() will return -1 with errno set to EIO
              // when process exit.
              // ref: https://unix.stackexchange.com/questions/538198
              read_finished = true;
            } else if (errno == EAGAIN) {
              // Read before the process begin.
              return;
            } else {
              CRANE_ERROR("Error when reading task #{} output, error {}",
                          g_task_mgr->task_id, std::strerror(errno));
              return;
            }
          }

          if (read_finished) {
            CRANE_TRACE("Task #{} to cfored {} finished its output.",
                        g_task_mgr->task_id, elem.cfored);
            h.close();
            close(elem.fd);

            bool ok_to_free = m_cfored_client_->TaskOutputFinish();
            if (ok_to_free) {
              CRANE_TRACE("It's ok to unregister task #{} on {}",
                          g_task_mgr->task_id, elem.cfored);
              UnregisterIOForward_();
            }
            return;
          }

          std::string output(buf, ret);
          CRANE_TRACE("Fwd to task #{}: {}", g_task_mgr->task_id, output);
          m_cfored_client_->TaskOutPutForward(output);
        });
    int ret = poll_handle->start(uvw::poll_handle::poll_event_flags::READABLE);
    if (ret < 0)
      CRANE_ERROR("poll_handle->start() error: {}", uv_strerror(ret));

    p.second.set_value(true);
  }
}

void CforedManager::TaskProcStopped() { m_task_stop_handle_->send(); }

void CforedManager::TaskStopCb_() {
  CRANE_TRACE("Task #{} to cfored {} just stopped its process.",
              g_task_mgr->task_id, m_cfored_client_->CforedName());
  bool ok_to_free = m_cfored_client_->TaskProcessStop();
  if (ok_to_free) {
    CRANE_TRACE("It's ok to unregister task #{} on {}", g_task_mgr->task_id,
                m_cfored_client_->CforedName());
    UnregisterIOForward_();
  }
}

void CforedManager::UnregisterIOForward_() { m_unregister_handle_->send(); }

void CforedManager::UnregisterCb_() {
  m_cfored_client_.reset();
  g_task_mgr->TaskStopAndDoStatusChange();
}

}  // namespace Supervisor
