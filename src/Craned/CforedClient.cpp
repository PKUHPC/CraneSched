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
namespace Craned {

using crane::grpc::StreamTaskIOReply;
using crane::grpc::StreamTaskIORequest;

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

  // Todo: Use cfored listen config
  if (g_config.ListenConf.UseTls) {
    m_cfored_channel_ = CreateTcpTlsChannelByHostname(
        cfored_name, kCforedDefaultPort, g_config.ListenConf.TlsCerts);
  } else {
    m_cfored_channel_ =
        CreateTcpInsecureChannel(cfored_name, kCforedDefaultPort);
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::CraneForeD::NewStub(m_cfored_channel_);

  m_fwd_thread_ = std::thread([this] { AsyncSendRecvThread_(); });
}

void CforedClient::CleanOutputQueueAndWriteToStreamThread_(
    grpc::ClientAsyncReaderWriter<StreamTaskIORequest, StreamTaskIOReply>*
        stream,
    std::atomic<bool>* write_pending) {
  CRANE_TRACE("CleanOutputQueueThread started.");
  std::pair<task_id_t, std::string> output;
  std::tuple<task_id_t, std::unique_ptr<char[]>, size_t> x11_output;
  bool ok = m_output_queue_.try_dequeue(output);
  bool x11_ok = m_x11_output_queue_.try_dequeue(x11_output);

  // Make sure before exit all output has been drained.
  while (!m_stopped_ || ok || x11_ok) {
    if (!ok && !x11_ok) {
      std::this_thread::sleep_for(std::chrono::milliseconds(75));
      ok = m_output_queue_.try_dequeue(output);
      x11_ok = m_x11_output_queue_.try_dequeue(x11_output);
      continue;
    }

    if (ok) {
      StreamTaskIORequest request;
      request.set_type(StreamTaskIORequest::CRANED_TASK_OUTPUT);

      auto* payload = request.mutable_payload_task_output_req();
      payload->set_msg(output.second), payload->set_task_id(output.first);

      while (write_pending->load(std::memory_order::acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(25));

      CRANE_TRACE("Writing output...");
      write_pending->store(true, std::memory_order::release);
      stream->Write(request, (void*)Tag::Write);

      ok = m_output_queue_.try_dequeue(output);
    }

    if (x11_ok) {
      StreamTaskIORequest request;
      request.set_type(StreamTaskIORequest::CRANED_TASK_X11_OUTPUT);

      auto* payload = request.mutable_payload_task_x11_output_req();

      auto& [task_id, p, len] = x11_output;
      payload->set_msg(p.get(), len);
      payload->set_task_id(task_id);

      while (write_pending->load(std::memory_order::acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(25));

      CRANE_TRACE("Forwarding x11 output from task to cfored {}",
                  this->m_cfored_name_);
      write_pending->store(true, std::memory_order::release);
      stream->Write(request, (void*)Tag::Write);

      x11_ok = m_x11_output_queue_.try_dequeue(x11_output);
    }
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
  StreamTaskIORequest request;
  StreamTaskIOReply reply;
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
        request.set_type(StreamTaskIORequest::CRANED_UNREGISTER);
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

      request.set_type(StreamTaskIORequest::CRANED_REGISTER);
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

      task_id_t task_id;
      const std::string* msg;

      if (reply.type() == StreamTaskIOReply::CRANED_TASK_X11_INPUT) {
        task_id = reply.payload_task_x11_input_req().task_id();
        msg = &reply.payload_task_x11_input_req().msg();
        CRANE_TRACE("TASK_X11_INPUT: task id #{}.", task_id);
      } else if (reply.type() == StreamTaskIOReply::CRANED_TASK_INPUT) {
        task_id = reply.payload_task_input_req().task_id();
        msg = &reply.payload_task_input_req().msg();
        CRANE_TRACE("TASK_INPUT: task id #{}.", task_id);
      } else [[unlikely]] {
        CRANE_ERROR("Expect TASK_INPUT or TASK_X11_INPUT, but got {}",
                    (int)reply.type());
        break;
      }

      m_mtx_.Lock();
      auto fwd_meta_it = m_task_fwd_meta_map_.find(task_id);
      if (fwd_meta_it != m_task_fwd_meta_map_.end()) {
        TaskFwdMeta& meta = fwd_meta_it->second;

        if (reply.type() == StreamTaskIOReply::CRANED_TASK_X11_INPUT) {
          if (!meta.x11_input_stopped)
            meta.x11_input_stopped = !meta.x11_input_cb(*msg);
        } else  // CRANED_TASK_INPUT
          if (!meta.input_stopped) meta.input_stopped = !meta.input_cb(*msg);

      } else {
        CRANE_ERROR("Cfored {} trying to send data to unknown task #{}",
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

      if (reply.type() != StreamTaskIOReply::CRANED_UNREGISTER_REPLY) {
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
    task_id_t task_id, std::function<bool(const std::string&)> task_input_cb) {
  absl::MutexLock lock(&m_mtx_);
  m_task_fwd_meta_map_[task_id].input_cb = std::move(task_input_cb);
}

void CforedClient::SetX11FwdInputCb(
    task_id_t task_id,
    std::function<bool(const std::string&)> task_x11_input_cb) {
  absl::MutexLock lock(&m_mtx_);
  m_task_fwd_meta_map_[task_id].x11_input_cb = std::move(task_x11_input_cb);
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

void CforedClient::TaskX11OutPutForward(task_id_t task_id,
                                        std::unique_ptr<char[]>&& data,
                                        size_t len) {
  CRANE_TRACE("Receive TaskX11OutPutForward from task #{}.", task_id);
  m_x11_output_queue_.enqueue({task_id, std::move(data), len});
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

void CforedManager::RegisterIOForward(const RegisterElem& elem,
                                      RegisterResult* result) {
  std::promise<RegisterResult> done;
  std::future<RegisterResult> done_fut = done.get_future();

  m_register_queue_.enqueue(std::make_pair(std::move(elem), std::move(done)));
  m_register_handle_->send();
  *result = done_fut.get();
}

void CforedManager::RegisterCb_() {
  std::pair<RegisterElem, std::promise<RegisterResult>> p;
  RegisterResult result{.ok = true};

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
        elem.task_id, [fd = elem.task_in_fd](const std::string& msg) -> bool {
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

    CRANE_TRACE("Registering fd {} for outputs of task #{}", elem.task_out_fd,
                elem.task_id);
    auto poll_handle = m_loop_->resource<uvw::poll_handle>(elem.task_out_fd);
    poll_handle->on<uvw::poll_event>([this, elem = elem](const uvw::poll_event&,
                                                         uvw::poll_handle& h) {
      CRANE_TRACE("Detect task #{} output.", elem.task_id);

      constexpr int MAX_BUF_SIZE = 4096;
      char buf[MAX_BUF_SIZE];

      auto ret = read(elem.task_out_fd, buf, MAX_BUF_SIZE);
      bool read_finished{false};

      if (ret == 0) {
        if (!elem.pty) {
          read_finished = true;
        } else {
          // For pty,do nothing, process exit on return -1 and error set to EIO
          CRANE_TRACE("Read EOF from pty task #{} on cfored {}", elem.task_id,
                      elem.cfored);
        }
      }

      if (ret == -1) {
        if (!elem.pty) {
          CRANE_ERROR("Error when reading task #{} output, error {}",
                      elem.task_id, std::strerror(errno));
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
                      elem.task_id, std::strerror(errno));
          return;
        }
      }

      if (read_finished) {
        CRANE_TRACE("Task #{} to cfored {} finished its output.", elem.task_id,
                    elem.cfored);
        h.close();
        close(elem.task_out_fd);

        bool ok_to_free =
            m_cfored_client_map_[elem.cfored]->TaskOutputFinish(elem.task_id);
        if (ok_to_free) {
          CRANE_TRACE("It's ok to unregister task #{} on {}", elem.task_id,
                      elem.cfored);
          UnregisterIOForward_(elem.cfored, elem.task_id);
        }
        return;
      }

      std::string output(buf, ret);
      CRANE_TRACE("Fwd to task #{}: {}", elem.task_id, output);
      m_cfored_client_map_[elem.cfored]->TaskOutPutForward(elem.task_id,
                                                           output);
    });

    int ret = poll_handle->start(uvw::poll_handle::poll_event_flags::READABLE);
    if (ret < 0) {
      CRANE_ERROR("poll_handle->start() error: {}", uv_strerror(ret));
      result.ok = false;
    }

    if (elem.x11_enable_forwarding) {
      CRANE_TRACE("Registering X11 forwarding for task #{}", elem.task_id);
      result.x11_port = SetupX11forwarding_(elem.cfored, elem.task_id);
    }

    p.second.set_value(result);
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

uint16_t CforedManager::SetupX11forwarding_(std::string const& cfored,
                                            task_id_t task_id) {
  int ret;

  auto x11_proxy_h = m_loop_->resource<uvw::tcp_handle>();
  constexpr uint16_t x11_port_begin = 6020;
  constexpr uint16_t x11_port_end = 6100;

  auto x11_data = std::make_shared<X11FdInfo>();
  x11_data->proxy_handle = x11_proxy_h;

  x11_proxy_h->data(x11_data);
  m_task_id_to_x11_map_.emplace(task_id, x11_data);

  uint16_t port;
  for (port = x11_port_begin; port < x11_port_end; ++port) {
    ret = x11_proxy_h->bind("127.0.0.1", port);
    if (ret == -1) {
      CRANE_TRACE("Failed to bind x11 proxy port {} for task #{}: {}.", port,
                  task_id, strerror(errno));
    } else
      break;
  }

  if (port == x11_port_end) {
    CRANE_ERROR("Failed to bind x11 proxy port within {}~{}!", x11_port_begin,
                x11_port_end - 1);
    x11_proxy_h->close();
    return 0;
  }

  CRANE_TRACE("X11 proxy for task #{} was bound to port {}. Start listening.",
              task_id, port);
  ret = x11_proxy_h->listen();
  if (ret == -1) {
    CRANE_ERROR("Failed to listening on x11 proxy port {}: {}.", port,
                strerror(errno));
    x11_proxy_h->close();
    return 0;
  }

  x11_proxy_h->on<uvw::listen_event>(
      [this, cfored, task_id](uvw::listen_event& e, uvw::tcp_handle& h) {
        CRANE_TRACE("Accepting connection on x11 proxy of task #{}", task_id);

        auto sock = h.parent().resource<uvw::tcp_handle>();

        sock->on<uvw::data_event>([this, cfored, task_id](uvw::data_event& e,
                                                          uvw::tcp_handle& s) {
          CRANE_TRACE("Read x11 output from task #{}. Forwarding...", task_id);
          m_cfored_client_map_[cfored]->TaskX11OutPutForward(
              task_id, std::move(e.data), e.length);
        });
        sock->on<uvw::write_event>(
            [this, task_id](const uvw::write_event&, uvw::tcp_handle&) {
              CRANE_TRACE("Write x11 input to task #{} done.", task_id);
            });

        sock->on<uvw::end_event>(
            [task_id](uvw::end_event&, uvw::tcp_handle& h) {
              // EOF
              CRANE_TRACE("X11 proxy connection of task #{} ended. Closing it.",
                          task_id);
              if (auto* p = h.data<X11FdInfo>().get(); p)
                p->sock_stopped = true;
              h.close();
            });
        sock->on<uvw::error_event>(
            [task_id](uvw::error_event& e, uvw::tcp_handle& h) {
              CRANE_ERROR("Error on x11 proxy of task #{}: {}. Closing it.",
                          task_id, e.what());
              if (auto* p = h.data<X11FdInfo>().get(); p)
                p->sock_stopped = true;
              h.close();
            });
        sock->on<uvw::close_event>([task_id](uvw::close_event&,
                                             uvw::tcp_handle& h) {
          CRANE_TRACE("X11 proxy connection of task #{} was closed.", task_id);
        });

        h.accept(*sock);

        h.data<X11FdInfo>()->fd = sock->fd();
        h.data<X11FdInfo>()->sock = sock;
        sock->read();

        // Currently only 1 connection of x11 client will be accepted.
        // Close it once we accept one x11 client.
        h.close();
      });

  x11_proxy_h->on<uvw::close_event>(
      [task_id](const uvw::close_event&, uvw::tcp_handle&) {
        CRANE_TRACE("X11 proxy listening port of task #{} closed.", task_id);
      });

  x11_proxy_h->on<uvw::error_event>(
      [task_id](const uvw::error_event& e, uvw::tcp_handle& h) {
        CRANE_ERROR("Error on x11 proxy of task #{}: {}", task_id, e.what());
        h.close();
      });
  x11_proxy_h->on<uvw::end_event>([task_id](uvw::end_event&,
                                            uvw::tcp_handle& h) {
    CRANE_TRACE("X11 proxy listening port of task #{} received EOF.", task_id);
    h.close();
  });

  m_cfored_client_map_[cfored]->SetX11FwdInputCb(
      task_id, [x11_data](const std::string& msg) -> bool {
        int fd = x11_data->fd;
        ssize_t sz_sent = 0, sz_written;
        while (sz_sent != msg.size()) {
          sz_written = write(fd, msg.c_str() + sz_sent, msg.size() - sz_sent);
          if (sz_written < 0) {
            CRANE_ERROR("Pipe to task x11 client was broken.");
            return false;
          }

          sz_sent += sz_written;
        }
        return true;
        // if (x11_proxy_fd < 0) {
        //   CRANE_ERROR("Invalid x11 proxy fd: {}", x11_proxy_fd);
        // }
        //
        // if (x11_data->sock_stopped) {
        //   CRANE_TRACE("Sock has stopped. Ignoring forwarding..");
        //   return false;
        // }
        //
        // CRANE_TRACE("Writing X11 to fd {}", x11_proxy_fd);
        //
        // std::unique_ptr<char[]> data(new char[msg.size()]);
        // memcpy(data.get(), msg.data(), msg.size());
        // int r = x11_data->sock->write(std::move(data), msg.size());
        //
        // CRANE_TRACE("Writing X11 to fd {} result: {}", x11_proxy_fd, r);
        // return r < 0;
      });

  CRANE_TRACE("Registering x11 outputs of task #{}", task_id);

  return port;
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

    auto it = m_task_id_to_x11_map_.find(elem.task_id);
    if (it != m_task_id_to_x11_map_.end()) {
      X11FdInfo* x11_fd_info = it->second.get();
      x11_fd_info->proxy_handle->close();
      if (x11_fd_info->sock) x11_fd_info->sock->close();

      m_task_id_to_x11_map_.erase(it);
    }

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
