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

#include "TaskManager.h"
#include "crane/String.h"
namespace Craned::Supervisor {

using crane::grpc::StreamStepIOReply;
using crane::grpc::StreamStepIORequest;

CforedClient::CforedClient() {
  m_loop_ = uvw::loop::create();

  m_clean_stdout_fwd_handler_queue_async_handle_ =
      m_loop_->resource<uvw::async_handle>();
  m_clean_stdout_fwd_handler_queue_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanStdoutFwdHandlerQueueCb_();
      });

  m_clean_x11_fwd_handler_queue_async_handle_ =
      m_loop_->resource<uvw::async_handle>();
  m_clean_x11_fwd_handler_queue_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanX11FwdHandlerQueueCb_();
      });

  m_clean_stop_task_io_queue_async_handle_ =
      m_loop_->resource<uvw::async_handle>();
  m_clean_stop_task_io_queue_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanStopTaskIOQueueCb_();
      });

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
  m_ev_thread_ = std::thread([this] {
    util::SetCurrentThreadName("CforedClient");
    m_loop_->run();
  });
}

CforedClient::~CforedClient() {
  CRANE_TRACE("CforedClient to {} is being destructed.", m_cfored_name_);

  m_stopped_ = true;

  if (m_fwd_thread_.joinable()) m_fwd_thread_.join();
  if (m_ev_thread_.joinable()) m_ev_thread_.join();

  m_cq_.Shutdown();
  CRANE_ASSERT(m_fwd_meta_map.empty());

  CRANE_TRACE("CforedClient to {} was destructed.", m_cfored_name_);
}

bool CforedClient::InitFwdMetaAndUvStdoutFwdHandler(task_id_t task_id,
                                                    int stdin_write,
                                                    int stdout_read,
                                                    int stderr_read, bool pty) {
  CRANE_DEBUG(
      "[Task #{}] Setting up task fwd for input_fd:{} output_fd:{} err_fd:{} "
      "pty:{}",
      task_id, stdin_write, stdout_read, stderr_read, pty);

  util::os::SetFdNonBlocking(stdout_read);

  TaskFwdMeta meta = {.task_id = task_id,
                      .pty = pty,
                      .stdin_write = stdin_write,
                      .stdout_read = stdout_read,
                      .stderr_read = stderr_read};
  if (stdin_write == -1) {
    meta.input_stopped = true;
  }
  CreateStdoutFwdQueueElem elem;
  elem.meta = std::move(meta);
  auto ok_future = elem.promise.get_future();
  m_create_stdout_fwd_queue_.enqueue(std::move(elem));
  m_clean_stdout_fwd_handler_queue_async_handle_->send();
  return ok_future.get();
}

uint16_t CforedClient::InitUvX11FwdHandler() {
  CreateX11FwdQueueElem elem;
  auto port_future = elem.promise.get_future();
  m_create_x11_fwd_queue_.enqueue(std::move(elem));
  m_clean_x11_fwd_handler_queue_async_handle_->send();
  return port_future.get();
}

uint16_t CforedClient::SetupX11forwarding_() {
  int ret;

  // This handle will be closed when CforedClient destructor called.
  auto x11_proxy_h = m_loop_->resource<uvw::tcp_handle>();
  constexpr uint16_t x11_port_begin = 6020;
  constexpr uint16_t x11_port_end = 6100;

  uint16_t port;
  for (port = x11_port_begin; port < x11_port_end; ++port) {
    ret = x11_proxy_h->bind("127.0.0.1", port);
    if (ret == -1) {
      CRANE_TRACE("Failed to bind x11 proxy port {} for task: {}.", port,
                  strerror(errno));
    } else
      break;
  }

  if (port == x11_port_end) {
    CRANE_ERROR("Failed to bind x11 proxy port within {}~{}!", x11_port_begin,
                x11_port_end - 1);
    x11_proxy_h->close();
    return 0;
  }

  CRANE_TRACE("X11 proxy bound to port {}. Start listening.", port);
  ret = x11_proxy_h->listen();
  if (ret == -1) {
    CRANE_ERROR("Failed to listening on x11 proxy port {}: {}.", port,
                strerror(errno));
    x11_proxy_h->close();
    return 0;
  }

  x11_proxy_h->on<uvw::listen_event>([this](uvw::listen_event& e,
                                            uvw::tcp_handle& h) {
    CRANE_TRACE("Accepting connection on x11 proxy.");

    x11_local_id_t x11_local_id = next_x11_id_++;
    this->StepX11ConnectForward(x11_local_id);
    auto x11_fd_info = std::make_shared<X11FdInfo>();
    auto sock = h.parent().resource<uvw::tcp_handle>();

    sock->on<uvw::data_event>(
        [x11_local_id, this](uvw::data_event& e, uvw::tcp_handle& s) {
          CRANE_TRACE("[X11 #{}] Read x11 output len [{}]. Forwarding...",
                      x11_local_id, e.length);
          StepX11OutPutForward(x11_local_id, std::move(e.data), e.length);
        });
    sock->on<uvw::write_event>(
        [x11_local_id, this](const uvw::write_event&, uvw::tcp_handle&) {
          CRANE_TRACE("[X11 #{}] Write x11 input done.", x11_local_id);
        });

    sock->on<uvw::end_event>(
        [x11_local_id, this](uvw::end_event&, uvw::tcp_handle& s) {
          // EOF
          CRANE_TRACE("[X11 #{}] connection ended. Closing it.", x11_local_id);
          if (auto* p = s.data<X11FdInfo>().get(); p) p->sock_stopped = true;
          s.close();
        });
    sock->on<uvw::error_event>([x11_local_id, this](uvw::error_event& e,
                                                    uvw::tcp_handle& s) {
      CRANE_ERROR("[X11 #{}] Error :{}. Closing it.", x11_local_id, e.what());
      if (auto* p = s.data<X11FdInfo>().get(); p) p->sock_stopped = true;
      s.close();
    });
    sock->on<uvw::close_event>(
        [x11_local_id, this](uvw::close_event&, uvw::tcp_handle& s) {
          CRANE_INFO("[X11 #{}] proxy connection was closed.", x11_local_id);
          this->StepX11OutputFinish(x11_local_id);
        });

    h.accept(*sock);

    x11_fd_info->fd = sock->fd();
    x11_fd_info->sock = sock;
    absl::MutexLock lock(&m_mtx_);
    m_x11_fd_info_map_[x11_local_id] = x11_fd_info;
    sock->read();
  });

  x11_proxy_h->on<uvw::close_event>(
      [](const uvw::close_event&, uvw::tcp_handle&) {
        CRANE_TRACE("X11 proxy listening port closed.");
      });

  x11_proxy_h->on<uvw::error_event>(
      [](const uvw::error_event& e, uvw::tcp_handle& h) {
        CRANE_ERROR("Error on x11 proxy: {}", e.what());
        h.close();
      });
  x11_proxy_h->on<uvw::end_event>([](uvw::end_event&, uvw::tcp_handle& h) {
    CRANE_TRACE("X11 proxy listening port received EOF.");
    h.close();
  });
  return port;
}

bool CforedClient::WriteStringToFd_(const std::string& msg, int fd,
                                    bool close_fd) {
  ssize_t sz_sent = 0, sz_written;
  while (sz_sent != msg.size()) {
    sz_written = write(fd, msg.c_str() + sz_sent, msg.size() - sz_sent);
    if (sz_written < 0) {
      CRANE_ERROR("Pipe to Crun task was broken.");
      return false;
    }

    sz_sent += sz_written;
  }
  if (close_fd) close(fd);
  return true;
}

void CforedClient::CleanStdoutFwdHandlerQueueCb_() {
  CreateStdoutFwdQueueElem elem;
  while (m_create_stdout_fwd_queue_.try_dequeue(elem)) {
    auto& meta = elem.meta;
    auto stdout_read = meta.stdout_read;
    auto stderr_read = meta.stderr_read;
    auto task_id = meta.task_id;
    // Called when both stdout and stderr pipes have reached EOF (or error),
    // meaning all process output has been read and enqueued as TASK_OUTPUT.
    // Non-PTY: triggered by the last of stdout end_event / stderr end_event.
    // PTY: triggered by the tty end_event (single fd covers both streams).
    // This queues the task into m_stop_task_io_queue_ for
    // CleanStopTaskIOQueueCb_ to close handles and (if the process has
    // already exited) enqueue the deferred TASK_EXIT_STATUS.
    auto on_finish = [this, task_id] {
      m_stop_task_io_queue_.enqueue(task_id);
      m_clean_stop_task_io_queue_async_handle_->send();
    };
    std::shared_ptr<uvw::pipe_handle> ph;
    std::shared_ptr<uvw::tty_handle> th;
    std::shared_ptr<uvw::pipe_handle> err_ph;
    int err = 0;
    if (!meta.pty) {
      if (stdout_read != -1) {
        // non pty: use pipe_handle
        ph = m_loop_->uninitialized_resource<uvw::pipe_handle>(false);
        err = ph->init();
        if (err) {
          CRANE_ERROR("[Task #{}] Failed to init pipe_handle for fd {}: {}",
                      meta.task_id, meta.stdout_read, uv_strerror(err));
          elem.promise.set_value(false);
          close(meta.stdout_read);
          continue;
        }

        err = ph->open(meta.stdout_read);
        if (err) {
          CRANE_ERROR("[Task #{}] Failed to open pipe_handle for fd {}: {}",
                      meta.task_id, meta.stdout_read, uv_strerror(err));
          elem.promise.set_value(false);
          close(meta.stdout_read);
          continue;
        }
        meta.out_handle.pipe = ph;

      ph->on<uvw::data_event>(
          [this, task_id](uvw::data_event& e, uvw::pipe_handle&) {
            if (e.length > 0) {
              this->TaskOutPutForward(task_id, std::move(e.data), e.length);
            }
          });

        ph->on<uvw::end_event>([this, tid = task_id, on_finish](
                                   uvw::end_event&, uvw::pipe_handle& h) {
          // EOF Received
          h.close();
          CRANE_INFO("[Task #{}] Output pipe received EOF.", tid);
          auto& meta = this->m_fwd_meta_map.at(tid);
          meta.output_stopped = true;
          if (meta.err_stopped) on_finish();
        });

        ph->on<uvw::error_event>([this, tid = meta.task_id, on_finish](
                                     uvw::error_event& e, uvw::pipe_handle& h) {
          CRANE_WARN("[Task #{}] output pipe error: {}. Closing.", tid,
                     e.what());
          h.close();
          auto& meta = this->m_fwd_meta_map.at(tid);
          meta.output_stopped = true;
          if (meta.err_stopped) on_finish();
        });

        ph->on<uvw::close_event>(
            [tid = meta.task_id](uvw::close_event&, uvw::pipe_handle&) {
              CRANE_TRACE("[Task #{}] Output pipe closed.", tid);
            });
      } else {
        meta.output_stopped = true;
      }

      if (stderr_read != -1) {
        err_ph = m_loop_->uninitialized_resource<uvw::pipe_handle>(false);
        err = err_ph->init();
        if (err) {
          CRANE_ERROR("[Task #{}] Failed to init pipe_handle for fd {}: {}",
                      meta.task_id, meta.stderr_read, uv_strerror(err));
          elem.promise.set_value(false);
          close(meta.stderr_read);
          continue;
        }

        err = err_ph->open(meta.stderr_read);
        if (err) {
          CRANE_ERROR("[Task #{}] Failed to open pipe_handle for fd {}: {}",
                      meta.task_id, meta.stderr_read, uv_strerror(err));
          elem.promise.set_value(false);
          close(meta.stderr_read);
          continue;
        }
        meta.err_handle = err_ph;

        err_ph->on<uvw::data_event>(
            [this](uvw::data_event& e, uvw::pipe_handle&) {
              if (e.length > 0) {
                this->TaskErrOutPutForward(std::move(e.data), e.length);
              }
            });

        err_ph->on<uvw::end_event>([this, tid = task_id, on_finish](
                                       uvw::end_event&, uvw::pipe_handle& h) {
          CRANE_INFO("[Task #{}] Stderr pipe received EOF.", tid);
          // EOF Received
          h.close();
          auto& meta = this->m_fwd_meta_map.at(tid);
          meta.err_stopped = true;
          if (meta.output_stopped) on_finish();
        });

        err_ph->on<uvw::error_event>(
            [this, tid = meta.task_id, on_finish](uvw::error_event& e,
                                                  uvw::pipe_handle& h) {
              CRANE_WARN("[Task #{}] Stderr pipe error: {}. Closing.", tid,
                         e.what());
              h.close();
              auto& meta = this->m_fwd_meta_map.at(tid);
              meta.err_stopped = true;
              if (meta.output_stopped) on_finish();
            });

        err_ph->on<uvw::close_event>(
            [tid = meta.task_id](uvw::close_event&, uvw::pipe_handle&) {
              CRANE_TRACE("[Task #{}] Stderr pipe closed.", tid);
            });
      } else {
        meta.err_stopped = true;
      }

    } else {
      meta.err_stopped = true;
      // pty: use tty_handle. The 2nd argument true means it's a readable tty.
      th = m_loop_->uninitialized_resource<uvw::tty_handle>(meta.stdout_read,
                                                            true);
      err = th->init();
      if (err) {
        CRANE_ERROR("[Task #{}] Failed to init tty_handle for pty fd {}: {}",
                    meta.task_id, meta.stdout_read, uv_strerror(err));
        close(meta.stdout_read);
        elem.promise.set_value(false);
        continue;
      }
      meta.out_handle.tty = th;

      th->on<uvw::data_event>(
          [this, task_id](uvw::data_event& e, uvw::tty_handle&) {
            if (e.length > 0) {
              this->TaskOutPutForward(task_id, std::move(e.data), e.length);
            }
          });

      th->on<uvw::end_event>([on_finish](uvw::end_event&, uvw::tty_handle& h) {
        // The remote end is closed, go to EOF process.
        h.close();
        on_finish();
      });

      th->on<uvw::error_event>([tid = meta.task_id, on_finish](
                                   uvw::error_event& e, uvw::tty_handle& h) {
        CRANE_WARN("[Task #{}] pty read error: {}. Closing.", tid, e.what());
        h.close();
        on_finish();
      });

      th->on<uvw::close_event>(
          [tid = meta.task_id](uvw::close_event&, uvw::tty_handle&) {
            CRANE_TRACE("[Task #{}] Pty closed.", tid);
          });
    }
    {
      absl::MutexLock lock(&m_mtx_);
      m_fwd_meta_map[task_id] = meta;
    }
    if (ph)
      ph->read();
    else if (th)
      th->read();
    if (err_ph) err_ph->read();

    elem.promise.set_value(true);
  }
}

void CforedClient::CleanX11FwdHandlerQueueCb_() {
  CreateX11FwdQueueElem elem;
  while (m_create_x11_fwd_queue_.try_dequeue(elem)) {
    CRANE_INFO("Setup X11 forwarding");
    elem.promise.set_value(SetupX11forwarding_());
  }
}

void CforedClient::CleanStopTaskIOQueueCb_() {
  task_id_t task_id;
  while (m_stop_task_io_queue_.try_dequeue(task_id)) {
    bool ok_to_free = false;
    {
      absl::MutexLock lock(&m_mtx_);
      auto it = m_fwd_meta_map.find(task_id);
      if (it == m_fwd_meta_map.end()) {
        CRANE_ERROR("[Task #{}] Cannot find fwd meta to stop task io.",
                    task_id);
        continue;
      }
      auto& meta = it->second;
      auto& output_handle = it->second.out_handle;
      if (!meta.pty && output_handle.pipe) output_handle.pipe->close();
      if (meta.pty && output_handle.tty) output_handle.tty->close();
      if (meta.err_handle) meta.err_handle->close();
      output_handle.pipe.reset();
      output_handle.tty.reset();
      meta.err_handle.reset();

      if (meta.stdout_read != -1) close(meta.stdout_read);
      if (meta.stderr_read != -1) close(meta.stderr_read);

      CRANE_DEBUG("[Task #{}] Finished its output and err output.", task_id);

      auto& m = m_fwd_meta_map[task_id];
      ok_to_free = m.proc_stopped;
      if (ok_to_free) {
        // Output fully drained and process already exited.
        // Now it is safe to send TASK_EXIT_STATUS — all TASK_OUTPUT
        // messages have already been enqueued before this point.
        m_task_fwd_req_queue_.enqueue(FwdRequest{
            .type = StreamStepIORequest::TASK_EXIT_STATUS,
            .data = TaskFinishStatus{.task_id = task_id,
                                     .exit_code = m.exit_code,
                                     .signaled = m.signaled},
        });
      }
    }

    if (ok_to_free) {
      CRANE_DEBUG("[Task #{}] It's ok to unregister.", task_id);
      this->TaskEnd(task_id);
    }
  };
}

void CforedClient::InitChannelAndStub(const std::string& cfored_name) {
  m_cfored_name_ = cfored_name;
  grpc::ChannelArguments channel_args;
  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  SetGrpcClientKeepAliveChannelArgs(&channel_args);
  // Todo: Use cfored listen config
  if (g_config.CforedListenConf.TlsConfig.Enabled) {
    m_cfored_channel_ = CreateTcpTlsChannelByHostname(
        cfored_name, kCforedDefaultPort,
        g_config.CforedListenConf.TlsConfig.TlsCerts,
        g_config.CforedListenConf.TlsConfig.DomainSuffix);
  } else {
    m_cfored_channel_ =
        CreateTcpInsecureChannel(cfored_name, kCforedDefaultPort);
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::CraneForeD::NewStub(m_cfored_channel_);
  m_fwd_thread_ = std::thread([this] { AsyncSendRecvThread_(); });
}

void CforedClient::CleanOutputQueueAndWriteToStreamThread_(
    grpc::ClientAsyncReaderWriter<StreamStepIORequest, StreamStepIOReply>*
        stream,
    std::atomic<bool>* write_pending) {
  CRANE_TRACE("CleanOutputQueueThread started.");
  FwdRequest fwd_req;
  bool ok = m_task_fwd_req_queue_.try_dequeue(fwd_req);

  // Make sure before exit all output has been drained.
  while (!m_wait_reconn_ && (!m_stopped_ || ok)) {
    if (!ok) {
      std::this_thread::sleep_for(std::chrono::milliseconds(75));
      ok = m_task_fwd_req_queue_.try_dequeue(fwd_req);
      CRANE_TRACE("Waiting for output to forward...");
      continue;
    }

    if (ok) {
      // Track bytes dequeued for TASK_OUTPUT to maintain m_output_queue_bytes_
      size_t tracked_len = 0;
      if (fwd_req.type == StreamStepIORequest::TASK_OUTPUT) {
        tracked_len = std::get<IOFwdRequest>(fwd_req.data).len;
        m_output_queue_bytes_.fetch_sub(tracked_len,
                                        std::memory_order::relaxed);
      }

      StreamStepIORequest request;
      request.set_type(fwd_req.type);
      std::visit(
          VariantVisitor{
              [&request](IOFwdRequest& req) {
                if (req.is_stdout) {
                  auto* payload = request.mutable_payload_task_output_req();
                  payload->set_msg(req.data.get(), req.len);
                } else {
                  auto* payload = request.mutable_payload_task_err_output_req();
                  payload->set_msg(req.data.get(), req.len);
                }
                payload->set_task_id(req.task_id);
              },
              [&request](X11FwdConnectReq& req) {
                auto* payload = request.mutable_payload_step_x11_fwd_conn_req();
                payload->set_local_id(req.x11_id);
                payload->set_craned_id(g_config.CranedIdOfThisNode);
              },
              [&request](X11FwdReq& req) {
                auto* payload = request.mutable_payload_step_x11_output_req();
                payload->set_local_id(req.x11_id);
                payload->set_craned_id(g_config.CranedIdOfThisNode);
                payload->set_msg(req.data.get(), req.len);
              },
              [&request](TaskFinishStatus& status) {
                auto* payload = request.mutable_payload_task_exit_status_req();
                payload->set_task_id(status.task_id);
                payload->set_exit_code(status.exit_code);
                payload->set_signaled(status.signaled);
              },
              [&request](X11FwdEofReq& req) {
                auto* payload = request.mutable_payload_step_x11_eof_req();
                payload->set_local_id(req.x11_id);
                payload->set_craned_id(g_config.CranedIdOfThisNode);
              }},
          fwd_req.data);

      // Wait for any pending write; detect reconnect mid-wait
      while (write_pending->load(std::memory_order::acquire)) {
        if (m_wait_reconn_.load(std::memory_order::acquire)) {
          // Reconnect exit: do NOT mark output as drained; data is preserved in
          // queue
          CRANE_TRACE(
              "CleanOutputQueueThread: reconnect exit, queue data preserved.");
          m_output_drained_.store(true, std::memory_order::release);
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
      }

      // One more check before issuing the write
      if (m_wait_reconn_.load(std::memory_order::acquire)) {
        CRANE_TRACE("CleanOutputQueueThread: reconnect exit");
        m_output_drained_.store(true, std::memory_order::release);
        return;
      }

      CRANE_TRACE("Writing output type: {}...", static_cast<int>(fwd_req.type));
      write_pending->store(true, std::memory_order::release);
      stream->Write(request, (void*)Tag::Write);

      ok = m_task_fwd_req_queue_.try_dequeue(fwd_req);
    }
  }

  m_output_drained_.store(true, std::memory_order::release);
  CRANE_TRACE("CleanOutputQueueThread: normal exit, output drained.");
}

void CforedClient::AsyncSendRecvThread_() {
  util::SetCurrentThreadName("CforedSendRecv");
  enum class State : int {
    Registering = 0,
    WaitRegisterAck = 1,
    Forwarding = 2,
    Draining,
    Unregistering,
    End,
  };

  while (!m_stopped_) {
    if (m_wait_reconn_) {
      uint32_t attempts = m_reconnect_attempts_.load();
      if (attempts > kMaxReconnectAttempts) {
        CRANE_ERROR("Cfored reconnect failed after {} attempts.", attempts);
        {
          absl::MutexLock lock(&m_mtx_);
          for (auto& task_id : m_fwd_meta_map | std::ranges::views::keys) {
            CRANE_ERROR(
                "[Task #{}] Marked task io stopped due to cfored conn failure.",
                task_id);
            m_stop_task_io_queue_.enqueue(task_id);
            m_clean_stop_task_io_queue_async_handle_->send();
          }
        }
        CRANE_ERROR("Terminating step due to cfored connection failure.");
        g_task_mgr->TerminateStepAsync(
            false, TerminatedBy::TERMINATION_BY_CFORED_CONN_FAILURE);
        m_stopped_ = true;
        m_wait_reconn_ = false;
        return;
      }

      // Exponential backoff with upper bound of kMaxReconnectIntervalSec
      int interval =
          static_cast<int>(std::min(attempts, kMaxReconnectIntervalSec));
      CRANE_INFO("Reconnecting to cfored {} (attempt {}/{}), waiting {}s...",
                 m_cfored_name_, attempts + 1, kMaxReconnectAttempts, interval);
      m_reconnect_attempts_++;

      // Trigger channel reconnect and check current state
      auto ch_state = m_cfored_channel_->GetState(true);
      if (ch_state != GRPC_CHANNEL_READY) {
        CRANE_TRACE("Channel state {} not yet usable, retrying...",
                    static_cast<int>(ch_state));
        std::this_thread::sleep_for(std::chrono::seconds(interval));
        continue;
      }

      // Channel is IDLE, CONNECTING, or READY: proceed to create new stream
      CRANE_INFO("Channel state {} usable, creating new stream...",
                 static_cast<int>(ch_state));
      m_wait_reconn_ = false;
    }

    std::thread output_clean_thread;
    std::atomic<bool> write_pending;

    bool ok;
    Tag tag;
    grpc::ClientContext context;
    StreamStepIORequest request;
    StreamStepIOReply reply;
    grpc::CompletionQueue::NextStatus next_status;

    auto stream =
        m_stub_->AsyncStepIOStream(&context, &m_cq_, (void*)Tag::Prepare);

    CRANE_TRACE("Preparing StepIOStream...");

    State state = State::Registering;
    while (true) {
      auto ddl =
          std::chrono::system_clock::now() + std::chrono::milliseconds(50);
      next_status = m_cq_.AsyncNext((void**)&tag, &ok, ddl);
      // CRANE_TRACE("NextStatus: {}, ok: {}, Tag received: {}, state: {}",
      //             int(next_status), ok, intptr_t(tag), int(state));

      if (next_status == grpc::CompletionQueue::SHUTDOWN) break;

      // TIMEOUT is like the Idle event in libuv and
      // thus a context switch of state machine.
      if (next_status == grpc::CompletionQueue::TIMEOUT) {
        if (m_stopped_) {
          CRANE_TRACE("TIMEOUT with m_stopped_=true state={}.",
                      static_cast<int>(state));
          if (state < State::Forwarding) {
            CRANE_TRACE("Waiting for register.");
            continue;
          }
          if (!m_output_drained_.load(std::memory_order::acquire)) {
            CRANE_TRACE("Waiting for output drained.");
            state = State::Draining;
            continue;
          }
          // No need to switch to Unregistering state if already switched.
          if (state == State::Unregistering) continue;
          // Wait for forwarding thread to drain output queue and stop.
          if (output_clean_thread.joinable()) output_clean_thread.join();
          // If some writes are pending, let state machine clean them up.
          if (write_pending.load(std::memory_order::acquire)) continue;

          // Cfored received stopping signal. Unregistering...
          CRANE_TRACE("Unregistering on cfored {}.", m_cfored_name_);

          request.Clear();
          request.set_type(StreamStepIORequest::SUPERVISOR_UNREGISTER);

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
        if (m_wait_reconn_ && tag != Tag::Prepare) {
          CRANE_TRACE(
              "Discarding event: tag={}, ok={}. Already waiting reconnect.",
              int(tag), ok);
          continue;
        }
        CRANE_DEBUG("Cfored connection failed, wait reconnect...");
        m_wait_reconn_ = true;

        // Close all active X11 proxy connections on disconnect
        {
          bool has_x11 = false;
          {
            absl::MutexLock lock(&m_mtx_);
            for (auto& [id, _] : m_x11_fd_info_map_) {
              m_x11_fwd_remote_eof_queue_.enqueue(id);
              has_x11 = true;
            }
          }
          if (has_x11) m_clean_x11_fwd_remote_eof_queue_async_handle_->send();
        }

        if (output_clean_thread.joinable()) output_clean_thread.join();
        break;
      }

      m_wait_reconn_ = false;
      m_reconnect_attempts_ = 0;

      switch (state) {
      case State::Registering:
        // Stream is ready. Start registering.
        CRANE_TRACE("Registering new stream on cfored {}", m_cfored_name_);

        CRANE_ASSERT_MSG_VA(tag == Tag::Prepare, "Tag: {}", int(tag));

        request.set_type(StreamStepIORequest::SUPERVISOR_REGISTER);
        request.mutable_payload_register_req()->set_craned_id(
            g_config.CranedIdOfThisNode);
        request.mutable_payload_register_req()->set_job_id(g_config.JobId);
        request.mutable_payload_register_req()->set_step_id(g_config.StepId);

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

          // Reset output drained flag for this new connection session
          m_output_drained_.store(false, std::memory_order::release);

          // Issue initial read request
          reply.Clear();
          stream->Read(&reply, (void*)Tag::Read);

          // Start output forwarding thread
          output_clean_thread = std::thread(
              &CforedClient::CleanOutputQueueAndWriteToStreamThread_, this,
              stream.get(), &write_pending);
        }
      } break;

      case State::Forwarding: {
        CRANE_TRACE("Forwarding State");
        // Do nothing for acknowledgements of successful writes in Forward
        // State.
        if (tag == Tag::Write) {
          write_pending.store(false, std::memory_order::release);
          break;
        }

        CRANE_ASSERT(tag == Tag::Read);
        const std::string* msg;

        if (reply.type() == StreamStepIOReply::STEP_X11_INPUT) {
          msg = &reply.payload_step_x11_input_req().msg();
          CRANE_TRACE("STEP_X11_INPUT len:{} EOF: {}.", msg->length(),
                      reply.payload_step_x11_input_req().eof());
        } else if (reply.type() == StreamStepIOReply::TASK_INPUT) {
          msg = &reply.payload_task_input_req().msg();
          CRANE_TRACE("TASK_INPUT len:{} EOF:{}.", msg->length(),
                      reply.payload_task_input_req().eof());
        } else [[unlikely]] {
          CRANE_ERROR("Expect TASK_INPUT or STEP_X11_INPUT, but got {}",
                      (int)reply.type());
          break;
        }

        m_mtx_.Lock();

      if (reply.type() == StreamStepIOReply::STEP_X11_INPUT) {
        x11_local_id_t x11_id = reply.payload_step_x11_input_req().local_id();
        bool eof = reply.payload_step_x11_input_req().eof();
        auto x11_fd_info_it = m_x11_fd_info_map_.find(x11_id);
        if (x11_fd_info_it != m_x11_fd_info_map_.end()) {
          auto& x11_fd_info = x11_fd_info_it->second;
          if (!x11_fd_info->x11_input_stopped)
            x11_fd_info->x11_input_stopped =
                !WriteStringToFd_(*msg, x11_fd_info->fd, eof);
          if (eof) {
            CRANE_DEBUG("[X11 #{}] Received EOF.", x11_id);
            // User closed X11 connection at crun
            x11_fd_info->sock->close();
          }
        } else {
          CRANE_WARN("Trying to write X11 input to unknown x11_local_id: {}.",
                     x11_id);
        }
      } else {
        // CRANED_TASK_INPUT
        bool eof = reply.payload_task_input_req().eof();
        if (reply.payload_task_input_req().has_task_id()) {
          task_id_t task_id = reply.payload_task_input_req().task_id();
          auto fwd_meta_it = m_fwd_meta_map.find(task_id);
          if (fwd_meta_it != m_fwd_meta_map.end()) {
            auto& fwd_meta = fwd_meta_it->second;
            if (!fwd_meta.input_stopped)
              fwd_meta.input_stopped = !WriteStringToFd_(
                  *msg, fwd_meta.stdin_write, !fwd_meta.pty && eof);
          } else {
            CRANE_WARN("Trying to write input to unknown task_id: {}.",
                       task_id);
          }
        } else {
          for (auto& fwd_meta : m_fwd_meta_map | std::views::values) {
            if (!fwd_meta.input_stopped)
              fwd_meta.input_stopped = !WriteStringToFd_(
                  *msg, fwd_meta.stdin_write, !fwd_meta.pty && eof);
          }
        }
      }

        m_mtx_.Unlock();

        reply.Clear();
        stream->Read(&reply, (void*)Tag::Read);
      } break;

      case State::Draining:
        // Write all pending outputs
        if (tag == Tag::Write) {
          CRANE_TRACE("Cfored {} drain 1 write event.", m_cfored_name_);
          write_pending.store(false, std::memory_order::release);
          break;
        }

        // Drop all read reply (task input from crun) here and
        // make sure there will always be a read request for unregister reply.
        //
        // Here, the last issued read request is for unregister reply.
        CRANE_TRACE("Cfored {} read type {} in Draining state. Dropped it.",
                    m_cfored_name_, static_cast<int>(reply.type()));
        reply.Clear();
        stream->Read(&reply, (void*)Tag::Read);
        break;

      case State::Unregistering:
        if (tag == Tag::Write) {
          CRANE_TRACE("UNREGISTER msg was sent. waiting for reply...");
          break;
        }
        CRANE_ASSERT(tag == Tag::Read);
        CRANE_TRACE("UNREGISTER_REPLY msg received.");

        if (reply.type() != StreamStepIOReply::SUPERVISOR_UNREGISTER_REPLY) {
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

      // Ignore logging for Forwarding state
      if (state == State::Forwarding) continue;
      CRANE_TRACE("Next state: {}", int(state));
      if (state == State::End) break;
    }
  }
} 

bool CforedClient::TaskProcessStop(task_id_t task_id, uint32_t exit_code,
                                   bool signaled) {
  CRANE_DEBUG("[Task #{}] Process stopped with exit_code: {}, signaled: {}.",
              task_id, exit_code, signaled);
  // Store exit info in meta. TASK_EXIT_STATUS is only enqueued after
  // output is fully drained, so that all TASK_OUTPUT messages precede it.
  absl::MutexLock lock(&m_mtx_);
  auto& meta = m_fwd_meta_map[task_id];
  meta.proc_stopped = true;
  meta.exit_code = exit_code;
  meta.signaled = signaled;
  if (meta.output_stopped) {
    // Output already drained — safe to send EXIT_STATUS now.
    m_task_fwd_req_queue_.enqueue(FwdRequest{
        .type = StreamStepIORequest::TASK_EXIT_STATUS,
        .data = TaskFinishStatus{.task_id = task_id,
                                 .exit_code = exit_code,
                                 .signaled = signaled},
    });
  }
  return meta.output_stopped;
}

void CforedClient::TaskEnd(task_id_t task_id) {
  CRANE_DEBUG("[Task #{}] Unregistered from cfored client.", task_id);
  {
    absl::MutexLock lock(&m_mtx_);
    m_fwd_meta_map.erase(task_id);
  }
  g_task_mgr->FinalizeTaskAsync(task_id);
};

void CforedClient::TaskOutPutForward(task_id_t task_id,
                                     std::unique_ptr<char[]>&& data,
                                     size_t len) {
  CRANE_TRACE("Receive TaskOutputForward task_id:{} len: {}.", task_id, len);

  // Enforce output queue size limit to prevent unbounded memory growth
  size_t prev =
      m_output_queue_bytes_.fetch_add(len, std::memory_order::relaxed);
  if (prev + len > kMaxOutputQueueBytes) {
    m_output_queue_bytes_.fetch_sub(len, std::memory_order::relaxed);
    CRANE_WARN(
        "Output queue overflow ({}/{} bytes), dropping {} bytes of task "
        "output.",
        prev, kMaxOutputQueueBytes, len);
    return;
  }

  m_task_fwd_req_queue_.enqueue(FwdRequest{
      .type = StreamStepIORequest::TASK_OUTPUT,
      .data =
         
          IOFwdRequest{.is_stdout = true, .task_id = task_id, .data = std::move(data), .len = len},
  });
}
void CforedClient::TaskErrOutPutForward(std::unique_ptr<char[]>&& data,
                                        size_t len) {
  CRANE_TRACE("Receive TaskErrOutputForward len: {}.", len);
  m_task_fwd_req_queue_.enqueue(FwdRequest{
      .type = StreamStepIORequest::TASK_ERR_OUTPUT,
      .data =
          IOFwdRequest{.is_stdout = false, .data = std::move(data), .len = len},
  });
}

void CforedClient::StepX11ConnectForward(x11_local_id_t x11_local_id) {
  CRANE_INFO("Receive StepX11ConnectForward id:{} .", x11_local_id);
  m_task_fwd_req_queue_.enqueue(
      FwdRequest{.type = StreamStepIORequest::STEP_X11_CONN,
                 .data = X11FwdConnectReq{.x11_id = x11_local_id}});
}

void CforedClient::StepX11OutPutForward(x11_local_id_t x11_local_id,
                                        std::unique_ptr<char[]>&& data,
                                        size_t len) {
  CRANE_TRACE("Receive StepX11OutPutForward len: {}.", len);
  m_task_fwd_req_queue_.enqueue(FwdRequest{
      .type = StreamStepIORequest::STEP_X11_OUTPUT,
      .data = X11FwdReq{.x11_id = x11_local_id,
                        .data = std::move(data),
                        .len = len},
  });
}

void CforedClient::StepX11OutputFinish(x11_local_id_t x11_local_id) {
  absl::MutexLock lock(&m_mtx_);
  CRANE_INFO("Receive StepX11OutputFinish id:{} .", x11_local_id);
  m_task_fwd_req_queue_.enqueue(
      FwdRequest{.type = StreamStepIORequest::STEP_X11_EOF,
                 .data = X11FwdEofReq{.x11_id = x11_local_id}});
  m_x11_fd_info_map_.erase(x11_local_id);
}

}  // namespace Craned::Supervisor
