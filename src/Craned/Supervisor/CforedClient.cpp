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

using crane::grpc::StreamTaskIOReply;
using crane::grpc::StreamTaskIORequest;

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

  CRANE_TRACE("CforedClient to {} was destructed.", m_cfored_name_);
}

bool CforedClient::InitFwdMetaAndUvStdoutFwdHandler(task_id_t task_id,
                                                    int stdin_write,
                                                    int stdout_read, bool pty) {
  CRANE_DEBUG(
      "[Task #{}] Setting up task fwd forinput_fd:{} output_fd:{} pty:{}",
      task_id, stdin_write, stdout_read, pty);

  util::os::SetFdNonBlocking(stdout_read);

  TaskFwdMeta meta = {.task_id = task_id,
                      .pty = pty,
                      .stdin_write = stdin_write,
                      .stdout_read = stdout_read};
  CreateStdoutFwdQueueElem elem;
  elem.meta = std::move(meta);
  auto ok_future = elem.promise.get_future();
  m_create_stdout_fwd_handler_queue_.enqueue(std::move(elem));
  m_clean_stdout_fwd_handler_queue_async_handle_->send();
  return ok_future.get();
}

uint16_t CforedClient::InitUvX11FwdHandler(task_id_t task_id) {
  CreateX11FwdQueueElem elem;
  elem.task_id = task_id;
  auto port_future = elem.promise.get_future();
  m_create_x11_fwd_handler_queue_.enqueue(std::move(elem));
  m_clean_x11_fwd_handler_queue_async_handle_->send();
  return port_future.get();
}

uint16_t CforedClient::SetupX11forwarding_() {
  int ret;

  auto x11_proxy_h = m_loop_->resource<uvw::tcp_handle>();
  constexpr uint16_t x11_port_begin = 6020;
  constexpr uint16_t x11_port_end = 6100;

  auto x11_data = std::make_shared<X11FdInfo>();
  x11_data->proxy_handle = x11_proxy_h;

  x11_proxy_h->data(x11_data);

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

    auto sock = h.parent().resource<uvw::tcp_handle>();

    sock->on<uvw::data_event>([this](uvw::data_event& e, uvw::tcp_handle& s) {
      CRANE_TRACE("Read x11 output. Forwarding...");

      TaskX11OutPutForward(std::move(e.data), e.length);
    });
    sock->on<uvw::write_event>(
        [this](const uvw::write_event&, uvw::tcp_handle&) {
          CRANE_TRACE("Write x11 input done.");
        });

    sock->on<uvw::end_event>([](uvw::end_event&, uvw::tcp_handle& h) {
      // EOF
      CRANE_TRACE("X11 proxy connection ended. Closing it.");
      if (auto* p = h.data<X11FdInfo>().get(); p) p->sock_stopped = true;
      h.close();
    });
    sock->on<uvw::error_event>([](uvw::error_event& e, uvw::tcp_handle& h) {
      CRANE_ERROR("Error on x11 proxy of {}. Closing it.", e.what());
      if (auto* p = h.data<X11FdInfo>().get(); p) p->sock_stopped = true;
      h.close();
    });
    sock->on<uvw::close_event>([](uvw::close_event&, uvw::tcp_handle& h) {
      CRANE_TRACE("X11 proxy connection was closed.");
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

  x11_data->port = port;
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
  while (m_create_stdout_fwd_handler_queue_.try_dequeue(elem)) {
    auto& meta = elem.meta;
    auto stdout_read = meta.stdout_read;
    auto task_id = meta.task_id;
    auto on_finish = [this, task_id] {
      m_stop_task_io_queue_.enqueue(task_id);
      m_clean_stop_task_io_queue_async_handle_->send();
    };
    std::shared_ptr<uvw::pipe_handle> ph;
    std::shared_ptr<uvw::tty_handle> th;
    int err = 0;
    if (!meta.pty) {
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

      ph->on<uvw::data_event>([this](uvw::data_event& e, uvw::pipe_handle&) {
        if (e.length > 0) {
          std::string output(e.data.get(), e.length);
          this->TaskOutPutForward(output);
        }
      });

      ph->on<uvw::end_event>([on_finish](uvw::end_event&, uvw::pipe_handle& h) {
        // EOF Received
        h.close();
        on_finish();
      });

      ph->on<uvw::error_event>([tid = meta.task_id, on_finish](
                                   uvw::error_event& e, uvw::pipe_handle& h) {
        CRANE_WARN("[Task #{}]  output pipe error: {}. Closing.", tid,
                   e.what());
        h.close();
        on_finish();
      });

      ph->on<uvw::close_event>(
          [tid = meta.task_id](uvw::close_event&, uvw::pipe_handle&) {
            CRANE_TRACE("[Task #{}] Output pipe closed.", tid);
          });

    } else {
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

      th->on<uvw::data_event>([this](uvw::data_event& e, uvw::tty_handle&) {
        if (e.length > 0) {
          std::string output(e.data.get(), e.length);
          this->TaskOutPutForward(output);
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

    elem.promise.set_value(true);
  }
}

void CforedClient::CleanX11FwdHandlerQueueCb_() {
  CreateX11FwdQueueElem elem;
  while (m_create_x11_fwd_handler_queue_.try_dequeue(elem)) {
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
      auto& output_handle = it->second.out_handle;
      if (!it->second.pty && output_handle.pipe) output_handle.pipe->close();
      if (it->second.pty && output_handle.tty) output_handle.tty->close();
      output_handle.pipe.reset();
      output_handle.tty.reset();

      close(it->second.stdout_read);

      CRANE_DEBUG("[Task #{}] Finished its output.", task_id);

      ok_to_free = this->TaskOutputFinishNoLock_(task_id);
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
    grpc::ClientAsyncReaderWriter<StreamTaskIORequest, StreamTaskIOReply>*
        stream,
    std::atomic<bool>* write_pending) {
  CRANE_TRACE("CleanOutputQueueThread started.");
  std::string output;
  std::pair<std::unique_ptr<char[]>, size_t> x11_output;
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
      request.set_type(StreamTaskIORequest::TASK_OUTPUT);

      auto* payload = request.mutable_payload_task_output_req();
      payload->set_msg(output);

      while (write_pending->load(std::memory_order::acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(25));

      CRANE_TRACE("Writing output...");
      write_pending->store(true, std::memory_order::release);
      stream->Write(request, (void*)Tag::Write);

      ok = m_output_queue_.try_dequeue(output);
    }

    if (x11_ok) {
      StreamTaskIORequest request;
      request.set_type(StreamTaskIORequest::TASK_X11_OUTPUT);

      auto* payload = request.mutable_payload_task_x11_output_req();

      auto& [p, len] = x11_output;
      payload->set_msg(p.get(), len);

      while (write_pending->load(std::memory_order::acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(25));

      CRANE_TRACE("Forwarding x11 output from task to cfored {}",
                  this->m_cfored_name_);
      write_pending->store(true, std::memory_order::release);
      stream->Write(request, (void*)Tag::Write);

      x11_ok = m_x11_output_queue_.try_dequeue(x11_output);
    }
  }

  m_output_drained_.store(true, std::memory_order::release);
  CRANE_TRACE("CleanOutputQueueThread exited.");
}

void CforedClient::AsyncSendRecvThread_() {
  enum class State : int {
    Registering = 0,
    WaitRegisterAck = 1,
    Forwarding = 2,
    Draining,
    Unregistering,
    End,
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
        request.set_type(StreamTaskIORequest::SUPERVISOR_UNREGISTER);

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
      absl::MutexLock lock(&m_mtx_);
      for (auto& task_id : m_fwd_meta_map | std::ranges::views::keys) {
        CRANE_ERROR(
            "[Task #{}] Markd task io stopped due to cfored conn failure.",
            task_id);
        m_stop_task_io_queue_.enqueue(task_id);
        m_clean_stop_task_io_queue_async_handle_->send();
      }
      CRANE_ERROR("Terminating all task due to cfored connection failure.");
      g_task_mgr->TerminateTaskAsync(
          false, TerminatedBy::TERMINATION_BY_CFORED_CONN_FAILURE);
      state = State::End;
    }

    switch (state) {
    case State::Registering:
      // Stream is ready. Start registering.
      CRANE_TRACE("Registering new stream on cfored {}", m_cfored_name_);

      CRANE_ASSERT_MSG_VA(tag == Tag::Prepare, "Tag: {}", int(tag));

      request.set_type(StreamTaskIORequest::SUPERVISOR_REGISTER);
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
      const std::string* msg;

      if (reply.type() == StreamTaskIOReply::TASK_X11_INPUT) {
        msg = &reply.payload_task_x11_input_req().msg();
        CRANE_TRACE("TASK_X11_INPUT len:{}.", msg->length());
      } else if (reply.type() == StreamTaskIOReply::TASK_INPUT) {
        msg = &reply.payload_task_input_req().msg();
        CRANE_TRACE("TASK_INPUT len:{} EOF:{}.", msg->length(),
                    reply.payload_task_input_req().eof());
      } else [[unlikely]] {
        CRANE_ERROR("Expect TASK_INPUT or TASK_X11_INPUT, but got {}",
                    (int)reply.type());
        break;
      }

      m_mtx_.Lock();

      if (reply.type() == StreamTaskIOReply::TASK_X11_INPUT) {
        for (auto& fwd_meta : m_fwd_meta_map | std::ranges::views::values) {
          CRANE_ASSERT(fwd_meta.x11_fd_info);
          if (!fwd_meta.x11_input_stopped)
            fwd_meta.x11_input_stopped =
                !WriteStringToFd_(*msg, fwd_meta.x11_fd_info->fd, false);
        }
      } else {
        bool eof = reply.payload_task_input_req().eof();
        // CRANED_TASK_INPUT
        for (auto& fwd_meta : m_fwd_meta_map | std::ranges::views::values) {
          if (!fwd_meta.input_stopped)
            fwd_meta.input_stopped = !WriteStringToFd_(
                *msg, fwd_meta.stdin_write, !fwd_meta.pty && eof);
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

      if (reply.type() != StreamTaskIOReply::SUPERVISOR_UNREGISTER_REPLY) {
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

bool CforedClient::TaskOutputFinishNoLock_(task_id_t task_id) {
  m_fwd_meta_map[task_id].output_stopped = true;
  return m_fwd_meta_map[task_id].proc_stopped;
};

bool CforedClient::TaskProcessStop(task_id_t task_id) {
  absl::MutexLock lock(&m_mtx_);
  m_fwd_meta_map[task_id].proc_stopped = true;
  return m_fwd_meta_map[task_id].output_stopped;
}

void CforedClient::TaskEnd(task_id_t task_id) {
  // FIXME: erase meta_map for task here!
  g_task_mgr->TaskStopAndDoStatusChange(task_id);
};

void CforedClient::TaskOutPutForward(const std::string& msg) {
  CRANE_TRACE("Receive TaskOutputForward len: {}.", msg.size());
  m_output_queue_.enqueue(msg);
}

void CforedClient::TaskX11OutPutForward(std::unique_ptr<char[]>&& data,
                                        size_t len) {
  CRANE_TRACE("Receive TaskX11OutPutForward len: {}.", len);
  m_x11_output_queue_.enqueue({std::move(data), len});
}

}  // namespace Craned::Supervisor