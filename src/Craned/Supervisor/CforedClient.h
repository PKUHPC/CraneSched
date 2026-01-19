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

#pragma once

#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "protos/Crane.grpc.pb.h"

namespace Craned::Supervisor {

class CforedClient {
  struct X11FdInfo {
    int fd;
    uint16_t port;
    std::shared_ptr<uvw::tcp_handle> sock;
    std::shared_ptr<uvw::tcp_handle> proxy_handle;
    std::atomic<bool> sock_stopped;
  };

  struct TaskFwdMeta {
    task_id_t task_id{0};
    bool pty{false};
    struct OutHandle {
      std::shared_ptr<uvw::pipe_handle> pipe;
      std::shared_ptr<uvw::tty_handle> tty;
    };
    OutHandle out_handle{};

    int stdin_write{-1};
    int stdout_read{-1};

    bool input_stopped{false};
    bool output_stopped{false};

    bool x11_input_stopped{false};
    std::shared_ptr<X11FdInfo> x11_fd_info{nullptr};

    bool proc_stopped{false};
  };

  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedClient();
  ~CforedClient();

  void InitChannelAndStub(const std::string& cfored_name);

  bool InitFwdMetaAndUvStdoutFwdHandler(task_id_t task_id, int stdin_write,
                                        int stdout_read, bool pty);

  uint16_t InitUvX11FwdHandler(task_id_t task_id);

  bool TaskProcessStop(task_id_t task_id);
  void TaskEnd(task_id_t task_id);

  const std::string& CforedName() const { return m_cfored_name_; }

 private:
  bool TaskOutputFinishNoLock_(task_id_t task_id);
  uint16_t SetupX11forwarding_();

  static bool WriteStringToFd_(const std::string& msg, int fd, bool close_fd);

  struct CreateStdoutFwdQueueElem {
    TaskFwdMeta meta;
    std::promise<bool> promise;
  };
  ConcurrentQueue<CreateStdoutFwdQueueElem> m_create_stdout_fwd_handler_queue_;
  std::shared_ptr<uvw::async_handle>
      m_clean_stdout_fwd_handler_queue_async_handle_;
  void CleanStdoutFwdHandlerQueueCb_();

  struct CreateX11FwdQueueElem {
    task_id_t task_id;
    std::promise<uint16_t> promise;
  };
  ConcurrentQueue<CreateX11FwdQueueElem> m_create_x11_fwd_handler_queue_;
  std::shared_ptr<uvw::async_handle>
      m_clean_x11_fwd_handler_queue_async_handle_;
  void CleanX11FwdHandlerQueueCb_();
  ConcurrentQueue<task_id_t> m_stop_task_io_queue_;
  std::shared_ptr<uvw::async_handle> m_clean_stop_task_io_queue_async_handle_;
  void CleanStopTaskIOQueueCb_();

  void AsyncSendRecvThread_();

  void TaskOutPutForward(const std::string& msg);

  void TaskX11OutPutForward(std::unique_ptr<char[]>&& data, size_t len);

  void CleanOutputQueueAndWriteToStreamThread_(
      grpc::ClientAsyncReaderWriter<crane::grpc::StreamTaskIORequest,
                                    crane::grpc::StreamTaskIOReply>* stream,
      std::atomic<bool>* write_pending);

  std::atomic<bool> m_stopped_{false};
  std::atomic<bool> m_wait_reconn_{false};
  std::atomic<bool> m_output_drained_{false};

  std::atomic<size_t> m_output_queue_bytes_{0};
  ConcurrentQueue<std::string> m_output_queue_;
  ConcurrentQueue<std::pair<std::unique_ptr<char[]>, size_t>>
      m_x11_input_queue_;
  ConcurrentQueue<std::pair<std::unique_ptr<char[]>, size_t>>
      m_x11_output_queue_;

  std::thread m_fwd_thread_;

  std::shared_ptr<uvw::loop> m_loop_;
  std::thread m_ev_thread_;

  std::atomic<uint32_t> m_reconnect_attempts_;

  std::string m_cfored_name_;
  std::unordered_map<task_id_t, TaskFwdMeta> m_fwd_meta_map
      ABSL_GUARDED_BY(m_mtx_);

  std::shared_ptr<grpc::Channel> m_cfored_channel_;
  std::unique_ptr<crane::grpc::CraneForeD::Stub> m_stub_;

  // Tag MUST have the same size of void* !!!!
  enum class Tag : intptr_t { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
};
}  // namespace Craned::Supervisor