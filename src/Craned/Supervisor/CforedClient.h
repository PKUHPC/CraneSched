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
    std::atomic<bool> sock_stopped;
    std::shared_ptr<uvw::tcp_handle> sock;
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

    bool proc_stopped{false};
  };

  using x11_local_id_t = uint32_t;
  using x11_id_t = std::pair<CranedId, uint32_t>;

  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedClient();
  ~CforedClient();

  void InitChannelAndStub(const std::string& cfored_name);

  bool InitFwdMetaAndUvStdoutFwdHandler(task_id_t task_id, int stdin_write,
                                        int stdout_read, bool pty);

  uint16_t InitUvX11FwdHandler();

  bool TaskProcessStop(task_id_t task_id, uint32_t exit_code, bool signaled);
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
  ConcurrentQueue<CreateStdoutFwdQueueElem> m_create_stdout_fwd_queue_;
  std::shared_ptr<uvw::async_handle>
      m_clean_stdout_fwd_handler_queue_async_handle_;
  void CleanStdoutFwdHandlerQueueCb_();

  struct CreateX11FwdQueueElem {
    std::promise<uint16_t> promise;
  };
  ConcurrentQueue<CreateX11FwdQueueElem> m_create_x11_fwd_queue_;
  void CleanX11FwdHandlerQueueCb_();
  std::shared_ptr<uvw::async_handle>
      m_clean_x11_fwd_handler_queue_async_handle_;

  ConcurrentQueue<x11_local_id_t> m_x11_fwd_remote_eof_queue_;
  std::shared_ptr<uvw::async_handle>
      m_clean_x11_fwd_remote_eof_queue_async_handle_;
  void CleanX11RemoteEofQueueCb_();
  ConcurrentQueue<task_id_t> m_stop_task_io_queue_;
  std::shared_ptr<uvw::async_handle> m_clean_stop_task_io_queue_async_handle_;
  void CleanStopTaskIOQueueCb_();

  void AsyncSendRecvThread_();

  void TaskOutPutForward(std::unique_ptr<char[]>&& data, size_t len);

  void TaskX11ConnectForward(x11_local_id_t x11_local_id);

  void TaskX11OutPutForward(x11_local_id_t x11_local_id,
                            std::unique_ptr<char[]>&& data, size_t len);

  void TaskX11OutputFinish(x11_local_id_t x11_local_id);

  void CleanOutputQueueAndWriteToStreamThread_(
      grpc::ClientAsyncReaderWriter<crane::grpc::StreamTaskIORequest,
                                    crane::grpc::StreamTaskIOReply>* stream,
      std::atomic<bool>* write_pending);

  std::atomic<bool> m_stopped_{false};
  std::atomic<bool> m_output_drained_{false};

  struct IOFwdRequest {
    std::unique_ptr<char[]> data;
    size_t len;
  };

  struct X11FwdConnectReq {
    x11_local_id_t x11_id;
  };
  struct X11FwdReq {
    x11_local_id_t x11_id;
    std::unique_ptr<char[]> data;
    size_t len;
  };

  struct X11FwdEofReq {
    x11_local_id_t x11_id;
  };

  struct TaskFinishStatus {
    task_id_t task_id{0};
    uint32_t exit_code{0};
    bool signaled{false};
  };

  struct FwdRequest {
    crane::grpc::StreamTaskIORequest::SupervisorRequestType type;
    std::variant<IOFwdRequest, X11FwdConnectReq, X11FwdReq, X11FwdEofReq,
                 TaskFinishStatus>
        data;
  };
  ConcurrentQueue<FwdRequest> m_task_fwd_req_queue_;

  std::thread m_fwd_thread_;

  std::shared_ptr<uvw::loop> m_loop_;
  std::thread m_ev_thread_;

  std::string m_cfored_name_;
  std::unordered_map<task_id_t, TaskFwdMeta> m_fwd_meta_map
      ABSL_GUARDED_BY(m_mtx_);

  x11_local_id_t next_x11_id_{0};
  std::unordered_map<x11_local_id_t, std::shared_ptr<X11FdInfo>>
      m_x11_fd_info_map_ ABSL_GUARDED_BY(m_mtx_);

  std::shared_ptr<grpc::Channel> m_cfored_channel_;
  std::unique_ptr<crane::grpc::CraneForeD::Stub> m_stub_;

  // Tag MUST have the same size of void* !!!!
  enum class Tag : intptr_t { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
};
}  // namespace Craned::Supervisor