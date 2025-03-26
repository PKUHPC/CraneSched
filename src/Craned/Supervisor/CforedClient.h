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

namespace Supervisor {

class CforedClient {
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedClient();
  ~CforedClient();

  void InitChannelAndStub(const std::string& cfored_name);

  uint16_t SetUpTaskFwd(pid_t pid, int task_input_fd, int task_output_fd,
                        bool pty, bool x11_fwd);

  bool TaskOutputFinish(pid_t pid);

  bool TaskProcessStop(pid_t pid);

  void TaskEnd(pid_t pid);

  std::string CforedName() const { return m_cfored_name_; }

 private:
  uint16_t SetupX11forwarding_();
  bool TaskInputNoLock_(const std::string& msg, int fd);

  void AsyncSendRecvThread_();

  void TaskOutPutForward(const std::string& msg);

  void TaskX11OutPutForward(std::unique_ptr<char[]>&& data, size_t len);

  struct X11FdInfo {
    int fd;
    uint16_t port;
    std::shared_ptr<uvw::tcp_handle> sock;
    std::shared_ptr<uvw::tcp_handle> proxy_handle;
    std::atomic<bool> sock_stopped;
  };

  struct TaskFwdMeta {
    int input_fd{-1};
    int output_fd{-1};
    pid_t pid{-1};
    bool pty{false};
    std::shared_ptr<X11FdInfo> x11_fd_info{nullptr};
    bool x11_input_stopped{false};
    bool input_stopped{false};
    bool output_stopped{false};
    bool proc_stopped{false};
  };

  void CleanOutputQueueAndWriteToStreamThread_(
      grpc::ClientAsyncReaderWriter<crane::grpc::StreamTaskIORequest,
                                    crane::grpc::StreamTaskIOReply>* stream,
      std::atomic<bool>* write_pending);

  std::atomic<bool> m_stopped_{false};

  ConcurrentQueue<std::string> m_output_queue_;
  ConcurrentQueue<std::pair<std::unique_ptr<char[]>, size_t>>
      m_x11_input_queue_;
  ConcurrentQueue<std::pair<std::unique_ptr<char[]>, size_t>>
      m_x11_output_queue_;

  std::thread m_fwd_thread_;

  std::shared_ptr<uvw::loop> m_loop_;
  std::thread m_ev_thread_;

  std::string m_cfored_name_;
  std::unordered_map<pid_t, TaskFwdMeta> m_fwd_meta_map;

  std::shared_ptr<grpc::Channel> m_cfored_channel_;
  std::unique_ptr<crane::grpc::CraneForeD::Stub> m_stub_;

  // Tag MUST have the same size of void* !!!!
  enum class Tag : intptr_t { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
};
}  // namespace Supervisor
