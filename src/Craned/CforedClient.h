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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "TaskManager.h"

namespace Craned {

class CforedClient {
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedClient();
  ~CforedClient();

  void InitChannelAndStub(const std::string& cfored_name);

  void AsyncSendRecvThread_();

  void InitTaskFwdAndSetInputCb(
      task_id_t task_id, std::function<bool(const std::string&)> task_input_cb);

  void SetX11FwdInputCb(
      task_id_t task_id,
      std::function<bool(const std::string&)> task_x11_input_cb);

  void TaskOutPutForward(task_id_t task_id, const std::string& msg);

  void TaskX11OutPutForward(task_id_t task_id, std::unique_ptr<char[]>&& data,
                            size_t len);

  bool TaskOutputFinish(task_id_t task_id);

  bool TaskProcessStop(task_id_t task_id);

 private:
  struct TaskFwdMeta {
    std::function<bool(const std::string&)> input_cb;
    bool input_stopped{false};

    std::function<bool(const std::string&)> x11_input_cb;
    bool x11_input_stopped{false};

    bool output_stopped{false};
    bool proc_stopped{false};
  };

  void CleanOutputQueueAndWriteToStreamThread_(
      grpc::ClientAsyncReaderWriter<crane::grpc::StreamTaskIORequest,
                                    crane::grpc::StreamTaskIOReply>* stream,
      std::atomic<bool>* write_pending);

  ConcurrentQueue<std::pair<task_id_t, std::string>> m_input_queue_;
  ConcurrentQueue<std::pair<task_id_t, std::string /*msg*/>> m_output_queue_;

  ConcurrentQueue<std::tuple<task_id_t, std::unique_ptr<char[]>, size_t>>
      m_x11_input_queue_;
  ConcurrentQueue<std::tuple<task_id_t, std::unique_ptr<char[]>, size_t>>
      m_x11_output_queue_;

  std::thread m_fwd_thread_;
  std::atomic<bool> m_stopped_{false};

  std::string m_cfored_name_;
  std::shared_ptr<grpc::Channel> m_cfored_channel_;
  std::unique_ptr<crane::grpc::CraneForeD::Stub> m_stub_;

  // Tag MUST have the same size of void* !!!!
  enum class Tag : intptr_t { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
  std::unordered_map<task_id_t, TaskFwdMeta> m_task_fwd_meta_map_;
};

class CforedManager {
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  struct RegisterElem {
    std::string cfored;
    task_id_t task_id;
    int task_in_fd;
    int task_out_fd;
    bool pty;

    bool x11_enable_forwarding;
  };

  struct RegisterResult {
    bool ok;
    uint16_t x11_port;
  };

  CforedManager() = default;
  ~CforedManager();

  bool Init();

  void RegisterIOForward(const RegisterElem& elem, RegisterResult* result);
  void TaskProcOnCforedStopped(std::string const& cfored, task_id_t task_id);

 private:
  struct TaskStopElem {
    std::string cfored;
    task_id_t task_id;
  };

  struct UnregisterElem {
    std::string cfored;
    task_id_t task_id;
  };

  struct X11FdInfo {
    int fd;
    std::shared_ptr<uvw::tcp_handle> sock;
    std::shared_ptr<uvw::tcp_handle> proxy_handle;
    std::atomic<bool> sock_stopped;
  };

  uint16_t SetupX11forwarding_(std::string const& cfored, task_id_t task_id);

  void UnregisterIOForward_(std::string const& cfored, task_id_t task_id);

  void EvLoopThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::atomic<bool> m_stopped_{false};
  std::shared_ptr<uvw::loop> m_loop_;
  std::thread m_ev_loop_thread_;

  std::shared_ptr<uvw::async_handle> m_register_handle_;
  ConcurrentQueue<std::pair<RegisterElem, std::promise<RegisterResult>>>
      m_register_queue_;
  void RegisterCb_();

  std::shared_ptr<uvw::async_handle> m_task_stop_handle_;
  ConcurrentQueue<TaskStopElem> m_task_stop_queue_;
  void TaskStopCb_();

  std::shared_ptr<uvw::async_handle> m_unregister_handle_;
  ConcurrentQueue<UnregisterElem> m_unregister_queue_;
  void UnregisterCb_();

  std::unordered_map<std::string /*cfored name*/, std::shared_ptr<CforedClient>>
      m_cfored_client_map_;

  std::unordered_map<std::string /*cfored name*/, uint32_t /*cfored refcount*/>
      m_cfored_client_ref_count_map_;

  std::unordered_map<task_id_t, std::shared_ptr<X11FdInfo>>
      m_task_id_to_x11_map_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::CforedManager> g_cfored_manager;