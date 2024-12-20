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

namespace Supervisor {

struct MetaInTaskInstance {
  std::string parsed_sh_script_path;
  virtual ~MetaInTaskInstance() = default;
};

struct BatchMetaInTaskInstance : MetaInTaskInstance {
  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
  ~BatchMetaInTaskInstance() override = default;
};

struct CrunMetaInTaskInstance : MetaInTaskInstance {
  int msg_fd;
  ~CrunMetaInTaskInstance() override = default;
};

struct ProcSigchldInfo {
  pid_t pid;
  bool is_terminated_by_signal;
  int value;

  std::shared_ptr<uvw::timer_handle> resend_timer{nullptr};
};

struct TaskInstance {
  TaskInstance(crane::grpc::ProcToD proc_to_d, std::string exec_path,
               std::list<std::string> arg_list)
      : task(proc_to_d),
        m_executive_path_(std::move(exec_path)),
        m_arguments_(std::move(arg_list)),
        m_pid_(0) {}

  ~TaskInstance() = default;

  [[nodiscard]] const std::string& GetExecPath() const {
    return m_executive_path_;
  }
  [[nodiscard]] const std::list<std::string>& GetArgList() const {
    return m_arguments_;
  }

  bool IsCrun() const;
  bool IsCalloc() const;

  void SetPid(pid_t pid) { m_pid_ = pid; }
  [[nodiscard]] pid_t GetPid() const { return m_pid_; }

  crane::grpc::ProcToD task;

  std::unique_ptr<MetaInTaskInstance> meta;

  bool orphaned{false};
  CraneErr err_before_exec{CraneErr::kOk};
  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};
  ProcSigchldInfo sigchld_info{};

 private:
  /* ------------- Fields set by SpawnProcessInInstance_  ---------------- */
  pid_t m_pid_;

  /* ------- Fields set by the caller of SpawnProcessInInstance_  -------- */
  std::string m_executive_path_;
  std::list<std::string> m_arguments_;
};

class TaskManager {
 public:
  explicit TaskManager(task_id_t task_id);
  ~TaskManager();

  void TaskStopAndDoStatusChange();

  void ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                 uint32_t exit_code,
                                 std::optional<std::string> reason);

  task_id_t task_id;

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  void EvSigchldCb_();
  void EvCleanSigchldQueueCb_();
  void EvSigchldTimerCb_(ProcSigchldInfo* sigchld_info);

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;
  ConcurrentQueue<std::unique_ptr<ProcSigchldInfo>> m_sigchld_queue_;

  std::shared_ptr<uvw::async_handle> m_process_sigchld_async_handle_;

  std::atomic_bool m_supervisor_exit_;
  std::thread m_uvw_thread_;

  std::unique_ptr<TaskInstance> m_process_;
  absl::Mutex m_mtx_;
};
}  // namespace Supervisor
inline std::unique_ptr<Supervisor::TaskManager> g_task_mgr;