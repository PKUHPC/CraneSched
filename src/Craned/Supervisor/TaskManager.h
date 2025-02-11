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
#include "crane/PasswordEntry.h"
// Precompiled header comes first.

namespace Supervisor {

using TaskSpec = crane::grpc::TaskToD;

struct SavedPrivilege {
  uid_t uid;
  gid_t gid;
};

struct MetaInProcessInstance {
  std::string parsed_sh_script_path;
  virtual ~MetaInProcessInstance() = default;
};

struct BatchMetaInProcessInstance : MetaInProcessInstance {
  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
  ~BatchMetaInProcessInstance() override = default;
};

struct CrunMetaInProcessInstance : MetaInProcessInstance {
  int msg_fd;
  ~CrunMetaInProcessInstance() override = default;
};

struct ProcSigchldInfo {
  pid_t pid;
  bool is_terminated_by_signal;
  int value;
};
class ProcessInstance {
 public:
  ProcessInstance(std::string exec_path, std::list<std::string> arg_list)
      : m_executive_path_(std::move(exec_path)),
        m_arguments_(std::move(arg_list)),
        m_pid_(0) {}

  ~ProcessInstance() = default;

  [[nodiscard]] const std::string& GetExecPath() const {
    return m_executive_path_;
  }
  [[nodiscard]] const std::list<std::string>& GetArgList() const {
    return m_arguments_;
  }

  void SetPid(pid_t pid) { m_pid_ = pid; }
  [[nodiscard]] pid_t GetPid() const { return m_pid_; }

  std::unique_ptr<MetaInProcessInstance> meta;

 private:
  /* ------------- Fields set by SpawnProcessInInstance_  ---------------- */
  pid_t m_pid_;

  /* ------- Fields set by the caller of SpawnProcessInInstance_  -------- */
  std::string m_executive_path_;
  std::list<std::string> m_arguments_;
};

struct TaskInstance {
  TaskInstance(const TaskSpec& task_spec) : task(task_spec) {}

  ~TaskInstance() = default;

  bool IsBatch() const;
  bool IsCrun() const;
  bool IsCalloc() const;

  EnvMap GetTaskEnvMap() const;
  TaskSpec task;

  PasswordEntry pwd_entry;

  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};

  bool orphaned{false};
  CraneErr err_before_exec{CraneErr::kOk};
  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};
  ProcSigchldInfo sigchld_info{};

  std::unique_ptr<ProcessInstance> process;
};

class TaskManager {
 public:
  explicit TaskManager();
  ~TaskManager();
  void Wait();

  template <typename Duration>
  void AddTerminationTimer_(TaskInstance* instance, Duration duration) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handel->start(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration),
        std::chrono::seconds(0));
    instance->termination_timer = termination_handel;
  }

  void AddTerminationTimer_(TaskInstance* instance, int64_t secs) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handel->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    instance->termination_timer = termination_handel;
  }

  static void DelTerminationTimer_(TaskInstance* instance) {
    // Close handle before free
    instance->termination_timer->close();
    instance->termination_timer.reset();
  }

  void TaskStopAndDoStatusChange();

  void ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                 uint32_t exit_code,
                                 std::optional<std::string> reason);

  static std::string ParseFilePathPattern_(const std::string& path_pattern,
                                           const std::string& cwd);

  std::future<CraneExpected<pid_t>> ExecuteTaskAsync(const TaskSpec& spec);
  void LaunchTaskInstance_();
  CraneErr SpawnTaskInstance_();
  CraneErr KillTaskInstance_(int signum);

  std::future<CraneExpected<pid_t>> CheckTaskStatusAsync();

  std::future<CraneErr> ChangeTaskTimeLimitAsync(absl::Duration time_limit);

  void TerminateTaskAsync(bool mark_as_orphaned);

  void TerminateSupervisor() { m_supervisor_exit_ = true; }

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct ExecuteTaskElem {
    std::unique_ptr<TaskInstance> task_instance;
    std::promise<CraneExpected<pid_t>> pid_prom;
  };

  struct TaskTerminateQueueElem {
    bool terminated_by_user{false};  // If the task is canceled by user,
    // task->status=Cancelled
    bool terminated_by_timeout{false};  // If the task is canceled by user,
    // task->status=Timeout
    bool mark_as_orphaned{false};
  };

  struct ChangeTaskTimeLimitQueueElem {
    absl::Duration time_limit;
    std::promise<CraneErr> ok_prom;
  };

  void EvSigchldCb_();
  void EvTaskTimerCb_();

  void EvCleanTerminateTaskQueueCb_();
  void EvCleanChangeTaskTimeLimitQueueCb_();
  void EvCleanCheckTaskStatusQueueCb_();
  void EvGrpcExecuteTaskCb_();

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;

  std::shared_ptr<uvw::async_handle> m_terminate_task_async_handle_;
  ConcurrentQueue<TaskTerminateQueueElem> m_task_terminate_queue_;

  std::shared_ptr<uvw::async_handle> m_change_task_time_limit_async_handle_;
  ConcurrentQueue<ChangeTaskTimeLimitQueueElem> m_task_time_limit_change_queue_;

  std::shared_ptr<uvw::async_handle> m_check_task_status_async_handle_;
  ConcurrentQueue<std::promise<CraneExpected<pid_t>>>
      m_check_task_status_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_task_async_handle_;
  ConcurrentQueue<ExecuteTaskElem> m_grpc_execute_task_queue_;

  std::atomic_bool m_supervisor_exit_;
  std::thread m_uvw_thread_;

  // Todo: support multiple task
  std::unique_ptr<TaskInstance> m_task_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::TaskManager> g_task_mgr;