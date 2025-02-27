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

struct MetaInExecution {
  std::string parsed_sh_script_path;
  virtual ~MetaInExecution() = default;
};

struct BatchMetaInExecution : MetaInExecution {
  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
  ~BatchMetaInExecution() override = default;
};

struct CrunMetaInExecution : MetaInExecution {
  int msg_fd;
  ~CrunMetaInExecution() override = default;
};

struct ProcSigchldInfo {
  pid_t pid;
  bool is_terminated_by_signal;
  int value;
};

class ExecutionInterface {
 public:
  ExecutionInterface(const TaskSpec& task_spec) : task(task_spec) {}
  virtual ~ExecutionInterface() = default;

  inline bool IsBatch() const;
  inline bool IsCrun() const;
  inline bool IsCalloc() const;
  [[nodiscard]] pid_t GetPid() const { return m_pid_; }

  // Interfaces must be implemented.
  virtual CraneErr Prepare() = 0;
  virtual CraneErr Spawn() = 0;
  virtual CraneErr Kill(int signum) = 0;
  virtual CraneErr Cleanup() = 0;

  static std::string ParseFilePathPattern_(const std::string& pattern,
                                           const std::string& cwd);

  // Set from TaskManager
  TaskSpec task;
  PasswordEntry pwd;
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  bool orphaned{false};
  CraneErr err_before_exec{CraneErr::kOk};
  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};
  ProcSigchldInfo sigchld_info{};

 protected:
  // Helper methods
  virtual void SetChildProcessSignalHandler_();
  virtual CraneErr SetChildProcessProperty_();
  virtual CraneErr SetChildProcessBatchFd_();
  virtual EnvMap GetChildProcessEnv_() const;
  virtual std::vector<std::string> PrepareChildProcessExec_() const;

  pid_t m_pid_{0};  // forked pid
  std::unique_ptr<MetaInExecution> m_meta_;

  std::string m_executable_path_;  // bash -c "m_executable_ [m_arguments_...]"
  std::list<std::string> m_arguments_;
};

class ContainerInstance : public ExecutionInterface {
 public:
  ContainerInstance(const TaskSpec& task_spec)
      : ExecutionInterface(task_spec) {}
  virtual ~ContainerInstance() = default;

  virtual CraneErr Prepare() override { return CraneErr::kOk; }
  virtual CraneErr Spawn() override { return CraneErr::kOk; }
  virtual CraneErr Kill(int signum) override { return CraneErr::kOk; }
  virtual CraneErr Cleanup() override { return CraneErr::kOk; }
};

class ProcessInstance : public ExecutionInterface {
 public:
  ProcessInstance(const TaskSpec& task_spec) : ExecutionInterface(task_spec) {}
  virtual ~ProcessInstance() = default;

  virtual CraneErr Prepare() override;
  virtual CraneErr Spawn() override;
  virtual CraneErr Kill(int signum) override;
  virtual CraneErr Cleanup() override;
};

class TaskManager {
 public:
  explicit TaskManager();
  ~TaskManager();
  void Wait();

  template <typename Duration>
  void AddTerminationTimer_(ExecutionInterface* instance, Duration duration) {
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

  void AddTerminationTimer_(ExecutionInterface* instance, int64_t secs) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handel->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    instance->termination_timer = termination_handel;
  }

  static void DelTerminationTimer_(ExecutionInterface* instance) {
    // Close handle before free
    instance->termination_timer->close();
    instance->termination_timer.reset();
  }

  void TaskStopAndDoStatusChange();

  void ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                 uint32_t exit_code,
                                 std::optional<std::string> reason);

  std::future<CraneExpected<pid_t>> ExecuteTaskAsync(const TaskSpec& spec);
  void LaunchExecution_();

  std::future<CraneExpected<pid_t>> CheckTaskStatusAsync();

  std::future<CraneErr> ChangeTaskTimeLimitAsync(absl::Duration time_limit);

  void TerminateTaskAsync(bool mark_as_orphaned, bool terminated_by_user);

  void TerminateSupervisor() { m_supervisor_exit_ = true; }

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct ExecuteTaskElem {
    std::unique_ptr<ExecutionInterface> instance;
    std::promise<CraneExpected<pid_t>> pid_prom;
  };

  struct TaskTerminateQueueElem {
    bool terminated_by_user{false};  // If the task is canceled by user,
    // task->status=Cancelled (From Craned)
    bool terminated_by_timeout{false};  // If the task is terminated by timeout,
    // task->status=Timeout (From internal queue)
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

  // TODO: Support multiple task (execution steps)
  std::unique_ptr<ExecutionInterface> m_instance_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::TaskManager> g_task_mgr;