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
#include <sched.h>

#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "CforedClient.h"
#include "crane/PasswordEntry.h"

namespace Supervisor {

class StepInstance {
 public:
  StepInstance() = default;
  explicit StepInstance(const StepToSupv& step) : m_step_to_supv_(step) {};

  bool IsBatch() const;
  bool IsCrun() const;
  bool IsCalloc() const;

  bool RequiresPty() const { return m_step_to_supv_.interactive_meta().pty(); }
  bool RequiresX11() const { return m_step_to_supv_.interactive_meta().x11(); }
  bool RequiresX11Fwd() const {
    return m_step_to_supv_.interactive_meta().x11_meta().enable_forwarding();
  }

  const StepToSupv& GetStep() const { return m_step_to_supv_; }

  void InitCforedClient() {
    m_cfored_client_ = std::make_unique<CforedClient>();
    m_cfored_client_->InitChannelAndStub(
        m_step_to_supv_.interactive_meta().cfored_name());
  }

  [[nodiscard]] const CforedClient* GetCforedClient() const {
    return m_cfored_client_.get();
  }

  [[nodiscard]] CforedClient* GetCforedClient() {
    return m_cfored_client_.get();
  }

 private:
  crane::grpc::TaskToD m_step_to_supv_;
  std::unique_ptr<CforedClient> m_cfored_client_;
};

struct TaskInstanceMeta {
  virtual ~TaskInstanceMeta() = default;

  std::string parsed_sh_script_path;
};

struct BatchInstanceMeta : TaskInstanceMeta {
  ~BatchInstanceMeta() override = default;

  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
};

struct CrunInstanceMeta : TaskInstanceMeta {
  ~CrunInstanceMeta() override = default;

  bool pty{false};

  int stdin_write;
  int stdout_write;

  int stdin_read;
  int stdout_read;

  std::string x11_target;
  uint16_t x11_port;
  std::string x11_auth_path;
};

struct TaskExitInfo {
  pid_t pid{0};
  bool is_terminated_by_signal{false};
  int status{0};
  int value{0};
};

class ITaskInstance {
 public:
  explicit ITaskInstance(StepInstance* step_inst)
      : m_parent_step_inst_(step_inst) {}
  virtual ~ITaskInstance();

  ITaskInstance(const ITaskInstance&) = delete;
  ITaskInstance(ITaskInstance&&) = delete;

  ITaskInstance& operator=(ITaskInstance&&) = delete;
  ITaskInstance& operator=(const ITaskInstance&) = delete;

  StepInstance* GetParentStepInstance() const { return m_parent_step_inst_; }
  const StepToSupv& GetParentStep() const {
    return m_parent_step_inst_->GetStep();
  }

  CrunInstanceMeta* GetCrunInstanceMeta() const {
    return dynamic_cast<CrunInstanceMeta*>(m_meta_.get());
  }

  void TaskProcStopped();
  [[nodiscard]] pid_t GetPid() const { return m_pid_; }
  [[nodiscard]] const TaskExitInfo& GetExitInfo() const { return m_exit_info_; }

  // FIXME: Remove this in future.
  // Before we distinguish TaskToD/SpecToSuper and JobSpec,
  // it's hard to remove this function.
  [[deprecated]] virtual EnvMap GetChildProcessEnv() const;

  // Interfaces must be implemented.
  virtual CraneErrCode Prepare() = 0;
  virtual CraneErrCode Spawn() = 0;
  virtual CraneErrCode Kill(int signum) = 0;
  virtual CraneErrCode Cleanup() = 0;

  virtual const TaskExitInfo& HandleSigchld(pid_t pid, int status) = 0;

  PasswordEntry pwd;

  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  CraneErrCode err_before_exec{CraneErrCode::SUCCESS};

  bool orphaned{false};
  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};

 protected:
  CrunInstanceMeta* GetCrunMeta() const {
    return dynamic_cast<CrunInstanceMeta*>(this->m_meta_.get());
  };

  // Helper methods
  // NOLINTBEGIN(readability-identifier-naming)
  CraneErrCode SetupCrunX11_();

  // Return error before fork.
  virtual CraneExpected<pid_t> ForkCrunAndInitMeta_();

  virtual void SetupCrunFwdAtParent_(uint16_t* x11_port);

  virtual void ResetChildProcSigHandler_();

  virtual CraneErrCode SetChildProcessProperty_();

  virtual CraneErrCode SetChildProcessBatchFd_();

  virtual void SetupCrunFwdAtChild_();

  virtual void SetupChildProcessCrunX11_();

  virtual CraneErrCode SetChildProcessEnv_() const;

  virtual std::vector<std::string> GetChildProcessExecArgv_() const;

  std::string ParseFilePathPattern_(const std::string& pattern,
                                    const std::string& cwd) const;

  // NOLINTEND(readability-identifier-naming)
  // NOLINTBEGIN(misc-non-private-member-variables-in-classes)

  StepInstance* m_parent_step_inst_;

  pid_t m_pid_{0};  // forked pid

  std::unique_ptr<TaskInstanceMeta> m_meta_{nullptr};
  TaskExitInfo m_exit_info_{};

  EnvMap m_env_;
  std::string m_executable_;  // bash -c "m_executable_ [m_arguments_...]"

  // NOTE: This is not used currently.
  // As we are using `bash -c` to launch process, all arguments can be passed in
  // m_executable_.
  std::vector<std::string> m_arguments_;
  // NOLINTEND(misc-non-private-member-variables-in-classes)
};

class ContainerInstance : public ITaskInstance {
 public:
  explicit ContainerInstance(StepInstance* step_spec)
      : ITaskInstance(step_spec) {}
  ~ContainerInstance() override = default;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  const TaskExitInfo& HandleSigchld(pid_t pid, int status) override;

 private:
  CraneErrCode ModifyOCIBundleConfig_(const std::string& src,
                                      const std::string& dst) const;
  std::string ParseOCICmdPattern_(const std::string& cmd) const;

  std::filesystem::path m_bundle_path_;
  std::filesystem::path m_temp_path_;
};

class ProcInstance : public ITaskInstance {
 public:
  explicit ProcInstance(StepInstance* step_spec) : ITaskInstance(step_spec) {}
  ~ProcInstance() override = default;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  const TaskExitInfo& HandleSigchld(pid_t pid, int status) override;
};

class TaskManager {
 public:
  explicit TaskManager();
  ~TaskManager();

  TaskManager(const TaskManager&) = delete;
  TaskManager(TaskManager&&) = delete;

  TaskManager& operator=(const TaskManager&) = delete;
  TaskManager& operator=(TaskManager&&) = delete;

  void Wait();

  // NOLINTBEGIN(readability-identifier-naming)
  template <typename Duration>
  void AddTerminationTimer_(ITaskInstance* instance, Duration duration) {
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

  void AddTerminationTimer_(ITaskInstance* instance, int64_t secs) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handel->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    instance->termination_timer = termination_handel;
  }

  static void DelTerminationTimer_(ITaskInstance* instance) {
    // Close handle before free
    if (instance->termination_timer) {
      instance->termination_timer->close();
      instance->termination_timer.reset();
    }
  }

  void ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                 uint32_t exit_code,
                                 std::optional<std::string> reason);
  void LaunchExecution_();
  // NOLINTEND(readability-identifier-naming)

  void TaskStopAndDoStatusChange();

  std::future<CraneErrCode> ExecuteTaskAsync();

  std::future<CraneExpected<EnvMap>> QueryStepEnvAsync();

  std::future<CraneExpected<pid_t>> CheckTaskStatusAsync();

  std::future<CraneErrCode> ChangeTaskTimeLimitAsync(absl::Duration time_limit);

  void TerminateTaskAsync(bool mark_as_orphaned, bool terminated_by_user);

  void Shutdown() { m_supervisor_exit_ = true; }

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct ExecuteTaskElem {
    std::unique_ptr<ITaskInstance> instance;
    std::promise<CraneErrCode> ok_prom;
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
    std::promise<CraneErrCode> ok_prom;
  };

  void EvSigchldCb_();
  void EvTaskTimerCb_();

  void EvCleanTerminateTaskQueueCb_();
  void EvCleanChangeTaskTimeLimitQueueCb_();
  void EvCleanCheckTaskStatusQueueCb_();
  void EvGrpcExecuteTaskCb_();
  void EvGrpcQueryStepEnvCb_();

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

  std::shared_ptr<uvw::async_handle> m_grpc_query_step_env_async_handle_;
  ConcurrentQueue<std::promise<CraneExpected<EnvMap>>>
      m_grpc_query_step_env_queue_;

  std::atomic_bool m_supervisor_exit_{false};
  std::thread m_uvw_thread_;

  StepInstance m_step_;
  // TODO: Support multiple tasks
  std::unique_ptr<ITaskInstance> m_task_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::TaskManager> g_task_mgr;