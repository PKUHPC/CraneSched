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

#include <string_view>

#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "CforedClient.h"
#include "crane/PasswordEntry.h"

namespace Craned::Supervisor {

enum class TerminatedBy : uint8_t {
  NONE = 0,
  CANCELLED_BY_USER,
  TERMINATION_BY_TIMEOUT,
  TERMINATION_BY_OOM
};

class ITaskInstance;

class StepInstance {
 public:
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  PasswordEntry pwd;
  bool orphaned{false};
  job_id_t job_id;
  step_id_t step_id;
  std::vector<task_id_t> task_ids;

  uid_t uid;
  gid_t gid;

  std::string container;
  std::optional<crane::grpc::InteractiveTaskType> interactive_type;
  bool pty;
  bool x11;
  bool x11_fwd;
  crane::grpc::TaskStatus status{crane::grpc::TaskStatus::Running};

  std::string cgroup_path;  // resolved cgroup path
  bool oom_baseline_inited{false};
  uint64_t baseline_oom_kill_count{0};  // v1 & v2
  uint64_t baseline_oom_count{0};       // v2 only

  StepInstance() = default;
  explicit StepInstance(const StepToSupv& step)
      : m_step_to_supv_(step),
        job_id(step.task_id()),
        step_id(0),
        task_ids({}),
        uid(step.uid()),
        gid(step.gid()),
        container(step.container()) {
    interactive_type =
        step.type() == crane::grpc::TaskType::Interactive
            ? std::optional(step.interactive_meta().interactive_type())
            : std::nullopt;
    pty = interactive_type.has_value() && step.interactive_meta().pty();
    x11 = interactive_type.has_value() && step.interactive_meta().x11();
    x11_fwd = interactive_type.has_value() &&
              step.interactive_meta().x11_meta().enable_forwarding();
  };
  ~StepInstance() = default;

  bool IsBatch() const noexcept;
  bool IsCrun() const noexcept;
  bool IsCalloc() const noexcept;

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

  void StopCforedClient() { m_cfored_client_.reset(); }

  EnvMap GetStepProcessEnv() const;

  // OOM monitoring methods
  void InitOomBaseline();
  bool EvaluateOomOnExit();

  void AddTaskInstance(task_id_t task_id,
                       std::unique_ptr<ITaskInstance>&& task);
  ITaskInstance* GetTaskInstance(task_id_t task_id);
  const ITaskInstance* GetTaskInstance(task_id_t task_id) const;
  std::unique_ptr<ITaskInstance> RemoveTaskInstance(task_id_t task_id);
  bool AllTaskFinished() const;

 private:
  crane::grpc::TaskToD m_step_to_supv_;
  std::unique_ptr<CforedClient> m_cfored_client_;
  std::unordered_map<task_id_t, std::unique_ptr<ITaskInstance>> m_task_map_;
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
      : m_parent_step_inst_(step_inst),
        task_id(step_inst->GetStep().task_id()) {}
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
  virtual CraneErrCode Suspend();
  virtual CraneErrCode Resume();
  virtual CraneErrCode Cleanup() = 0;

  virtual std::optional<const TaskExitInfo> HandleSigchld(pid_t pid,
                                                          int status) = 0;

  task_id_t task_id;

  CraneErrCode err_before_exec{CraneErrCode::SUCCESS};

  TerminatedBy terminated_by{TerminatedBy::NONE};

 protected:
  CrunInstanceMeta* GetCrunMeta() const {
    return dynamic_cast<CrunInstanceMeta*>(this->m_meta_.get());
  };

  // Helper methods
  // NOLINTBEGIN(readability-identifier-naming)
  CraneErrCode SetupCrunX11_();

  // Return error before fork.
  virtual CraneExpected<pid_t> ForkCrunAndInitMeta_();

  virtual bool SetupCrunFwdAtParent_(uint16_t* x11_port);

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
  CraneErrCode Suspend() override;
  CraneErrCode Resume() override;
  CraneErrCode Cleanup() override;

  std::optional<const TaskExitInfo> HandleSigchld(pid_t pid,
                                                  int status) override;

 private:
  CraneErrCode ModifyOCIBundleConfig_(const std::string& src,
                                      const std::string& dst) const;
  std::string ParseOCICmdPattern_(const std::string& cmd) const;
  CraneErrCode ExecuteContainerCommand_(const std::string& cmd_template,
                                        std::string_view action) const;

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

  std::optional<const TaskExitInfo> HandleSigchld(pid_t pid,
                                                  int status) override;
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

  void ShutdownSupervisor();

  // NOLINTBEGIN(readability-identifier-naming)
  template <typename Duration>
  void AddTerminationTimer_(Duration duration) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handel->start(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration),
        std::chrono::seconds(0));
    m_step_.termination_timer = termination_handel;
  }

  void AddTerminationTimer_(int64_t secs) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handel->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    m_step_.termination_timer = termination_handel;
  }

  void DelTerminationTimer_() {
    // Close handle before free
    if (m_step_.termination_timer) {
      m_step_.termination_timer->close();
      m_step_.termination_timer.reset();
    }
  }

  void ActivateTaskStatusChange_(task_id_t task_id,
                                 crane::grpc::TaskStatus new_status,
                                 uint32_t exit_code,
                                 std::optional<std::string> reason);
  void LaunchExecution_(ITaskInstance* task);
  // NOLINTEND(readability-identifier-naming)

  void TaskStopAndDoStatusChange(task_id_t task_id);

  std::future<CraneErrCode> ExecuteTaskAsync();

  std::future<CraneExpected<EnvMap>> QueryStepEnvAsync();

  std::future<CraneErrCode> ChangeTaskTimeLimitAsync(absl::Duration time_limit);

  void TerminateTaskAsync(bool mark_as_orphaned, bool terminated_by_user);

  std::future<CraneErrCode> SuspendJobAsync();

  std::future<CraneErrCode> ResumeJobAsync();

  void Shutdown() { m_supervisor_exit_ = true; }

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct ExecuteTaskElem {
    std::unique_ptr<ITaskInstance> instance;
    std::promise<CraneErrCode> ok_prom;
  };

  struct TaskTerminateQueueElem {
    TerminatedBy termination_reason{TerminatedBy::NONE};
    bool mark_as_orphaned{false};
  };

  struct ChangeTaskTimeLimitQueueElem {
    absl::Duration time_limit;
    std::promise<CraneErrCode> ok_prom;
  };

  struct TaskSignalQueueElem {
    enum class Action { Suspend, Resume } action;
    std::promise<CraneErrCode> prom;
  };

  void EvSigchldCb_();
  void EvSigchldTimerCb_();
  void EvCleanSigchldQueueCb_();
  void EvTaskTimerCb_();
  void EvCleanTaskStopQueueCb_();

  void EvCleanTerminateTaskQueueCb_();
  void EvCleanChangeTaskTimeLimitQueueCb_();
  void EvCleanTaskSignalQueueCb_();

  void EvGrpcExecuteTaskCb_();
  void EvGrpcQueryStepEnvCb_();

  CraneErrCode SuspendRunningTasks_();
  CraneErrCode ResumeSuspendedTasks_();

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;
  std::shared_ptr<uvw::timer_handle> m_sigchld_timer_handle_;
  std::shared_ptr<uvw::async_handle> m_process_sigchld_async_handle_;
  ConcurrentQueue<std::pair<pid_t, int>> m_sigchld_queue_;

  std::shared_ptr<uvw::async_handle> m_task_stop_async_handle_;
  ConcurrentQueue<task_id_t> m_task_stop_queue_;

  std::shared_ptr<uvw::async_handle> m_terminate_task_async_handle_;
  ConcurrentQueue<TaskTerminateQueueElem> m_task_terminate_queue_;

  std::shared_ptr<uvw::async_handle> m_change_task_time_limit_async_handle_;
  ConcurrentQueue<ChangeTaskTimeLimitQueueElem> m_task_time_limit_change_queue_;

  std::shared_ptr<uvw::async_handle> m_task_signal_async_handle_;
  ConcurrentQueue<TaskSignalQueueElem> m_task_signal_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_task_async_handle_;
  ConcurrentQueue<ExecuteTaskElem> m_grpc_execute_task_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_query_step_env_async_handle_;
  ConcurrentQueue<std::promise<CraneExpected<EnvMap>>>
      m_grpc_query_step_env_queue_;

  std::atomic_bool m_supervisor_exit_{false};
  std::thread m_uvw_thread_;

  StepInstance m_step_;
  std::unordered_map<pid_t, task_id_t> m_pid_task_id_map_;
};

}  // namespace Craned::Supervisor

inline std::unique_ptr<Craned::Supervisor::TaskManager> g_task_mgr;