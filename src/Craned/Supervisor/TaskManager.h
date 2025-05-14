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

#include "CforedClient.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"

namespace Supervisor {
struct StepSpec {
  crane::grpc::TaskToD spec;
  std::unique_ptr<CforedClient> cfored_client;
  inline bool IsBatch() const;
  inline bool IsCrun() const;
  inline bool IsCalloc() const;
};

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
  int task_input_fd;
  int task_output_fd;

  std::string x11_target;
  uint16_t x11_port;
  std::string x11_auth_path;
  ~CrunMetaInExecution() override = default;
};

struct ExitInfo {
  pid_t pid;
  int status;
  int value;
  bool is_terminated_by_signal;
};

class ExecutionInterface {
 public:
  explicit ExecutionInterface(const StepSpec* step_spec)
      : step_spec(step_spec) {}
  virtual ~ExecutionInterface();

  [[nodiscard]] pid_t GetPid() const { return m_pid_; }
  [[nodiscard]] const ExitInfo& GetExitInfo() const { return m_exit_info_; }

  // Interfaces must be implemented.
  virtual CraneErrCode Prepare() = 0;
  virtual CraneErrCode Spawn() = 0;
  virtual CraneErrCode Kill(int signum) = 0;
  virtual CraneErrCode Cleanup() = 0;

  // When exiting, the instance uses this function to properly set the fields of
  // ProcessExitInfo.
  virtual const ExitInfo& HandleAndSetExitInfo(ExitInfo exit_info) = 0;

  // Some task requires polling to track the status of the job.
  virtual bool IsPollingNeeded() const { return false; }
  virtual std::optional<ExitInfo> Polling() { return std::nullopt; }

  // Set from TaskManager
  const StepSpec* step_spec;
  PasswordEntry pwd;

  // Set from TaskManager, timer for job time limit
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};

  // Set from TaskManager, timer for polling job status (only for containers)
  std::shared_ptr<uvw::timer_handle> polling_timer{nullptr};

  bool orphaned{false};
  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};

  CraneErrCode err_before_exec{CraneErrCode::SUCCESS};

 protected:
  CrunMetaInExecution* GetCrunMeta() const {
    return dynamic_cast<CrunMetaInExecution*>(this->m_meta_.get());
  };

  // FIXME: Remove this in future.
  // Before we distinguish TaskToD/SpecToSuper and JobSpec, it's hard to remove
  // this function.
  [[deprecated]] virtual EnvMap GetChildProcessEnv_() const;
  std::string ParseFilePathPattern_(const std::string& pattern,
                                    const std::string& cwd) const;

  // Helper methods (in parent process)
  // Return error before fork.
  virtual CraneExpected<pid_t> Fork_(bool* launch_pty,
                                     std::vector<int>* to_crun_pipe,
                                     std::vector<int>* from_crun_pipe,
                                     int* crun_pty_fd);
  virtual CraneErrCode SetCrunX11_();
  virtual uint16_t SetCrunMsgFwd_(bool launch_pty,
                                  const std::vector<int>& to_crun_pipe,
                                  const std::vector<int>& from_crun_pipe,
                                  int crun_pty_fd);

  // Helper methods (in child process)
  virtual void SetChildProcessSignalHandler_();
  virtual CraneErrCode SetChildProcessEnv_() const;
  virtual CraneErrCode SetChildProcessProperty_();
  virtual CraneErrCode SetChildProcessBatchFd_();

  virtual void SetChildProcessCrunFd_(bool launch_pty,
                                      const std::vector<int>& to_crun_pipe,
                                      const std::vector<int>& from_crun_pipe,
                                      int crun_pty_fd);
  virtual void SetChildProcessCrunX11_(uint16_t port);

  virtual std::vector<std::string> GetChildProcessExecArgv_() const;

  pid_t m_pid_{0};  // forked pid
  std::unique_ptr<MetaInExecution> m_meta_{nullptr};
  ExitInfo m_exit_info_{};

  EnvMap m_env_{};
  std::string m_executable_;  // bash -c "m_executable_ [m_arguments_...]"

  // NOTE: This is not used currently.
  // As we are using `bash -c` to launch process, all arguments can be passed in
  // m_executable_.
  std::vector<std::string> m_arguments_;
};

class ContainerInstance : public ExecutionInterface {
 public:
  explicit ContainerInstance(const StepSpec* step_spec)
      : ExecutionInterface(step_spec) {}
  ~ContainerInstance() override = default;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  const ExitInfo& HandleAndSetExitInfo(ExitInfo exit_info) override;

  // For container w/o OCI run support, Polling() is mandatory.
  bool IsPollingNeeded() const override { return !m_has_run_cmd_; }
  std::optional<ExitInfo> Polling() override;

  // See:
  // https://github.com/opencontainers/runtime-spec/blob/main/runtime.md#state
  enum class ContainerStatus {
    CREATING = 0,
    CREATED = 1,
    RUNNING = 2,
    STOPPED = 3,
  };

  struct ContainerState {
    ContainerStatus status;
    pid_t pid;
  };

  static std::expected<ContainerStatus, CraneErrCode> ParseContainerStatus(
      const std::string& str) {
    static const std::unordered_map<std::string, ContainerStatus> status_map = {
        {"CREATING", ContainerStatus::CREATING},
        {"CREATED", ContainerStatus::CREATED},
        {"RUNNING", ContainerStatus::RUNNING},
        {"STOPPED", ContainerStatus::STOPPED}};

    auto it = status_map.find(absl::AsciiStrToUpper(str));
    if (it != status_map.end()) return it->second;
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  };

 private:
  CraneErrCode ModifyOCIBundleConfig_(const std::string& src,
                                      const std::string& dst) const;

  // Get container state from OCI runtime
  std::expected<ContainerState, CraneErrCode> GetContainerState_() const;

  // Create container
  CraneErrCode CreateContainer_();

  // Start container
  CraneErrCode StartContainer_();

  // Run container using fork()
  CraneErrCode RunContainer_();

  std::string ParseOCICmdPattern_(const std::string&) const;

  bool m_has_run_cmd_{false};
  std::filesystem::path m_bundle_path_;
  std::filesystem::path m_temp_path_;
};

class ProcessInstance : public ExecutionInterface {
 public:
  explicit ProcessInstance(const StepSpec* step_spec)
      : ExecutionInterface(step_spec) {}
  ~ProcessInstance() override = default;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  const ExitInfo& HandleAndSetExitInfo(ExitInfo exit_info) override;

  // Polling() is not used for process.
};

class TaskManager {
 public:
  explicit TaskManager();
  ~TaskManager();
  void Wait();

  template <typename Duration>
  void AddTerminationTimer_(ExecutionInterface* instance, Duration duration) {
    auto termination_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handle->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });
    termination_handle->start(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration),
        std::chrono::seconds(0));
    instance->termination_timer = termination_handle;
  }

  void AddTerminationTimer_(ExecutionInterface* instance, int64_t secs) {
    auto termination_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handle->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskTimerCb_();
        });

    termination_handle->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    instance->termination_timer = termination_handle;
  }

  static void DelTerminationTimer_(ExecutionInterface* instance) {
    // Close handle before free
    instance->termination_timer->close();
    instance->termination_timer.reset();
  }

  void AddPollingTimer_(ExecutionInterface* instance, int64_t secs,
                        std::function<std::optional<ExitInfo>()> func) {
    auto polling_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    polling_handle->on<uvw::timer_event>(
        [&, this](const uvw::timer_event&, uvw::timer_handle& h) {
          EvTaskPollingCb_(func);
        });

    polling_handle->start(std::chrono::seconds(secs),
                          std::chrono::seconds(secs));
    instance->polling_timer = polling_handle;
  }

  void DelPollingTimer_(ExecutionInterface* instance) {
    instance->polling_timer->close();
    instance->polling_timer.reset();
  }

  void TaskStoppedAndDoStatusChange();

  void ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                 uint32_t exit_code,
                                 std::optional<std::string> reason);

  std::future<CraneExpected<pid_t>> ExecuteTaskAsync(const StepToSuper& spec);
  void LaunchExecution_();

  std::future<CraneExpected<pid_t>> CheckTaskStatusAsync();

  std::future<CraneErrCode> ChangeTaskTimeLimitAsync(absl::Duration time_limit);

  void TerminateTaskAsync(bool mark_as_orphaned, bool terminated_by_user);

  void Shutdown() { m_supervisor_exit_ = true; }

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
    std::promise<CraneErrCode> ok_prom;
  };

  void EvSigchldCb_();
  void EvTaskTimerCb_();
  void EvTaskPollingCb_(std::function<std::optional<ExitInfo>()> func);

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

  StepSpec m_step_spec_;
  // TODO: Support multiple tasks
  std::unique_ptr<ExecutionInterface> m_instance_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::TaskManager> g_task_mgr;