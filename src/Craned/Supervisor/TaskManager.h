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

#include <expected>

#include "PublicDefs.pb.h"
#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "CforedClient.h"
#include "crane/BindFs.h"
#include "crane/CriClient.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "cri/api.pb.h"

namespace Craned::Supervisor {

enum class TerminatedBy : uint8_t {
  NONE = 0,
  CANCELLED_BY_USER,
  TERMINATION_BY_TIMEOUT,
  TERMINATION_BY_OOM,
  TERMINATION_BY_CFORED_CONN_FAILURE
};

class ITaskInstance;

class StepInstance {
 public:
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  std::shared_ptr<uvw::timer_handle> signal_timer{nullptr};
  PasswordEntry pwd;

  bool orphaned{false};

  job_id_t job_id{};
  step_id_t step_id{};
  std::vector<task_id_t> task_ids;

  uid_t uid{};
  std::vector<gid_t> gids;

  // TODO: Move these into ProcInstance
  std::optional<crane::grpc::InteractiveTaskType> interactive_type;
  bool x11{};
  bool x11_fwd{};
  bool pty{};

  std::optional<std::filesystem::path> script_path;

  std::string cgroup_path;  // resolved cgroup path
  bool oom_baseline_inited{false};
  uint64_t baseline_oom_kill_count{0};  // v1 & v2
  uint64_t baseline_oom_count{0};       // v2 only

  explicit StepInstance(const StepToSupv& step)
      : m_step_to_supv_(step),
        job_id(step.job_id()),
        step_id(step.step_id()),
        task_ids({0}),  // TODO: Set task_id here
        uid(step.uid()),
        gids(step.gid().begin(), step.gid().end()) {
    interactive_type =
        step.type() == crane::grpc::TaskType::Interactive
            ? std::optional(step.interactive_meta().interactive_type())
            : std::nullopt;
    pty = interactive_type.has_value() && step.interactive_meta().pty();
    x11 = interactive_type.has_value() && step.interactive_meta().x11();
    x11_fwd = interactive_type.has_value() &&
              step.interactive_meta().x11_meta().enable_forwarding();
  };

  StepInstance(const StepInstance&) = delete;
  StepInstance(StepInstance&&) = delete;
  StepInstance& operator=(const StepInstance&) = delete;
  StepInstance& operator=(StepInstance&&) = delete;

  ~StepInstance() = default;

  // Do some preparation needed by all tasks.
  CraneErrCode Prepare();
  void CleanUp();

  /*
  A Step can be classified from multiple perspectives:

  1. Interactivity: whether the Step is interactive (IsInteractive()), and for
  interactive Steps, its subtypes (IsCrun / IsCalloc).
  2. Role in a Job: whether it is a Common Step or a Daemon Step (IsDaemon()).
  3. Container support: whether it runs as a Pod or a Container (IsPod(),
  IsContainer()).

  Across different perspectives, these Is-methods are not mutually exclusive.
  When adding new methods, please ensure they are orthogonal to existing ones.
  */

  // Perspective 1: Interactivity
  [[nodiscard]] bool IsInteractive() const noexcept;
  [[nodiscard]] bool IsCrun() const noexcept;
  [[nodiscard]] bool IsCalloc() const noexcept;

  // Perspective 2: Role in a Job
  [[nodiscard]] bool IsDaemon() const noexcept;

  // Perspective 3: Container support
  [[nodiscard]] bool IsPod() const noexcept;
  [[nodiscard]] bool IsContainer() const noexcept;

  const StepToSupv& GetStep() const { return m_step_to_supv_; }

  // Cfored client in step
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

  // CRI client in step
  void InitCriClient() {
    CRANE_ASSERT(g_config.Container.Enabled);
    m_cri_client_ = std::make_unique<cri::CriClient>();
    m_cri_client_->InitChannelAndStub(g_config.Container.RuntimeEndpoint,
                                      g_config.Container.ImageEndpoint);
  }
  [[nodiscard]] const cri::CriClient* GetCriClient() const {
    return m_cri_client_.get();
  }
  [[nodiscard]] cri::CriClient* GetCriClient() { return m_cri_client_.get(); }
  void StopCriClient() { m_cri_client_.reset(); }

  // Just a convenient method for iteration on the map.
  std::unordered_set<task_id_t> GetTaskIds() const {
    return m_task_map_ | std::views::keys |
           std::ranges::to<std::unordered_set>();
  }

  ITaskInstance* GetTaskInstance(task_id_t task_id) {
    if (!m_task_map_.contains(task_id)) return nullptr;
    return m_task_map_.at(task_id).get();
  }

  const ITaskInstance* GetTaskInstance(task_id_t task_id) const {
    if (!m_task_map_.contains(task_id)) return nullptr;
    return m_task_map_.at(task_id).get();
  }

  void AddTaskInstance(task_id_t task_id,
                       std::unique_ptr<ITaskInstance>&& task);
  std::unique_ptr<ITaskInstance> RemoveTaskInstance(task_id_t task_id);

  bool AllTaskFinished() const;

  EnvMap GetStepProcessEnv() const;

  // OOM monitoring methods
  void InitOomBaseline();
  bool EvaluateOomOnExit();

 private:
  crane::grpc::StepToD m_step_to_supv_;
  std::unique_ptr<cri::CriClient> m_cri_client_;
  std::unique_ptr<CforedClient> m_cfored_client_;
  std::unordered_map<task_id_t, std::unique_ptr<ITaskInstance>> m_task_map_;
};

struct TaskExitInfo {
  pid_t pid{0};
  bool is_terminated_by_signal{false};
  int value{0};
};

using TaskExecId = std::variant<std::string, pid_t>;

class ITaskInstance {
 public:
  explicit ITaskInstance(StepInstance* step_inst)
      : m_parent_step_inst_(step_inst) {}

  virtual ~ITaskInstance() = default;

  ITaskInstance(const ITaskInstance&) = delete;
  ITaskInstance(ITaskInstance&&) = delete;

  ITaskInstance& operator=(ITaskInstance&&) = delete;
  ITaskInstance& operator=(const ITaskInstance&) = delete;

  // Helper methods shared by all task instances
  StepInstance* GetParentStepInstance() const { return m_parent_step_inst_; }
  const StepToSupv& GetParentStep() const {
    return m_parent_step_inst_->GetStep();
  }

  [[nodiscard]] const TaskExitInfo& GetExitInfo() const { return m_exit_info_; }

  // Interfaces must be implemented.
  virtual CraneErrCode Prepare() = 0;
  virtual CraneErrCode Spawn() = 0;
  virtual CraneErrCode Kill(int signum) = 0;
  virtual CraneErrCode Cleanup() = 0;

  virtual std::optional<TaskExecId> GetExecId() const = 0;

  // Set environment variables for the task instance. Can be overridden.
  virtual void InitEnvMap();

  task_id_t task_id{0};
  CraneErrCode err_before_exec{CraneErrCode::SUCCESS};
  TerminatedBy terminated_by{TerminatedBy::NONE};

 protected:
  StepInstance* m_parent_step_inst_;
  TaskExitInfo m_exit_info_{};
  EnvMap m_env_;
};

class PodInstance : public ITaskInstance {
 public:
  explicit PodInstance(StepInstance* step_spec) : ITaskInstance(step_spec) {}
  ~PodInstance() override = default;

  PodInstance(const PodInstance&) = delete;
  PodInstance(PodInstance&&) = delete;

  PodInstance& operator=(PodInstance&&) = delete;
  PodInstance& operator=(const PodInstance&) = delete;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  std::optional<TaskExecId> GetExecId() const override {
    if (m_pod_id_.empty()) return std::nullopt;
    return m_pod_id_;
  }

  const TaskExitInfo& HandlePodExited();

  // NOTE: These are public constants and will be used ContainerInstance.
  // We use this to get a consistent data dir for pod/containers.
  static constexpr std::string_view kPodLogDirPattern = "{}.out";
  // NOTE: Should be consistent with ContainerInstance.
  // We get PodSandboxConfig from this file in another common steps.
  static constexpr std::string_view kPodConfigFilePattern = ".{}.bin";
  static constexpr std::string_view kPodIdLockFilePattern = ".{}.lock";

 private:
  static constexpr size_t kCriDnsMaxLabelLen = 63;  // DNS-1123 len limit

  static std::string MakeHashId_(job_id_t job_id, const std::string& job_name,
                                 const std::string& node_name);
  static CraneErrCode ResolveUserNsMapping_(
      const PasswordEntry& pwd, cri::api::LinuxSandboxSecurityContext* sec_ctx);

  CraneErrCode SetPodSandboxConfig_(
      const crane::grpc::PodTaskAdditionalMeta& pod_meta);
  CraneErrCode PersistPodSandboxInfo_();

  cri::api::PodSandboxConfig m_pod_config_;
  std::string m_pod_id_;

  std::filesystem::path m_log_dir_;
  std::filesystem::path m_config_file_;
  std::filesystem::path m_lock_file_;
};

class ContainerInstance : public ITaskInstance {
 public:
  explicit ContainerInstance(StepInstance* step_spec)
      : ITaskInstance(step_spec) {}
  ~ContainerInstance() override = default;

  ContainerInstance(const ContainerInstance&) = delete;
  ContainerInstance(ContainerInstance&&) = delete;

  ContainerInstance& operator=(ContainerInstance&&) = delete;
  ContainerInstance& operator=(const ContainerInstance&) = delete;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  std::optional<TaskExecId> GetExecId() const override {
    if (m_container_id_.empty()) return std::nullopt;
    return m_container_id_;
  }

  void InitEnvMap() override;

  const TaskExitInfo& HandleContainerExited(
      const cri::api::ContainerStatus& status);

 private:
  // Container related constants (step_id.node_id.log)
  static constexpr std::string_view kContainerLogFilePattern = "{}.{}.log";

  // This is wrapper for SetupIdMappedMounts_ and SetupIdMappedBindFs_.
  // NOTE: Must be called only after userns, run_as_* fields are set in config.
  CraneErrCode ApplyIdMappedMounts_(const PasswordEntry& pwd,
                                    cri::api::ContainerConfig* config,
                                    bool use_bindfs);

  // Setup id-mapped mounts in rootless containers.
  // NOTE: Called only after userns, run_as_* fields are set in config.
  CraneErrCode SetupIdMappedMounts_(const PasswordEntry& pwd,
                                    cri::api::ContainerConfig* config);

  // Setup id-mapped bindfs for a mount (workaround for no idmap fs).
  // NOTE: Called only after userns, run_as_* fields are set in config.
  CraneErrCode SetupIdMappedBindFs_(const PasswordEntry& pwd,
                                    cri::api::ContainerConfig* config);

  CraneErrCode LoadPodSandboxInfo_(
      const crane::grpc::PodTaskAdditionalMeta* pod_meta);
  CraneErrCode SetContainerConfig_(
      const crane::grpc::ContainerTaskAdditionalMeta& ca_meta,
      const crane::grpc::PodTaskAdditionalMeta* pod_meta);

  std::string m_image_id_;
  std::string m_pod_id_;
  std::string m_container_id_;

  cri::api::PodSandboxConfig m_pod_config_;
  cri::api::ContainerConfig m_container_config_;

  std::filesystem::path m_log_dir_;
  std::filesystem::path m_log_file_;
  std::vector<std::unique_ptr<bindfs::IdMappedBindFs>> m_bindfs_mounts_;
};

struct ProcInstanceMeta {
  virtual ~ProcInstanceMeta() = default;
  ProcInstanceMeta() = default;
  ProcInstanceMeta(const ProcInstanceMeta&) = delete;
  ProcInstanceMeta(ProcInstanceMeta&&) = delete;
  ProcInstanceMeta& operator=(const ProcInstanceMeta&) = delete;
  ProcInstanceMeta& operator=(ProcInstanceMeta&&) = delete;

  std::string parsed_sh_script_path;
};

struct BatchInstanceMeta final : ProcInstanceMeta {
  ~BatchInstanceMeta() override = default;
  BatchInstanceMeta() = default;
  BatchInstanceMeta(const BatchInstanceMeta&) = delete;
  BatchInstanceMeta(BatchInstanceMeta&&) = delete;
  BatchInstanceMeta& operator=(const BatchInstanceMeta&) = delete;
  BatchInstanceMeta& operator=(BatchInstanceMeta&&) = delete;

  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
};

struct CrunInstanceMeta final : ProcInstanceMeta {
  ~CrunInstanceMeta() override = default;
  CrunInstanceMeta() = default;
  CrunInstanceMeta(const CrunInstanceMeta&) = delete;
  CrunInstanceMeta(CrunInstanceMeta&&) = delete;
  CrunInstanceMeta& operator=(const CrunInstanceMeta&) = delete;
  CrunInstanceMeta& operator=(CrunInstanceMeta&&) = delete;

  int stdin_write{};
  int stdout_write{};
  int stdin_read{};
  int stdout_read{};

  std::string x11_target;
  uint16_t x11_port{};
  std::string x11_auth_path;
};

class ProcInstance : public ITaskInstance {
 public:
  explicit ProcInstance(StepInstance* step_spec) : ITaskInstance(step_spec) {}

  ~ProcInstance() override;

  ProcInstance(const ProcInstance&) = delete;
  ProcInstance(ProcInstance&&) = delete;

  ProcInstance& operator=(ProcInstance&&) = delete;
  ProcInstance& operator=(const ProcInstance&) = delete;

  CraneErrCode Prepare() override;
  CraneErrCode Spawn() override;
  CraneErrCode Kill(int signum) override;
  CraneErrCode Cleanup() override;

  void InitEnvMap() override;

  std::optional<TaskExecId> GetExecId() const override {
    if (m_pid_ == 0) return std::nullopt;
    return m_pid_;
  }

  std::optional<const TaskExitInfo> HandleSigchld(pid_t pid, int status);

 private:
  // Methods related to Crun only

  // NOTE: If called before meta is set, will give nullptr.
  CrunInstanceMeta* GetCrunMeta_() const {
    return dynamic_cast<CrunInstanceMeta*>(this->m_meta_.get());
  };

  CraneExpected<pid_t> ForkCrunAndInitMeta_();

  bool SetupCrunFwdAtParent_(uint16_t* x11_port);
  void SetupCrunFwdAtChild_();

  CraneErrCode PrepareXauthFiles_();
  void SetupChildProcCrunX11_();

  // Methods related to Batch only
  CraneErrCode SetChildProcBatchFd_();

  // Methods shared by Batch/Crun
  static void ResetChildProcSigHandler_();

  CraneErrCode SetChildProcProperty_();

  CraneErrCode SetChildProcEnv_() const;

  std::vector<std::string> GetChildProcExecArgv_() const;

  /* Perform file name substitutions
   * %j - Job ID
   * %u - Username
   * %x - Job name
   */
  std::string ParseFilePathPattern_(const std::string& pattern,
                                    const std::string& cwd) const;

  std::unique_ptr<ProcInstanceMeta> m_meta_;
  pid_t m_pid_{0};  // forked pid

  std::string m_executable_;  // bash -c "m_executable_ [m_arguments_...]"
  std::vector<std::string> m_arguments_;  // NOTE: Not used currently.
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
  void ShutdownSupervisorAsync(
      crane::grpc::TaskStatus new_status = StepStatus::Completed,
      uint32_t exit_code = 0, std::string reason = "");

  // NOLINTBEGIN(readability-identifier-naming)
  template <typename Duration>
  void AddTerminationTimer_(Duration duration) {
    auto termination_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handle->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& /* h */) {
          EvTaskTimerCb_();
        });
    termination_handle->start(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration),
        std::chrono::seconds(0));
    m_step_.termination_timer = termination_handle;
  }

  void AddTerminationTimer_(int64_t secs) {
    auto termination_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handle->on<uvw::timer_event>(
        [this](const uvw::timer_event&, uvw::timer_handle& /* h */) {
          EvTaskTimerCb_();
        });
    termination_handle->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    m_step_.termination_timer = termination_handle;
  }

  void DelTerminationTimer_() {
    // Close handle before free
    if (m_step_.termination_timer) {
      m_step_.termination_timer->close();
      m_step_.termination_timer.reset();
    }
  }

  void AddSignalTimer_(int64_t secs, int signal_number) {
    auto signal_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    signal_handle->on<uvw::timer_event>(
        [this, signal_number, TaskIds = m_step_.GetTaskIds()](
            const uvw::timer_event&, uvw::timer_handle& h) {
          for (task_id_t task_id : TaskIds) {
            auto* task = m_step_.GetTaskInstance(task_id);
            if (task != nullptr && task->GetExecId().has_value()) {
              task->Kill(signal_number);
            }
          }
        });
    signal_handle->start(std::chrono::seconds(secs), std::chrono::seconds(0));
    m_step_.signal_timer = signal_handle;
  }

  void DelSignalTimer_() {
    if (m_step_.signal_timer) {
      m_step_.signal_timer->close();
      m_step_.signal_timer.reset();
    }
  }

  void TaskFinish_(task_id_t task_id, crane::grpc::TaskStatus new_status,
                   uint32_t exit_code, std::optional<std::string> reason);
  CraneErrCode LaunchExecution_(ITaskInstance* task);
  // NOLINTEND(readability-identifier-naming)

  // Just like a SIGCHLD sig handler (implictly set by uvw).
  // Will be called in CliClient thread.
  void HandleCriEvents(std::vector<cri::api::ContainerStatus>&& events) {
    m_cri_event_queue_.enqueue_bulk(std::make_move_iterator(events.begin()),
                                    events.size());
    m_cri_event_handle_->send();
  }

  void TaskStopAndDoStatusChange(task_id_t task_id);

  std::future<CraneErrCode> ExecuteTaskAsync();

  std::future<CraneExpected<EnvMap>> QueryStepEnvAsync();

  std::future<CraneErrCode> ChangeTaskTimeLimitAsync(absl::Duration time_limit);

  void TerminateTaskAsync(bool mark_as_orphaned, TerminatedBy terminated_by);

  void SetActivelyShutdown() { m_active_shutdown_ = true; }

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

  void EvShutdownSupervisorCb_();

  // Process exited
  void EvSigchldCb_();
  void EvSigchldTimerCb_();
  void EvCleanSigchldQueueCb_();

  // Container exited
  void EvCleanCriEventQueueCb_();

  // Handle stopped tasks
  void EvCleanTaskStopQueueCb_();

  // Handle task termination requests
  void EvTaskTimerCb_();
  void EvCleanTerminateTaskQueueCb_();
  void EvCleanChangeTaskTimeLimitQueueCb_();

  void EvGrpcExecuteTaskCb_();
  void EvGrpcQueryStepEnvCb_();

  std::shared_ptr<uvw::loop> m_uvw_loop_;
  ConcurrentQueue<std::tuple<crane::grpc::TaskStatus, uint32_t, std::string>>
      m_shutdown_status_queue_;
  std::shared_ptr<uvw::async_handle> m_shutdown_supervisor_handle_;

  // Handle SIGCHLD for ProcInstance
  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;
  std::shared_ptr<uvw::timer_handle> m_sigchld_timer_handle_;
  std::shared_ptr<uvw::async_handle> m_process_sigchld_async_handle_;
  ConcurrentQueue<std::pair<pid_t, int>> m_sigchld_queue_;

  // Handle event stream for ContainerInstance
  std::shared_ptr<uvw::async_handle> m_cri_event_handle_;
  ConcurrentQueue<cri::api::ContainerStatus> m_cri_event_queue_;

  // Task is already terminated
  std::shared_ptr<uvw::async_handle> m_task_stopped_async_handle_;
  ConcurrentQueue<task_id_t> m_task_stopped_queue_;

  // Task is requested to be terminated
  std::shared_ptr<uvw::async_handle> m_terminate_task_async_handle_;
  std::shared_ptr<uvw::timer_handle> m_terminate_task_timer_handle_;
  ConcurrentQueue<TaskTerminateQueueElem> m_task_terminate_queue_;

  std::shared_ptr<uvw::async_handle> m_change_task_time_limit_async_handle_;
  std::shared_ptr<uvw::timer_handle> m_change_task_time_limit_timer_handle_;
  ConcurrentQueue<ChangeTaskTimeLimitQueueElem> m_task_time_limit_change_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_task_async_handle_;
  ConcurrentQueue<ExecuteTaskElem> m_grpc_execute_task_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_query_step_env_async_handle_;
  ConcurrentQueue<std::promise<CraneExpected<EnvMap>>>
      m_grpc_query_step_env_queue_;

  std::atomic_bool m_supervisor_exit_;
  std::thread m_uvw_thread_;

  // This is the gate for daemon step. Daemon step will not exit when all tasks
  // are finished till Shutdown is called in gRPC, where ActivelyShutdown is set
  // to true.
  std::atomic_bool m_active_shutdown_{false};

  StepInstance m_step_;
  std::unordered_map<TaskExecId, task_id_t> m_exec_id_task_id_map_;
};

}  // namespace Craned::Supervisor

inline std::unique_ptr<Craned::Supervisor::TaskManager> g_task_mgr;
