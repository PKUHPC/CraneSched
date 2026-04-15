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

#include "PublicDefs.pb.h"
#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "CforedClient.h"
#include "Supervisor.grpc.pb.h"
#include "crane/BindFs.h"
#include "crane/CriClient.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "cri/api.pb.h"

namespace Craned::Supervisor {

using TaskExecId = std::variant<std::string, pid_t>;

enum class TaskFinalizeCause : uint8_t {
  NORMAL = 0,
  TASK_PREPARE_FAILED,
  TASK_SPAWN_FAILED,
  STEP_PWD_LOOKUP_FAILED,
  STEP_PREPARE_FAILED,
  STEP_CGROUP_FAILED,
  CANCELLED_BY_USER,
  CFORED_DISCONNECTED,
  DAEMON_POD_SHUTDOWN_REQUESTED,
  INTERACTIVE_NO_PROCESS,
  TIMEOUT,
  DEADLINE
};

class ITaskInstance;

class StepInstance {
 public:
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  std::vector<std::shared_ptr<uvw::timer_handle>> signal_timers;
  PasswordEntry pwd;

  bool orphaned{false};

  job_id_t job_id{};
  step_id_t step_id{};
  std::vector<task_id_t> task_ids;

  uid_t uid{};
  std::vector<gid_t> gids;

  // TODO: Move these into ProcInstance
  std::optional<crane::grpc::InteractiveJobType> interactive_type;
  bool x11{};
  bool x11_fwd{};
  bool pty{};

  struct X11Meta {
    uint16_t x11_port;
    std::string x11_auth_path;
  };

  std::optional<X11Meta> x11_meta;

  std::optional<std::filesystem::path> script_path;

  // Cgroup of this step
  // For daemon step, this cgroup will be used for SSH user.
  // For common step, this cgroup will be the parent of all task cgroups.
  std::unique_ptr<Common::CgroupInterface> step_user_cg;

  struct TerminationStatus {
    // Will return to crun for interactive steps
    uint32_t max_exit_code{0};
    StepStatus final_status_on_termination{StepStatus::Completed};
    std::string final_reason_on_termination;
  };

  TerminationStatus final_termination_status;
  bool oom_baseline_inited{false};
  uint64_t baseline_oom_kill_count{0};  // v1 & v2
  uint64_t baseline_oom_count{0};       // v2 only

  explicit StepInstance(const StepToSupv& step)
      : m_step_to_supv_(step),
        job_id(step.job_id()),
        step_id(step.step_id()),
        task_ids(step.task_res_map() | std::views::keys |
                 std::ranges::to<std::vector<task_id_t>>()),
        uid(step.uid()),
        gids(step.gid().begin(), step.gid().end()) {
    interactive_type =
        step.type() == crane::grpc::JobType::Interactive
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

  // Do not do any clean up action in destructor. This will only be called when
  // supervisor is exiting.
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
  [[nodiscard]] bool IsBatch() const noexcept;
  [[nodiscard]] bool IsInteractive() const noexcept;
  [[nodiscard]] bool IsCrun() const noexcept;
  [[nodiscard]] bool IsCalloc() const noexcept;

  // Perspective 2: Role in a Job
  [[nodiscard]] bool IsDaemon() const noexcept;
  [[nodiscard]] bool IsPrimary() const noexcept;

  // Perspective 3: Container support
  [[nodiscard]] bool IsPod() const noexcept;
  [[nodiscard]] bool IsContainer() const noexcept;

  [[nodiscard]] StepStatus GetStatus() const noexcept { return m_status_; }
  [[nodiscard]] bool IsRunning() const noexcept {
    return m_status_ == StepStatus::Running;
  }

  const StepToSupv& GetStep() const noexcept { return m_step_to_supv_; }
  StepToSupv& GetMutableStep() { return m_step_to_supv_; }

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

  void GotNewStatus(StepStatus new_status);

  // OOM monitoring methods
  void InitOomBaseline();
  bool EvaluateOomOnExit();

 private:
  std::atomic<StepStatus> m_status_{StepStatus::Configuring};
  crane::grpc::StepToD m_step_to_supv_;
  std::unique_ptr<cri::CriClient> m_cri_client_;
  std::unique_ptr<CforedClient> m_cfored_client_;
  std::unordered_map<task_id_t, std::unique_ptr<ITaskInstance>> m_task_map_;
};

// Task exiting info (after task is spawned)
struct TaskExitInfo {
  pid_t pid{0};
  bool is_terminated_by_signal{false};
  int value{0};
};

struct TaskFinalInfo {
  // Semantic cause that drives final status resolution.
  TaskFinalizeCause cause{TaskFinalizeCause::NORMAL};

  // Task exited after spawning. Raw exit info from waitpid/CRI events.
  std::optional<TaskExitInfo> raw_exit{std::nullopt};

  // Extra diagnostic information for status reporting.
  std::optional<std::string> reason;
};

class ITaskInstance {
 public:
  explicit ITaskInstance(StepInstance* step_inst, task_id_t task_id)
      : task_id(task_id), m_parent_step_inst_(step_inst) {}

  virtual ~ITaskInstance() = default;

  ITaskInstance(const ITaskInstance&) = delete;
  ITaskInstance(ITaskInstance&&) = delete;

  ITaskInstance& operator=(ITaskInstance&&) = delete;
  ITaskInstance& operator=(const ITaskInstance&) = delete;

  // Helper methods shared by all task instances
  const StepToSupv& GetParentStep() const {
    return m_parent_step_inst_->GetStep();
  }

  [[nodiscard]] TaskFinalInfo* GetFinalInfo() { return &m_final_info_; }

  // Interfaces must be implemented.
  virtual CraneErrCode Prepare() = 0;
  virtual CraneErrCode Spawn() = 0;
  virtual CraneErrCode Kill(int signum) = 0;
  virtual CraneErrCode Cleanup() = 0;

  virtual std::optional<TaskExecId> GetExecId() const = 0;

  // Set environment variables for the task instance. Can be overridden.
  virtual void InitEnvMap();

  task_id_t task_id{0};

 protected:
  // NOLINTBEGIN(misc-non-private-member-variables-in-classes)
  StepInstance* m_parent_step_inst_;
  EnvMap m_env_;
  TaskFinalInfo m_final_info_{};
  // NOLINTEND(misc-non-private-member-variables-in-classes)
};

// NOTE: Pod instance is only used in daemon step.
// Its creation and termination are DIFFERENT from other tasks.
class PodInstance : public ITaskInstance {
 public:
  explicit PodInstance(StepInstance* step_spec, task_id_t task_id)
      : ITaskInstance(step_spec, task_id) {}
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

  void SetPodLabels_(const crane::grpc::PodJobAdditionalMeta& pod_meta,
                     uid_t uid, job_id_t job_id, const std::string& job_name);
  void SetPodAnnotations_(const crane::grpc::PodJobAdditionalMeta& pod_meta,
                          uid_t uid, job_id_t job_id,
                          const std::string& job_name,
                          const std::string& hostname);

  CraneErrCode SetPodSandboxConfig_(
      const crane::grpc::PodJobAdditionalMeta& pod_meta);
  CraneErrCode PersistPodSandboxInfo_();
  CraneErrCode StopAndRemovePodSandbox_(std::string_view context);

  cri::api::PodSandboxConfig m_pod_config_;
  std::string m_pod_id_;

  std::filesystem::path m_log_dir_;
  std::filesystem::path m_config_file_;
  std::filesystem::path m_lock_file_;
};

class ContainerInstance : public ITaskInstance {
 public:
  explicit ContainerInstance(StepInstance* step_spec, task_id_t task_id)
      : ITaskInstance(step_spec, task_id) {}
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

  void SetContainerLabels_(uid_t uid, job_id_t job_id, step_id_t step_id,
                           const std::string& job_name,
                           const std::string& step_name);

  CraneErrCode LoadPodSandboxInfo_(
      const crane::grpc::PodJobAdditionalMeta* pod_meta);
  CraneErrCode SetContainerConfig_(
      const crane::grpc::ContainerJobAdditionalMeta& ca_meta,
      const crane::grpc::PodJobAdditionalMeta* pod_meta);

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

  std::string parsed_sh_script_path{};
  // Empty parsed pattern will redirect to /dev/null
  std::string parsed_input_file_pattern{};
  std::string parsed_output_file_pattern{};
  std::string parsed_error_file_pattern{};
};

struct BatchInstanceMeta final : ProcInstanceMeta {
  ~BatchInstanceMeta() override = default;
  BatchInstanceMeta() = default;
  BatchInstanceMeta(const BatchInstanceMeta&) = delete;
  BatchInstanceMeta(BatchInstanceMeta&&) = delete;
  BatchInstanceMeta& operator=(const BatchInstanceMeta&) = delete;
  BatchInstanceMeta& operator=(BatchInstanceMeta&&) = delete;
};

struct CrunInstanceMeta final : ProcInstanceMeta {
  ~CrunInstanceMeta() override = default;
  CrunInstanceMeta() = default;
  CrunInstanceMeta(const CrunInstanceMeta&) = delete;
  CrunInstanceMeta(CrunInstanceMeta&&) = delete;
  CrunInstanceMeta& operator=(const CrunInstanceMeta&) = delete;
  CrunInstanceMeta& operator=(CrunInstanceMeta&&) = delete;

  int stdin_write;
  int stdout_write;
  int stderr_write;
  int stdin_read;
  int stdout_read;
  int stderr_read;

  bool fwd_stdin;
  bool fwd_stdout;
  bool fwd_stderr;
};

class ProcInstance : public ITaskInstance {
 public:
  explicit ProcInstance(StepInstance* step_spec, task_id_t task_id)
      : ITaskInstance(step_spec, task_id) {}

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
  // returns: pair of (parsed file path, true need forward to cfored)
  std::pair<std::string, bool> CrunParseFilePattern_(
      const std::string& pattern) const;

  CraneExpected<pid_t> ForkCrunAndInitIOfd_();

  bool SetupCrunFwdAtParent_();
  void SetupCrunFwdAtChild_();

  void SetupChildProcCrunX11_();

  // Methods related to Batch only
  CraneErrCode SetChildProcBatchFd_();

  // Methods shared by Batch/Crun
  static void ResetChildProcSigHandler_();

  CraneErrCode SetChildProcProperty_();

  CraneErrCode SetChildProcEnv_() const;

  std::vector<std::string> GetChildProcExecArgv_() const;

  /* Perform file name substitutions
   * @return: pair of (parsed file path, true if file on local machine)
   */
  std::string ParseFilePathPattern_(const std::string& pattern,
                                    const std::string& cwd,
                                    bool is_batch_stdout,
                                    bool* is_local_file) const;

  // Process task has its own cgroup.
  std::unique_ptr<Common::CgroupInterface> m_task_cg_;

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

  void SupervisorFinishInit(StepStatus status);

  void Wait();
  // Shutdown supervisor asynchronously with given status, exit code and reason.
  // Status change will be sent only if daemon step.
  void ShutdownSupervisorAsync(
      crane::grpc::JobStatus new_status = StepStatus::Completed,
      uint32_t exit_code = 0, std::string reason = "");

  // NOLINTBEGIN(readability-identifier-naming)
  template <typename Duration>
  void AddTerminationTimer_(Duration duration, bool is_deadline) {
    auto termination_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handle->on<uvw::timer_event>(
        [this, is_deadline](const uvw::timer_event&, uvw::timer_handle& h) {
          EvStepTimerCb_(is_deadline);
        });
    termination_handle->start(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration),
        std::chrono::seconds(0));
    m_step_.termination_timer = termination_handle;
  }

  void AddTerminationTimer_(int64_t secs, bool is_deadline) {
    auto termination_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handle->on<uvw::timer_event>(
        [this, is_deadline](const uvw::timer_event&, uvw::timer_handle& h) {
          EvStepTimerCb_(is_deadline);
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
    m_step_.signal_timers.emplace_back(std::move(signal_handle));
  }

  void DelSignalTimers_() {
    for (const auto& signal_timer : m_step_.signal_timers) {
      signal_timer->close();
    }
    m_step_.signal_timers.clear();
  }

  CraneErrCode LaunchExecution_(ITaskInstance* task);
  // NOLINTEND(readability-identifier-naming)

  // Just like a SIGCHLD sig handler (implictly set by uvw).
  // Will be called in CliClient thread.
  void HandleCriEvents(std::vector<cri::api::ContainerStatus>&& events) {
    m_cri_event_queue_.enqueue_bulk(std::make_move_iterator(events.begin()),
                                    events.size());
    m_cri_event_handle_->send();
  }

  StepStatus GetStepStatus() const { return m_step_.GetStatus(); }
  StepInstance::TerminationStatus GetFinalTerminationStatus() const {
    return m_step_.final_termination_status;
  }

  void FinalizeTaskAsync(task_id_t task_id);
  void FinalizeTaskAsync(task_id_t task_id, TaskFinalizeCause cause,
                         std::optional<std::string> reason = std::nullopt);

  std::future<CraneErrCode> ExecuteStepAsync();

  std::future<CraneErrCode> ExecutePodInDaemonStepAsync();

  std::future<CraneExpected<EnvMap>> QueryStepEnvAsync();

  std::future<CraneErrCode> ChangeStepTimeConstraintAsync(
      std::optional<int64_t> time_limit, std::optional<int64_t> deadline_time);

  void TerminateStepAsync(bool mark_as_orphaned, TaskFinalizeCause cause);

  // A dedicated termination path for pod in daemon step.
  void TerminatePodInDaemonStepAsync();

  void CheckStatusAsync(crane::grpc::supervisor::CheckStatusReply* response);

  std::future<CraneErrCode> MigrateSshProcToCgroupAsync(pid_t pid);

  void SetAllowDaemonShutdown() { m_allow_daemon_shutdown_ = true; }

  void Shutdown() { m_supervisor_exit_ = true; }

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct ExecuteStepElem {
    std::promise<CraneErrCode> ok_prom;
  };

  struct StepTerminateQueueElem {
    TaskFinalizeCause cause{TaskFinalizeCause::NORMAL};
    bool mark_as_orphaned{false};
  };

  struct DaemonPodTerminateQueueElem {};

  struct ChangeStepTimeConstraintQueueElem {
    std::promise<CraneErrCode> ok_prom;
    std::optional<int64_t> time_limit{std::nullopt};
    std::optional<int64_t> deadline_time{std::nullopt};
  };
  void EvShutdownSupervisorCb_();

  // Handle pod creation for daemon step with container support.
  void EvExecuteDaemonPodCb_();
  // Handle daemon pod termination requests from ShutdownSupervisor.
  void EvCleanTerminateDaemonPodQueueCb_();

  // Process exited
  void EvSigchldCb_();
  void EvSigchldTimerCb_();
  void EvCleanSigchldQueueCb_();

  // Container exited
  void EvCleanCriEventQueueCb_();

  // Handle stopped tasks
  void EvCleanFinalizingTaskQueueCb_();

  // Handle step termination requests
  void EvStepTimerCb_(bool is_deadline);
  void EvCleanTerminateStepQueueCb_();
  void EvCleanChangeStepTimeConstraintQueueCb_();

  void EvGrpcExecuteStepCb_();
  void EvGrpcQueryStepEnvCb_();
  void EvGrpcCheckStatusCb_();
  void EvGrpcMigrateSshProcToCgroupCb_();

  // Task sink for finalized tasks. Task will be removed here.
  // Should called in uvw thread, otherwise data race may happen.
  void ResolveFinishedTask_(task_id_t task_id, StepStatus new_status,
                            uint32_t exit_code,
                            std::optional<std::string> reason);

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::async_handle> m_supervisor_finish_init_handle_;
  ConcurrentQueue<std::tuple<crane::grpc::JobStatus, uint32_t, std::string>>
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

  // Create pod for daemon step of container job.
  std::shared_ptr<uvw::async_handle> m_execute_daemon_pod_async_handle_;

  // Actively shutdown a running daemon pod.
  std::shared_ptr<uvw::async_handle> m_terminate_daemon_pod_async_handle_;
  ConcurrentQueue<DaemonPodTerminateQueueElem> m_daemon_pod_terminate_queue_;

  // Task is already terminated
  std::shared_ptr<uvw::async_handle> m_task_finalizing_async_handle_;
  ConcurrentQueue<task_id_t> m_task_finalizing_queue_;

  // Step is requested to be terminated
  std::shared_ptr<uvw::async_handle> m_terminate_step_async_handle_;
  std::shared_ptr<uvw::timer_handle> m_terminate_step_timer_handle_;
  ConcurrentQueue<StepTerminateQueueElem> m_step_terminate_queue_;

  std::shared_ptr<uvw::async_handle>
      m_change_step_time_constraint_async_handle_;
  std::shared_ptr<uvw::timer_handle>
      m_change_step_time_constraint_timer_handle_;
  ConcurrentQueue<ChangeStepTimeConstraintQueueElem>
      m_step_time_constraint_change_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_step_async_handle_;
  ConcurrentQueue<ExecuteStepElem> m_grpc_execute_step_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_query_step_env_async_handle_;
  ConcurrentQueue<std::promise<CraneExpected<EnvMap>>>
      m_grpc_query_step_env_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_check_status_async_handle_;
  ConcurrentQueue<std::promise<StepStatus>> m_grpc_check_status_queue_;

  std::shared_ptr<uvw::async_handle>
      m_grpc_migrate_ssh_proc_to_cgroup_async_handle_;
  ConcurrentQueue<std::pair<pid_t, std::promise<CraneErrCode>>>
      m_grpc_migrate_ssh_proc_to_cgroup_queue_;

  std::atomic_bool m_supervisor_exit_;
  std::thread m_uvw_thread_;

  // This is the gate for daemon step. Daemon step will not exit when all tasks
  // are finished till Shutdown is called in gRPC, where ActivelyShutdown is set
  // to true.
  std::atomic_bool m_allow_daemon_shutdown_{false};

  StepInstance m_step_;
  std::unordered_map<TaskExecId, task_id_t> m_exec_id_task_id_map_;
};

}  // namespace Craned::Supervisor

inline std::unique_ptr<Craned::Supervisor::TaskManager> g_task_mgr;
