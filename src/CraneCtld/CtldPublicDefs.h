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

#include "CtldPreCompiledHeader.h"
#include "protos/PublicDefs.pb.h"
// Precompiled header come first!

namespace Ctld {

using moodycamel::ConcurrentQueue;
using RegToken = google::protobuf::Timestamp;

using task_db_id_t = int64_t;
using step_db_id_t = int64_t;

// *****************************************************
// TaskScheduler Constants

constexpr uint32_t kTaskScheduleIntervalMs = 1000;
constexpr uint32_t kNodePerRpcWorker = 100;
constexpr std::size_t kMinRpcWorkerNum = 4;

// Clean TaskHoldTimerQueue when timeout or exceeding batch num
constexpr uint32_t kTaskHoldTimerTimeoutMs = 500;
constexpr uint32_t kTaskHoldTimerBatchNum = 1000;

// Clean CancelTaskQueue when timeout or exceeding batch num
constexpr uint32_t kCancelTaskTimeoutMs = 500;
constexpr uint32_t kCancelTaskBatchNum = 1000;

// Clean SubmitTaskQueue when timeout or exceeding batch num
constexpr uint32_t kSubmitTaskTimeoutMs = 500;
constexpr uint32_t kSubmitTaskBatchNum = 1000;

// Clean TaskStatusChangeQueue when timeout or exceeding batch num
constexpr uint32_t kTaskStatusChangeTimeoutMS = 100;
constexpr uint32_t kTaskStatusChangeBatchNum = 1000;

//*********************************************************

// CranedKeeper Constants
constexpr uint32_t kConcurrentStreamQuota = 3000;
constexpr uint32_t kCompletionQueueCapacity = 5000;
constexpr uint16_t kCompletionQueueConnectingTimeoutSeconds = 3;
constexpr uint16_t kCompletionQueueEstablishedTimeoutSeconds = 45;

constexpr uint16_t kProxiedCriReqTimeoutSeconds = 180;

// Since Unqlite has a limitation of about 900000 tasks per transaction,
// we use this value to set the batch size of one dequeue action on
// pending concurrent queue.
constexpr uint32_t kPendingQueueMaxSize = 900000;
constexpr uint32_t kMaxScheduledBatchSize = 200000;
constexpr uint32_t kDefaultScheduledBatchSize = 100000;

constexpr int64_t kCtldRpcTimeoutSeconds = 5;
constexpr bool kDefaultRejectTasksBeyondCapacity = false;
constexpr bool kDefaultJobFileOpenModeAppend = false;

struct Config {
  struct CraneCtldConf {
    uint32_t CranedTimeout;
    uint64_t CraneCtldMaxLogFileSize{kDefaultCraneCtldMaxLogFileSize};
    uint64_t CraneCtldMaxLogFileNum;
  };
  CraneCtldConf CtldConf;

  struct Node {
    uint32_t cpu;
    uint64_t memory_bytes;
    DedicatedResourceInNode dedicated_resource;
  };

  struct Partition {
    std::string nodelist_str;
    uint32_t priority{0};
    uint64_t default_mem_per_cpu{0};
    // optional, 0 indicates no limit
    uint64_t max_mem_per_cpu{0};
    std::unordered_set<std::string> nodes;
    std::unordered_set<std::string> allowed_accounts;
    std::unordered_set<std::string> denied_accounts;
  };

  struct CraneCtldListenConf {
    std::string CraneCtldListenAddr;
    std::string CraneCtldListenPort;
    std::string CraneCtldForInternalListenPort;

    struct TlsCertsConfig {
      bool Enabled{false};
      TlsCertificates InternalCerts;
      TlsCertificates ExternalCerts;
      std::unordered_set<std::string> AllowedNodes;
      std::string CaFilePath;
      std::string CaContent;
      std::string DomainSuffix;
    };
    TlsCertsConfig TlsConfig;
  };
  CraneCtldListenConf ListenConf;

  struct CranedListenConf {
    std::string CranedListenPort;
  };
  CranedListenConf CranedListenConf;

  struct VaultConfig {
    bool Enabled{false};
    std::string Addr;
    std::string Port;
    std::string Username;
    std::string Password;
    bool Tls;
    uint64_t ExpirationMinutes;
  };
  VaultConfig VaultConf;

  struct Priority {
    enum TypeEnum { Basic, MultiFactor };
    TypeEnum Type;

    // Config of multifactorial job priority sorting.
    bool FavorSmall{true};
    uint64_t MaxAge;
    uint32_t WeightAge;
    uint32_t WeightFairShare;
    uint32_t WeightJobSize;
    uint32_t WeightPartition;
    uint32_t WeightQOS;
  };

  struct PluginConfig {
    bool Enabled{false};
    std::string PlugindSockPath;
  };
  PluginConfig Plugin;

  struct ContainerConfig {
    bool Enabled{false};
  };
  ContainerConfig Container;

  bool CompressedRpc{};

  std::string CraneClusterName;
  uint32_t ConfigCrcVal;
  std::string CraneCtldDebugLevel;
  std::filesystem::path CraneCtldLogFile;

  std::string CraneEmbeddedDbBackend;
  std::filesystem::path CraneCtldDbPath;

  std::filesystem::path CraneBaseDir;
  std::filesystem::path CraneCtldMutexFilePath;

  bool CraneCtldForeground{};

  std::string Hostname;
  std::unordered_map<std::string, std::shared_ptr<Node>> Nodes;
  std::unordered_map<std::string, Partition> Partitions;
  std::string DefaultPartition;

  Priority PriorityConfig;

  // Database config
  std::string DbUser;
  std::string DbPassword;
  std::string DbHost;
  std::string DbPort;
  std::string DbRSName;
  std::string DbName;

  uint32_t PendingQueueMaxSize;
  uint32_t ScheduledBatchSize;
  bool RejectTasksBeyondCapacity{false};
  bool JobFileOpenModeAppend{false};
  bool IgnoreConfigInconsistency{false};
};

struct RunTimeStatus {
  std::atomic_bool srv_ready{false};
  std::shared_ptr<spdlog::async_logger> conn_logger;
  std::shared_ptr<spdlog::async_logger> db_logger;
};

}  // namespace Ctld

inline Ctld::Config g_config{};

inline Ctld::RunTimeStatus g_runtime_status{};

namespace Ctld {

/**
 * The static information on a Craned (the static part of CranedMeta). This
 * structure is provided when a new Craned node is to be registered in
 * CranedMetaContainer.
 */
struct CranedStaticMeta {
  std::string hostname;  // the hostname corresponds to the node index
  uint32_t port;

  std::list<std::string> partition_ids;  // Partitions to which
                                         // this craned belongs to
  ResourceInNode res;
};

struct CranedRemoteMeta {
  DedicatedResourceInNode dres_in_node;
  SystemRelInfo sys_rel_info;
  std::string craned_version;
  absl::Time craned_start_time;
  absl::Time system_boot_time;

  std::vector<crane::grpc::NetworkInterface> network_interfaces;

  CranedRemoteMeta() = default;
  explicit CranedRemoteMeta(const crane::grpc::CranedRemoteMeta& grpc_meta);
};

/**
 * Represent the runtime status on a Craned node.
 * A Node is uniquely identified by (partition id, node index).
 */
struct CranedMeta {
  CranedStaticMeta static_meta;
  CranedRemoteMeta remote_meta;

  bool alive{false};
  crane::grpc::CranedPowerState power_state{
      crane::grpc::CranedPowerState::CRANE_POWER_IDLE};

  // total = avail + in-use
  ResourceInNode res_total;  // A copy of res in CranedStaticMeta,
  ResourceInNode res_avail;
  ResourceInNode res_in_use;

  bool drain{false};
  std::string state_reason;
  absl::Time last_busy_time;

  absl::flat_hash_map<task_id_t, ResourceInNode> rn_task_res_map;

  absl::flat_hash_map<ResvId, std::pair<absl::Time, absl::Time>>
      resv_in_node_map;
};

struct ResvMeta {
  ResvId name;
  PartitionId part_id;
  absl::Time start_time;
  absl::Time end_time;

  bool accounts_black_list{false};
  bool users_black_list{false};
  std::unordered_set<std::string> accounts;
  std::unordered_set<std::string> users;

  absl::flat_hash_set<CranedId> craned_ids;
  ResourceV2 res_total;
  ResourceV2 res_avail;
  absl::flat_hash_map<task_id_t, ResourceV2> rn_job_res_map;
  absl::flat_hash_set<task_id_t> pd_job_ids;
};

struct PartitionGlobalMeta {
  // total = avail + in-use
  ResourceView res_total;
  ResourceView res_avail;
  ResourceView res_in_use;

  // Include resources in unavailable nodes.
  ResourceView res_total_inc_dead;

  std::string name;
  std::string nodelist_str;

  std::unordered_set<std::string> allowed_accounts;
  std::unordered_set<std::string> denied_accounts;

  uint32_t node_cnt;
  uint32_t alive_craned_cnt;
};

struct PartitionMeta {
  PartitionGlobalMeta partition_global_meta;
  std::unordered_set<CranedId> craned_ids;
};

struct InteractiveMetaInTask {
  InteractiveMetaInTask() = default;
  InteractiveMetaInTask& operator=(const InteractiveMetaInTask& other) {
    interactive_type = other.interactive_type;
    cb_task_res_allocated = other.cb_task_res_allocated;
    cb_task_completed = other.cb_task_completed;
    cb_task_cancel = other.cb_task_cancel;
    has_been_cancelled_on_front_end =
        other.has_been_cancelled_on_front_end.load();
    has_been_terminated_on_craned = other.has_been_terminated_on_craned.load();
    return *this;
  }

  InteractiveMetaInTask(const InteractiveMetaInTask& other) {
    interactive_type = other.interactive_type;
    cb_task_res_allocated = other.cb_task_res_allocated;
    cb_task_completed = other.cb_task_completed;
    cb_task_cancel = other.cb_task_cancel;
    has_been_cancelled_on_front_end =
        other.has_been_cancelled_on_front_end.load();
    has_been_terminated_on_craned = other.has_been_terminated_on_craned.load();
  }
  crane::grpc::InteractiveTaskType interactive_type;

  std::function<void(task_id_t, std::string const&,
                     std::vector<std::string> const&)>
      cb_task_res_allocated;

  std::function<void(task_id_t, bool)> cb_task_completed;

  // This will ask front end like crun/calloc to exit
  std::function<void(task_id_t)> cb_task_cancel;

  // ccancel for an interactive CALLOC task should call the front end to kill
  // the user's shell, let Cfored to inform CraneCtld of task completion rather
  // than directly sending TerminateTask to its craned node.
  //
  // However, when TIMEOUT event on its craned node happens, Cranectld should
  // also send TaskCancelRequest to the front end. So we need a flag
  // ` has_been_cancelled_on_front_end` to record whether the front end for the
  // task has been sent a TaskCancelRequest and a flag
  // `has_been_terminated_on_craned` to record whether the task resource
  // (cgroup) has been destroyed on its craned node.
  //
  // If `has_been_cancelled` is true, no more TaskCancelRequest should be sent
  // to the front end. It is set when the user runs ccancel command or the
  // timeout event has been triggered on the craned node.
  //
  // If `has_been_terminated_on_craned` is true, no more TerminateTask RPC
  // should be sent to its craned node. It is set when the timeout event is
  // triggered on the craned node or a TaskTerminate RPC has been sent
  // (triggered by either normal shell exit or ccancel).
  std::atomic<bool> has_been_cancelled_on_front_end{false};
  std::atomic<bool> has_been_terminated_on_craned{false};
};

struct BatchMetaInTask {
  std::string sh_script;
  std::string interpreter;
  std::string output_file_pattern;
  std::string error_file_pattern;
};

struct ContainerMetaInTask {
  struct ImageInfo {
    std::string image;
    std::string username;
    std::string password;
    std::string server_address;
    std::string pull_policy;
  };

  ImageInfo image_info{};

  std::string name;
  std::unordered_map<std::string, std::string> labels;
  std::unordered_map<std::string, std::string> annotations;

  std::string command;
  std::vector<std::string> args;
  std::string workdir;
  std::unordered_map<std::string, std::string> env;

  bool detached{true};
  bool tty{false};
  bool stdin{false};
  bool stdin_once{false};

  bool userns{true};
  uid_t run_as_user{0};
  gid_t run_as_group{0};

  std::unordered_map<std::string, std::string> mounts;
  std::unordered_map<uint32_t, uint32_t> port_mappings;

 public:
  ContainerMetaInTask() = default;

  explicit ContainerMetaInTask(
      const crane::grpc::ContainerTaskAdditionalMeta& rhs);
  explicit operator crane::grpc::ContainerTaskAdditionalMeta() const;
};

struct TaskInCtld;
struct StepInCtld;
using StepInteractiveMeta = InteractiveMetaInTask;

struct StepStatusChangeContext {
  /* ------------------------------ steps ------------------------------ */

  // Step to alloc
  std::unordered_map<CranedId, std::vector<crane::grpc::StepToD>>
      craned_step_alloc_map;

  // Steps will execute on craned
  std::unordered_map<CranedId,
                     std::unordered_map<job_id_t, std::set<step_id_t>>>
      craned_step_free_map;
  // Steps will execute on craned
  std::unordered_map<CranedId,
                     std::unordered_map<job_id_t, std::set<step_id_t>>>
      craned_step_exec_map;
  // Error steps to terminate with orphaned status
  std::unordered_map<CranedId,
                     std::unordered_map<job_id_t, std::set<step_id_t>>>
      craned_orphaned_steps{};
  // Common step to cancel, caused by a finished primary step
  std::unordered_map<CranedId,
                     std::unordered_map<job_id_t, std::set<step_id_t>>>
      craned_cancel_steps{};
  // Steps will update in embeddedDb
  std::unordered_set<StepInCtld*> rn_step_raw_ptrs;
  std::unordered_set<StepInCtld*> step_raw_ptrs;
  // Carry the ownership of StepInCtld for completed step automatic
  // destruction.
  std::unordered_set<std::unique_ptr<StepInCtld>> step_ptrs;

  /* ------------------------------ jobs ------------------------------ */

  std::unordered_map<CranedId, std::vector<job_id_t>> craned_jobs_to_free;
  // Jobs will update in embedded db
  std::unordered_set<TaskInCtld*> rn_job_raw_ptrs{};
  // Carry the ownership of TaskInCtld for automatic destruction.
  std::unordered_set<std::unique_ptr<TaskInCtld>> job_ptrs;
  // Ended jobs will transfer from embedded db to mongodb
  std::unordered_set<TaskInCtld*> job_raw_ptrs;
};

// Abstract interface of all the steps in Ctld.
struct StepInCtld {
 public:
  crane::grpc::TaskType type;

  TaskInCtld* job;
  job_id_t job_id;

  uid_t uid;
  std::vector<gid_t> gids;
  std::string name;

  uint32_t ntasks_per_node{0};
  cpu_t cpus_per_task{0.0F};

  bool requeue_if_failed{false};
  bool get_user_env{false};
  std::unordered_map<std::string, std::string> env;

  absl::Duration time_limit;
  ResourceView requested_node_res_view;
  uint32_t node_num{0};
  std::unordered_set<std::string> included_nodes;
  std::unordered_set<std::string> excluded_nodes;

  // TODO: Find somewhere else to put this field?
  std::optional<ContainerMetaInTask> container_meta;

 protected:
  /* ------------- [2] -------------
   * Fields that won't change after this task is accepted.
   * Also, these fields are persisted on the disk.
   * ------------------------------- */
  step_db_id_t m_step_db_id_{0};
  step_id_t m_step_id_{0};
  crane::grpc::StepType step_type{crane::grpc::StepType::INVALID};

  /* Fields that may change at run time.*/
  std::int32_t m_requeue_count_{0};
  ResourceV2 m_allocated_res_;

  std::unordered_set<CranedId> m_craned_ids_;
  std::unordered_set<CranedId> m_execute_nodes_;
  std::unordered_set<CranedId> m_configuring_nodes_;
  std::unordered_set<CranedId> m_running_nodes_;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time m_submit_time_;
  absl::Time m_start_time_;
  absl::Time m_end_time_;

  crane::grpc::TaskStatus m_error_status{crane::grpc::TaskStatus::Invalid};
  uint32_t m_error_exit_code_{0u};
  crane::grpc::TaskStatus m_status_{crane::grpc::TaskStatus::Invalid};
  uint32_t m_exit_code_{};

  bool m_held_{false};

 private:
  /* ------ duplicate of the fields [1] above just for convenience ----- */
  crane::grpc::StepToCtld m_step_to_ctld_;

  /* ------ duplicate of the fields [2][3] above just for convenience ----- */
  crane::grpc::RuntimeAttrOfStep m_runtime_attr_;

 public:
  virtual ~StepInCtld() = default;

  void SetStepType(crane::grpc::StepType type);
  crane::grpc::StepType StepType() const;

  void SetStepToCtld(const crane::grpc::StepToCtld& step_to_ctld) {
    m_step_to_ctld_ = step_to_ctld;
  }
  const crane::grpc::StepToCtld& StepToCtld() const;
  crane::grpc::StepToCtld* MutableStepToCtld();

  void SetStepId(step_id_t id);
  step_id_t StepId() const { return m_step_id_; }

  void SetStepDbId(step_db_id_t id);
  step_db_id_t StepDbId() const { return m_step_db_id_; }

  void SetRequeueCount(std::int32_t count);
  std::int32_t RequeueCount() const { return m_requeue_count_; }

  void SetAllocatedRes(const ResourceV2& res);
  ResourceV2 AllocatedRes() const { return m_allocated_res_; }

  void SetCranedIds(const std::unordered_set<CranedId>& craned_list);
  const std::unordered_set<CranedId>& CranedIds() const {
    return m_craned_ids_;
  }

  void SetExecutionNodes(const std::unordered_set<CranedId>& nodes);
  std::unordered_set<CranedId> ExecutionNodes() const {
    return m_execute_nodes_;
  }

  void SetConfiguringNodes(const std::unordered_set<CranedId>& nodes);
  void NodeConfigured(const CranedId& node);
  bool AllNodesConfigured() const { return m_configuring_nodes_.empty(); }

  void SetRunningNodes(const std::unordered_set<CranedId>& nodes);
  std::unordered_set<CranedId> RunningNodes() const { return m_running_nodes_; }
  void StepOnNodeFinish(const CranedId& node);
  bool AllNodesFinished() const { return m_running_nodes_.empty(); }

  void SetSubmitTime(absl::Time submit_time);
  absl::Time SubmitTime() const { return m_submit_time_; }

  void SetStartTime(absl::Time start_time);
  absl::Time StartTime() const { return m_start_time_; }

  void SetEndTime(absl::Time end_time);
  absl::Time EndTime() const { return m_end_time_; }

  void SetErrorStatus(crane::grpc::TaskStatus failed_status);
  std::optional<crane::grpc::TaskStatus> PrevErrorStatus() {
    if (m_error_status == crane::grpc::TaskStatus::Invalid) {
      return std::nullopt;
    }
    return m_error_status;
  }
  void SetErrorExitCode(uint32_t exit_code);
  uint32_t PrevErrorExitCode() { return m_error_exit_code_; }
  void SetStatus(crane::grpc::TaskStatus new_status);
  crane::grpc::TaskStatus Status() const { return m_status_; }

  void SetExitCode(uint32_t exit_code);
  uint32_t ExitCode() const { return m_exit_code_; }

  void SetHeld(bool held);
  bool Held() const { return m_held_; }

  crane::grpc::RuntimeAttrOfStep const& RuntimeAttr() const {
    return m_runtime_attr_;
  }

  // Interface methods
  [[nodiscard]] virtual crane::grpc::StepToD GetStepToD(
      const CranedId& craned_id) const = 0;
  virtual void RecoverFromDb(const TaskInCtld& job,
                             crane::grpc::StepInEmbeddedDb const& step_in_db);
};

struct DaemonStepInCtld : StepInCtld {
  std::string partition;
  std::string account;
  std::string qos;

  ~DaemonStepInCtld() override = default;

  void InitFromJob(const TaskInCtld& job);
  [[nodiscard]] crane::grpc::JobToD GetJobToD(const CranedId& craned_id) const;

  // Interface methods
  [[nodiscard]] crane::grpc::StepToD GetStepToD(
      const CranedId& craned_id) const override;

  std::optional<std::pair<crane::grpc::TaskStatus, uint32_t /*exit code*/>>
  StepStatusChange(crane::grpc::TaskStatus new_status, uint32_t exit_code,
                   const std::string& reason, const CranedId& craned_id,
                   StepStatusChangeContext* context);

  void RecoverFromDb(const TaskInCtld& job,
                     const crane::grpc::StepInEmbeddedDb& step_in_db) override;
};

struct CommonStepInCtld : StepInCtld {
  /* -------- [1] Fields that are set at the submission time. ------- */
  std::string cmd_line;
  std::string cwd;
  std::string extra_attr;

  // TODO: fill this field
  std::optional<StepInteractiveMeta> ia_meta;

  /* -----------
   * Fields that may change at run time.
   * However, these fields are NOT persisted on the disk.
   * ----------- */

  std::string allocated_craneds_regex;
  std::string pending_reason;

  ~CommonStepInCtld() override = default;

  void InitPrimaryStepFromJob(const TaskInCtld& job);
  bool IsPrimaryStep() const noexcept;
  [[nodiscard]] bool SetFieldsByStepToCtld(
      const crane::grpc::StepToCtld& step_to_ctld);

  // Interface methods

  [[nodiscard]] crane::grpc::StepToD GetStepToD(
      const CranedId& craned_id) const override;

  void StepStatusChange(crane::grpc::TaskStatus new_status, uint32_t exit_code,
                        const std::string& reason, const CranedId& craned_id,
                        StepStatusChangeContext* context);
  void RecoverFromDb(const TaskInCtld& job,
                     const crane::grpc::StepInEmbeddedDb& step_in_db) override;
};

struct TaskInCtld {
  /* -------- [1] Fields that are set at the submission time. ------- */
  absl::Duration time_limit;

  PartitionId partition_id;

  // Set by user request and probably include untyped devices.
  ResourceView requested_node_res_view;

  crane::grpc::TaskType type;

  uid_t uid;
  gid_t gid;
  std::string account;
  std::string name;
  std::string qos;

  uint32_t node_num{0};
  uint32_t ntasks_per_node{0};
  cpu_t cpus_per_task{0.0F};

  std::unordered_set<std::string> included_nodes;
  std::unordered_set<std::string> excluded_nodes;

  bool requeue_if_failed{false};
  bool get_user_env{false};

  std::string cmd_line;
  std::unordered_map<std::string, std::string> env;
  std::string cwd;

  std::string extra_attr;

  std::variant<InteractiveMetaInTask, BatchMetaInTask, ContainerMetaInTask>
      meta;

  std::string reservation;
  absl::Time begin_time{absl::InfinitePast()};

  bool exclusive{false};

 private:
  /* ------------- [2] -------------
   * Fields that won't change after this task is accepted.
   * Also, these fields are persisted on the disk.
   * ------------------------------- */
  task_id_t task_id{0};
  task_db_id_t task_db_id{0};
  std::string username;

  /* ----------- [3] ----------------
   * Fields that may change at run time.
   * Also, these fields are persisted on the disk.
   * -------------------------------- */
  int32_t requeue_count{0};
  std::vector<CranedId> craned_ids;
  crane::grpc::TaskStatus primary_status{};
  crane::grpc::TaskStatus status{};
  uint32_t primary_exit_code{};
  uint32_t exit_code{};
  bool held{false};
  // DAEMON step
  std::unique_ptr<DaemonStepInCtld> m_daemon_step_;
  // BATCH or INTERACTIVE step
  std::unique_ptr<CommonStepInCtld> m_primary_step_;
  // COMMON steps
  std::unordered_map<step_id_t, std::unique_ptr<CommonStepInCtld>> m_steps_;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time submit_time;
  absl::Time start_time;
  absl::Time end_time;

  // persisted for querying priority of running tasks
  double cached_priority{0.0};

  // Might change at each scheduling cycle.
  ResourceV2 allocated_res;

  /* ------ duplicate of the fields [1] above just for convenience ----- */
  crane::grpc::TaskToCtld task_to_ctld;

  /* ------ duplicate of the fields [2][3] above just for convenience ----- */
  crane::grpc::RuntimeAttrOfTask runtime_attr;

 public:
  /* -----------
   * Fields that will not change at run time.
   * However, these fields are NOT persisted on the disk.
   * These fields are cached for performance purpose.
   * ----------- */
  // set in SetFieldsByTaskToCtld from uid
  std::unique_ptr<PasswordEntry> password_entry;

  // Set in TaskScheduler->AcquireAttributes()
  uint32_t partition_priority{0};
  uint32_t qos_priority{0};

  /* -----------
   * Fields that may change at run time.
   * However, these fields are NOT persisted on the disk.
   * ----------- */

  // Aggregated from resources of all nodes.
  // Might change at each scheduling cycle.
  ResourceView allocated_res_view;

  uint32_t nodes_alloc;
  std::vector<CranedId> executing_craned_ids;
  std::string allocated_craneds_regex;
  std::string pending_reason;

  double mandated_priority{0.0};

  // Helper function
 public:
  // =================== Get Attr ==================
  bool IsBatch() const { return type == crane::grpc::Batch; }
  bool IsInteractive() const { return type == crane::grpc::Interactive; }
  bool IsCalloc() const {
    return type == crane::grpc::TaskType::Interactive &&
           task_to_ctld.interactive_meta().interactive_type() ==
               crane::grpc::InteractiveTaskType::Calloc;
  }
  bool IsContainer() const { return type == crane::grpc::Container; }
  bool IsX11() const;
  bool IsX11WithPty() const;
  bool ShouldLaunchOnAllNodes() const;

  crane::grpc::TaskToCtld const& TaskToCtld() const { return task_to_ctld; }
  crane::grpc::TaskToCtld* MutableTaskToCtld() { return &task_to_ctld; }

  crane::grpc::RuntimeAttrOfTask const& RuntimeAttr() { return runtime_attr; }

  // =================== Setter/Getter ===================

  void SetTaskId(task_id_t id);
  task_id_t TaskId() const { return task_id; }

  void SetTaskDbId(task_db_id_t id);
  task_id_t TaskDbId() const { return task_db_id; }

  void SetUsername(std::string const& val);
  std::string const& Username() const { return username; }

  void SetCranedIds(std::vector<CranedId>&& val);
  std::vector<CranedId> const& CranedIds() const { return craned_ids; }
  void CranedIdsClear();
  void CranedIdsAdd(CranedId const& i);

  void SetPrimaryStepStatus(crane::grpc::TaskStatus val);
  crane::grpc::TaskStatus PrimaryStepStatus() const { return primary_status; }

  void SetStatus(crane::grpc::TaskStatus val);
  crane::grpc::TaskStatus Status() const { return status; }

  void SetPrimaryStepExitCode(uint32_t val);
  uint32_t PrimaryStepExitCode() const { return primary_exit_code; }

  void SetExitCode(uint32_t val);
  uint32_t ExitCode() const { return exit_code; }

  void SetSubmitTime(absl::Time const& val);
  void SetSubmitTimeByUnixSecond(uint64_t val);
  absl::Time const& SubmitTime() const { return submit_time; }
  int64_t SubmitTimeInUnixSecond() const { return ToUnixSeconds(submit_time); }

  void SetStartTime(absl::Time const& val);
  void SetStartTimeByUnixSecond(uint64_t val);
  absl::Time const& StartTime() const { return start_time; }
  int64_t StartTimeInUnixSecond() const { return ToUnixSeconds(start_time); }

  void SetEndTime(absl::Time const& val);
  void SetEndTimeByUnixSecond(uint64_t val);
  absl::Time const& EndTime() const { return end_time; }
  int64_t EndTimeInUnixSecond() const { return ToUnixSeconds(end_time); }

  void SetHeld(bool val);
  bool const& Held() const { return held; }

  void SetDaemonStep(std::unique_ptr<DaemonStepInCtld>&& step) {
    CRANE_ASSERT(!m_daemon_step_);
    m_daemon_step_ = std::move(step);
  }
  DaemonStepInCtld* DaemonStep() const { return m_daemon_step_.get(); }
  DaemonStepInCtld* ReleaseDaemonStep() { return m_daemon_step_.release(); }

  void SetPrimaryStep(std::unique_ptr<CommonStepInCtld>&& step) {
    m_primary_step_ = std::move(step);
  }
  CommonStepInCtld* PrimaryStep() const { return m_primary_step_.get(); }
  CommonStepInCtld* ReleasePrimaryStep() { return m_primary_step_.release(); }

  void AddStep(std::unique_ptr<CommonStepInCtld>&& step) {
    // Common step can only be interactive step started by crun.
    CRANE_ASSERT(step->type == crane::grpc::TaskType::Interactive);
    CRANE_ASSERT(step->StepToCtld().interactive_meta().interactive_type() ==
                 crane::grpc::InteractiveTaskType::Crun);
    m_steps_.emplace(step->StepId(), std::move(step));
  }

  CommonStepInCtld* GetStep(step_id_t step) const {
    if (m_primary_step_ && m_primary_step_->StepId() == step) {
      return m_primary_step_.get();
    }
    if (m_steps_.contains(step)) {
      return m_steps_.at(step).get();
    }
    return nullptr;
  }
  std::unique_ptr<CommonStepInCtld> EraseStep(step_id_t step_id) {
    if (m_steps_.contains(step_id)) {
      auto step =
          std::unique_ptr<CommonStepInCtld>(m_steps_.at(step_id).release());
      m_steps_.erase(step_id);
      return step;
    }
    return nullptr;
  }
  std::unordered_map<step_id_t, std::unique_ptr<CommonStepInCtld>> const&
  Steps() {
    return m_steps_;
  }

  void SetCachedPriority(const double val);
  double CachedPriority() const { return cached_priority; }

  void SetAllocatedRes(ResourceV2&& val);
  ResourceV2 const& AllocatedRes() const { return allocated_res; }

  void SetFieldsByTaskToCtld(crane::grpc::TaskToCtld const& val);

  void SetFieldsByRuntimeAttr(crane::grpc::RuntimeAttrOfTask const& val);

  // Helper function to set the fields of TaskInfo using info in
  // TaskInCtld. Note that mutable_elapsed_time() is not set here for
  // performance reason. The caller should set it manually.
  void SetFieldsOfTaskInfo(crane::grpc::TaskInfo* task_info);
};

struct Qos {
  bool deleted = false;
  std::string name;
  std::string description;
  uint32_t reference_count = 0;
  uint32_t priority;
  uint32_t max_jobs_per_user;
  uint32_t max_running_tasks_per_user;
  absl::Duration max_time_limit_per_task;
  uint32_t max_cpus_per_user;
  uint32_t max_cpus_per_account;

  static constexpr const char* FieldStringOfDeleted() { return "deleted"; }
  static constexpr const char* FieldStringOfName() { return "name"; }
  static constexpr const char* FieldStringOfDescription() {
    return "description";
  }
  static constexpr const char* FieldStringOfReferenceCount() {
    return "reference_count";
  }
  static constexpr const char* FieldStringOfPriority() { return "priority"; }
  static constexpr const char* FieldStringOfMaxJobsPerUser() {
    return "max_jobs_per_user";
  }
  static constexpr const char* FieldStringOfMaxTimeLimitPerTask() {
    return "max_time_limit_per_task";
  }
  static constexpr const char* FieldStringOfMaxCpusPerUser() {
    return "max_cpus_per_user";
  }
  static constexpr const char* FieldStringOfMaxCpusPerAccount() {
    return "max_cpus_per_account";
  }

  std::string QosToString() const {
    return fmt::format(
        "name: {}, description: {}, reference_count: {}, priority: {}, "
        "max_jobs_per_user: {}, max_running_tasks_per_user: {}, "
        "max_time_limit_per_task: {}, max_cpus_per_user: {}, "
        "max_cpus_per_account: {}",
        name, description, reference_count, priority, max_jobs_per_user,
        max_running_tasks_per_user,
        absl::FormatDuration(max_time_limit_per_task), max_cpus_per_user,
        max_cpus_per_account);
  }
};

struct Account {
  bool deleted = false;
  bool blocked = false;
  std::string name;
  std::string description;
  std::list<std::string> users;
  std::list<std::string> child_accounts;
  std::string parent_account;
  std::list<std::string> allowed_partition;
  std::string default_qos;
  std::list<std::string> allowed_qos_list;
  std::list<std::string> coordinators;

  std::string AccountToString() const {
    return fmt::format(
        "name: {}, description: {}, blocked: {}, parent_account: {}, "
        "default_qos: {}, users: [{}], child_accounts: [{}], "
        "allowed_partition: [{}], allowed_qos_list: [{}], coordinators: [{}]",
        name, description, blocked, parent_account, default_qos,
        fmt::join(users, ", "), fmt::join(child_accounts, ", "),
        fmt::join(allowed_partition, ", "), fmt::join(allowed_qos_list, ", "),
        fmt::join(coordinators, ", "));
  }
};

struct User {
  // Root corresponds to root user in Linux System and have any permission over
  // the whole system.
  // Admin and Operator are created for just compatability of existing systems
  // like Slurm.
  // Root, Admin and Operator actually have no difference when controlling Crane
  // system in the current stage.
  // However, Crane system follows the rule that the users with the same admin
  // level can't control each other, but users with higher level can control
  // users with lower level. Thus, Root level is created for the integrity of
  // Crane system to guarantee that there will always a superuser to control all
  // the administrator of the whole system.
  enum AdminLevel { None, Operator, Admin, Root };

  using PartToAllowedQosMap = std::unordered_map<
      std::string /*partition name*/,
      std::pair<std::string /*default qos*/,
                std::list<std::string> /*allowed qos list*/>>;

  struct AttrsInAccount {
    PartToAllowedQosMap allowed_partition_qos_map;
    bool blocked;
  };

  /* Map<account name, item> */
  using AccountToAttrsMap = std::unordered_map<std::string, AttrsInAccount>;

  bool deleted = false;
  uid_t uid;
  std::string name;
  std::string default_account;
  AccountToAttrsMap account_to_attrs_map;
  std::list<std::string> coordinator_accounts;
  AdminLevel admin_level;
  std::string cert_number;

  std::string UserToString() const {
    std::string accounts_info = "{";
    for (auto it = account_to_attrs_map.begin();
         it != account_to_attrs_map.end(); ++it) {
      if (it != account_to_attrs_map.begin()) {
        accounts_info += ", ";
      }

      std::string partition_qos_info = "{";
      for (auto pit = it->second.allowed_partition_qos_map.begin();
           pit != it->second.allowed_partition_qos_map.end(); ++pit) {
        if (pit != it->second.allowed_partition_qos_map.begin()) {
          partition_qos_info += ", ";
        }
        partition_qos_info +=
            fmt::format("{}: default={}, allowed=[{}]", pit->first,
                        pit->second.first, fmt::join(pit->second.second, ", "));
      }
      partition_qos_info += "}";

      accounts_info +=
          fmt::format("{}: {{blocked: {}, partition_qos: {}}}", it->first,
                      it->second.blocked, partition_qos_info);
    }
    accounts_info += "}";

    return fmt::format(
        "uid: {}, name: {}, default_account: {}, admin_level: {}, "
        "coordinator_accounts: [{}], account_attributes: {}",
        uid, name, default_account, static_cast<int>(admin_level),
        fmt::join(coordinator_accounts, ", "), accounts_info);
  }

  static const char* AdminLevelToString(AdminLevel level) {
    switch (level) {
    case None:
      return "None";
    case Operator:
      return "Operator";
    case Admin:
      return "Admin";
    case Root:
      return "Root";
    default:
      return "Unknown";
    }
  }
};

inline bool CheckIfTimeLimitSecIsValid(int64_t sec) {
  return sec >= kTaskMinTimeLimitSec && sec <= kTaskMaxTimeLimitSec;
}

inline bool CheckIfTimeLimitIsValid(absl::Duration d) {
  int64_t sec = ToInt64Seconds(d);
  return CheckIfTimeLimitSecIsValid(sec);
}

struct QosResource {
  ResourceView resource;
  uint32_t jobs_per_user;
};

// Transaction
struct Txn {
  uint64_t creation_time;
  std::string actor;
  std::string target;
  crane::grpc::TxnAction action;
  std::string info;
};

}  // namespace Ctld

inline std::unique_ptr<BS::thread_pool> g_thread_pool;