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
// Precompiled header come first!

namespace Ctld {

using moodycamel::ConcurrentQueue;
using RegToken = google::protobuf::Timestamp;

using task_db_id_t = int64_t;

// *****************************************************
// TaskScheduler Constants

constexpr uint32_t kTaskScheduleIntervalMs = 1000;

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
constexpr uint32_t kTaskStatusChangeTimeoutMS = 500;
constexpr uint32_t kTaskStatusChangeBatchNum = 1000;

//*********************************************************

// CranedKeeper Constants
constexpr uint32_t kConcurrentStreamQuota = 3000;
constexpr uint32_t kCompletionQueueCapacity = 5000;
constexpr uint16_t kCompletionQueueConnectingTimeoutSeconds = 3;
constexpr uint16_t kCompletionQueueEstablishedTimeoutSeconds = 45;

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
  struct Node {
    uint32_t cpu;
    uint64_t memory_bytes;
    DedicatedResourceInNode dedicated_resource;
  };

  struct Partition {
    std::string nodelist_str;
    uint32_t priority;
    uint64_t default_mem_per_cpu;
    // optional, 0 indicates no limit
    uint64_t max_mem_per_cpu;
    std::unordered_set<std::string> nodes;
    std::unordered_set<std::string> allowed_accounts;
    std::unordered_set<std::string> denied_accounts;
  };

  struct CraneCtldListenConf {
    std::string CraneCtldListenAddr;
    std::string CraneCtldListenPort;
    std::string CraneCtldForInternalListenPort;

    bool UseTls{false};
    TlsCertificates Certs;
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

  bool CompressedRpc{};

  std::string CraneClusterName;
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

  // Plugin config
  PluginConfig Plugin;

  uint32_t PendingQueueMaxSize;
  uint32_t ScheduledBatchSize;
  bool RejectTasksBeyondCapacity{false};
  bool JobFileOpenModeAppend{false};
};

struct RunTimeStatus {
  std::atomic_bool srv_ready{false};
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

  // *********************************************************
  // TODO: Refactor as Unused LogicalPartition
  // Store the information of the slices of allocated resource.
  // One task id owns one shard of allocated resource.
  absl::flat_hash_map<task_id_t, ResourceInNode> rn_task_res_map;
  // *********************************************************

  // *********************************************************
  // TODO: Refactor as Reservation LogicalPartition (Might be pointer)
  struct ResvInNode {
    absl::Time start_time;
    absl::Time end_time;
    ResourceInNode res_total;
  };

  // Store total resource of each reservation.
  absl::flat_hash_map<ResvId, ResvInNode> resv_in_node_map;
  // **********************************************************
};

struct LogicalPartition {
  absl::Time start_time;
  absl::Time end_time;

  ResourceV2 res_total;
  ResourceV2 res_avail;
  ResourceV2 res_in_use;

  std::list<CranedId> craned_ids;

  struct RnTaskRes {
    absl::Time end_time;  // sync with TaskInCtld
    ResourceV2 resources;
  };

  absl::flat_hash_map<task_id_t, RnTaskRes> rn_task_res_map;
};

struct ResvMeta {
  ResvId name;
  PartitionId part_id;
  LogicalPartition logical_part;

  bool accounts_black_list{false};
  bool users_black_list{false};
  std::unordered_set<std::string> accounts;
  std::unordered_set<std::string> users;
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
  crane::grpc::InteractiveTaskType interactive_type;

  std::function<void(task_id_t, std::string const&,
                     std::list<std::string> const&)>
      cb_task_res_allocated;

  std::function<void(task_id_t, bool)> cb_task_completed;

  // This will ask front end like crun/calloc to exit
  std::function<void(task_id_t)> cb_task_cancel;

  // only for crun.
  size_t status_change_cnt{0};

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
  cpu_t cpus_per_task{0};

  std::unordered_set<std::string> included_nodes;
  std::unordered_set<std::string> excluded_nodes;

  bool requeue_if_failed{false};
  bool get_user_env{false};

  std::string cmd_line;
  std::unordered_map<std::string, std::string> env;
  std::string cwd;
  std::string container;

  std::string extra_attr;

  std::variant<InteractiveMetaInTask, BatchMetaInTask> meta;

  std::string reservation;

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
  std::list<CranedId> craned_ids;
  crane::grpc::TaskStatus status{};
  uint32_t exit_code{};
  bool held{false};

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

  void SetCranedIds(std::list<CranedId>&& val);
  std::list<CranedId> const& CranedIds() const { return craned_ids; }
  void CranedIdsClear();
  void CranedIdsAdd(CranedId const& i);

  void SetStatus(crane::grpc::TaskStatus val);
  crane::grpc::TaskStatus Status() const { return status; }

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

  crane::grpc::TaskToD GetTaskToD(const CranedId& craned_id) const;

  crane::grpc::JobToD GetJobToD(const CranedId& craned_id) const;
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

}  // namespace Ctld

inline std::unique_ptr<BS::thread_pool> g_thread_pool;