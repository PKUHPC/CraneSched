/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "CtldPreCompiledHeader.h"
// Precompiled header come first!

namespace Ctld {

using moodycamel::ConcurrentQueue;

using task_db_id_t = int64_t;

// *****************************************************
// TaskScheduler Constants

constexpr uint32_t kTaskScheduleIntervalMs = 1000;

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
constexpr uint32_t kPendingConcurrentQueueBatchSize = 600000;

constexpr int64_t kCtldRpcTimeoutSeconds = 5;

struct Config {
  struct Node {
    uint32_t cpu;
    uint64_t memory_bytes;
  };

  struct Partition {
    std::string nodelist_str;
    uint32_t priority;
    std::unordered_set<std::string> nodes;
    std::unordered_set<std::string> AllowAccounts;
  };

  struct CraneCtldListenConf {
    std::string CraneCtldListenAddr;
    std::string CraneCtldListenPort;

    bool UseTls{false};
    std::string DomainSuffix;
    std::string ServerCertFilePath;
    std::string ServerCertContent;
    std::string ServerKeyFilePath;
    std::string ServerKeyContent;
  };

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
  Priority PriorityConfig;

  CraneCtldListenConf ListenConf;
  bool CompressedRpc{};

  std::string CraneCtldDebugLevel;
  std::string CraneCtldLogFile;

  std::string CraneEmbeddedDbBackend;
  std::string CraneCtldDbPath;

  std::string CraneBaseDir;
  std::string CraneCtldMutexFilePath;

  bool CraneCtldForeground{};

  std::string Hostname;
  std::unordered_map<std::string, std::shared_ptr<Node>> Nodes;
  std::unordered_map<std::string, Partition> Partitions;
  std::string DefaultPartition;

  std::string DbUser;
  std::string DbPassword;
  std::string DbHost;
  std::string DbPort;
  std::string DbRSName;
  std::string DbName;
};

}  // namespace Ctld

inline Ctld::Config g_config;

namespace Ctld {

namespace result = cpp_result;

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
  Resources res;
};

/**
 * Represent the runtime status on a Craned node.
 * A Node is uniquely identified by (partition id, node index).
 */
struct CranedMeta {
  CranedStaticMeta static_meta;

  bool alive{false};

  // total = avail + in-use
  Resources res_total;  // A copy of res in CranedStaticMeta,
  // just for convenience.
  Resources res_avail;
  Resources res_in_use;

  // Store the information of the slices of allocated resource.
  // One task id owns one shard of allocated resource.
  absl::flat_hash_map<task_id_t, Resources> running_task_resource_map;
};

struct PartitionGlobalMeta {
  // total = avail + in-use
  Resources m_resource_total_;
  Resources m_resource_avail_;
  Resources m_resource_in_use_;

  // Include resources in unavailable nodes.
  Resources m_resource_total_inc_dead_;

  std::string name;
  std::string nodelist_str;
  uint32_t node_cnt;
  uint32_t alive_craned_cnt;
};

struct PartitionMeta {
  PartitionGlobalMeta partition_global_meta;
  std::unordered_set<CranedId> craned_ids;
};

struct InteractiveMetaInTask {
  std::function<void(task_id_t, std::string const&)> cb_task_res_allocated;
  std::function<void(task_id_t)> cb_task_completed;
  std::function<void(task_id_t)> cb_task_cancel;

  // ccancel for an interactive task should call the front end to kill the
  // user's shell, let Cfored to inform CraneCtld of task completion rather than
  // directly sending TerminateTask to its craned node.
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
  std::string output_file_pattern;
};

struct TaskInCtld {
  /* -------- [1] Fields that are set at the submission time. ------- */
  absl::Duration time_limit;

  PartitionId partition_id;
  Resources resources;

  crane::grpc::TaskType type;

  uid_t uid;
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

  std::variant<InteractiveMetaInTask, BatchMetaInTask> meta;

 private:
  /* ------------- [2] -------------
   * Fields that won't change after this task is accepted.
   * Also, these fields are persisted on the disk.
   * ------------------------------- */
  task_id_t task_id{0};
  task_db_id_t task_db_id{0};
  gid_t gid;
  std::string username;

  /* ----------- [3] ----------------
   * Fields that may change at run time.
   * Also, these fields are persisted on the disk.
   * -------------------------------- */
  int32_t requeue_count{0};
  std::list<CranedId> craned_ids;
  crane::grpc::TaskStatus status;
  uint32_t exit_code;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time submit_time;
  absl::Time start_time;
  absl::Time end_time;

  /* ------ duplicate of the fields [1] above just for convenience ----- */
  crane::grpc::TaskToCtld task_to_ctld;

  /* ------ duplicate of the fields [2][3] above just for convenience ----- */
  crane::grpc::PersistedPartOfTaskInCtld persisted_part;

 public:
  /* -----------
   * Fields that will not change at run time.
   * However, these fields are NOT persisted on the disk.
   * ----------- */
  // set in SetFieldsByTaskToCtld from uid
  std::unique_ptr<PasswordEntry> password_entry;

  // Cached fields for performance.
  // Set in TaskScheduler->AcquireAttributes()
  uint32_t partition_priority{0};
  uint32_t qos_priority{0};

  /* -----------
   * Fields that may change at run time.
   * However, these fields are NOT persisted on the disk.
   * ----------- */
  uint32_t nodes_alloc;
  CranedId executing_craned_id;  // The root process of the task started on this
                                 // node id.
  std::string allocated_craneds_regex;

  double schedule_priority{0.0};

  // Helper function
 public:
  crane::grpc::TaskToCtld const& TaskToCtld() const { return task_to_ctld; }
  crane::grpc::TaskToCtld* MutableTaskToCtld() { return &task_to_ctld; }

  crane::grpc::PersistedPartOfTaskInCtld const& PersistedPart() {
    return persisted_part;
  }

  void SetTaskId(task_id_t id) {
    task_id = id;
    persisted_part.set_task_id(id);
  }
  task_id_t TaskId() const { return task_id; }

  void SetTaskDbId(task_db_id_t id) {
    task_db_id = id;
    persisted_part.set_task_db_id(id);
  }
  task_id_t TaskDbId() const { return task_db_id; }

  void SetGid(gid_t id) {
    gid = id;
    persisted_part.set_gid(id);
  }
  uid_t Gid() const { return gid; }

  void SetUsername(std::string const& val) {
    username = val;
    persisted_part.set_username(val);
  }
  std::string const& Username() const { return username; }

  void SetCranedIds(std::list<CranedId>&& val) {
    persisted_part.mutable_craned_ids()->Assign(val.begin(), val.end());
    craned_ids = val;
  }
  std::list<CranedId> const& CranedIds() const { return craned_ids; }
  void CranedIdsClear() {
    craned_ids.clear();
    persisted_part.mutable_craned_ids()->Clear();
  }

  void CranedIdsAdd(CranedId const& i) {
    craned_ids.emplace_back(i);
    *persisted_part.mutable_craned_ids()->Add() = i;
  }

  void SetStatus(crane::grpc::TaskStatus val) {
    status = val;
    persisted_part.set_status(val);
  }
  crane::grpc::TaskStatus Status() const { return status; }

  void SetExitCode(uint32_t val) {
    exit_code = val;
    persisted_part.set_exit_code(val);
  }
  uint32_t ExitCode() const { return exit_code; }

  void SetSubmitTime(absl::Time const& val) {
    submit_time = val;
    persisted_part.mutable_submit_time()->set_seconds(
        ToUnixSeconds(submit_time));
  }
  void SetSubmitTimeByUnixSecond(uint64_t val) {
    submit_time = absl::FromUnixSeconds(val);
    persisted_part.mutable_submit_time()->set_seconds(val);
  }
  absl::Time const& SubmitTime() const { return submit_time; }
  int64_t SubmitTimeInUnixSecond() const { return ToUnixSeconds(submit_time); }

  void SetStartTime(absl::Time const& val) {
    start_time = val;
    persisted_part.mutable_start_time()->set_seconds(ToUnixSeconds(start_time));
  }
  void SetStartTimeByUnixSecond(uint64_t val) {
    start_time = absl::FromUnixSeconds(val);
    persisted_part.mutable_start_time()->set_seconds(val);
  }
  absl::Time const& StartTime() const { return start_time; }
  int64_t StartTimeInUnixSecond() const { return ToUnixSeconds(start_time); }

  void SetEndTime(absl::Time const& val) {
    end_time = val;
    persisted_part.mutable_end_time()->set_seconds(ToUnixSeconds(end_time));
  }
  void SetEndTimeByUnixSecond(uint64_t val) {
    end_time = absl::FromUnixSeconds(val);
    persisted_part.mutable_end_time()->set_seconds(val);
  }
  absl::Time const& EndTime() const { return end_time; }
  int64_t EndTimeInUnixSecond() const { return ToUnixSeconds(end_time); }

  void SetFieldsByTaskToCtld(crane::grpc::TaskToCtld const& val) {
    task_to_ctld = val;

    partition_id = (val.partition_name().empty()) ? g_config.DefaultPartition
                                                  : val.partition_name();
    resources.allocatable_resource = val.resources().allocatable_resource();
    time_limit = absl::Seconds(val.time_limit().seconds());

    type = val.type();

    if (type == crane::grpc::Batch) {
      meta.emplace<BatchMetaInTask>(BatchMetaInTask{
          .sh_script = val.batch_meta().sh_script(),
          .output_file_pattern = val.batch_meta().output_file_pattern(),
      });
    }

    node_num = val.node_num();
    ntasks_per_node = val.ntasks_per_node();
    cpus_per_task = cpu_t(val.cpus_per_task());

    uid = val.uid();
    password_entry = std::make_unique<PasswordEntry>(uid);
    account = val.account();
    name = val.name();
    qos = val.qos();
    cmd_line = val.cmd_line();

    for (auto& [k, v] : val.env()) env[k] = v;

    cwd = val.cwd();
    qos = val.qos();

    get_user_env = val.get_user_env();
  }

  void SetFieldsByPersistedPart(
      crane::grpc::PersistedPartOfTaskInCtld const& val) {
    persisted_part = val;

    task_id = persisted_part.task_id();
    task_db_id = persisted_part.task_db_id();
    gid = persisted_part.gid();
    username = persisted_part.username();

    nodes_alloc = craned_ids.size();

    status = persisted_part.status();

    if (status != crane::grpc::TaskStatus::Pending) {
      craned_ids.assign(persisted_part.craned_ids().begin(),
                        persisted_part.craned_ids().end());
      executing_craned_id = craned_ids.front();
    }

    start_time = absl::FromUnixSeconds(persisted_part.start_time().seconds());
    end_time = absl::FromUnixSeconds(persisted_part.end_time().seconds());
  }
};

struct TaskInDb {
  crane::grpc::TaskType type;
  task_id_t task_id;
  std::string name;
  PartitionId partition_id;
  uid_t uid;

  gid_t gid;
  absl::Duration time_limit;
  google::protobuf::Timestamp submit_time;
  google::protobuf::Timestamp start_time;
  google::protobuf::Timestamp end_time;
  std::string account;

  uint32_t node_num{0};
  std::string cmd_line;
  std::string cwd;
  std::string username;
  std::string qos;

  Resources resources;
  uint32_t exit_code;
  crane::grpc::TaskStatus status;
  std::string allocated_craneds_regex;

  void SetSubmitTimeByUnixSecond(uint64_t val) { submit_time.set_seconds(val); }
  void SetStartTimeByUnixSecond(uint64_t val) { start_time.set_seconds(val); }

  void SetEndTimeByUnixSecond(uint64_t val) { end_time.set_seconds(val); }
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
  enum AdminLevel { None, Operator, Admin };

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

}  // namespace Ctld

inline std::unique_ptr<BS::thread_pool> g_thread_pool;