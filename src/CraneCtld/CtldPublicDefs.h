#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)

#include <boost/container_hash/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <chrono>
#include <string>
#include <unordered_map>
#include <variant>

#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"

#if Boost_MINOR_VERSION >= 71
#  include <boost/uuid/uuid_hash.hpp>
#endif

namespace Ctld {

using task_db_id_t = int64_t;

constexpr uint64_t kTaskScheduleIntervalMs = 1000;

struct Config {
  struct Node {
    uint32_t cpu;
    uint64_t memory_bytes;

    std::string partition_name;
  };

  struct Partition {
    std::string nodelist_str;
    std::unordered_set<std::string> nodes;
    std::unordered_set<std::string> AllowAccounts;
  };

  struct CraneCtldListenConf {
    std::string CraneCtldListenAddr;
    std::string CraneCtldListenPort;

    bool UseTls{false};
    std::string CertFilePath;
    std::string CertContent;
    std::string KeyFilePath;
    std::string KeyContent;
  };

  CraneCtldListenConf ListenConf;

  std::string CraneCtldDebugLevel;
  std::string CraneCtldLogFile;

  std::string CraneCtldDbPath;

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
struct InteractiveTaskAllocationDetail {
  uint32_t craned_index;
  std::string ipv4_addr;
  uint32_t port;
  boost::uuids::uuid resource_uuid;
};

/**
 * The static information on a Craned (the static part of CranedMeta). This
 * structure is provided when a new Craned node is to be registered in
 * CranedMetaContainer.
 */
struct CranedStaticMeta {
  uint32_t craned_index;  // Allocated when this node is up.

  std::string hostname;  // the hostname corresponds to the node index
  uint32_t port;

  uint32_t partition_id;  // Allocated if partition_name is new or
                          // use existing partition id of the partition_name.
  std::string partition_name;  // a partition_name corresponds to partition id.
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
  absl::flat_hash_map<uint32_t /*task id*/, Resources>
      running_task_resource_map;
};

/**
 * A map from index to craned information within a partition.
 */
using CranedMetaMap = std::unordered_map<uint32_t, CranedMeta>;

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

struct PartitionMetas {
  PartitionGlobalMeta partition_global_meta;
  CranedMetaMap craned_meta_map;
};

struct InteractiveMetaInTask {
  boost::uuids::uuid resource_uuid;
};

struct BatchMetaInTask {
  std::string sh_script;
  std::string output_file_pattern;
};

struct TaskInCtld {
  /* -------- [1] Fields that are set at the submission time. ------- */
  absl::Duration time_limit;

  std::string partition_name;
  Resources resources;

  crane::grpc::TaskType type;

  uid_t uid;

  std::string name;

  uint32_t node_num{0};
  uint32_t ntasks_per_node{0};
  double cpus_per_task{0.0};

  bool requeue_if_failed{false};

  std::string cmd_line;
  std::string env;
  std::string cwd;

  std::variant<InteractiveMetaInTask, BatchMetaInTask> meta;

 private:
  /* ------------- [2] -------------
   * Fields that won't change after this task is accepted.
   * Also, these fields are persisted on the disk.
   * ------------------------------- */
  task_id_t task_id;
  task_db_id_t task_db_id;
  gid_t gid;
  std::string account;

  /* ----------- [3] ----------------
   * Fields that may change at run time.
   * Also, these fields are persisted on the disk.
   * -------------------------------- */
  int32_t requeue_count{0};
  uint32_t partition_id;
  std::list<uint32_t> node_indexes;
  std::list<std::string> nodes;
  crane::grpc::TaskStatus status;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time start_time;
  absl::Time end_time;

  /* ------ duplicate of the fields [1] above just for convenience ----- */
  crane::grpc::TaskToCtld task_to_ctld;

  /* ------ duplicate of the fields [2][3] above just for convenience ----- */
  crane::grpc::PersistedPartOfTaskInCtld persisted_part;

 public:
  /* -----------
   * Fields that may change at run time.
   * However, these fields are NOT persisted on the disk.
   * ----------- */
  uint32_t nodes_alloc;
  CranedId executing_node_id;  // The root process of the task started on this
                               // node id.
  std::string allocated_craneds_regex;

  // Helper function
 public:
  crane::grpc::TaskToCtld const& TaskToCtld() { return task_to_ctld; }
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

  void SetPartitionId(uint32_t id) {
    partition_id = id;
    persisted_part.set_partition_id(id);
  }
  uint32_t PartitionId() const { return partition_id; }

  void SetGid(gid_t id) {
    gid = id;
    persisted_part.set_gid(id);
  }
  uid_t Gid() const { return gid; }

  void SetAccount(std::string const& val) {
    account = std::move(val);
    persisted_part.set_account(val);
  }
  std::string const& Account() const { return account; }

  void SetNodeIndexes(std::list<uint32_t>&& val) {
    persisted_part.mutable_node_indexes()->Assign(val.begin(), val.end());
    node_indexes = val;
  }
  std::list<uint32_t> const& NodeIndexes() { return node_indexes; }

  void NodeIndexesAdd(uint32_t i) {
    node_indexes.emplace_back(i);
    *persisted_part.mutable_node_indexes()->Add() = i;
  }

  void SetNodes(std::list<std::string>&& val) {
    persisted_part.mutable_nodes()->Assign(val.begin(), val.end());
    nodes = std::move(val);
  }
  std::list<std::string> const& Nodes() const { return nodes; }
  void NodesAdd(std::string const& val) {
    nodes.emplace_back(val);
    persisted_part.mutable_nodes()->Add()->assign(val);
  }

  void SetStatus(crane::grpc::TaskStatus val) {
    status = val;
    persisted_part.set_status(val);
  }
  crane::grpc::TaskStatus Status() const { return status; }

  void SetStartTime(absl::Time const& val) {
    start_time = val;
    persisted_part.mutable_start_time()->set_seconds(ToUnixSeconds(start_time));
  }
  void SetStartTimeByUnixSecond(uint64_t val) {
    start_time = absl::FromUnixSeconds(val);
    persisted_part.mutable_start_time()->set_seconds(val);
  }
  absl::Time const& StartTime() const { return start_time; }
  uint64_t StartTimeInUnixSecond() const { return ToUnixSeconds(start_time); }

  void SetEndTime(absl::Time const& val) {
    end_time = val;
    persisted_part.mutable_end_time()->set_seconds(ToUnixSeconds(end_time));
  }
  void SetEndTimeByUnixSecond(uint64_t val) {
    end_time = absl::FromUnixSeconds(val);
    persisted_part.mutable_end_time()->set_seconds(val);
  }
  absl::Time const& EndTime() const { return end_time; }
  uint64_t EndTimeInUnixSecond() const { return ToUnixSeconds(end_time); }

  void SetFieldsByTaskToCtld(crane::grpc::TaskToCtld const& val) {
    task_to_ctld = val;

    partition_name = (val.partition_name().empty()) ? g_config.DefaultPartition
                                                    : val.partition_name();
    resources.allocatable_resource = val.resources().allocatable_resource();
    time_limit = absl::Seconds(val.time_limit().seconds());

    type = val.type();

    if (type == crane::grpc::Batch) {
      meta = BatchMetaInTask{};
      auto& batch_meta = std::get<BatchMetaInTask>(meta);
      batch_meta.sh_script = val.batch_meta().sh_script();
      batch_meta.output_file_pattern = val.batch_meta().output_file_pattern();
    }

    node_num = val.node_num();
    ntasks_per_node = val.ntasks_per_node();
    cpus_per_task = val.cpus_per_task();

    uid = val.uid();
    name = val.name();
    cmd_line = val.cmd_line();
    env = val.env();
    cwd = val.cwd();
  }

  void SetFieldsByPersistedPart(
      crane::grpc::PersistedPartOfTaskInCtld const& val) {
    persisted_part = val;

    task_id = persisted_part.task_id();
    task_db_id = persisted_part.task_db_id();
    partition_id = persisted_part.partition_id();
    gid = persisted_part.gid();

    node_indexes.assign(persisted_part.node_indexes().begin(),
                        persisted_part.node_indexes().end());
    executing_node_id = {partition_id, node_indexes.front()};
    nodes_alloc = node_indexes.size();
    allocated_craneds_regex = util::HostNameListToStr(nodes);

    nodes.assign(persisted_part.nodes().begin(), persisted_part.nodes().end());
    status = persisted_part.status();

    start_time = absl::FromUnixSeconds(persisted_part.start_time().seconds());
    end_time = absl::FromUnixSeconds(persisted_part.end_time().seconds());
  }
};

struct Qos {
  bool deleted = false;
  std::string name;
  std::string description;
  uint32_t priority;
  uint32_t max_jobs_per_user;
};

struct Account {
  bool deleted = false;
  std::string name;
  std::string description;
  std::list<std::string> users;
  std::list<std::string> child_accounts;
  std::string parent_account;
  std::list<std::string> allowed_partition;
  //  std::unordered_map<std::string, bool> allowed_partition;  /*partition
  //  name, enable*/
  std::string default_qos;
  std::list<std::string> allowed_qos_list;
};

struct User {
  enum AdminLevel { None, Operator, Admin };

  bool deleted = false;
  uid_t uid;
  std::string name;
  std::string account;
  std::unordered_map<std::string /*partition name*/,
                     std::pair<std::string /*default qos*/,
                               std::list<std::string> /*allowed qos list*/>>
      allowed_partition_qos_map;
  AdminLevel admin_level;
};

}  // namespace Ctld
