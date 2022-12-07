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

#if Boost_MINOR_VERSION >= 71
#  include <boost/uuid/uuid_hash.hpp>
#endif

namespace Ctld {

using task_id_t = uint32_t;
using task_db_id_t = int64_t;

constexpr uint64_t kTaskScheduleIntervalMs = 1000;

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

  /* ------ duplicate of the fields [1] above just for convenience ----- */
  crane::grpc::TaskToCtld task_to_ctld;


  /* ------------- [2] -------------
   * Fields that won't change after this task is accepted.
   * Also, these fields are persisted on the disk.
   * ------------------------------- */
  task_id_t task_id;
  task_db_id_t task_db_id;
  uint32_t partition_id;
  gid_t gid;
  std::string account;

  /* ----------- [3] ----------------
   * Fields that may change at run time.
   * Also, these fields are persisted on the disk.
   * -------------------------------- */
  int32_t requeue_count{0};

  /* -----------
   * Fields that may change at run time.
   * However, these fields are NOT persisted on the disk.
   * ----------- */
  crane::grpc::TaskStatus status;

  uint32_t nodes_alloc;
  std::list<std::string> nodes;
  std::list<uint32_t> node_indexes;
  CranedId executing_node_id;  // The root process of the task started on this
                               // node id.
  std::string allocated_craneds_regex;

  // If this task is PENDING, start_time is either not set (default constructed)
  // or an estimated start time.
  // If this task is RUNNING, start_time is the actual starting time.
  absl::Time start_time;

  absl::Time end_time;
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
