#pragma once

#include "CranedPreCompiledHeader.h"
// Precompiled header comes first

#include "cgroup.linux.h"
#include "crane/PublicHeader.h"

namespace Craned {

struct TaskStatusChange {
  task_id_t task_id{};
  crane::grpc::TaskStatus new_status{};
  std::optional<std::string> reason;
};

struct TaskInfoOfUid {
  uint32_t job_cnt;
  uint32_t first_task_id;
  bool cgroup_exists;
  std::string cgroup_path;
};

struct Node {
  uint32_t cpu;
  uint64_t memory_bytes;
};

struct Partition {
  std::unordered_set<std::string> nodes;
  std::unordered_set<std::string> AllowAccounts;
};

struct Config {
  struct CranedListenConf {
    std::string CranedListenAddr;
    std::string CranedListenPort;

    bool UseTls{false};
    std::string DomainSuffix;
    std::string ServerCertFilePath;
    std::string ServerCertContent;
    std::string ServerKeyFilePath;
    std::string ServerKeyContent;

    std::string UnixSocketListenAddr;
  };

  CranedListenConf ListenConf;

  std::string ControlMachine;
  std::string CraneCtldListenPort;
  std::string CranedDebugLevel;
  std::string CranedLogFile;

  bool CranedForeground{};

  std::string Hostname;
  CranedId NodeId;

  std::unordered_map<std::string, std::string> Ipv4ToNodesHostname;
  std::unordered_map<std::string, std::shared_ptr<Node>> Nodes;
  std::unordered_map<std::string, Partition> Partitions;
};

inline Config g_config;

inline std::unique_ptr<BS::thread_pool> g_thread_pool;

}  // namespace Craned