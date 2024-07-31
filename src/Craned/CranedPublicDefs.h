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

#include <google/protobuf/util/time_util.h>

#include <array>
#include <fpm/fixed.hpp>
#include <optional>
#include <unordered_map>
#include <variant>

#include "CranedPreCompiledHeader.h"
// Precompiled header comes first

#include "crane/PublicHeader.h"
#include "protos/Crane.pb.h"

namespace Craned {

struct TaskStatusChange {
  task_id_t task_id{};
  crane::grpc::TaskStatus new_status{};
  uint32_t exit_code{};
  std::optional<std::string> reason;
};

struct TaskInfoOfUid {
  uint32_t job_cnt;
  uint32_t first_task_id;
  bool cgroup_exists;
  std::string cgroup_path;
};

struct CranedNode {
  uint32_t cpu;
  uint64_t memory_bytes;
  DedicatedResource dedicated_resource;
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
  bool CompressedRpc{};

  std::string ControlMachine;
  std::string CraneCtldListenPort;
  std::string CranedDebugLevel;

  std::string CraneBaseDir;
  std::string CranedLogFile;
  std::string CranedMutexFilePath;
  std::string CranedScriptDir;
  std::string CranedUnixSockPath;

  bool CranedForeground{};

  std::string Hostname;
  CranedId CranedIdOfThisNode;

  std::unordered_map<std::string, std::string> Ipv4ToCranedHostname;
  std::unordered_map<std::string, std::shared_ptr<CranedNode>> CranedNodes;
  std::unordered_map<std::string, Partition> Partitions;
};

std::optional<std::tuple<unsigned int, unsigned int, char>>
GetDeviceFileMajorMinorOpType(const std::string& path);

struct Device {
  unsigned int major;
  unsigned int minor;
  char op_type;
  // set to true when allocated
  bool busy = false;
  // set to true in allocation result to indicate the task own this device
  bool alloc = false;
  std::string path;
  // device type e.g a100
  std::string type;
  std::string name;
  std::vector<int> cpu_affinity;
  // link count to #index device;
  std::vector<int> links;

  Device(const std::string& device_name, const std::string& device_type,
         const std::string& device_path);
  bool Init();
  operator std::string() const;
};

bool operator==(const Device& lhs, const Device& rhs);

inline Config g_config;

inline std::vector<Device> g_this_node_device;
}  // namespace Craned

inline std::unique_ptr<BS::thread_pool> g_thread_pool;