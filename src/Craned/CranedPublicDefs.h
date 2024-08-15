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

#include "CranedPreCompiledHeader.h"
// Precompiled header comes first

#include "crane/PublicHeader.h"
#include "protos/Crane.pb.h"

namespace Craned {

using EnvPair = std::pair<std::string, std::string>;

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
  DedicatedResourceInNode dedicated_resource;
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
    TlsCertificates TlsCerts;

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

inline Config g_config;
}  // namespace Craned

inline std::unique_ptr<BS::thread_pool> g_thread_pool;