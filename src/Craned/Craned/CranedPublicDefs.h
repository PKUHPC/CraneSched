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

#include "CranedPreCompiledHeader.h"
// Precompiled header comes first

#include "crane/OS.h"

namespace Craned {

inline constexpr uint64_t kEvSigChldResendMs = 500;

using EnvMap = std::unordered_map<std::string, std::string>;

struct TaskStatusChangeQueueElem {
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

  struct PluginConfig {
    bool Enabled{false};
    std::string PlugindSockPath;
  };
  PluginConfig Plugin;

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

  struct CranedMeta {
    SystemRelInfo SysInfo;
    absl::Time CranedStartTime;
    absl::Time SystemBootTime;
  };

  CranedMeta CranedMeta;

  std::unordered_map<ipv4_t, std::string> Ipv4ToCranedHostname;
  std::unordered_map<ipv6_t, std::string, absl::Hash<ipv6_t>>
      Ipv6ToCranedHostname;
  std::unordered_map<std::string, std::shared_ptr<ResourceInNode>> CranedRes;
  std::unordered_map<std::string, Partition> Partitions;
};

inline Config g_config;
}  // namespace Craned

inline std::unique_ptr<BS::thread_pool> g_thread_pool;