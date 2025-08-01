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

#include <filesystem>

#include "CranedPreCompiledHeader.h"
// Precompiled header comes first

#include "crane/Network.h"
#include "crane/OS.h"

namespace Craned {

inline constexpr uint64_t kEvSigChldResendMs = 500;
inline constexpr uint64_t kRegisterOperationTimeoutMs = 5'000;

using EnvMap = std::unordered_map<std::string, std::string>;
using RegToken = google::protobuf::Timestamp;

enum class CallbackInvokeMode : std::uint8_t { SYNC = 0, ASYNC };

template <typename Cb, typename... Args>
  requires std::invocable<Cb, Args...>
struct CallbackWrapper {
  Cb cb;
  CallbackInvokeMode mode;
  bool consume;
};

struct TaskStatusChangeQueueElem {
  task_id_t step_id{};
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
  std::unordered_set<std::string> AllowedAccounts;
};

struct Config {
  struct CranedListenConf {
    std::string CranedListenAddr;
    std::string CranedListenPort;

    bool UseTls{false};
    TlsCertificates TlsCerts;

    std::string UnixSocketListenAddr;
    std::string UnixSocketForPamListenAddr;
  };
  struct ContainerConfig {
    bool Enabled{false};
    std::filesystem::path TempDir;
    std::string RuntimeBin;
    std::string RuntimeState;
    std::string RuntimeRun;
    std::string RuntimeKill;
    std::string RuntimeDelete;
  };
  ContainerConfig Container;

  struct PluginConfig {
    bool Enabled{false};
    std::string PlugindSockPath;
  };
  PluginConfig Plugin;

  struct SupervisorConfig {
    std::filesystem::path Path;
    std::string DebugLevel;
    std::filesystem::path LogDir;
  };
  SupervisorConfig Supervisor;

  CranedListenConf ListenConf;
  bool CompressedRpc{};

  std::string ControlMachine;
  std::string CraneCtldForInternalListenPort;
  std::string CranedDebugLevel;
  std::string CraneClusterName;

  std::filesystem::path CraneBaseDir;
  std::filesystem::path CranedLogFile;
  std::filesystem::path CranedMutexFilePath;
  std::filesystem::path CranedScriptDir;
  std::filesystem::path CranedUnixSockPath;
  std::filesystem::path CranedForPamUnixSockPath;

  bool CranedForeground{};

  std::string Hostname;
  CranedId CranedIdOfThisNode;

  struct CranedMeta {
    SystemRelInfo SysInfo;
    absl::Time CranedStartTime;
    absl::Time SystemBootTime;
    std::vector<crane::NetworkInterface> NetworkInterfaces;
  };
  CranedMeta CranedMeta;

  std::unordered_map<ipv4_t, std::string> Ipv4ToCranedHostname;
  std::unordered_map<ipv6_t, std::string, absl::Hash<ipv6_t>>
      Ipv6ToCranedHostname;
  std::unordered_map<std::string, std::shared_ptr<ResourceInNode>> CranedRes;
  std::unordered_map<std::string, Partition> Partitions;
};

inline Config g_config{};
}  // namespace Craned

inline std::unique_ptr<BS::thread_pool> g_thread_pool;