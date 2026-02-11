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

#include "PreCompiledHeader.h"
// Precompiled header comes first

#include "CgroupManager.h"
#include "CommonPublicDefs.h"
#include "crane/Network.h"
#include "crane/OS.h"

namespace Craned {

inline constexpr uint64_t kEvSigChldResendMs = 500;
constexpr uint64_t kCtldClientTimeoutSec = 30;
constexpr int64_t kCranedRpcTimeoutSeconds = 5;

using ::Craned::Common::CgroupInterface;
using ::Craned::Common::CgroupManager;
using ::Craned::Common::EnvMap;
using RegToken = google::protobuf::Timestamp;
using StepStatus = crane::grpc::TaskStatus;

enum class CallbackInvokeMode : std::uint8_t { SYNC = 0, ASYNC };

template <typename Cb, typename... Args>
  requires std::invocable<Cb, Args...>
struct CallbackWrapper {
  Cb cb;
  CallbackInvokeMode mode;
  bool consume;
};

inline std::string GetStepIdStr(const crane::grpc::StepToD& step) {
  return fmt::format("{}.{}", step.job_id(), step.step_id());
}

struct StepStatusChangeQueueElem {
  job_id_t job_id;
  step_id_t step_id;
  crane::grpc::TaskStatus new_status{};
  uint32_t exit_code{};
  std::optional<std::string> reason;
  google::protobuf::Timestamp timestamp;
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
  struct CranedConfig {
    uint32_t PingIntervalSec;
    uint32_t CtldTimeoutSec;
    uint64_t MaxLogFileSize;
    uint64_t MaxLogFileNum;
    uint32_t NodeHealthCheckInterval;
  };
  CranedConfig CranedConf;
  struct CranedListenConf {
    std::string CranedListenAddr;
    std::string CranedListenPort;

    struct TlsCertsConfig {
      bool Enabled{false};
      TlsCertificates TlsCerts;
      std::string CaFilePath;
      std::string CaContent;
      std::string DomainSuffix;
    };

    TlsCertsConfig TlsConfig;

    std::string UnixSocketListenAddr;
    std::string UnixSocketForPamListenAddr;
  };
  struct ContainerConfig {
    bool Enabled{false};
    std::filesystem::path TempDir;
    std::filesystem::path RuntimeEndpoint;
    std::filesystem::path ImageEndpoint;

    struct DnsConfig {
      std::string ClusterDomain{"cluster.local"};
      std::vector<std::string> Servers{"127.0.0.1"};
      std::vector<std::string> Searches;
      std::vector<std::string> Options;
    };
    DnsConfig Dns;

    struct BindFsConfig {
      bool Enabled{false};
      std::filesystem::path BindfsBinary{"bindfs"};
      std::filesystem::path FusermountBinary{"fusermount3"};
      std::filesystem::path MountBaseDir{"/mnt/crane"};
    };
    BindFsConfig BindFs;
    struct SubIdConfig {
      bool Managed{true};
      uint64_t RangeSize{65536};
      uint64_t BaseOffset{100000};
    };
    SubIdConfig SubId;
  };
  ContainerConfig Container;

  struct HealthCheckConfig {
    std::string Program;
    uint64_t Interval{0};
    uint32_t NodeState{0};
    bool Cycle{false};
  };
  HealthCheckConfig HealthCheck;

  struct PluginConfig {
    bool Enabled{false};
    std::string PlugindSockPath;
  };
  PluginConfig Plugin;

  struct SupervisorConfig {
    std::filesystem::path Path;
    std::string DebugLevel;
    std::filesystem::path LogDir;
    uint64_t MaxLogFileSize;
    uint64_t MaxLogFileNum;
  };
  SupervisorConfig Supervisor;

  CranedListenConf ListenConf;
  bool CompressedRpc{};

  std::string ControlMachine;
  std::string CraneCtldForInternalListenPort;
  std::string CranedDebugLevel;
  std::string CraneClusterName;
  uint32_t ConfigCrcVal;

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

  struct JobLifecycleHookConfig {
    std::vector<std::string> Prologs;
    std::vector<std::string> Epilogs;
    uint32_t PrologTimeout{0};
    uint32_t EpilogTimeout{0};
    uint32_t PrologEpilogTimeout{0};
    int PrologFlags{0};
    uint64_t MaxOutputSize{0};

    std::vector<std::string> TaskPrologs;
    std::vector<std::string> TaskEpilogs;
  };

  JobLifecycleHookConfig JobLifecycleHook;
};

inline Config g_config{};

struct RunTimeStatus {
  std::shared_ptr<spdlog::async_logger> conn_logger;
};

inline RunTimeStatus g_runtime_status{};

enum HealthCheckNodeStateEnum : std::uint8_t {
  ANY = 1,
  IDLE = 2,
  ALLOC = 4,
  MIXED = 8,
  NONDRAINED_IDLE = 16,
  START_ONLY = 32
};

}  // namespace Craned

inline std::unique_ptr<BS::thread_pool> g_thread_pool;
