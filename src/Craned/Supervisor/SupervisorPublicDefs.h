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

#include "SupervisorPreCompiledHeader.h"
// Precompiled header comes first

#include "crane/OS.h"

namespace Supervisor {

using EnvMap = std::unordered_map<std::string, std::string>;

using StepToSuper = crane::grpc::TaskToD;

struct TaskStatusChangeQueueElem {
  task_id_t task_id{};
  crane::grpc::TaskStatus new_status{};
  uint32_t exit_code{};
  std::optional<std::string> reason;
};

struct Config {
  struct CforedListenConf {
    bool UseTls{false};
    TlsCertificates TlsCerts;
  };
  CforedListenConf CforedListenConf;
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

  bool CompressedRpc{};

  std::string SupervisorDebugLevel;

  std::filesystem::path CraneBaseDir;
  std::filesystem::path CraneScriptDir;
  std::filesystem::path CranedUnixSocketPath;

  // Only for debugging
  std::filesystem::path SupervisorLogFile;

  CranedId CranedIdOfThisNode;

  std::filesystem::path SupervisorUnixSockPath;

  task_id_t JobId;
  EnvMap JobEnv;
  step_id_t StepId;
  StepToSuper StepSpec;
  std::atomic_int TaskCount;
};

inline Config g_config;
}  // namespace Supervisor

inline std::unique_ptr<BS::thread_pool> g_thread_pool;