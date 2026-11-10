/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <unistd.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "crane/GrpcHelper.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.pb.h"

namespace mpi {

struct Config {
  bool UseTls{false};
  TlsCertificates TlsCerts;
  bool CompressedRpc{false};
  std::filesystem::path CraneBaseDir;
  std::filesystem::path CraneScriptDir;
  std::filesystem::path CranedUnixSocketPath;
  CranedId CranedIdOfThisNode;
  std::unordered_map<CranedId, std::string> CranedIdToNodeHostname;
  std::unordered_map<CranedId, std::string> CranedIdToNodeAddr;
  bool EnableSlurmCompatibleEnv{false};
};

// Prepared before fork: the child must never acquire a runtime mutex inherited
// from another supervisor thread. Each process owns its own copy of this fd.
struct LaunchInfo {
  std::unordered_map<std::string, std::string> env;
  int fd{-1};
  LaunchInfo() = default;
  ~LaunchInfo() { Close(); }
  LaunchInfo(const LaunchInfo&) = delete;
  LaunchInfo& operator=(const LaunchInfo&) = delete;
  LaunchInfo(LaunchInfo&& other) noexcept
      : env(std::move(other.env)), fd(std::exchange(other.fd, -1)) {}
  LaunchInfo& operator=(LaunchInfo&& other) noexcept {
    if (this != &other) {
      Close();
      env = std::move(other.env);
      fd = std::exchange(other.fd, -1);
    }
    return *this;
  }
  void Close() {
    if (fd >= 0) close(std::exchange(fd, -1));
  }
};

class Runtime {
 public:
  virtual ~Runtime() = default;
  virtual bool Init(const Config& config, const crane::grpc::StepToD& step) = 0;
  virtual std::optional<LaunchInfo> PrepareTask(uint32_t rank) = 0;
  virtual bool ReceivePeers(
      const std::vector<std::pair<CranedId, std::string>>& peers) = 0;
  virtual void TaskExited(uint32_t rank) = 0;
};

std::unique_ptr<Runtime> CreateRuntime(std::string_view protocol);

}  // namespace mpi
