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

#include <chrono>
#include <functional>
#include <memory>

#include "Mpi.h"

namespace pmi2 {

// One server per local step. A single I/O loop serves all local ranks; peer
// collectives run separately so a fence cannot stall local client progress.
class Server {
 public:
  Server();
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  bool Init(const mpi::Config& config, const crane::grpc::StepToD& step,
            std::function<void()> on_failure = {},
            std::chrono::seconds timeout = std::chrono::seconds(60));
  std::optional<mpi::LaunchInfo> PrepareTask(uint32_t rank);
  bool ReceivePeers(const std::vector<std::pair<CranedId, std::string>>& peers);
  void TaskExited(uint32_t rank);
  std::string Port() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace pmi2
