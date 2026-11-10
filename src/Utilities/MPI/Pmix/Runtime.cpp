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

#include "Pmix/Runtime.h"

#include <algorithm>

#include "Server.h"

namespace pmix {
namespace {
class PmixRuntime final : public mpi::Runtime {
 public:
  bool Init(const mpi::Config& config,
            const crane::grpc::StepToD& step) override {
    m_config_ = config;
    m_nodes_.assign(step.nodelist().begin(), step.nodelist().end());
    return m_server_.Init(config, step);
  }
  std::optional<mpi::LaunchInfo> PrepareTask(uint32_t rank) override {
    auto env = m_server_.SetupFork(rank);
    if (!env) return std::nullopt;
    mpi::LaunchInfo launch;
    launch.env = std::move(*env);
    return launch;
  }
  bool ReceivePeers(
      const std::vector<std::pair<CranedId, std::string>>& peers) override {
    auto* client = m_server_.GetPmixClient();
    if (!client) return false;
    for (const auto& [id, port] : peers) {
      if (id == m_config_.CranedIdOfThisNode ||
          std::ranges::find(m_nodes_, id) == m_nodes_.end() ||
          !m_config_.CranedIdToNodeHostname.contains(id) ||
          !m_config_.CranedIdToNodeAddr.contains(id))
        return false;
    }
    for (const auto& [id, port] : peers) client->EmplacePmixStub(id, port);
    return true;
  }
  void TaskExited(uint32_t) override {}

 private:
  mpi::Config m_config_;
  std::vector<CranedId> m_nodes_;
  pmix::PmixServer m_server_;
};
}  // namespace
std::unique_ptr<mpi::Runtime> CreateRuntime() {
  return std::make_unique<PmixRuntime>();
}
}  // namespace pmix
