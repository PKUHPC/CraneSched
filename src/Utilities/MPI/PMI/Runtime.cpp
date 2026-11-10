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

#include "PMI/Runtime.h"

#include "Server/Server.h"
#include "StepControlClient.h"

namespace pmi {
namespace {
class PmiRuntime final : public mpi::Runtime {
 public:
  bool Init(const mpi::Config& config,
            const crane::grpc::StepToD& step) override {
    m_step_client_ = std::make_unique<mpi::StepControlClient>(
        step.job_id(), step.step_id(), config.CranedIdOfThisNode,
        std::vector<CranedId>(step.nodelist().begin(), step.nodelist().end()),
        crane::grpc::mpi::PROTOCOL_PMI);
    m_step_client_->InitChannelAndStub(config.CranedUnixSocketPath);
    if (!m_server_.Init(config, step,
                        [this] { m_step_client_->TerminateSteps(); }))
      return false;
    return m_step_client_->ReportEndpoint(m_server_.Port());
  }
  std::optional<mpi::LaunchInfo> PrepareTask(uint32_t rank) override {
    return m_server_.PrepareTask(rank);
  }
  bool ReceivePeers(
      const std::vector<std::pair<CranedId, std::string>>& peers) override {
    return m_server_.ReceivePeers(peers);
  }
  void TaskExited(uint32_t rank) override { m_server_.TaskExited(rank); }

 private:
  // The server joins its callbacks before the step client is destroyed.
  std::unique_ptr<mpi::StepControlClient> m_step_client_;
  Server m_server_;
};
}  // namespace
std::unique_ptr<mpi::Runtime> CreateRuntime() {
  return std::make_unique<PmiRuntime>();
}
}  // namespace pmi
