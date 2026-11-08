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

#include "Mpi.h"

#include <algorithm>

#include "Pmix.h"
#ifdef HAVE_PMI2
#  include "Pmi2.h"
#endif
#include "protos/Crane.grpc.pb.h"

namespace mpi {
namespace {
class PmixRuntime final : public Runtime {
 public:
  bool Init(const Config& config, const crane::grpc::StepToD& step) override {
    config_ = config;
    nodes_.assign(step.nodelist().begin(), step.nodelist().end());
    return server_.Init(config, step);
  }
  std::optional<LaunchInfo> PrepareTask(uint32_t rank) override {
    auto env = server_.SetupFork(rank);
    if (!env) return std::nullopt;
    LaunchInfo launch;
    launch.env = std::move(*env);
    return launch;
  }
  bool ReceivePeers(
      const std::vector<std::pair<CranedId, std::string>>& peers) override {
    auto* client = server_.GetPmixClient();
    if (!client) return false;
    for (const auto& [id, port] : peers) {
      if (id == config_.CranedIdOfThisNode ||
          std::ranges::find(nodes_, id) == nodes_.end() ||
          !config_.CranedIdToNodeHostname.contains(id) ||
          !config_.CranedIdToNodeAddr.contains(id))
        return false;
    }
    for (const auto& [id, port] : peers) client->EmplacePmixStub(id, port);
    return true;
  }
  void TaskExited(uint32_t) override {}

 private:
  Config config_;
  std::vector<CranedId> nodes_;
  pmix::PmixServer server_;
};

#ifdef HAVE_PMI2
class Pmi2Runtime final : public Runtime {
 public:
  bool Init(const Config& config, const crane::grpc::StepToD& step) override {
    step_ = step;
    auto channel = CreateUnixInsecureChannel(
        "unix://" + config.CranedUnixSocketPath.string());
    craned_ = crane::grpc::Craned::NewStub(channel);
    if (!server_.Init(config, step, [this] { TerminateLocalStep(); })) {
      CRANE_ERROR("Failed to initialize PMI-2 server for step {}.{}",
                  step.job_id(), step.step_id());
      return false;
    }
    if (step.nodelist_size() == 1) return true;
    crane::grpc::BroadcastPmi2PortRequest request;
    crane::grpc::BroadcastPmi2PortReply reply;
    request.set_job_id(step.job_id());
    request.set_step_id(step.step_id());
    request.set_craned_id(config.CranedIdOfThisNode);
    request.set_port(server_.Port());
    *request.mutable_craned_ids() = step.nodelist();
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    auto status = craned_->BroadcastPmi2Port(&context, request, &reply);
    if (!status.ok() || !reply.ok())
      CRANE_ERROR("PMI-2 endpoint announcement failed for step {}.{}: {} ({})",
                  step.job_id(), step.step_id(), status.error_message(),
                  static_cast<int>(status.error_code()));
    return status.ok() && reply.ok();
  }
  std::optional<LaunchInfo> PrepareTask(uint32_t rank) override {
    return server_.PrepareTask(rank);
  }
  bool ReceivePeers(
      const std::vector<std::pair<CranedId, std::string>>& peers) override {
    return server_.ReceivePeers(peers);
  }
  void TaskExited(uint32_t rank) override { server_.TaskExited(rank); }

 private:
  void TerminateLocalStep() {
    // The protocol server propagates failure to the other step supervisors.
    // This RPC targets only our own step; it never cancels another allocation.
    crane::grpc::TerminateStepsRequest request;
    crane::grpc::TerminateStepsReply reply;
    (*request.mutable_job_step_ids_map())[step_.job_id()].add_steps(
        step_.step_id());
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    auto status = craned_->TerminateSteps(&context, request, &reply);
    if (!status.ok() || !reply.ok())
      CRANE_ERROR("Failed to terminate local PMI-2 step {}.{}", step_.job_id(),
                  step_.step_id());
  }
  crane::grpc::StepToD step_;
  std::unique_ptr<crane::grpc::Craned::Stub> craned_;
  // Destroy the server (and join its callbacks) before destroying craned_.
  pmi2::Server server_;
};
#endif
}  // namespace

std::unique_ptr<Runtime> CreateRuntime(std::string_view protocol) {
#ifdef HAVE_PMIX
  if (protocol == kMpiTypePmix) return std::make_unique<PmixRuntime>();
#endif
#ifdef HAVE_PMI2
  if (protocol == kMpiTypePmi2) return std::make_unique<Pmi2Runtime>();
#endif
  return nullptr;
}
}  // namespace mpi
