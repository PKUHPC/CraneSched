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

#include "StepControlClient.h"

#include "crane/Logger.h"

namespace mpi {
namespace {
constexpr auto kControlTimeout = std::chrono::seconds(5);
}

StepControlClient::StepControlClient(job_id_t job_id, step_id_t step_id,
                                     CranedId craned_id,
                                     std::vector<CranedId> nodes,
                                     crane::grpc::mpi::Protocol protocol)
    : m_job_id_(job_id),
      m_step_id_(step_id),
      m_craned_id_(std::move(craned_id)),
      m_nodes_(std::move(nodes)),
      m_protocol_(protocol) {}

void StepControlClient::InitChannelAndStub(const std::string& endpoint) {
  m_stub_ = crane::grpc::Craned::NewStub(
      CreateUnixInsecureChannel("unix://" + endpoint));
}

bool StepControlClient::ReportEndpoint(std::string_view address) {
  if (m_nodes_.size() == 1) return true;
  if (!m_stub_) return false;
  crane::grpc::mpi::BroadcastMpiEndpointRequest request;
  request.set_job_id(m_job_id_);
  request.set_step_id(m_step_id_);
  request.set_protocol(m_protocol_);
  request.mutable_craned_ids()->Add(m_nodes_.begin(), m_nodes_.end());
  request.mutable_endpoint()->set_craned_id(m_craned_id_);
  request.mutable_endpoint()->set_address(address.data(), address.size());
  crane::grpc::mpi::BroadcastMpiEndpointReply reply;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kControlTimeout);
  auto status = m_stub_->BroadcastMpiEndpoint(&context, request, &reply);
  if (!status.ok() || !reply.ok())
    CRANE_ERROR("[Step #{}.{}] MPI endpoint announcement failed: {}", m_job_id_,
                m_step_id_, status.error_message());
  return status.ok() && reply.ok();
}

void StepControlClient::TerminateSteps() {
  if (!m_stub_) return;
  crane::grpc::TerminateStepsRequest request;
  (*request.mutable_job_step_ids_map())[m_job_id_].add_steps(m_step_id_);
  crane::grpc::TerminateStepsReply reply;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + kControlTimeout);
  auto status = m_stub_->TerminateSteps(&context, request, &reply);
  if (!status.ok() || !reply.ok())
    CRANE_ERROR("[Step #{}.{}] MPI step termination failed: {}", m_job_id_,
                m_step_id_, status.error_message());
}
}  // namespace mpi
