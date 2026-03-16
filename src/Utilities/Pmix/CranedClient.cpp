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

#include "CranedClient.h"

#include "Pmix.h"
#include "crane/Logger.h"
#include "fmt/format.h"
#include "grpcpp/create_channel.h"

namespace pmix {

void CranedClient::InitChannelAndStub(const std::string& endpoint) {
  std::string craned_unix_socket_address = fmt::format("unix://{}", endpoint);

  m_channel_ = grpc::CreateChannel(craned_unix_socket_address,
                                   grpc::InsecureChannelCredentials());
  m_stub_ = crane::grpc::Craned::NewStub(m_channel_);
}

void CranedClient::TerminateTasks() {
  using crane::grpc::TerminateStepsReply;
  using crane::grpc::TerminateStepsRequest;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(kRpcTimeoutSeconds));
  TerminateStepsRequest request;
  TerminateStepsReply reply;

  auto &job_step_map = *request.mutable_job_step_ids_map();

  job_step_map[m_pmix_job_info_.job_id]
      .mutable_steps()->Add(m_pmix_job_info_.step_id);

  if (m_stub_ == nullptr) {
    CRANE_ERROR("Failed to terminate tasks, stub is null");
    return;
  }

  auto ok = m_stub_->TerminateSteps(&client_context, request, &reply);
  if (!ok.ok()) {
    CRANE_ERROR("Failed to terminate tasks, grpc error: {}", ok.error_message());
    return;
  }

  if (!reply.ok()) {
    CRANE_ERROR("Failed to terminate tasks, craned returned not ok, reason: {}",
                reply.reason());
    return;
  }
}

bool CranedClient::BroadcastPmixPort(const std::string& pmix_port) {

  if (m_pmix_job_info_.node_list.size() == 1) {
    CRANE_TRACE("Only one node in job, no need to broadcast pmix port.");
    return true;
  }

  using crane::grpc::BroadcastPmixPortReply;
  using crane::grpc::BroadcastPmixPortRequest;

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(kRpcTimeoutSeconds));
  BroadcastPmixPortRequest request;
  BroadcastPmixPortReply reply;

  request.set_job_id(m_pmix_job_info_.job_id);
  request.set_step_id(m_pmix_job_info_.step_id);
  request.set_port(pmix_port);
  request.set_craned_id(m_pmix_job_info_.hostname);
  request.mutable_craned_ids()->Add(m_pmix_job_info_.node_list.begin(),
                                    m_pmix_job_info_.node_list.end());

  if (m_stub_ == nullptr) {
    CRANE_ERROR("Failed to broadcast PMIX port, stub is null");
    return false;
  }

  auto ok = m_stub_->BroadcastPmixPort(&client_context, request, &reply);

  return ok.ok() && reply.ok();
}

}  // namespace pmix