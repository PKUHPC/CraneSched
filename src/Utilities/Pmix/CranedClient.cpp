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

bool CranedClient::TerminateTasks() {
  using crane::grpc::TerminateStepsReply;
  using crane::grpc::TerminateStepsRequest;

  grpc::ClientContext client_context;
  TerminateStepsRequest request;
  TerminateStepsReply reply;

  request.mutable_task_id_list()->Add(g_pmix_server->GetTaskId());

  if (m_stub_ == nullptr) return false;

  auto ok = m_stub_->TerminateSteps(&client_context, request, &reply);
  if (!ok.ok() || !reply.ok()) return false;

  return true;
}

bool CranedClient::BroadcastPmixPort(uint32_t pmix_port) {
  using crane::grpc::BroadcastPmixPortReply;
  using crane::grpc::BroadcastPmixPortRequest;

  grpc::ClientContext client_context;
  BroadcastPmixPortRequest request;
  BroadcastPmixPortReply reply;

  request.set_task_id(g_pmix_server->GetTaskId());
  request.set_port(pmix_port);
  request.set_craned_id(g_pmix_server->GetHostname());
  request.mutable_craned_ids()->Add(g_pmix_server->GetNodeList().begin(),
                                    g_pmix_server->GetNodeList().end());

  if (m_stub_ == nullptr) return false;

  auto ok = m_stub_->BroadcastPmixPort(&client_context, request, &reply);
  if (!ok.ok() || !reply.ok()) return false; // Remove redundant boolean literal

  return true;
}

}  // namespace pmix