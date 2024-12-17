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

#include "crane/GrpcHelper.h"

namespace Supervisor {

using grpc::ClientContext;
using grpc::Status;

void CranedClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::Craned::NewStub(m_channel_);
}

void CranedClient::TaskStatusChange(uint32_t task_id,
                                    crane::grpc::TaskStatus new_status,
                                    uint32_t exit_code,
                                    std::optional<std::string> reason) {
  ClientContext context;
  context.set_wait_for_ready(true);

  Status status;
  crane::grpc::TaskStatusChangeRequest request;
  crane::grpc::TaskStatusChangeReply reply;
  request.set_reason(reason.value_or(""));
  request.set_exit_code(exit_code);
  request.set_task_id(task_id);
  request.set_new_status(new_status);

  m_stub_->TaskStatusChange(&context, request, &reply);
}

}  // namespace Supervisor
