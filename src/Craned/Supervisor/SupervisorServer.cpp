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

#include "SupervisorServer.h"

grpc::Status Supervisor::SupervisorServiceImpl::StartTask(
    grpc::ServerContext* context,
    const crane::grpc::TaskExecutionRequest* request,
    crane::grpc::TaskExecutionReply* response) {
  // todo: Launch task
  response->set_ok(true);
  return Status::OK;
}
Supervisor::SupervisorServer::SupervisorServer(task_id_t task_id) {
  m_service_impl_ = std::make_unique<SupervisorServiceImpl>();

  auto unix_socket_path = fmt::format("unix:/tmp/crane/task_{}.sock", task_id);
  grpc::ServerBuilder builder;
  ServerBuilderAddUnixInsecureListeningPort(&builder, unix_socket_path);
  builder.RegisterService(m_service_impl_.get());
}
