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

#include "SharedTestImpl/greeter_service_impl.h"

#include <thread>

#include "crane/Logger.h"

grpc::Status GreeterServiceImpl::SayHello(
    grpc::ServerContext* context, const grpc_example::HelloRequest* request,
    grpc_example::HelloReply* reply) {
  std::string prefix("Hello ");
  reply->set_message(prefix + request->name());
  return grpc::Status::OK;
}

grpc::Status GreeterServiceImpl::SleepSeconds(
    grpc::ServerContext* context, const grpc_example::SleepRequest* request,
    grpc_example::SleepReply* response) {
  response->set_ok(true);

  std::this_thread::sleep_for(std::chrono::seconds(request->seconds()));

  return grpc::Status::OK;
}

GreeterSyncServer::GreeterSyncServer(const std::string& server_address) {
  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&m_service_impl_);
  // Finally assemble the server.
  m_server_ = builder.BuildAndStart();

  CRANE_INFO("Server listening on {}", server_address);
}
