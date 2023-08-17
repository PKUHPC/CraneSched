/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
