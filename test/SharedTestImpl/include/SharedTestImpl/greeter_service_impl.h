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

#pragma once

#include <grpc++/grpc++.h>

#include "protos/greeter.grpc.pb.h"
#include "protos/greeter.pb.h"

class GreeterServiceImpl final : public grpc_example::Greeter::Service {
  grpc::Status SayHello(grpc::ServerContext* context,
                        const grpc_example::HelloRequest* request,
                        grpc_example::HelloReply* reply) override;

  grpc::Status SleepSeconds(grpc::ServerContext* context,
                            const grpc_example::SleepRequest* request,
                            grpc_example::SleepReply* response) override;
};

class GreeterSyncServer {
 public:
  GreeterSyncServer(const std::string& server_address);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

 private:
  std::unique_ptr<grpc::Server> m_server_;
  GreeterServiceImpl m_service_impl_;
};