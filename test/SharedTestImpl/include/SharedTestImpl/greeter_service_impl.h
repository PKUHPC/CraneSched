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