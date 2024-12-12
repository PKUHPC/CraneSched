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

#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "protos/Supervisor.grpc.pb.h"
#include "protos/Supervisor.pb.h"

namespace Supervisor {
using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::supervisor::Supervisor;
class SupervisorServiceImpl : public Supervisor::Service {
 public:
  SupervisorServiceImpl() = default;
  grpc::Status ExecuteTask(
      grpc::ServerContext* context,
      const crane::grpc::supervisor::TaskExecutionRequest* request,
      crane::grpc::supervisor::TaskExecutionReply* response) override;

  grpc::Status CheckTaskStatus(
      grpc::ServerContext* context,
      const crane::grpc::supervisor::CheckTaskStatusRequest* request,
      crane::grpc::supervisor::CheckTaskStatusReply* response) override;

  grpc::Status ChangeTaskTimeLimit(
      grpc::ServerContext* context,
      const crane::grpc::supervisor::ChangeTaskTimeLimitRequest* request,
      crane::grpc::supervisor::ChangeTaskTimeLimitReply* response) override;

  grpc::Status TerminateTask(
      grpc::ServerContext* context,
      const crane::grpc::supervisor::TerminateTaskRequest* request,
      crane::grpc::supervisor::TerminateTaskReply* response) override;
};

class SupervisorServer {
 public:
  explicit SupervisorServer();

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

 private:
  std::unique_ptr<SupervisorServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class SupervisorServiceImpl;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::SupervisorServer> g_server;