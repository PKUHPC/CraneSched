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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Craned {

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::Craned;

class CranedServiceImpl : public Craned::Service {
 public:
  CranedServiceImpl() = default;

  grpc::Status ExecuteTask(grpc::ServerContext *context,
                           const crane::grpc::ExecuteTasksRequest *request,
                           crane::grpc::ExecuteTasksReply *response) override;

  grpc::Status TerminateTasks(
      grpc::ServerContext *context,
      const crane::grpc::TerminateTasksRequest *request,
      crane::grpc::TerminateTasksReply *response) override;

  grpc::Status TerminateOrphanedTask(
      grpc::ServerContext *context,
      const crane::grpc::TerminateOrphanedTaskRequest *request,
      crane::grpc::TerminateOrphanedTaskReply *response) override;

  grpc::Status CheckTaskStatus(
      grpc::ServerContext *context,
      const crane::grpc::CheckTaskStatusRequest *request,
      crane::grpc::CheckTaskStatusReply *response) override;

  grpc::Status QueryTaskIdFromPort(
      grpc::ServerContext *context,
      const crane::grpc::QueryTaskIdFromPortRequest *request,
      crane::grpc::QueryTaskIdFromPortReply *response) override;

  grpc::Status QueryTaskIdFromPortForward(
      grpc::ServerContext *context,
      const crane::grpc::QueryTaskIdFromPortForwardRequest *request,
      crane::grpc::QueryTaskIdFromPortForwardReply *response) override;

  grpc::Status MigrateSshProcToCgroup(
      grpc::ServerContext *context,
      const crane::grpc::MigrateSshProcToCgroupRequest *request,
      crane::grpc::MigrateSshProcToCgroupReply *response) override;

  grpc::Status QueryTaskEnvVariables(
      grpc::ServerContext *context,
      const ::crane::grpc::QueryTaskEnvVariablesRequest *request,
      crane::grpc::QueryTaskEnvVariablesReply *response) override;

  grpc::Status QueryTaskEnvVariablesForward(
      grpc::ServerContext *context,
      const ::crane::grpc::QueryTaskEnvVariablesForwardRequest *request,
      crane::grpc::QueryTaskEnvVariablesForwardReply *response) override;

  grpc::Status CreateCgroupForTasks(
      grpc::ServerContext *context,
      const crane::grpc::CreateCgroupForTasksRequest *request,
      crane::grpc::CreateCgroupForTasksReply *response) override;

  grpc::Status ReleaseCgroupForTasks(
      grpc::ServerContext *context,
      const crane::grpc::ReleaseCgroupForTasksRequest *request,
      crane::grpc::ReleaseCgroupForTasksReply *response) override;

  grpc::Status ChangeTaskTimeLimit(
      grpc::ServerContext *context,
      const crane::grpc::ChangeTaskTimeLimitRequest *request,
      crane::grpc::ChangeTaskTimeLimitReply *response) override;

  grpc::Status QueryCranedRemoteMeta(
      grpc::ServerContext *context,
      const ::crane::grpc::QueryCranedRemoteMetaRequest *request,
      crane::grpc::QueryCranedRemoteMetaReply *response) override;
};

class CranedServer {
 public:
  explicit CranedServer(const Config::CranedListenConf &listen_conf);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

 private:
  std::unique_ptr<CranedServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class CranedServiceImpl;
};
}  // namespace Craned

// The initialization of CranedServer requires some parameters.
// We can't use the Singleton pattern here. So we use one global variable.
inline std::unique_ptr<Craned::CranedServer> g_server;
