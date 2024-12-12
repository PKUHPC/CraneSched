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

#include "CtldClient.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Craned {

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::Craned;

enum class RequestSource : std::int8_t {
  CTLD = 0,
  PAM = 1,
  SUPERVISOR = 2,
  INVALID
};

class CranedServiceImpl : public Craned::Service {
 public:
  CranedServiceImpl() = default;

  grpc::Status Configure(grpc::ServerContext *context,
                         const crane::grpc::ConfigureCranedRequest *request,
                         google::protobuf::Empty *response) override;

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

  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::TaskStatusChangeRequest *request,
      crane::grpc::TaskStatusChangeReply *response) override;
};

class CranedServer {
 public:
  explicit CranedServer(const Config::CranedListenConf &listen_conf);

  void Shutdown() { m_server_->Shutdown(); }

  void Wait() { m_server_->Wait(); }

  void SetGrpcSrvReady(bool ready) {
    m_grpc_srv_ready_.store(ready, std::memory_order_release);
  }

  [[nodiscard]] bool ReadyFor(RequestSource request_source) const {
    if (!m_supervisor_recovered_.load(std::memory_order_acquire)) return false;

    if (request_source == RequestSource::CTLD)
      return m_grpc_srv_ready_.load(std::memory_order_acquire);

    return true;
  }

  void FinishSupervisorRecovery() {
    CRANE_DEBUG("Craned finished recover.");
    m_supervisor_recovered_.store(true, std::memory_order_release);
  }

  void FinishRecover() {
    CRANE_DEBUG("Craned finished recover.");
    m_recovered_.store(true, std::memory_order_release);
  }

 private:
  std::unique_ptr<CranedServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  std::atomic_bool m_grpc_srv_ready_{false};

  std::atomic_bool m_supervisor_recovered_{false};

  friend class CranedServiceImpl;
};
}  // namespace Craned

// The initialization of CranedServer requires some parameters.
// We can't use the Singleton pattern here. So we use one global variable.
inline std::unique_ptr<Craned::CranedServer> g_server;
