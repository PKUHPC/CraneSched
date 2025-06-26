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

  grpc::Status ExecuteSteps(grpc::ServerContext *context,
                            const crane::grpc::ExecuteStepsRequest *request,
                            crane::grpc::ExecuteStepsReply *response) override;

  grpc::Status TerminateSteps(
      grpc::ServerContext *context,
      const crane::grpc::TerminateStepsRequest *request,
      crane::grpc::TerminateStepsReply *response) override;

  grpc::Status TerminateOrphanedStep(
      grpc::ServerContext *context,
      const crane::grpc::TerminateOrphanedStepRequest *request,
      crane::grpc::TerminateOrphanedStepReply *response) override;

  grpc::Status QueryStepFromPort(
      grpc::ServerContext *context,
      const crane::grpc::QueryStepFromPortRequest *request,
      crane::grpc::QueryStepFromPortReply *response) override;
      crane::grpc::QueryTaskIdFromPortReply *response) override;

  grpc::Status QuerySshStepEnvVariables(
      grpc::ServerContext *context,
      const ::crane::grpc::QuerySshStepEnvVariablesRequest *request,
      crane::grpc::QuerySshStepEnvVariablesReply *response) override;

  grpc::Status CreateCgroupForJobs(
      grpc::ServerContext *context,
      const crane::grpc::CreateCgroupForJobsRequest *request,
      crane::grpc::CreateCgroupForJobsReply *response) override;

  grpc::Status ReleaseCgroupForJobs(
      grpc::ServerContext *context,
      const crane::grpc::ReleaseCgroupForJobsRequest *request,
      crane::grpc::ReleaseCgroupForJobsReply *response) override;

  grpc::Status ChangeJobTimeLimit(
      grpc::ServerContext *context,
      const crane::grpc::ChangeJobTimeLimitRequest *request,
      crane::grpc::ChangeJobTimeLimitReply *response) override;

  grpc::Status StepStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::StepStatusChangeRequest *request,
      crane::grpc::StepStatusChangeReply *response) override;
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