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

#include <shared_mutex>

#include "CommonPublicDefs.h"
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

enum class RequestSource : std::int8_t { CTLD = 0, PAM, SUPERVISOR, INVALID };

enum class CranedStatus : std::int8_t {
  INITIALIZING = 0,
  RUNNING,
  RECONFIGURING,
  CLEANING,
  STOPPING,
};

class CranedServiceImpl : public Craned::Service {
 public:
  CranedServiceImpl() = default;

  grpc::Status Configure(grpc::ServerContext *context,
                         const crane::grpc::ConfigureCranedRequest *request,
                         google::protobuf::Empty *response) override;

  grpc::Status UpdateTraceConfig(
      grpc::ServerContext *context,
      const crane::grpc::UpdateTraceConfigRequest *request,
      crane::grpc::UpdateTraceConfigReply *response) override;

  grpc::Status ExecuteSteps(grpc::ServerContext *context,
                            const crane::grpc::ExecuteStepsRequest *request,
                            crane::grpc::ExecuteStepsReply *response) override;

  grpc::Status TerminateSteps(
      grpc::ServerContext *context,
      const crane::grpc::TerminateStepsRequest *request,
      crane::grpc::TerminateStepsReply *response) override;

  grpc::Status QueryStepFromPort(
      grpc::ServerContext *context,
      const crane::grpc::QueryStepFromPortRequest *request,
      crane::grpc::QueryStepFromPortReply *response) override;

  grpc::Status QuerySshStepEnvVariables(
      grpc::ServerContext *context,
      const ::crane::grpc::QuerySshStepEnvVariablesRequest *request,
      crane::grpc::QuerySshStepEnvVariablesReply *response) override;

  grpc::Status AllocJobs(grpc::ServerContext *context,
                         const crane::grpc::AllocJobsRequest *request,
                         crane::grpc::AllocJobsReply *response) override;

  grpc::Status FreeJobs(grpc::ServerContext *context,
                        const crane::grpc::FreeJobsRequest *request,
                        crane::grpc::FreeJobsReply *response) override;

  grpc::Status AllocSteps(grpc::ServerContext *context,
                          const crane::grpc::AllocStepsRequest *request,
                          crane::grpc::AllocStepsReply *response) override;

  grpc::Status FreeSteps(grpc::ServerContext *context,
                         const crane::grpc::FreeStepsRequest *request,
                         crane::grpc::FreeStepsReply *response) override;

  grpc::Status ChangeJobTimeConstraint(
      grpc::ServerContext *context,
      const crane::grpc::ChangeJobTimeConstraintRequest *request,
      crane::grpc::ChangeJobTimeConstraintReply *response) override;

  grpc::Status SuspendJobs(grpc::ServerContext *context,
                           const crane::grpc::SuspendJobsRequest *request,
                           crane::grpc::SuspendJobsReply *response) override;

  grpc::Status ResumeJobs(grpc::ServerContext *context,
                          const crane::grpc::ResumeJobsRequest *request,
                          crane::grpc::ResumeJobsReply *response) override;

  grpc::Status StepStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::StepStatusChangeRequest *request,
      crane::grpc::StepStatusChangeReply *response) override;

  grpc::Status AttachContainerStep(
      grpc::ServerContext *context,
      const crane::grpc::AttachContainerStepRequest *request,
      crane::grpc::AttachContainerStepReply *response) override;

  grpc::Status ExecInContainerStep(
      grpc::ServerContext *context,
      const crane::grpc::ExecInContainerStepRequest *request,
      crane::grpc::ExecInContainerStepReply *response) override;

  grpc::Status BroadcastPmixPort(
      grpc::ServerContext *context,
      const crane::grpc::BroadcastPmixPortRequest *request,
      crane::grpc::BroadcastPmixPortReply *response) override;

  grpc::Status ReceivePmixPort(
      grpc::ServerContext *context,
      const crane::grpc::ReceivePmixPortRequest *request,
      crane::grpc::ReceivePmixPortReply *response) override;
};

class CranedServer {
 public:
  void Start(const Config::CranedListenConf &listen_conf);

  // Hold a shared lease for the full duration of each work RPC. Status
  // reports use the JobManager inbox, which stays open through cleanup.
  [[nodiscard]] std::shared_lock<std::shared_mutex> AdmitWork(
      std::optional<RequestSource> source = std::nullopt) {
    if (m_work_closed_.load()) return {};
    std::shared_lock lock(m_work_mutex_);
    if (m_work_closed_ || (source && !ReadyFor(*source))) lock.unlock();
    return lock;
  }

  void CloseWorkAdmission() {
    m_work_closed_ = true;
    // Synchronize with work already admitted. New arrivals cannot delay drain.
    std::unique_lock lock(m_work_mutex_);
  }

  void Shutdown() {
    m_status_ = CranedStatus::STOPPING;
    m_server_->Shutdown(std::chrono::system_clock::now() +
                        std::chrono::seconds(1));
  }

  void Wait() { m_server_->Wait(); }

  void SetGrpcSrvReady(bool ready) {
    CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger,
                       "SetGrpcSrvReady to {}, current {}", ready,
                       static_cast<std::uint8_t>(m_status_.load()));
    if (m_status_ == CranedStatus::STOPPING) {
      CRANE_LOGGER_DEBUG(
          g_runtime_status.conn_logger,
          "Craned is stopping, SetGrpcSrvReady to {} not applied.", ready);
      return;
    }
    if (ready) {
      CRANE_ASSERT_MSG(m_status_ == CranedStatus::INITIALIZING ||
                           m_status_ == CranedStatus::RECONFIGURING,
                       fmt::format("m_status_={}", static_cast<std::uint8_t>(
                                                       m_status_.load())));
      m_status_ = CranedStatus::RUNNING;
    } else {
      if (m_status_ == CranedStatus::RUNNING) {
        CRANE_LOGGER_TRACE(
            g_runtime_status.conn_logger,
            "CraneCtld disconnected, craned server status set to "
            "RECONFIGURING, prev status RUNNING.");
        m_status_ = CranedStatus::RECONFIGURING;
      } else {
        CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                           "CraneCtld disconnected, current status {}.",
                           static_cast<std::uint8_t>(m_status_.load()));
        CRANE_ASSERT(m_status_ == CranedStatus::INITIALIZING ||
                     m_status_ == CranedStatus::RECONFIGURING);
        // INITIALIZING or RECONFIGURING;
      }
    }
  }

  [[nodiscard]] bool ReadyFor(RequestSource request_source) const {
    CRANE_ASSERT(request_source != RequestSource::INVALID);
    if (m_status_ == CranedStatus::INITIALIZING ||
        m_status_ == CranedStatus::STOPPING)
      return false;
    if (m_status_ == CranedStatus::RECONFIGURING)
      // Only PAM can send requests during recovery or stopping.
      return request_source == RequestSource::PAM ||
             request_source == RequestSource::SUPERVISOR;

    return true;
  }

  // Local restore can finish before registration. Its supervisors may be
  // completing work when a stop request arrives during that interval.
  [[nodiscard]] bool AcceptsStepStatus() const {
    return ReadyFor(RequestSource::SUPERVISOR) ||
           (m_work_closed_ &&
            m_supervisor_recovered_.load(std::memory_order_acquire));
  }

  void MarkSupervisorAsRecovered() {
    CRANE_DEBUG("Craned finished recover.");
    m_supervisor_recovered_.store(true, std::memory_order_release);
  }

 private:
  std::shared_mutex m_work_mutex_;
  std::atomic_bool m_work_closed_{false};
  std::unique_ptr<CranedServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  std::atomic<CranedStatus> m_status_{CranedStatus::INITIALIZING};

  std::atomic_bool m_grpc_srv_ready_{false};

  std::atomic_bool m_supervisor_recovered_{false};

  friend class CranedServiceImpl;
};
}  // namespace Craned

// The initialization of CranedServer requires some parameters.
// We can't use the Singleton pattern here. So we use one global variable.
inline std::unique_ptr<Craned::CranedServer> g_server;
