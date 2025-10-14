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
#include "SupervisorKeeper.h"

#include <protos/Supervisor.grpc.pb.h>

namespace Craned {
using grpc::ClientContext;

CraneErrCode SupervisorStub::ExecuteStep() {
  ClientContext context;
  crane::grpc::supervisor::TaskExecutionRequest request;
  crane::grpc::supervisor::TaskExecutionReply reply;

  auto ok = m_stub_->ExecuteTask(&context, request, &reply);
  if (!ok.ok()) {
    CRANE_ERROR("ExecuteStep failed: reply {},{}", ok.ok(), ok.error_message());
    return CraneErrCode::ERR_RPC_FAILURE;
  }

  return reply.code();
}

CraneExpected<EnvMap> SupervisorStub::QueryStepEnv() {
  ClientContext context;
  crane::grpc::supervisor::QueryStepEnvRequest request;
  crane::grpc::supervisor::QueryStepEnvReply reply;

  auto ok = m_stub_->QueryEnvMap(&context, request, &reply);
  if (ok.ok()) {
    return std::unordered_map(reply.env().begin(), reply.env().end());
  }

  CRANE_ERROR("QueryStepEnv failed: reply {},{}", reply.ok(),
              ok.error_message());
  return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
}

CraneExpected<std::tuple<job_id_t, step_id_t, pid_t, StepStatus>>
SupervisorStub::CheckStatus() {
  ClientContext context;
  crane::grpc::supervisor::CheckStatusRequest request;
  crane::grpc::supervisor::CheckStatusReply reply;

  auto ok = m_stub_->CheckStatus(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return std::make_tuple(reply.job_id(), reply.step_id(),
                           reply.supervisor_pid(), reply.status());
  }

  CRANE_WARN("CheckStatus failed: reply {},{}", reply.ok(), ok.error_message());
  return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
}

CraneErrCode SupervisorStub::TerminateTask(bool mark_as_orphaned,
                                           bool terminated_by_user) {
  ClientContext context;
  crane::grpc::supervisor::TerminateTaskRequest request;
  crane::grpc::supervisor::TerminateTaskReply reply;

  request.set_mark_orphaned(mark_as_orphaned);
  request.set_terminated_by_user(terminated_by_user);

  auto ok = m_stub_->TerminateTask(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return CraneErrCode::SUCCESS;
  }
  CRANE_ERROR("TerminateTask failed: reply {},{}", reply.ok(),
              ok.error_message());

  CRANE_WARN("TerminateTask failed: reply {},{}", reply.ok(),
             ok.error_message());
  return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorStub::ChangeTaskTimeConstraint(
    std::optional<absl::Duration> time_limit,
    std::optional<int64_t> deadline_time) {
  ClientContext context;
  crane::grpc::supervisor::ChangeTaskTimeConstraintRequest request;
  crane::grpc::supervisor::ChangeTaskTimeConstraintReply reply;

  if (time_limit) {
    request.set_time_limit_seconds(absl::ToInt64Seconds(time_limit.value()));
  }

  if (deadline_time) {
    request.set_deadline_time(deadline_time.value());
  }

  auto ok = m_stub_->ChangeTaskTimeConstraint(&context, request, &reply);
  if (ok.ok() && reply.ok()) return CraneErrCode::SUCCESS;

  CRANE_WARN("ChangeTaskTimeLimit failed: reply {},{}", reply.ok(),
             ok.error_message());
  return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorStub::ShutdownSupervisor() {
  ClientContext context;
  crane::grpc::supervisor::ShutdownSupervisorRequest request;
  crane::grpc::supervisor::ShutdownSupervisorReply reply;

  auto ok = m_stub_->ShutdownSupervisor(&context, request, &reply);
  if (ok.ok()) return CraneErrCode::SUCCESS;

  CRANE_WARN("ShutdownSupervisor failed: ok,{}", ok.error_message());
  return CraneErrCode::ERR_RPC_FAILURE;
}

void SupervisorStub::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::supervisor::Supervisor::NewStub(m_channel_);
}

CraneExpected<absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                                  std::pair<pid_t, StepStatus>>>
SupervisorKeeper::InitAndGetRecoveredMap() {
  static constexpr LazyRE2 supervisor_sock_pattern(
      R"(step_(\d+)\.(\d+)\.sock$)");
  try {
    std::filesystem::path path = kDefaultSupervisorUnixSockDir;
    if (!std::filesystem::exists(path) ||
        !std::filesystem::is_directory(path)) {
      CRANE_WARN("Supervisor socket dir doesn't not exists. Skip recovery.");
      return {};
    }

    std::vector<std::filesystem::path> files;
    for (const auto& it : std::filesystem::directory_iterator(path)) {
      if (std::filesystem::is_socket(it.path()) &&
          RE2::PartialMatch(it.path().c_str(), *supervisor_sock_pattern))
        files.emplace_back(it.path());
    }

    absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                        std::pair<pid_t, StepStatus>>
        supervisor_status_map;
    supervisor_status_map.reserve(files.size());
    std::latch latch(files.size());
    for (const auto& file : files) {
      g_thread_pool->detach_task([this, file, &latch, &supervisor_status_map] {
        auto sock_path = fmt::format("unix://{}", file.string());
        std::shared_ptr stub = std::make_shared<SupervisorStub>();
        stub->InitChannelAndStub(sock_path);

        CraneExpected<std::tuple<job_id_t, step_id_t, pid_t, StepStatus>>
            supv_ids = stub->CheckStatus();
        if (!supv_ids) {
          CRANE_ERROR("CheckTaskStatus for {} failed, removing it.",
                      file.string());
          std::filesystem::remove(file);
          latch.count_down();
          return;
        }
        auto [job_id, step_id, pid, status] = supv_ids.value();
        CRANE_DEBUG("[Step #{}.{}] Supervisor socket {} recovered, pid: {}",
                    job_id, step_id, file.string(), pid);

        absl::WriterMutexLock lk(&m_mutex_);
        m_supervisor_map_.emplace(std::make_pair(job_id, step_id), stub);
        supervisor_status_map.emplace(std::make_pair(job_id, step_id),
                                      std::make_pair(pid, status));

        latch.count_down();
      });
    }

    latch.wait();
    return supervisor_status_map;

  } catch (const std::exception& e) {
    CRANE_ERROR("An error occurred when recovering supervisor: {}", e.what());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }
}

void SupervisorKeeper::AddSupervisor(job_id_t job_id, step_id_t step_id) {
  auto sock_path = fmt::format("unix://{}/step_{}.{}.sock",
                               kDefaultSupervisorUnixSockDir, job_id, step_id);

  std::shared_ptr stub = std::make_shared<SupervisorStub>();
  stub->InitChannelAndStub(sock_path);

  absl::WriterMutexLock lk(&m_mutex_);
  if (auto it = m_supervisor_map_.find({job_id, step_id});
      it != m_supervisor_map_.end()) {
    CRANE_ERROR("[Step #{}.{}] Duplicate supervisor", job_id, step_id);
    return;
  }

  m_supervisor_map_.emplace(std::make_pair(job_id, step_id), stub);
}

void SupervisorKeeper::RemoveSupervisor(job_id_t job_id, step_id_t step_id) {
  absl::WriterMutexLock lk(&m_mutex_);
  if (auto it = m_supervisor_map_.find({job_id, step_id});
      it != m_supervisor_map_.end()) {
    CRANE_TRACE("[Step #{}.{}] Removing supervisor", job_id, step_id);
    m_supervisor_map_.erase(it);
  } else {
    CRANE_ERROR("[Step #{}.{}] Try to remove non-existent supervisor", job_id,
                step_id);
  }
}

std::shared_ptr<SupervisorStub> SupervisorKeeper::GetStub(job_id_t job_id,
                                                          step_id_t step_id) {
  absl::ReaderMutexLock lk(&m_mutex_);
  if (auto it = m_supervisor_map_.find({job_id, step_id});
      it != m_supervisor_map_.end()) {
    return it->second;
  }

  return nullptr;
}

std::set<std::pair<job_id_t, step_id_t>> SupervisorKeeper::GetRunningSteps() {
  absl::ReaderMutexLock lk(&m_mutex_);
  return m_supervisor_map_ | std::views::keys |
         std::ranges::to<std::set<std::pair<job_id_t, step_id_t>>>();
}

}  // namespace Craned