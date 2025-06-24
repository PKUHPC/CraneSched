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

CraneExpected<pid_t> SupervisorStub::ExecuteTask() {
  ClientContext context;
  crane::grpc::supervisor::TaskExecutionRequest request;
  crane::grpc::supervisor::TaskExecutionReply reply;

  auto ok = m_stub_->ExecuteTask(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return reply.pid();
  }

  return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
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

CraneExpected<std::pair<task_id_t, pid_t>> SupervisorStub::CheckStatus() {
  ClientContext context;
  crane::grpc::supervisor::CheckStatusRequest request;
  crane::grpc::supervisor::CheckStatusReply reply;

  auto ok = m_stub_->CheckStatus(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return std::pair{reply.job_id(), reply.supervisor_pid()};
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

  return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorStub::ChangeTaskTimeLimit(absl::Duration time_limit) {
  ClientContext context;
  crane::grpc::supervisor::ChangeTaskTimeLimitRequest request;
  crane::grpc::supervisor::ChangeTaskTimeLimitReply reply;

  request.set_time_limit_seconds(absl::ToInt64Seconds(time_limit));
  auto ok = m_stub_->ChangeTaskTimeLimit(&context, request, &reply);
  if (ok.ok() && reply.ok()) return CraneErrCode::SUCCESS;

  return CraneErrCode::ERR_RPC_FAILURE;
}

void SupervisorStub::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::supervisor::Supervisor::NewStub(m_channel_);
}

CraneExpected<std::unordered_map<task_id_t, pid_t>> SupervisorKeeper::Init() {
  try {
    std::filesystem::path path = kDefaultSupervisorUnixSockDir;
    if (!std::filesystem::exists(path) ||
        !std::filesystem::is_directory(path)) {
      CRANE_WARN("Supervisor socket dir doesn't not exists. Skip recovery.");
      return {};
    }

    std::vector<std::filesystem::path> files;
    for (const auto& it : std::filesystem::directory_iterator(path)) {
      if (std::filesystem::is_socket(it.path())) files.emplace_back(it.path());
    }

    std::unordered_map<task_id_t, pid_t> supervisor_pid;
    supervisor_pid.reserve(files.size());

    std::latch latch(files.size());
    for (const auto& file : files) {
      g_thread_pool->detach_task([this, file, &latch, &supervisor_pid] {
        auto sock_path = fmt::format("unix://{}", file.string());
        std::shared_ptr stub = std::make_shared<SupervisorStub>();
        stub->InitChannelAndStub(sock_path);

        CraneExpected<std::pair<task_id_t, pid_t>> supervisor_status =
            stub->CheckStatus();
        if (!supervisor_status) {
          CRANE_ERROR("CheckTaskStatus for {} failed, removing it.",
                      file.string());
          std::filesystem::remove(file);
          latch.count_down();
          return;
        }

        absl::WriterMutexLock lk(&m_mutex_);
        m_supervisor_map_.emplace(supervisor_status.value().first, stub);
        supervisor_pid.emplace(supervisor_status.value());

        latch.count_down();
      });
    }

    latch.wait();
    return supervisor_pid;

  } catch (const std::exception& e) {
    CRANE_ERROR("An error occurred when recovering supervisor: {}", e.what());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }
}

void SupervisorKeeper::AddSupervisor(task_id_t task_id) {
  auto sock_path = fmt::format("unix://{}/task_{}.sock",
                               kDefaultSupervisorUnixSockDir, task_id);

  std::shared_ptr stub = std::make_shared<SupervisorStub>();
  stub->InitChannelAndStub(sock_path);

  absl::WriterMutexLock lk(&m_mutex_);
  if (auto it = m_supervisor_map_.find(task_id);
      it != m_supervisor_map_.end()) {
    CRANE_ERROR("Duplicate supervisor for task #{}", task_id);
    return;
  }

  m_supervisor_map_.emplace(task_id, stub);
}

void SupervisorKeeper::RemoveSupervisor(task_id_t task_id) {
  absl::WriterMutexLock lk(&m_mutex_);
  if (auto it = m_supervisor_map_.find(task_id);
      it != m_supervisor_map_.end()) {
    CRANE_TRACE("Removing supervisor for task #{}", task_id);
    m_supervisor_map_.erase(it);
  } else {
    CRANE_ERROR("Try to remove non-existent supervisor for task #{}", task_id);
  }
}

std::shared_ptr<SupervisorStub> SupervisorKeeper::GetStub(task_id_t task_id) {
  absl::ReaderMutexLock lk(&m_mutex_);
  if (auto it = m_supervisor_map_.find(task_id);
      it != m_supervisor_map_.end()) {
    return it->second;
  }

  return nullptr;
}

std::set<task_id_t> SupervisorKeeper::GetRunningSteps() {
  absl::ReaderMutexLock lk(&m_mutex_);
  return m_supervisor_map_ | std::views::keys |
         std::ranges::to<std::set<task_id_t>>();
}

}  // namespace Craned