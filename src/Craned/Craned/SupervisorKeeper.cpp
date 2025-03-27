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

CraneExpected<pid_t> SupervisorClient::ExecuteTask(
    const crane::grpc::TaskToD& task) {
  ClientContext context;
  crane::grpc::supervisor::TaskExecutionRequest request;
  crane::grpc::supervisor::TaskExecutionReply reply;
  *request.mutable_task() = task;
  auto ok = m_stub_->ExecuteTask(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return reply.pid();
  } else {
    return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
  }
}

CraneExpected<std::pair<task_id_t, pid_t>> SupervisorClient::CheckTaskStatus() {
  ClientContext context;
  crane::grpc::supervisor::CheckTaskStatusRequest request;
  crane::grpc::supervisor::CheckTaskStatusReply reply;
  auto ok = m_stub_->CheckTaskStatus(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return std::pair{reply.job_id(), reply.pid()};
  } else {
    CRANE_WARN("CheckTaskStatus failed: reply {},{}", reply.ok(),ok.error_message());
    return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
  }
}

CraneErrCode SupervisorClient::TerminateTask(bool mark_as_orphaned,
                                             bool terminated_by_user) {
  ClientContext context;
  crane::grpc::supervisor::TerminateTaskRequest request;
  crane::grpc::supervisor::TerminateTaskReply reply;
  request.set_mark_orphaned(mark_as_orphaned);
  request.set_terminated_by_user(terminated_by_user);
  auto ok = m_stub_->TerminateTask(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return CraneErrCode::SUCCESS;
  } else
    return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorClient::ChangeTaskTimeLimit(absl::Duration time_limit) {
  ClientContext context;
  crane::grpc::supervisor::ChangeTaskTimeLimitRequest request;
  crane::grpc::supervisor::ChangeTaskTimeLimitReply reply;
  request.set_time_limit_seconds(absl::ToInt64Seconds(time_limit));
  auto ok = m_stub_->ChangeTaskTimeLimit(&context, request, &reply);
  if (ok.ok() && reply.ok())
    return CraneErrCode::SUCCESS;
  else
    return CraneErrCode::ERR_RPC_FAILURE;
}

void SupervisorClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::supervisor::Supervisor::NewStub(m_channel_);
}

CraneExpected<std::unordered_map<task_id_t, pid_t>> SupervisorKeeper::Init() {
  try {
    std::filesystem::path path = kDefaultSupervisorUnixSockDir;
    if (std::filesystem::exists(path) && std::filesystem::is_directory(path)) {
      std::vector<std::filesystem::path> files;
      for (const auto& it : std::filesystem::directory_iterator(path)) {
        if (std::filesystem::is_socket(it.path())) {
          files.emplace_back(it.path());
        }
      }
      std::unordered_map<task_id_t, pid_t> tasks;
      tasks.reserve(files.size());
      std::latch all_supervisor_reply(files.size());
      absl::Mutex mtx;
      for (const auto& file : files) {
        g_thread_pool->detach_task(
            [this, file, &all_supervisor_reply, &mtx, &tasks]() {
              std::shared_ptr stub = std::make_shared<SupervisorClient>();
              auto sock_path = fmt::format("unix://{}", file.string());
              stub->InitChannelAndStub(sock_path);
              CraneExpected<std::pair<task_id_t, pid_t>> task_status =
                  stub->CheckTaskStatus();
              if (!task_status) {
                CRANE_ERROR(
                    "CheckTaskStatus for {} failed, removing it.",
                    file.string());
                std::filesystem::remove(file);
                all_supervisor_reply.count_down();
                return;
              }
              absl::WriterMutexLock lk(&mtx);
              m_supervisor_map.emplace(task_status.value().first, stub);
              tasks.emplace(task_status.value());
              all_supervisor_reply.count_down();
            });
      }
      all_supervisor_reply.wait();
      return tasks;
    } else {
      CRANE_WARN("Supervisor socket dir dose not exit, skip recovery.");
      return {};
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Error: {}, when recover supervisor", e.what());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }
}

void SupervisorKeeper::AddSupervisor(task_id_t task_id) {
  auto sock_path = fmt::format("unix://{}/task_{}.sock",
                               kDefaultSupervisorUnixSockDir, task_id);
  std::shared_ptr stub = std::make_shared<SupervisorClient>();
  stub->InitChannelAndStub(sock_path);
  absl::WriterMutexLock lk(&m_mutex);
  if (auto it = m_supervisor_map.find(task_id); it != m_supervisor_map.end()) {
    CRANE_ERROR("Duplicate supervisor for task #{}", task_id);
    return;
  }
  m_supervisor_map.emplace(task_id, stub);
}

void SupervisorKeeper::RemoveSupervisor(task_id_t task_id) {
  absl::WriterMutexLock lk(&m_mutex);
  if (auto it = m_supervisor_map.find(task_id); it != m_supervisor_map.end()) {
    CRANE_TRACE("Removing supervisor for task #{}", task_id);
    m_supervisor_map.erase(it);
  } else {
    CRANE_ERROR("Try to remove nonexistent supervisor for task #{}", task_id);
  }
}

std::shared_ptr<SupervisorClient> SupervisorKeeper::GetStub(task_id_t task_id) {
  absl::ReaderMutexLock lk(&m_mutex);
  if (auto it = m_supervisor_map.find(task_id); it != m_supervisor_map.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

}  // namespace Craned