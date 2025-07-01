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

#include "SupervisorServer.h"

#include "TaskManager.h"

namespace Supervisor {

grpc::Status SupervisorServiceImpl::ExecuteTask(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::TaskExecutionRequest* request,
    crane::grpc::supervisor::TaskExecutionReply* response) {
  std::future<CraneExpected<pid_t>> pid_future = g_task_mgr->ExecuteTaskAsync();
  pid_future.wait();

  CraneExpected<pid_t> pid = pid_future.get();
  if (pid.has_value()) {
    response->set_ok(true);
    response->set_pid(pid.value());
  } else {
    response->set_ok(false);
  }
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::QueryEnvMap(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::QueryStepEnvRequest* request,
    crane::grpc::supervisor::QueryStepEnvReply* response) {
  auto env_future = g_task_mgr->QueryStepEnvAsync();
  std::expected env_expt = env_future.get();
  if (env_expt.has_value()) {
    auto* grep_env = response->mutable_env();
    for (auto& [key, value] : env_expt.value()) {
      (*grep_env)[key] = value;
    }
  }
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::CheckStatus(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::CheckStatusRequest* request,
    crane::grpc::supervisor::CheckStatusReply* response) {
  response->set_job_id(g_config.JobId);
  response->set_supervisor_pid(getpid());
  response->set_ok(true);
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::ChangeTaskTimeLimit(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::ChangeTaskTimeLimitRequest* request,
    crane::grpc::supervisor::ChangeTaskTimeLimitReply* response) {
  auto ok = g_task_mgr->ChangeTaskTimeLimitAsync(
      absl::Seconds(request->time_limit_seconds()));
  ok.wait();
  if (ok.get() != CraneErrCode::SUCCESS) {
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::TerminateTask(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::TerminateTaskRequest* request,
    crane::grpc::supervisor::TerminateTaskReply* response) {
  g_task_mgr->TerminateTaskAsync(request->mark_orphaned(),
                                 request->terminated_by_user());
  response->set_ok(true);
  return Status::OK;
}

SupervisorServer::SupervisorServer() {
  m_service_impl_ = std::make_unique<SupervisorServiceImpl>();

  std::filesystem::path supervisor_sock_path =
      std::filesystem::path(kDefaultSupervisorUnixSockDir) /
      fmt::format("task_{}.sock", g_config.JobId);
  auto unix_socket_path = fmt::format("unix://{}", supervisor_sock_path);
  grpc::ServerBuilder builder;
  ServerBuilderAddUnixInsecureListeningPort(&builder, unix_socket_path);
  builder.RegisterService(m_service_impl_.get());

  chmod(supervisor_sock_path.c_str(), 0600);
  m_server_ = builder.BuildAndStart();
}
}  // namespace Supervisor
