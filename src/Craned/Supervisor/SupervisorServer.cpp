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

grpc::Status Supervisor::SupervisorServiceImpl::ExecuteTask(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::TaskExecutionRequest* request,
    crane::grpc::supervisor::TaskExecutionReply* response) {
  std::future<CraneExpected<pid_t>> pid_future =
      g_task_mgr->ExecuteTaskAsync(request->task());
  pid_future.wait();

  auto pid = pid_future.get();
  if (pid.has_value()) {
    response->set_ok(true);
    response->set_pid(pid.value());
  } else {
    response->set_ok(false);
  }
  return Status::OK;
}

grpc::Status Supervisor::SupervisorServiceImpl::CheckTaskStatus(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::CheckTaskStatusRequest* request,
    crane::grpc::supervisor::CheckTaskStatusReply* response) {
  auto pid_future = g_task_mgr->CheckTaskStatusAsync();
  pid_future.wait();
  auto pid_expt = pid_future.get();
  if (pid_expt.has_value()) {
    response->set_job_id(g_config.JobId);
    response->set_pid(pid_expt.value());
    response->set_ok(true);
    return Status::OK;
  } else {
    response->set_ok(false);
    return Status::OK;
  }
}

grpc::Status Supervisor::SupervisorServiceImpl::ChangeTaskTimeLimit(
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

grpc::Status Supervisor::SupervisorServiceImpl::TerminateTask(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::TerminateTaskRequest* request,
    crane::grpc::supervisor::TerminateTaskReply* response) {
  g_task_mgr->TerminateTaskAsync(request->mark_orphaned(),
                                 request->terminated_by_user());
  response->set_ok(true);
  return Status::OK;
}

Supervisor::SupervisorServer::SupervisorServer() {
  m_service_impl_ = std::make_unique<SupervisorServiceImpl>();

  auto unix_socket_path = fmt::format(
      "unix://{}/task_{}.sock", kDefaultSupervisorUnixSockDir, g_config.JobId);
  grpc::ServerBuilder builder;
  ServerBuilderAddUnixInsecureListeningPort(&builder, unix_socket_path);
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
}
