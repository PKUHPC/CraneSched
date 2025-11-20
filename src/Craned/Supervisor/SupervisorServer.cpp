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

#include "CranedClient.h"
#include "TaskManager.h"
#include "crane/PublicHeader.h"

namespace Craned::Supervisor {

grpc::Status SupervisorServiceImpl::ExecuteTask(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::TaskExecutionRequest* request,
    crane::grpc::supervisor::TaskExecutionReply* response) {
  CRANE_ASSERT(g_config.StepSpec.step_type() != crane::grpc::StepType::DAEMON);
  std::future<CraneErrCode> code_future = g_task_mgr->ExecuteTaskAsync();
  code_future.wait();

  CraneErrCode ok = code_future.get();
  response->set_code(ok);

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
  response->set_step_id(g_config.StepId);
  response->set_supervisor_pid(getpid());
  response->set_status(g_runtime_status.Status);
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
                                 request->terminated_by_user()
                                     ? TerminatedBy::CANCELLED_BY_USER
                                     : TerminatedBy::NONE);
  response->set_ok(true);
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::SuspendJob(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::SuspendJobRequest* request,
    crane::grpc::supervisor::SuspendJobReply* response) {
  auto future = g_task_mgr->SuspendJobAsync();
  future.wait();
  CraneErrCode err = future.get();
  if (err == CraneErrCode::SUCCESS) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(std::string(CraneErrStr(err)));
  }
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::ResumeJob(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::ResumeJobRequest* request,
    crane::grpc::supervisor::ResumeJobReply* response) {
  auto future = g_task_mgr->ResumeJobAsync();
  future.wait();
  CraneErrCode err = future.get();
  if (err == CraneErrCode::SUCCESS) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(std::string(CraneErrStr(err)));
  }
  return Status::OK;
}

grpc::Status SupervisorServiceImpl::ShutdownSupervisor(
    grpc::ServerContext* context,
    const crane::grpc::supervisor::ShutdownSupervisorRequest* request,
    crane::grpc::supervisor::ShutdownSupervisorReply* response) {
  CRANE_INFO("Grpc shutdown request received.");
  g_thread_pool->detach_task([] { g_task_mgr->ShutdownSupervisor(); });
  return Status::OK;
}

SupervisorServer::SupervisorServer() {
  m_service_impl_ = std::make_unique<SupervisorServiceImpl>();

  auto unix_socket_path =
      fmt::format("unix://{}", g_config.SupervisorUnixSockPath);
  grpc::ServerBuilder builder;
  ServerBuilderAddUnixInsecureListeningPort(&builder, unix_socket_path);
  builder.RegisterService(m_service_impl_.get());

  chmod(g_config.SupervisorUnixSockPath.c_str(), 0600);
  m_server_ = builder.BuildAndStart();
}
}  // namespace Craned::Supervisor
