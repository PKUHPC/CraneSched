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

#include "CranedServer.h"

#include <yaml-cpp/yaml.h>

#include <unordered_map>

#include "CgroupManager.h"
#include "CranedForPamServer.h"
#include "CranedPublicDefs.h"
#include "CtldClient.h"
#include "JobManager.h"
#include "SupervisorKeeper.h"
#include "crane/CriClient.h"
#include "crane/String.h"

namespace Craned {

grpc::Status CranedServiceImpl::Configure(
    grpc::ServerContext *context,
    const crane::grpc::ConfigureCranedRequest *request,
    google::protobuf::Empty *response) {
  bool ok = g_ctld_client_sm->EvRecvConfigFromCtld(*request);

  CRANE_TRACE("Recv Configure RPC from Ctld. Configuration result: {}", ok);
  return Status::OK;
}

grpc::Status CranedServiceImpl::ExecuteSteps(
    grpc::ServerContext *context,
    const crane::grpc::ExecuteStepsRequest *request,
    crane::grpc::ExecuteStepsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps_map;
  for (const auto &[job_id, steps] : request->job_step_ids_map()) {
    job_steps_map[job_id].insert(steps.steps().begin(), steps.steps().end());
  }

  CRANE_INFO("Receive ExecuteSteps for steps [{}]",
             util::JobStepsToString(job_steps_map));
  g_job_mgr->ExecuteStepAsync(std::move(job_steps_map));

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateSteps(
    grpc::ServerContext *context,
    const crane::grpc::TerminateStepsRequest *request,
    crane::grpc::TerminateStepsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_reason("CranedServer is not ready");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps_map;
  for (const auto &[job_id, steps] : request->job_step_ids_map()) {
    job_steps_map[job_id].insert(steps.steps().begin(), steps.steps().end());
  }
  CRANE_TRACE("Receive TerminateSteps for steps [{}]",
              util::JobStepsToString(job_steps_map));

  for (const auto [job_id, steps] : job_steps_map)
    for (const auto step_id : steps)
      g_job_mgr->TerminateStepAsync(job_id, step_id);
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateOrphanedStep(
    grpc::ServerContext *context,
    const crane::grpc::TerminateOrphanedStepRequest *request,
    crane::grpc::TerminateOrphanedStepReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_reason("CranedServer is not ready");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }
  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps_map;
  for (const auto &[job_id, steps] : request->job_step_ids_map()) {
    job_steps_map[job_id].insert(steps.steps().begin(), steps.steps().end());
  }
  CRANE_TRACE("Receive TerminateOrphanedStep for steps [{}]",
              util::JobStepsToString(job_steps_map));

  for (const auto [job_id, steps] : job_steps_map)
    for (const auto step_id : steps)
      g_job_mgr->MarkStepAsOrphanedAndTerminateAsync(job_id, step_id);

  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryStepFromPort(
    grpc::ServerContext *context,
    const crane::grpc::QueryStepFromPortRequest *request,
    crane::grpc::QueryStepFromPortReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  CRANE_TRACE("Receive QueryStepFromPort RPC from {}: port: {}",
              context->peer(), request->port());

  ino_t inode;
  bool inode_found = false;

  // find inode
  // 1._find_match_in_tcp_file
  inode_found =
      crane::FindTcpInodeByPort("/proc/net/tcp", request->port(), &inode);
  if (!inode_found) {
    CRANE_TRACE(
        "Inode num for port {} is not found in /proc/net/tcp, try "
        "/proc/net/tcp6.",
        request->port());
    inode_found =
        crane::FindTcpInodeByPort("/proc/net/tcp6", request->port(), &inode);
  }

  if (!inode_found) {
    CRANE_TRACE("Inode num for port {} is not found.", request->port());
    response->set_ok(false);
    return Status::OK;
  }

  // 2.find_pid_by_inode
  pid_t pid_i = -1;
  std::filesystem::path proc_path{"/proc"};
  for (auto const &dir_entry : std::filesystem::directory_iterator(proc_path)) {
    if (isdigit(dir_entry.path().filename().string()[0])) {
      std::string pid_s = dir_entry.path().filename().string();
      std::string proc_fd_path =
          fmt::format("{}/{}/fd", proc_path.string(), pid_s);
      if (!std::filesystem::exists(proc_fd_path)) {
        continue;
      }
      for (auto const &fd_dir_entry :
           std::filesystem::directory_iterator(proc_fd_path)) {
        struct stat statbuf{};
        std::string fdpath = fmt::format(
            "{}/{}", proc_fd_path, fd_dir_entry.path().filename().string());
        const char *fdchar = fdpath.c_str();
        if (stat(fdchar, &statbuf) != 0) {
          continue;
        }
        if (statbuf.st_ino == inode) {
          pid_i = std::stoi(pid_s);
          CRANE_TRACE("Pid for the process that owns port {} is {}",
                      request->port(), pid_i);
          break;
        }
      }
    }
    if (pid_i != -1) {
      break;
    }
  }
  if (pid_i == -1) {
    CRANE_TRACE("Pid for the process that owns port {} is not found.",
                request->port());
    response->set_ok(false);
    return Status::OK;
  }

  // 3. pid2jobid
  do {
    auto pid_to_ids_expt = CgroupManager::GetIdsByPid(pid_i);
    if (pid_to_ids_expt.has_value()) {
      auto [job_id_opt, step_id_opt, task_id_opt] = pid_to_ids_expt.value();

      // FIXME: Use step_id_opt after multi-step is supported.
      CRANE_ASSERT_MSG(job_id_opt.has_value(),
                       "Job ID always has value when RE2 matches.");

      response->set_ok(true);
      response->set_job_id(job_id_opt.value());
      return Status::OK;
    }

    std::string proc_dir = fmt::format("/proc/{}/status", pid_i);
    YAML::Node proc_details = YAML::LoadFile(proc_dir);
    if (proc_details["PPid"]) {
      pid_t ppid = std::stoi(proc_details["PPid"].as<std::string>());
      CRANE_TRACE("Pid {} not found in TaskManager. Checking ppid {}", pid_i,
                  ppid);
      pid_i = ppid;
    } else {
      CRANE_TRACE(
          "Pid {} not found in TaskManager. "
          "However ppid is 1. Break the loop.",
          pid_i);
      pid_i = 1;
    }

  } while (pid_i > 1);

  response->set_ok(false);
  return Status::OK;
}

grpc::Status CranedServiceImpl::AllocJobs(
    grpc::ServerContext *context, const crane::grpc::AllocJobsRequest *request,
    crane::grpc::AllocJobsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  std::vector<JobInD> jobs;
  for (const auto &job_to_d : request->jobs()) {
    CRANE_INFO("Allocating job #{}, uid {}", job_to_d.job_id(), job_to_d.uid());
    jobs.emplace_back(job_to_d);
  }

  bool ok = g_job_mgr->AllocJobs(std::move(jobs));
  if (!ok) {
    CRANE_ERROR("Failed to alloc some jobs.");
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::AllocSteps(
    grpc::ServerContext *context, const crane::grpc::AllocStepsRequest *request,
    crane::grpc::AllocStepsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  g_job_mgr->AllocSteps(request->steps() | std::ranges::to<std::vector>());
  return Status::OK;
}

grpc::Status CranedServiceImpl::FreeSteps(
    grpc::ServerContext *context, const crane::grpc::FreeStepsRequest *request,
    crane::grpc::FreeStepsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps_map;
  for (const auto &[job_id, steps] : request->job_step_ids_map()) {
    job_steps_map[job_id].insert(steps.steps().begin(), steps.steps().end());
  }
  CRANE_TRACE("Receive FreeSteps RPC for [{}]",
              util::JobStepsToString(job_steps_map));
  g_job_mgr->FreeSteps(std::move(job_steps_map));

  return Status::OK;
}

grpc::Status CranedServiceImpl::FreeJobs(
    grpc::ServerContext *context, const crane::grpc::FreeJobsRequest *request,
    crane::grpc::FreeJobsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  CRANE_TRACE("Receive FreeJobs RPC for Job [{}]",
              absl::StrJoin(request->job_id_list(), ","));

  g_job_mgr->FreeJobs(
      std::set(request->job_id_list().begin(), request->job_id_list().end()));

  return Status::OK;
}

grpc::Status CranedServiceImpl::QuerySshStepEnvVariables(
    grpc::ServerContext *context,
    const ::crane::grpc::QuerySshStepEnvVariablesRequest *request,
    crane::grpc::QuerySshStepEnvVariablesReply *response) {
  response->set_ok(false);
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  auto stub = g_supervisor_keeper->GetStub(request->task_id(), kDaemonStepId);
  if (!stub) {
    CRANE_ERROR("Failed to get stub of task #{}", request->task_id());
    return Status::OK;
  }

  auto task_env_map = stub->QueryStepEnv();
  if (task_env_map.has_value()) {
    for (const auto &[name, value] : task_env_map.value())
      response->mutable_env_map()->emplace(name, value);
    response->set_ok(true);
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::ChangeJobTimeConstraint(
    grpc::ServerContext *context,
    const crane::grpc::ChangeJobTimeConstraintRequest *request,
    crane::grpc::ChangeJobTimeConstraintReply *response) {
      response->set_ok(false);
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  std::optional<int64_t> time_limit_seconds =
      request->has_time_limit_seconds()
          ? std::optional<int64_t>(request->time_limit_seconds())
          : std::nullopt;

  std::optional<int64_t> deadline_time =
      request->has_deadline_time()
          ? std::optional<int64_t>(request->deadline_time())
          : std::nullopt;


  auto stub = g_supervisor_keeper->GetStub(request->task_id(), kPrimaryStepId);
  if (!stub) {
    CRANE_ERROR("Supervisor for task #{} not found", request->task_id());
    return Status::OK;
  }

  auto err =
      stub->ChangeTaskTimeConstraint(time_limit_seconds,deadline_time);
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR("[Step #{}.{}] Failed to change task time constraint",
                request->task_id(), kPrimaryStepId);
    return Status::OK;
  }
  response->set_ok(true);
  return Status::OK;
}

grpc::Status CranedServiceImpl::AttachInContainerTask(
    grpc::ServerContext *context,
    const crane::grpc::AttachInContainerTaskRequest *request,
    crane::grpc::AttachInContainerTaskReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_DEBUG("CranedServer is not ready.");
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_RPC_FAILURE);
    err->set_description("CranedServer is not ready.");
    response->set_ok(false);
    return {grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  if (!g_config.Container.Enabled || g_cri_client == nullptr) {
    // Should never happen.
    CRANE_ERROR(
        "AttachInContainerTask request received but Container is not enabled.");
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_GENERIC_FAILURE);
    err->set_description("Container feature is not enabled.");
    response->set_ok(false);
    return Status::OK;
  }

  // TODO: After we move CriEventStreaming to Craned, we can get container_id
  // directly.
  // TODO: Only support single step in job now. Need to support multi-step
  // later.
  std::unordered_map<std::string, std::string> label_selector{
      {std::string(cri::kCriDefaultLabel), "true"},
      {std::string(cri::kCriLabelJobIdKey), std::to_string(request->task_id())},
      {std::string(cri::kCriLabelUidKey), std::to_string(request->uid())},
  };
  auto container_expt = g_cri_client->SelectContainerId(label_selector);
  if (!container_expt) {
    const auto &rich_err = container_expt.error();
    CRANE_ERROR("Failed to find container for task #{}: {}", request->task_id(),
                rich_err.description());

    // NOTE: This could because the container is creating/starting.
    // The caller should retry later. Fix this after we add CONFIGURING state.
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
    err->set_description(
        std::format("Container not found, possibly initializing: {}",
                    rich_err.description()));
    response->set_ok(false);
    return Status::OK;
  }

  const auto &container_id = container_expt.value();
  auto url_expt =
      g_cri_client->Attach(container_id, request->tty(), request->stdin(),
                           request->stdout(), request->stderr());

  if (!url_expt) {
    const auto &rich_err = url_expt.error();
    CRANE_ERROR("Failed to attach to container for #{}: {}", request->task_id(),
                rich_err.description());
    auto *err = response->mutable_status();
    err->CopyFrom(rich_err);  // Directly copy RichError with detailed info
    response->set_ok(false);
    return Status::OK;
  }

  response->set_ok(true);
  response->set_url(url_expt.value());

  return Status::OK;
}

grpc::Status CranedServiceImpl::ExecInContainerTask(
    grpc::ServerContext *context,
    const crane::grpc::ExecInContainerTaskRequest *request,
    crane::grpc::ExecInContainerTaskReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_DEBUG("CranedServer is not ready.");
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_RPC_FAILURE);
    err->set_description("CranedServer is not ready.");
    response->set_ok(false);
    return {grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  if (!g_config.Container.Enabled || g_cri_client == nullptr) {
    // Should never happen.
    CRANE_ERROR(
        "ExecInContainerTask request received but Container is not enabled.");
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_GENERIC_FAILURE);
    err->set_description("Container feature is not enabled.");
    response->set_ok(false);
    return Status::OK;
  }

  // Validate command
  if (request->command_size() == 0) {
    CRANE_ERROR("ExecInContainerTask request has empty command.");
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description("Command cannot be empty.");
    response->set_ok(false);
    return Status::OK;
  }

  // TODO: After we move CriEventStreaming to Craned, we can get container_id
  // directly.
  std::unordered_map<std::string, std::string> label_selector{
      {std::string(cri::kCriDefaultLabel), "true"},
      {std::string(cri::kCriLabelJobIdKey), std::to_string(request->task_id())},
      {std::string(cri::kCriLabelUidKey), std::to_string(request->uid())},
  };
  auto container_expt = g_cri_client->SelectContainerId(label_selector);
  if (!container_expt) {
    const auto &rich_err = container_expt.error();
    CRANE_ERROR("Failed to find container for task #{}: {}", request->task_id(),
                rich_err.description());

    // NOTE: This could because the container is creating/starting.
    // The caller should retry later. Fix this after we add CONFIGURING state.
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
    err->set_description(
        std::format("Container not found, possibly initializing: {}",
                    rich_err.description()));
    response->set_ok(false);
    return Status::OK;
  }

  const auto &container_id = container_expt.value();

  // Convert command from protobuf to vector
  std::vector<std::string> command;
  command.reserve(request->command_size());
  for (const auto &cmd : request->command()) {
    command.push_back(cmd);
  }

  auto url_expt = g_cri_client->Exec(container_id, command, request->tty(),
                                     request->stdin(), request->stdout(),
                                     request->stderr());

  if (!url_expt) {
    const auto &rich_err = url_expt.error();
    CRANE_ERROR("Failed to exec in container for #{}: {}", request->task_id(),
                rich_err.description());
    auto *err = response->mutable_status();
    err->CopyFrom(rich_err);  // Directly copy RichError with detailed info
    response->set_ok(false);
    return Status::OK;
  }

  response->set_ok(true);
  response->set_url(url_expt.value());

  return Status::OK;
}

grpc::Status CranedServiceImpl::StepStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::StepStatusChangeRequest *request,
    crane::grpc::StepStatusChangeReply *response) {
  if (!g_server->ReadyFor(RequestSource::SUPERVISOR)) {
    CRANE_DEBUG("CranedServer is not ready.");
    response->set_ok(false);
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }
  g_job_mgr->StepStatusChangeAsync(request->job_id(), request->step_id(),
                                   request->new_status(), request->exit_code(),
                                   request->reason(), request->timestamp());
  response->set_ok(true);
  return Status::OK;
}

CranedServer::CranedServer(const Config::CranedListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CranedServiceImpl>();

  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);
  ServerBuilderAddUnixInsecureListeningPort(&builder,
                                            listen_conf.UnixSocketListenAddr);

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  std::string craned_listen_addr = listen_conf.CranedListenAddr;
  if (listen_conf.TlsConfig.Enabled) {
    ServerBuilderAddTcpTlsListeningPortForInternal(
        &builder, craned_listen_addr, listen_conf.CranedListenPort,
        listen_conf.TlsConfig.TlsCerts, listen_conf.TlsConfig.CaContent);
  } else {
    ServerBuilderAddTcpInsecureListeningPort(&builder, craned_listen_addr,
                                             listen_conf.CranedListenPort);
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  CRANE_INFO("Craned is listening on [{}, {}:{}]",
             listen_conf.UnixSocketListenAddr, craned_listen_addr,
             listen_conf.CranedListenPort);

  chmod(g_config.CranedUnixSockPath.c_str(), 0600);
}

}  // namespace Craned