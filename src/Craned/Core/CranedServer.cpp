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

#include "CgroupManager.h"
#include "CranedForPamServer.h"
#include "CtldClient.h"
#include "JobManager.h"
#include "SupervisorKeeper.h"

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

    for (auto const &step_to_d : request->tasks())
      response->add_failed_task_id_list(step_to_d.task_id());
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  CRANE_TRACE("Requested from CraneCtld to execute {} tasks.",
              request->tasks_size());

  CraneErrCode err;
  for (auto const &step_to_d : request->tasks()) {
    err = g_job_mgr->ExecuteStepAsync(step_to_d);
    if (err != CraneErrCode::SUCCESS)
      response->add_failed_task_id_list(step_to_d.task_id());
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateSteps(
    grpc::ServerContext *context,
    const crane::grpc::TerminateStepsRequest *request,
    crane::grpc::TerminateStepsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_reason("CranedServer is not ready");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  CRANE_TRACE("Receive TerminateSteps for steps {}",
              absl::StrJoin(request->task_id_list(), ","));

  for (step_id_t id : request->task_id_list())
    g_job_mgr->TerminateStepAsync(id);
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
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }
  for (task_id_t job_id : request->task_id_list())
    g_job_mgr->MarkStepAsOrphanedAndTerminateAsync(job_id);
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
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
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
      response->set_task_id(job_id_opt.value());
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

grpc::Status CranedServiceImpl::CreateCgroupForJobs(
    grpc::ServerContext *context,
    const crane::grpc::CreateCgroupForJobsRequest *request,
    crane::grpc::CreateCgroupForJobsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  std::vector<JobInD> jobs;
  for (const auto &job_to_d : request->job_list()) {
    CRANE_TRACE("Allocating job #{}, uid {}", job_to_d.job_id(),
                job_to_d.uid());
    jobs.emplace_back(job_to_d);
  }

  bool ok = g_job_mgr->AllocJobs(std::move(jobs));
  if (!ok) {
    CRANE_ERROR("Failed to alloc some jobs.");
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::FreeSteps(
    grpc::ServerContext *context, const crane::grpc::FreeStepsRequest *request,
    crane::grpc::FreeStepsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  CRANE_TRACE("Receive FreeSteps RPC for [{}].",
              absl::StrJoin(request->job_id_list(), ","));

  std::latch latch(request->job_id_list().size());
  for (job_id_t job_id : request->job_id_list()) {
    g_thread_pool->detach_task([&latch, job_id] {
      auto stub = g_supervisor_keeper->GetStub(job_id);
      if (!stub) {
        CRANE_ERROR("[Job #{}.{}]Failed to get stub.", job_id, 0);
      } else {
        stub->ShutdownSupervisor();
      }
      g_supervisor_keeper->RemoveSupervisor(job_id);
      latch.count_down();
    });
  }
  latch.wait();

  return Status::OK;
}

grpc::Status CranedServiceImpl::ReleaseCgroupForJobs(
    grpc::ServerContext *context,
    const crane::grpc::ReleaseCgroupForJobsRequest *request,
    crane::grpc::ReleaseCgroupForJobsReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  CRANE_TRACE("Receive ReleaseCgroupForTasks RPC for [{}]",
              absl::StrJoin(request->task_id_list(), ","));
  g_job_mgr->FreeJobs(
      std::set(request->task_id_list().begin(), request->task_id_list().end()));

  return Status::OK;
}

grpc::Status CranedServiceImpl::QuerySshStepEnvVariables(
    grpc::ServerContext *context,
    const ::crane::grpc::QuerySshStepEnvVariablesRequest *request,
    crane::grpc::QuerySshStepEnvVariablesReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }
  auto stub = g_supervisor_keeper->GetStub(request->task_id());
  if (!stub) {
    CRANE_ERROR("Failed to get stub of task #{}", request->task_id());
    response->set_ok(false);
    return Status::OK;
  }

  auto task_env_map = stub->QueryStepEnv();
  if (task_env_map.has_value()) {
    for (const auto &[name, value] : task_env_map.value())
      response->mutable_env_map()->emplace(name, value);
    response->set_ok(true);
  } else
    response->set_ok(false);

  return Status::OK;
}

grpc::Status CranedServiceImpl::ChangeJobTimeLimit(
    grpc::ServerContext *context,
    const crane::grpc::ChangeJobTimeLimitRequest *request,
    crane::grpc::ChangeJobTimeLimitReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }
  bool ok = g_job_mgr->ChangeJobTimeLimitAsync(
      request->task_id(), absl::Seconds(request->time_limit_seconds()));
  response->set_ok(ok);

  return Status::OK;
}

grpc::Status CranedServiceImpl::StepStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::StepStatusChangeRequest *request,
    crane::grpc::StepStatusChangeReply *response) {
  if (!g_server->ReadyFor(RequestSource::SUPERVISOR)) {
    CRANE_DEBUG("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }
  g_job_mgr->StepStopAndDoStatusChangeAsync(
      request->task_id(), request->new_status(), request->exit_code(),
      request->reason());
  response->set_ok(true);
  return Status::OK;
}

grpc::Status CranedServiceImpl::BroadcastPmixPort(
    grpc::ServerContext *context,
    const crane::grpc::BroadcastPmixPortRequest *request,
    crane::grpc::BroadcastPmixPortReply *response) {
  if (!g_server->ReadyFor(RequestSource::SUPERVISOR)) {
    CRANE_DEBUG("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }
  g_ctld_client->BroadcastPmixPort(*request, response);

  return Status::OK;
}

grpc::Status CranedServiceImpl::ReceivePmixPort(
    grpc::ServerContext *context,
    const crane::grpc::ReceivePmixPortRequest *request,
    crane::grpc::ReceivePmixPortReply *response) {

  auto stub = g_supervisor_keeper->GetStub(request->task_id());
  if (!stub) {
    response->set_ok(false);
    return Status::OK;
  }

  std::vector<std::pair<std::string, CranedId>> pmix_ports;
  for (const auto& pmix_port : request->pmix_ports()) {
    pmix_ports.emplace_back(pmix_port.port(), pmix_port.craned_id());
  }

  auto result = stub->ReceivePmixPort(request->task_id(), pmix_ports);
  if (result != CraneErrCode::SUCCESS) {
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }

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
        listen_conf.TlsConfig.TlsCerts);
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