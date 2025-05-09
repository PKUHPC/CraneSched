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

#include "CranedForPamServer.h"
#include "CtldClient.h"
#include "JobManager.h"
#include "TaskManager.h"

namespace Craned {

grpc::Status CranedServiceImpl::Configure(
    grpc::ServerContext *context,
    const crane::grpc::ConfigureCranedRequest *request,
    google::protobuf::Empty *response) {
  bool ok = g_ctld_client_sm->EvRecvConfigFromCtld(*request);

  CRANE_TRACE("Recv Configure RPC from Ctld. Configuration result: {}", ok);
  return Status::OK;
}

grpc::Status CranedServiceImpl::ExecuteTask(
    grpc::ServerContext *context,
    const crane::grpc::ExecuteTasksRequest *request,
    crane::grpc::ExecuteTasksReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");

    for (auto const &task_to_d : request->tasks())
      response->add_failed_task_id_list(task_to_d.task_id());
    return Status{grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready"};
  }

  CRANE_TRACE("Requested from CraneCtld to execute {} tasks.",
              request->tasks_size());

  CraneErrCode err;
  for (auto const &task_to_d : request->tasks()) {
    err = g_task_mgr->ExecuteTaskAsync(task_to_d);
    if (err != CraneErrCode::SUCCESS)
      response->add_failed_task_id_list(task_to_d.task_id());
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateTasks(
    grpc::ServerContext *context,
    const crane::grpc::TerminateTasksRequest *request,
    crane::grpc::TerminateTasksReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_reason("CranedServer is not ready");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  CRANE_TRACE("Receive TerminateTasks for tasks {}",
              absl::StrJoin(request->task_id_list(), ","));

  for (task_id_t id : request->task_id_list())
    g_task_mgr->TerminateTaskAsync(id);
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateOrphanedTask(
    grpc::ServerContext *context,
    const crane::grpc::TerminateOrphanedTaskRequest *request,
    crane::grpc::TerminateOrphanedTaskReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_reason("CranedServer is not ready");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }
  for (task_id_t job_id : request->task_id_list())
    g_task_mgr->MarkTaskAsOrphanedAndTerminateAsync(job_id);
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryTaskIdFromPort(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskIdFromPortRequest *request,
    crane::grpc::QueryTaskIdFromPortReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  CRANE_TRACE("Receive QueryTaskIdFromPort RPC from {}: port: {}",
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
    auto task_id_expt = g_cg_mgr->GetJobIdFromPid(pid_i);
    if (task_id_expt.has_value()) {
      CRANE_TRACE("Task id for pid {} is #{}", pid_i, task_id_expt.value());
      response->set_ok(true);
      response->set_task_id(task_id_expt.value());
      return Status::OK;
    } else {
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
    }
  } while (pid_i > 1);

  response->set_ok(false);
  return Status::OK;
}

grpc::Status CranedServiceImpl::CreateCgroupForTasks(
    grpc::ServerContext *context,
    const crane::grpc::CreateCgroupForTasksRequest *request,
    crane::grpc::CreateCgroupForTasksReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  std::vector<JobToD> jobs;
  for (const auto &job : request->job_list()) {
    CRANE_TRACE("Allocating job #{}, uid {}", job.job_id(), job.uid());
    jobs.emplace_back(job);
  }

  bool ok = g_job_mgr->AllocJobs(std::move(jobs));
  if (!ok) {
    CRANE_ERROR("Failed to alloc some jobs.");
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::ReleaseCgroupForTasks(
    grpc::ServerContext *context,
    const crane::grpc::ReleaseCgroupForTasksRequest *request,
    crane::grpc::ReleaseCgroupForTasksReply *response) {
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

Status CranedServiceImpl::QueryTaskEnvVariables(
    grpc::ServerContext *context,
    const ::crane::grpc::QueryTaskEnvVariablesRequest *request,
    crane::grpc::QueryTaskEnvVariablesReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  auto task_env_map = g_task_mgr->QueryTaskEnvMapAsync(request->task_id());
  if (task_env_map.has_value()) {
    for (const auto &[name, value] : task_env_map.value())
      response->mutable_env_map()->emplace(name, value);
    response->set_ok(true);
  } else
    response->set_ok(false);

  return Status::OK;
}

grpc::Status CranedServiceImpl::ChangeTaskTimeLimit(
    grpc::ServerContext *context,
    const crane::grpc::ChangeTaskTimeLimitRequest *request,
    crane::grpc::ChangeTaskTimeLimitReply *response) {
  if (!g_server->ReadyFor(RequestSource::CTLD)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  bool ok = g_task_mgr->ChangeTaskTimeLimitAsync(
      request->task_id(), absl::Seconds(request->time_limit_seconds()));
  response->set_ok(ok);

  return Status::OK;
}

grpc::Status CranedServiceImpl::UpdateLeaderId(
    grpc::ServerContext *context,
    const crane::grpc::UpdateLeaderIdRequest *request,
    google::protobuf::Empty *response) {
  g_ctld_client->SetLeaderId(request->cur_leader_id());
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
  if (listen_conf.UseTls) {
    ServerBuilderAddTcpTlsListeningPort(&builder, craned_listen_addr,
                                        listen_conf.CranedListenPort,
                                        listen_conf.TlsCerts);
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

  g_task_mgr->SetSigintCallback([p_server = m_server_.get()] {
    g_craned_for_pam_server->Shutdown();
    p_server->Shutdown();
    CRANE_INFO("Grpc Server Shutdown() was called.");
  });
}

}  // namespace Craned