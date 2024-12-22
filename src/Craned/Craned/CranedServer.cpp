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

#include "JobManager.h"

namespace Craned {

grpc::Status CranedServiceImpl::ExecuteTask(
    grpc::ServerContext *context,
    const crane::grpc::ExecuteTasksRequest *request,
    crane::grpc::ExecuteTasksReply *response) {
  CRANE_TRACE("Requested from CraneCtld to execute {} tasks.",
              request->tasks_size());

  CraneErr err;
  for (auto const &task_to_d : request->tasks()) {
    err = g_job_mgr->ExecuteTaskAsync(task_to_d);
    if (err != CraneErr::kOk)
      response->add_failed_task_id_list(task_to_d.task_id());
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateTasks(
    grpc::ServerContext *context,
    const crane::grpc::TerminateTasksRequest *request,
    crane::grpc::TerminateTasksReply *response) {
  CRANE_TRACE("Receive TerminateTasks for tasks {}",
              absl::StrJoin(request->task_id_list(), ","));

  for (task_id_t id : request->task_id_list())
    g_job_mgr->TerminateTaskAsync(id);
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateOrphanedTask(
    grpc::ServerContext *context,
    const crane::grpc::TerminateOrphanedTaskRequest *request,
    crane::grpc::TerminateOrphanedTaskReply *response) {
  g_job_mgr->MarkTaskAsOrphanedAndTerminateAsync(request->task_id());
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryTaskIdFromPort(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskIdFromPortRequest *request,
    crane::grpc::QueryTaskIdFromPortReply *response) {
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
        struct stat statbuf {};
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
    auto task_id_expt = g_job_mgr->QueryTaskIdFromPidAsync(pid_i);
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
  std::vector<CgroupSpec> cg_specs;
  for (int i = 0; i < request->task_id_list_size(); i++) {
    task_id_t task_id = request->task_id_list(i);
    uid_t uid = request->uid_list(i);
    const crane::grpc::ResourceInNode &res = request->res_list(i);

    CgroupSpec spec{.uid = uid,
                    .task_id = task_id,
                    .res_in_node = res,
                    .execution_node = request->execution_node(i)};
    CRANE_TRACE("Receive CreateCgroup for task #{}, uid {}", task_id, uid);
    cg_specs.emplace_back(std::move(spec));
  }

  bool ok = g_cg_mgr->CreateCgroups(std::move(cg_specs));
  if (!ok) {
    CRANE_ERROR("Failed to create cgroups for some tasks.");
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::ReleaseCgroupForTasks(
    grpc::ServerContext *context,
    const crane::grpc::ReleaseCgroupForTasksRequest *request,
    crane::grpc::ReleaseCgroupForTasksReply *response) {
  for (int i = 0; i < request->task_id_list_size(); ++i) {
    task_id_t task_id = request->task_id_list(i);
    uid_t uid = request->uid_list(i);

    CRANE_DEBUG("Release Cgroup for task #{}", task_id);

    bool ok = g_cg_mgr->ReleaseCgroup(task_id, uid);
    if (!ok) {
      CRANE_ERROR("Failed to release cgroup for task #{}, uid {}", task_id,
                  uid);
    }
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryTaskIdFromPortForward(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskIdFromPortForwardRequest *request,
    crane::grpc::QueryTaskIdFromPortForwardReply *response) {
  bool ok;
  bool task_id_found = false;
  bool remote_is_craned = false;

  // May be craned or cfored
  std::string crane_port;
  std::string crane_addr = request->ssh_remote_address();

  int ip_ver = crane::GetIpAddrVer(request->ssh_remote_address());

  ipv4_t crane_addr4;
  ipv6_t crane_addr6;
  if (ip_ver == 4 && crane::StrToIpv4(crane_addr, &crane_addr4)) {
    if (g_config.Ipv4ToCranedHostname.contains(crane_addr4)) {
      CRANE_TRACE(
          "Receive QueryTaskIdFromPortForward from Pam module: "
          "ssh_remote_port: {}, ssh_remote_address: {}. "
          "This ssh comes from a CraneD node. uid: {}",
          request->ssh_remote_port(), request->ssh_remote_address(),
          request->uid());
      // In the addresses of CraneD nodes. This ssh request comes from a
      // CraneD node. Check if the remote port belongs to a task. If so, move
      // it in to the cgroup of this task.
      crane_port = g_config.ListenConf.CranedListenPort;
      remote_is_craned = true;
    }
  } else if (ip_ver == 6 && crane::StrToIpv6(crane_addr, &crane_addr6)) {
    if (g_config.Ipv6ToCranedHostname.contains(crane_addr6)) {
      CRANE_TRACE(
          "Receive QueryTaskIdFromPortForward from Pam module: "
          "ssh_remote_port: {}, ssh_remote_address: {}. "
          "This ssh comes from a CraneD node. uid: {}",
          request->ssh_remote_port(), request->ssh_remote_address(),
          request->uid());
      crane_port = g_config.ListenConf.CranedListenPort;
      remote_is_craned = true;
    }
  } else {
    CRANE_ERROR(
        "Unknown ip version for address {} or error converting ip to uint",
        crane_addr);
    response->set_ok(false);
    return Status::OK;
  }

  if (!remote_is_craned) {
    // Not in the addresses of CraneD nodes. This ssh request comes from a user.
    // Check if the user's uid is running a task. If so, move it in to the
    // cgroup of his first task. If not so, reject this ssh request.
    CRANE_TRACE(
        "Receive QueryTaskIdFromPortForward from Pam module: "
        "ssh_remote_port: {}, ssh_remote_address: {}. "
        "This ssh comes from a user machine. uid: {}",
        request->ssh_remote_port(), request->ssh_remote_address(),
        request->uid());

    response->set_from_user(true);
    crane_port = kCforedDefaultPort;
  }

  std::shared_ptr<Channel> channel_of_remote_service;
  if (g_config.ListenConf.UseTls) {
    std::string remote_hostname;
    if (ip_ver == 4) {
      ok = crane::ResolveHostnameFromIpv4(crane_addr4, &remote_hostname);
    } else {
      CRANE_ASSERT(ip_ver == 6);
      ok = crane::ResolveHostnameFromIpv6(crane_addr6, &remote_hostname);
    }

    if (ok) {
      CRANE_TRACE("Remote address {} was resolved as {}",
                  request->ssh_remote_address(), remote_hostname);

      channel_of_remote_service = CreateTcpTlsChannelByHostname(
          remote_hostname, crane_port, g_config.ListenConf.TlsCerts);
    } else {
      CRANE_ERROR("Failed to resolve remote address {}.",
                  request->ssh_remote_address());
    }
  } else {
    channel_of_remote_service =
        CreateTcpInsecureChannel(crane_addr, crane_port);
  }

  if (!channel_of_remote_service) {
    CRANE_ERROR("Failed to create channel to {}.",
                request->ssh_remote_address());
    response->set_ok(false);
    return Status::OK;
  }

  crane::grpc::QueryTaskIdFromPortRequest request_to_remote_service;
  crane::grpc::QueryTaskIdFromPortReply reply_from_remote_service;
  grpc::ClientContext context_of_remote_service;
  Status status_remote_service;

  request_to_remote_service.set_port(request->ssh_remote_port());

  if (remote_is_craned) {
    std::unique_ptr<crane::grpc::Craned::Stub> stub_of_remote_craned =
        crane::grpc::Craned::NewStub(channel_of_remote_service);
    status_remote_service = stub_of_remote_craned->QueryTaskIdFromPort(
        &context_of_remote_service, request_to_remote_service,
        &reply_from_remote_service);
  } else {
    std::unique_ptr<crane::grpc::CraneForeD::Stub> stub_of_remote_cfored =
        crane::grpc::CraneForeD::NewStub(channel_of_remote_service);
    status_remote_service = stub_of_remote_cfored->QueryTaskIdFromPort(
        &context_of_remote_service, request_to_remote_service,
        &reply_from_remote_service);
  }

  if (!status_remote_service.ok()) {
    CRANE_WARN("QueryTaskIdFromPort gRPC call failed: {}. Remote is craned: {}",
               status_remote_service.error_message(), remote_is_craned);
  }

  if (status_remote_service.ok() && reply_from_remote_service.ok()) {
    response->set_ok(true);
    response->set_task_id(reply_from_remote_service.task_id());

    CRANE_TRACE(
        "ssh client with remote port {} belongs to task #{}. "
        "Moving this ssh session process into the task's cgroup",
        request->ssh_remote_port(), reply_from_remote_service.task_id());
    return Status::OK;
  } else {
    TaskInfoOfUid info{};
    ok = g_cg_mgr->QueryTaskInfoOfUidAsync(request->uid(), &info);
    if (ok) {
      CRANE_TRACE(
          "Found a task #{} belonging to uid {}. "
          "This ssh session process is going to be moved into the task's "
          "cgroup",
          info.first_task_id, request->uid());
      response->set_task_id(info.first_task_id);
      response->set_ok(true);
    } else {
      CRANE_TRACE(
          "This ssh session can't be moved into uid {}'s tasks. "
          "This uid has {} task(s) and cgroup found: {}. "
          "Reject this ssh request.",
          request->uid(), info.job_cnt, info.cgroup_exists);
      response->set_ok(false);
    }
    return Status::OK;
  }
}

grpc::Status CranedServiceImpl::MigrateSshProcToCgroup(
    grpc::ServerContext *context,
    const crane::grpc::MigrateSshProcToCgroupRequest *request,
    crane::grpc::MigrateSshProcToCgroupReply *response) {
  CRANE_TRACE("Moving pid {} to cgroup of task #{}", request->pid(),
              request->task_id());
  bool ok =
      g_cg_mgr->MigrateProcToCgroupOfTask(request->pid(), request->task_id());

  if (!ok) {
    CRANE_INFO("GrpcMigrateSshProcToCgroup failed on pid: {}, task #{}",
               request->pid(), request->task_id());
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }

  return Status::OK;
}

Status CranedServiceImpl::QueryTaskEnvVariables(
    grpc::ServerContext *context,
    const ::crane::grpc::QueryTaskEnvVariablesRequest *request,
    crane::grpc::QueryTaskEnvVariablesReply *response) {
  auto task_env_map = g_job_mgr->QueryTaskEnvMapAsync(request->task_id());
  if (task_env_map.has_value()) {
    for (const auto &[name, value] : task_env_map.value())
      response->mutable_env_map()->emplace(name, value);
    response->set_ok(true);
  } else
    response->set_ok(false);

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryTaskEnvVariablesForward(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskEnvVariablesForwardRequest *request,
    crane::grpc::QueryTaskEnvVariablesForwardReply *response) {
  // First query local device related env list
  auto res_envs_opt = g_cg_mgr->GetResourceEnvMapOfTask(request->task_id());
  if (!res_envs_opt.has_value()) {
    response->set_ok(false);
    return Status::OK;
  }
  for (const auto &[name, value] : res_envs_opt.value()) {
    response->mutable_env_map()->emplace(name, value);
  }

  std::optional execution_node_opt =
      g_cg_mgr->QueryTaskExecutionNode(request->task_id());
  if (!execution_node_opt.has_value()) {
    response->set_ok(false);
    return Status::OK;
  }

  std::string execution_node = execution_node_opt.value();
  if (!g_config.CranedRes.contains(execution_node)) {
    response->set_ok(false);
    return Status::OK;
  }

  std::shared_ptr<Channel> channel_of_remote_service;
  if (g_config.ListenConf.UseTls)
    channel_of_remote_service = CreateTcpTlsChannelByHostname(
        execution_node, g_config.ListenConf.CranedListenPort,
        g_config.ListenConf.TlsCerts);
  else
    channel_of_remote_service = CreateTcpInsecureChannel(
        execution_node, g_config.ListenConf.CranedListenPort);

  if (!channel_of_remote_service) {
    CRANE_ERROR("Failed to create channel to {}.", execution_node);
    response->set_ok(false);
    return Status::OK;
  }

  crane::grpc::QueryTaskEnvVariablesRequest request_to_remote_service;
  crane::grpc::QueryTaskEnvVariablesReply reply_from_remote_service;
  grpc::ClientContext context_of_remote_service;
  Status status_remote_service;

  request_to_remote_service.set_task_id(request->task_id());
  std::unique_ptr<crane::grpc::Craned::Stub> stub_of_remote_craned =
      crane::grpc::Craned::NewStub(channel_of_remote_service);
  status_remote_service = stub_of_remote_craned->QueryTaskEnvVariables(
      &context_of_remote_service, request_to_remote_service,
      &reply_from_remote_service);
  if (!status_remote_service.ok() || !reply_from_remote_service.ok()) {
    CRANE_WARN(
        "QueryTaskEnvVariables gRPC call failed: {}. Remote is craned: {}",
        status_remote_service.error_message(), execution_node);
    response->set_ok(false);
    return Status::OK;
  }

  response->set_ok(true);
  for (const auto &[name, value] : reply_from_remote_service.env_map()) {
    response->mutable_env_map()->emplace(name, value);
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::CheckTaskStatus(
    grpc::ServerContext *context,
    const crane::grpc::CheckTaskStatusRequest *request,
    crane::grpc::CheckTaskStatusReply *response) {
  crane::grpc::TaskStatus status{};

  bool exist = g_job_mgr->CheckTaskStatusAsync(request->task_id(), &status);
  response->set_ok(exist);
  response->set_status(status);

  return Status::OK;
}

grpc::Status CranedServiceImpl::ChangeTaskTimeLimit(
    grpc::ServerContext *context,
    const crane::grpc::ChangeTaskTimeLimitRequest *request,
    crane::grpc::ChangeTaskTimeLimitReply *response) {
  bool ok = g_job_mgr->ChangeTaskTimeLimitAsync(
      request->task_id(), absl::Seconds(request->time_limit_seconds()));
  response->set_ok(ok);

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryCranedRemoteMeta(
    grpc::ServerContext *context,
    const ::crane::grpc::QueryCranedRemoteMetaRequest *request,
    crane::grpc::QueryCranedRemoteMetaReply *response) {
  auto *grpc_meta = response->mutable_craned_remote_meta();

  auto &dres = g_config.CranedRes[g_config.CranedIdOfThisNode]->dedicated_res;
  grpc_meta->mutable_dres_in_node()->CopyFrom(
      static_cast<crane::grpc::DedicatedResourceInNode>(dres));

  grpc_meta->set_craned_version(CRANE_VERSION_STRING);

  const SystemRelInfo &sys_info = g_config.CranedMeta.SysInfo;
  auto *grpc_sys_rel_info = grpc_meta->mutable_sys_rel_info();
  grpc_sys_rel_info->set_name(sys_info.name);
  grpc_sys_rel_info->set_release(sys_info.release);
  grpc_sys_rel_info->set_version(sys_info.version);

  grpc_meta->mutable_craned_start_time()->set_seconds(
      ToUnixSeconds(g_config.CranedMeta.CranedStartTime));
  grpc_meta->mutable_system_boot_time()->set_seconds(
      ToUnixSeconds(g_config.CranedMeta.SystemBootTime));

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

  g_job_mgr->SetSigintCallback([p_server = m_server_.get()] {
    p_server->Shutdown();
    CRANE_INFO("Grpc Server Shutdown() was called.");
  });
}

}  // namespace Craned
