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

#include "CranedForPamServer.h"

#include "CranedServer.h"
#include "JobManager.h"
#include "TaskManager.h"

namespace Craned {

grpc::Status CranedForPamServiceImpl::QueryTaskIdFromPortForward(
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
    std::optional<TaskInfoOfUid> info_opt =
        g_job_mgr->QueryTaskInfoOfUid(request->uid());
    if (info_opt.has_value()) {
      auto info = info_opt.value();
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
          "This uid has no task on this node. "
          "Reject this ssh request.");
      response->set_ok(false);
    }
    return Status::OK;
  }
}

grpc::Status CranedForPamServiceImpl::MigrateSshProcToCgroup(
    grpc::ServerContext *context,
    const crane::grpc::MigrateSshProcToCgroupRequest *request,
    crane::grpc::MigrateSshProcToCgroupReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  CRANE_TRACE("Moving pid {} to cgroup of task #{}", request->pid(),
              request->task_id());

  bool ok =
      g_job_mgr->MigrateProcToCgroupOfJob(request->pid(), request->task_id());
  if (!ok) {
    CRANE_INFO("GrpcMigrateSshProcToCgroup failed on pid: {}, task #{}",
               request->pid(), request->task_id());
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }

  return Status::OK;
}

grpc::Status CranedForPamServiceImpl::QueryTaskEnvVariablesForward(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskEnvVariablesForwardRequest *request,
    crane::grpc::QueryTaskEnvVariablesForwardReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  // First query local device related env list
  auto job_expt = g_job_mgr->QueryJob(request->task_id());
  if (!job_expt) {
    response->set_ok(false);
    return Status::OK;
  }

  JobToD &job_to_d = job_expt.value();
  for (const auto &[name, value] : JobInstance::GetJobEnvMap(job_to_d)) {
    response->mutable_env_map()->emplace(name, value);
  }

  const std::string &execution_node = job_to_d.exec_node;
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

CranedForPamServer::CranedForPamServer(
    const Config::CranedListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CranedForPamServiceImpl>();

  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);

  ServerBuilderAddUnixInsecureListeningPort(
      &builder, listen_conf.UnixSocketForPamListenAddr);

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  chmod(g_config.CranedUnixSockPath.c_str(), 0600);

  CRANE_INFO("Craned for pam unix socket is listening on {}",
             listen_conf.UnixSocketForPamListenAddr);
}

}  // namespace Craned