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
#include "crane/String.h"

namespace Craned {

grpc::Status CranedForPamServiceImpl::QueryStepFromPortForward(
    grpc::ServerContext *context,
    const crane::grpc::QueryStepFromPortForwardRequest *request,
    crane::grpc::QueryStepFromPortForwardReply *response) {
  const auto &address = request->ssh_remote_address();
  const int ip_ver = crane::GetIpAddrVer(address);
  ipv4_t addr4;
  ipv6_t addr6;
  CranedId remote_id;
  if (ip_ver == 4 && crane::StrToIpv4(address, &addr4)) {
    auto it = g_config.Ipv4ToCranedHostname.find(addr4);
    if (it != g_config.Ipv4ToCranedHostname.end()) remote_id = it->second;
  } else if (ip_ver == 6 && crane::StrToIpv6(address, &addr6)) {
    auto it = g_config.Ipv6ToCranedHostname.find(addr6);
    if (it != g_config.Ipv6ToCranedHostname.end()) remote_id = it->second;
  } else {
    CRANE_ERROR("Invalid SSH remote address {}", address);
    response->set_ok(false);
    return Status::OK;
  }

  // Remote port ownership is only a hint. The job must also be allocated on
  // this node and belong to the SSH user.
  auto accept_job = [&](job_id_t job_id) {
    auto job = g_job_mgr->QueryJob(job_id);
    if (!job || job->Uid() != request->uid()) return false;
    response->set_job_id(job_id);
    response->set_ok(true);
    return true;
  };

  std::string remote_hostname;
  if (g_config.ListenConf.TlsConfig.Enabled) {
    if (!remote_id.empty()) {
      remote_hostname = g_config.CranedIdToNodeHostname.at(remote_id);
    } else {
      const bool resolved =
          ip_ver == 4 ? crane::ResolveHostnameFromIpv4(addr4, &remote_hostname)
                      : crane::ResolveHostnameFromIpv6(addr6, &remote_hostname);
      if (!resolved)
        CRANE_WARN("Failed to resolve PAM remote address {}", address);
    }
  }

  auto query_remote = [&](bool is_craned) {
    const std::string port =
        is_craned ? g_config.ListenConf.CranedListenPort : kCforedDefaultPort;
    std::shared_ptr<Channel> channel;
    if (g_config.ListenConf.TlsConfig.Enabled) {
      if (remote_hostname.empty()) return false;
      grpc::ChannelArguments args;
      SetTlsTargetNameOverride(
          &args, util::TlsHostname(remote_hostname,
                                   g_config.ListenConf.TlsConfig.DomainSuffix));
      channel = CreateTcpTlsCustomChannelByIp(
          address, port, g_config.ListenConf.TlsConfig.TlsCerts, args);
    } else {
      channel = CreateTcpInsecureChannel(address, port);
    }
    if (!channel) return false;

    crane::grpc::QueryStepFromPortRequest remote_request;
    crane::grpc::QueryStepFromPortReply remote_reply;
    remote_request.set_port(request->ssh_remote_port());
    grpc::ClientContext remote_context;
    remote_context.set_deadline(
        std::min(context->deadline(),
                 std::chrono::system_clock::now() +
                     std::chrono::seconds(kCranedRpcTimeoutSeconds)));
    Status status;
    if (is_craned) {
      auto stub = crane::grpc::Craned::NewStub(channel);
      status = stub->QueryStepFromPort(&remote_context, remote_request,
                                       &remote_reply);
    } else {
      auto stub = crane::grpc::CraneForeD::NewStub(channel);
      status = stub->QueryStepFromPort(&remote_context, remote_request,
                                       &remote_reply);
    }
    if (!status.ok()) {
      CRANE_DEBUG("PAM port query to {}:{} failed: {}", address, port,
                  status.error_message());
      return false;
    }
    if (!remote_reply.ok() || !accept_job(remote_reply.job_id())) return false;
    response->set_from_user(!is_craned);
    return true;
  };

  response->set_from_user(remote_id.empty());
  // Static addresses are resolved once at startup. An unknown source can be
  // either a login host or a dynamically bound craned.
  if (remote_id.empty() && query_remote(false)) return Status::OK;
  if (query_remote(true)) return Status::OK;

  auto info = g_job_mgr->QueryJobInfoOfUid(request->uid());
  if (info && accept_job(info->first_job_id)) return Status::OK;

  CRANE_TRACE("No local job for PAM uid {}. Rejecting SSH request.",
              request->uid());
  response->set_ok(false);
  return Status::OK;
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

  CRANE_TRACE("Moving pid {} to cgroup of job #{}", request->pid(),
              request->job_id());

  bool ok =
      g_job_mgr->MigrateProcToCgroupOfJob(request->pid(), request->job_id());
  if (!ok) {
    CRANE_INFO("GrpcMigrateSshProcToCgroup failed on pid: {}, job #{}",
               request->pid(), request->job_id());
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }

  return Status::OK;
}

grpc::Status CranedForPamServiceImpl::QuerySshStepEnvVariablesForward(
    grpc::ServerContext *context,
    const crane::grpc::QuerySshStepEnvVariablesForwardRequest *request,
    crane::grpc::QuerySshStepEnvVariablesForwardReply *response) {
  if (!g_server->ReadyFor(RequestSource::PAM)) {
    CRANE_ERROR("CranedServer is not ready.");
    response->set_ok(false);
    return Status(grpc::StatusCode::UNAVAILABLE, "CranedServer is not ready");
  }

  auto job_env_map =
      g_job_mgr->QuerySshStepEnvVariables(request->job_id(), kDaemonStepId);
  if (!job_env_map.has_value()) {
    CRANE_ERROR("Failed to get env of job #{}", request->job_id());
    response->set_ok(false);
    return Status::OK;
  }
  for (const auto &[name, value] : job_env_map.value())
    response->mutable_env_map()->emplace(name, value);
  response->set_ok(true);
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
  // Pam run as root
  chmod(listen_conf.UnixSocketForPamListenAddr.c_str(), 0600);

  m_server_ = builder.BuildAndStart();

  CRANE_INFO("Craned for pam unix socket is listening on {}",
             listen_conf.UnixSocketForPamListenAddr);
}

}  // namespace Craned
