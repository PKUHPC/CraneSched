/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "CranedServer.h"

#include <arpa/inet.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include "CtldClient.h"

namespace Craned {

grpc::Status CranedServiceImpl::ExecuteTask(
    grpc::ServerContext *context,
    const crane::grpc::ExecuteTasksRequest *request,
    crane::grpc::ExecuteTasksReply *response) {
  CRANE_TRACE("Requested from CraneCtld to execute {} tasks.",
              request->tasks_size());

  CraneErr err;
  for (auto const &task_to_d : request->tasks()) {
    err = g_task_mgr->ExecuteTaskAsync(task_to_d);
    if (err != CraneErr::kOk)
      response->add_failed_task_id_list(task_to_d.task_id());
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateTasks(
    grpc::ServerContext *context,
    const crane::grpc::TerminateTasksRequest *request,
    crane::grpc::TerminateTasksReply *response) {
  for (const auto &id : request->task_id_list())
    g_task_mgr->TerminateTaskAsync(id);
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateOrphanedTask(
    grpc::ServerContext *context,
    const crane::grpc::TerminateOrphanedTaskRequest *request,
    crane::grpc::TerminateOrphanedTaskReply *response) {
  g_task_mgr->MarkTaskAsOrphanedAndTerminateAsync(request->task_id());
  response->set_ok(true);

  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryTaskIdFromPort(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskIdFromPortRequest *request,
    crane::grpc::QueryTaskIdFromPortReply *response) {
  CRANE_TRACE("Receive QueryTaskIdFromPort RPC from {}: port: {}",
              context->peer(), request->port());

  std::string port_hex = fmt::format("{:0>4X}", request->port());

  ino_t inode;

  // find inode
  // 1._find_match_in_tcp_file
  std::string tcp_path{"/proc/net/tcp"};
  std::ifstream tcp_in(tcp_path, std::ios::in);
  std::string tcp_line;
  bool inode_found = false;
  if (tcp_in) {
    getline(tcp_in, tcp_line);  // Skip the header line
    while (getline(tcp_in, tcp_line)) {
      tcp_line = absl::StripAsciiWhitespace(tcp_line);
      std::vector<std::string> tcp_line_vec =
          absl::StrSplit(tcp_line, absl::ByAnyChar(" :"), absl::SkipEmpty());
      CRANE_TRACE("Checking port {} == {}", port_hex, tcp_line_vec[2]);
      if (port_hex == tcp_line_vec[2]) {
        inode_found = true;
        inode = std::stoul(tcp_line_vec[13]);
        CRANE_TRACE("Inode num for port {} is {}", request->port(), inode);
        break;
      }
    }
    if (!inode_found) {
      CRANE_TRACE("Inode num for port {} is not found.", request->port());
      response->set_ok(false);
      return Status::OK;
    }
  } else {  // can't find file
    CRANE_ERROR("Can't open file: {}", tcp_path);
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
    std::optional<uint32_t> task_id_opt =
        g_task_mgr->QueryTaskIdFromPidAsync(pid_i);
    if (task_id_opt.has_value()) {
      CRANE_TRACE("Task id for pid {} is #{}", pid_i, task_id_opt.value());
      response->set_ok(true);
      response->set_task_id(task_id_opt.value());
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
  std::vector<std::pair<task_id_t, uid_t>> task_id_uid_pairs;
  for (int i = 0; i < request->task_id_list_size(); i++) {
    task_id_t task_id = request->task_id_list(i);
    uid_t uid = request->uid_list(i);

    CRANE_TRACE("Receive CreateCgroup for task #{}, uid {}", task_id, uid);
    task_id_uid_pairs.emplace_back(task_id, uid);
  }

  bool ok = g_task_mgr->CreateCgroupsAsync(std::move(task_id_uid_pairs));
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

    bool ok = g_task_mgr->ReleaseCgroupAsync(task_id, uid);
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
  std::string_view crane_service_port;

  // Check whether the remote address is in the addresses of CraneD nodes.
  if (g_config.Ipv4ToCranedHostname.contains(request->ssh_remote_address())) {
    CRANE_TRACE(
        "Receive QueryTaskIdFromPortForward from Pam module: "
        "ssh_remote_port: {}, ssh_remote_address: {}. "
        "This ssh comes from a CraneD node. uid: {}",
        request->ssh_remote_port(), request->ssh_remote_address(),
        request->uid());
    // In the addresses of CraneD nodes. This ssh request comes from a CraneD
    // node. Check if the remote port belongs to a task. If so, move it in to
    // the cgroup of this task.
    crane_service_port = kCranedDefaultPort;
    remote_is_craned = true;
  } else {
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
    crane_service_port = kCforedDefaultPort;
  }

  std::shared_ptr<Channel> channel_of_remote_service;
  if (g_config.ListenConf.UseTls) {
    std::string remote_hostname;
    ok = crane::ResolveHostnameFromIpv4(request->ssh_remote_address(),
                                        &remote_hostname);
    if (ok) {
      CRANE_TRACE("Remote address {} was resolved as {}",
                  request->ssh_remote_address(), remote_hostname);

      std::string target_service =
          fmt::format("{}.{}:{}", remote_hostname,
                      g_config.ListenConf.DomainSuffix, crane_service_port);

      grpc::SslCredentialsOptions ssl_opts;
      // pem_root_certs is actually the certificate of server side rather than
      // CA certificate. CA certificate is not needed.
      // Since we use the same cert/key pair for both cranectld/craned,
      // pem_root_certs is set to the same certificate.
      ssl_opts.pem_root_certs = g_config.ListenConf.ServerCertContent;
      ssl_opts.pem_cert_chain = g_config.ListenConf.ServerCertContent;
      ssl_opts.pem_private_key = g_config.ListenConf.ServerKeyContent;

      channel_of_remote_service =
          grpc::CreateChannel(target_service, grpc::SslCredentials(ssl_opts));
    } else {
      CRANE_ERROR("Failed to resolve remote address {}.",
                  request->ssh_remote_address());
    }
  } else {
    std::string target_service =
        fmt::format("{}:{}", request->ssh_remote_address(), crane_service_port);
    channel_of_remote_service =
        grpc::CreateChannel(target_service, grpc::InsecureChannelCredentials());
  }

  if (!channel_of_remote_service) {
    CRANE_ERROR("Failed to create channel to {}.",
                request->ssh_remote_address());
    response->set_ok(false);
    return Status::OK;
  }

  crane::grpc::QueryTaskIdFromPortRequest request_to_remote_service;
  crane::grpc::QueryTaskIdFromPortReply reply_from_remote_service;
  ClientContext context_of_remote_service;
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
    ok = g_task_mgr->QueryTaskInfoOfUidAsync(request->uid(), &info);
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
      g_task_mgr->MigrateProcToCgroupOfTask(request->pid(), request->task_id());

  if (!ok) {
    CRANE_INFO("GrpcMigrateSshProcToCgroup failed on pid: {}, task #{}",
               request->pid(), request->task_id());
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::CheckTaskStatus(
    grpc::ServerContext *context,
    const crane::grpc::CheckTaskStatusRequest *request,
    crane::grpc::CheckTaskStatusReply *response) {
  crane::grpc::TaskStatus status{};

  bool exist = g_task_mgr->CheckTaskStatusAsync(request->task_id(), &status);
  response->set_ok(exist);
  response->set_status(status);

  return Status::OK;
}

grpc::Status CranedServiceImpl::ChangeTaskTimeLimit(
    grpc::ServerContext *context,
    const crane::grpc::ChangeTaskTimeLimitRequest *request,
    crane::grpc::ChangeTaskTimeLimitReply *response) {
  bool ok = g_task_mgr->ChangeTaskTimeLimitAsync(
      request->task_id(), absl::Seconds(request->time_limit_seconds()));
  response->set_ok(ok);

  return Status::OK;
}

CranedServer::CranedServer(const Config::CranedListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CranedServiceImpl>();

  grpc::ServerBuilder builder;
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA,
                             0 /*no limit*/);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
                             1 /*true*/);
  builder.AddChannelArgument(
      GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,
      kCranedGrpcServerPingRecvMinIntervalSec * 1000 /*ms*/);
  builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES,
                             0 /* unlimited */);

  if (g_config.CompressedRpc)
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  builder.AddListeningPort(listen_conf.UnixSocketListenAddr,
                           grpc::InsecureServerCredentials());

  std::string listen_addr_port = fmt::format(
      "{}:{}", listen_conf.CranedListenAddr, listen_conf.CranedListenPort);
  if (listen_conf.UseTls) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair pem_key_cert_pair;
    pem_key_cert_pair.cert_chain = listen_conf.ServerCertContent;
    pem_key_cert_pair.private_key = listen_conf.ServerKeyContent;

    grpc::SslServerCredentialsOptions ssl_opts;
    // pem_root_certs is actually the certificate of server side rather than
    // CA certificate. CA certificate is not needed.
    // Since we use the same cert/key pair for both cranectld/craned,
    // pem_root_certs is set to the same certificate.
    ssl_opts.pem_root_certs = listen_conf.ServerCertContent;
    ssl_opts.pem_key_cert_pairs.emplace_back(std::move(pem_key_cert_pair));
    ssl_opts.client_certificate_request =
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    builder.AddListeningPort(listen_addr_port,
                             grpc::SslServerCredentials(ssl_opts));
  } else {
    builder.AddListeningPort(listen_addr_port,
                             grpc::InsecureServerCredentials());
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  CRANE_INFO("Craned is listening on [{}, {}]",
             listen_conf.UnixSocketListenAddr, listen_addr_port);

  g_task_mgr->SetSigintCallback([p_server = m_server_.get()] {
    p_server->Shutdown();
    CRANE_TRACE("Grpc Server Shutdown() was called.");
  });
}

}  // namespace Craned
