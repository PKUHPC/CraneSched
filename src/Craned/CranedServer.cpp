#include "CranedServer.h"

#include <arpa/inet.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include "CtldClient.h"

namespace Craned {

using boost::uuids::uuid;

Status CranedServiceImpl::SrunXStream(
    ServerContext *context,
    ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest> *stream) {
  CRANE_DEBUG("SrunX connects from {}", context->peer());

  enum class StreamState {
    kNegotiation = 0,
    kCheckResource,
    kExecutiveInfo,
    kWaitForEofOrSigOrTaskEnd,
    kFinish,
    kAbort
  };

  CraneErr err;
  bool ok;
  SrunXStreamRequest request;
  SrunXStreamReply reply;

  // gRPC doesn't support parallel Write() on the same stream.
  // Use mutex to guarantee serial Write() in SrunXStream.
  std::mutex stream_w_mtx;

  // A task id is bound to one connection.
  uint32_t task_id;
  // A resource uuid is bound to one task.
  uuid resource_uuid;

  StreamState state = StreamState::kNegotiation;
  while (true) {
    switch (state) {
      case StreamState::kNegotiation:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::NegotiationType) {
            CRANE_DEBUG("Expect negotiation from peer {}, but none.",
                        context->peer());
            state = StreamState::kAbort;
          } else {
            CRANE_DEBUG("Negotiation from peer: {}", context->peer());
            reply.Clear();
            reply.set_type(SrunXStreamReply::ResultType);
            reply.mutable_result()->set_ok(true);

            stream_w_mtx.lock();
            stream->Write(reply);
            stream_w_mtx.unlock();

            state = StreamState::kCheckResource;
          }
        } else {
          CRANE_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }
        break;

      case StreamState::kCheckResource:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::CheckResourceType) {
            CRANE_DEBUG("Expect CheckResource from peer {}, but got {}.",
                        context->peer(), request.GetTypeName());
            state = StreamState::kAbort;
          } else {
            std::copy(request.check_resource().resource_uuid().begin(),
                      request.check_resource().resource_uuid().end(),
                      resource_uuid.data);

            task_id = request.check_resource().task_id();
            // Check the validity of resource uuid provided by client.
            err = g_server->CheckValidityOfResourceUuid(resource_uuid, task_id);
            if (err != CraneErr::kOk) {
              // The resource uuid provided by Client is invalid. Reject.
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);

              auto *result = reply.mutable_result();
              result->set_ok(false);
              result->set_reason(
                  fmt::format("Resource uuid invalid: {}",
                              (err == CraneErr::kNonExistent
                                   ? "Not Existent"
                                   : "It doesn't match with task_id")));

              stream_w_mtx.lock();
              stream->Write(reply, grpc::WriteOptions());
              stream_w_mtx.unlock();

              state = StreamState::kFinish;
            } else {
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);
              reply.mutable_result()->set_ok(true);

              stream_w_mtx.lock();
              stream->Write(reply);
              stream_w_mtx.unlock();

              state = StreamState::kExecutiveInfo;
            }
          }
        } else {
          CRANE_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }

        break;
      case StreamState::kExecutiveInfo:
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::ExecutiveInfoType) {
            CRANE_DEBUG("Expect CheckResource from peer {}, but got {}.",
                        context->peer(), request.GetTypeName());
            state = StreamState::kAbort;
          } else {
            std::forward_list<std::string> arguments;
            auto iter = arguments.before_begin();
            for (const auto &arg : request.exec_info().arguments()) {
              iter = arguments.emplace_after(iter, arg);
            }

            // We have checked the validity of resource uuid. Now execute it.

            // It's safe to call stream->Write() on a closed stream.
            // (stream->Write() just return false rather than throwing an
            // exception).
            auto output_callback = [stream, &write_mtx = stream_w_mtx](
                                       std::string &&buf, void *user_data) {
              CRANE_TRACE("Output Callback called. buf: {}", buf);
              crane::grpc::SrunXStreamReply reply;
              reply.set_type(SrunXStreamReply::IoRedirectionType);

              std::string *reply_buf = reply.mutable_io()->mutable_buf();
              *reply_buf = std::move(buf);

              write_mtx.lock();
              stream->Write(reply);
              write_mtx.unlock();

              CRANE_TRACE("stream->Write() done.");
            };

            // Call stream->Write() and cause the grpc thread
            // that owns 'stream' to stop the connection handling and quit.
            auto finish_callback =
                [stream, &write_mtx = stream_w_mtx /*, context*/](
                    bool is_terminated_by_signal, int value, void *user_data) {
                  CRANE_TRACE("Finish Callback called. signaled: {}, value: {}",
                              is_terminated_by_signal, value);
                  crane::grpc::SrunXStreamReply reply;
                  reply.set_type(SrunXStreamReply::ExitStatusType);

                  crane::grpc::StreamReplyExitStatus *stat =
                      reply.mutable_exit_status();
                  stat->set_reason(
                      is_terminated_by_signal
                          ? crane::grpc::StreamReplyExitStatus::Signal
                          : crane::grpc::StreamReplyExitStatus::Normal);
                  stat->set_value(value);

                  // stream->WriteLast() shall not be used here.
                  // On the server side, WriteLast cause all the Write() to be
                  // blocked until the service handler returned.
                  // WriteLast() should actually be called on the client side.
                  write_mtx.lock();
                  stream->Write(reply, grpc::WriteOptions());
                  write_mtx.unlock();

                  // If this line is appended, when SrunX has no response to
                  // WriteLast, the connection can stop anyway. Otherwise, the
                  // connection will stop (i.e. stream->Read() returns false)
                  // only if 1. SrunX calls stream->WriteLast() or 2. the
                  // underlying channel is broken. However, the 2 situations
                  // cover all situations that we can meet, so the following
                  // line should not be added except when debugging.
                  //
                  // context->TryCancel();
                };

            std::list<std::string> args;
            for (auto &&arg : request.exec_info().arguments())
              args.push_back(arg);

            err = g_task_mgr->SpawnInteractiveTaskAsync(
                task_id, request.exec_info().executive_path(), std::move(args),
                std::move(output_callback), std::move(finish_callback));
            if (err == CraneErr::kOk) {
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);

              auto *result = reply.mutable_result();
              result->set_ok(true);

              stream_w_mtx.lock();
              stream->Write(reply);
              stream_w_mtx.unlock();

              state = StreamState::kWaitForEofOrSigOrTaskEnd;
            } else {
              reply.Clear();
              reply.set_type(SrunXStreamReply::ResultType);

              auto *result = reply.mutable_result();
              result->set_ok(false);

              if (err == CraneErr::kSystemErr)
                result->set_reason(
                    fmt::format("System error: {}", strerror(errno)));
              else if (err == CraneErr::kStop)
                result->set_reason("Server is stopping");
              else
                result->set_reason(fmt::format("Unknown failure. Code: . ",
                                               uint16_t(err),
                                               CraneErrStr(err)));

              stream_w_mtx.lock();
              stream->Write(reply);
              stream_w_mtx.unlock();

              state = StreamState::kFinish;
            }
          }
        } else {
          CRANE_DEBUG(
              "Connection error when trying reading negotiation from peer {}",
              context->peer());
          state = StreamState::kAbort;
        }
        break;

      case StreamState::kWaitForEofOrSigOrTaskEnd: {
        ok = stream->Read(&request);
        if (ok) {
          if (request.type() != SrunXStreamRequest::SignalType) {
            CRANE_DEBUG("Expect signal from peer {}, but none.",
                        context->peer());
            state = StreamState::kAbort;
          } else {
            // If ctrl+C is pressed before the task ends, inform TaskManager
            // of the interrupt and wait for TaskManager to stop the Task.

            CRANE_TRACE("Receive signum {} from client. Killing task {}",
                        request.signum(), task_id);

            // Todo: Sometimes, TaskManager can't kill a task, there're some
            //  problems here.

            g_task_mgr->TerminateTaskAsync(task_id);

            // The state machine does not switch the state here.
            // We just use stream->Read() to wait for the task to end.
            // When the task ends, the finish_callback will shut down the
            // stream and cause stream->Read() to return with false.
          }
        } else {
          // If the task ends, the callback which handles the end of a task in
          // TaskManager will send the task end message to client. The client
          // will call stream->Write() to end the stream. Then the
          // stream->Read() returns with ok = false.

          state = StreamState::kFinish;
        }
        break;
      }

      case StreamState::kAbort: {
        CRANE_DEBUG("Connection from peer {} aborted.", context->peer());

        // Invalidate resource uuid and free the resource in use.
        g_server->RevokeResourceToken(resource_uuid);

        return Status::CANCELLED;
      }

      case StreamState::kFinish: {
        CRANE_TRACE("Connection from peer {} finished normally",
                    context->peer());

        // Invalidate resource uuid and free the resource in use.
        g_server->RevokeResourceToken(resource_uuid);

        return Status::OK;
      }

      default:
        CRANE_ERROR("Unexpected CranedServer State: {}", uint(state));
        return Status::CANCELLED;
    }
  }
}

void CranedServer::GrantResourceToken(const uuid &resource_uuid,
                                      uint32_t task_id) {
  LockGuard guard(m_mtx_);
  m_resource_uuid_map_[resource_uuid] = task_id;
}

CraneErr CranedServer::RevokeResourceToken(const uuid &resource_uuid) {
  LockGuard guard(m_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter == m_resource_uuid_map_.end()) {
    return CraneErr::kNonExistent;
  }

  m_resource_uuid_map_.erase(iter);

  return CraneErr::kOk;
}

grpc::Status CranedServiceImpl::ExecuteTask(
    grpc::ServerContext *context,
    const crane::grpc::ExecuteTasksRequest *request,
    crane::grpc::ExecuteTasksReply *response) {
  CRANE_TRACE("Requested from CraneCtld to execute {} tasks.",
              request->tasks_size());

  for (auto const &task_to_d : request->tasks()) {
    g_task_mgr->ExecuteTaskAsync(task_to_d);
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::TerminateTask(
    grpc::ServerContext *context,
    const crane::grpc::TerminateTaskRequest *request,
    crane::grpc::TerminateTaskReply *response) {
  g_task_mgr->TerminateTaskAsync(request->task_id());
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
      boost::trim(tcp_line);
      std::vector<std::string> tcp_line_vec;
      boost::split(tcp_line_vec, tcp_line, boost::is_any_of(" :"),
                   boost::token_compress_on);
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

grpc::Status CranedServiceImpl::CreateCgroupForTask(
    grpc::ServerContext *context,
    const crane::grpc::CreateCgroupForTasksRequest *request,
    crane::grpc::CreateCgroupForTasksReply *response) {
  for (int i = 0; i < request->task_id_list_size(); i++) {
    task_id_t task_id = request->task_id_list(i);
    uid_t uid = request->uid_list(i);

    CRANE_TRACE("Receive CreateCgroup for task #{}, uid {}", task_id, uid);
    bool ok = g_task_mgr->CreateCgroupAsync(task_id, uid);
    if (!ok) {
      CRANE_ERROR("Failed to create cgroup for task #{}, uid {}", task_id, uid);
    }
  }

  return Status::OK;
}

grpc::Status CranedServiceImpl::ReleaseCgroupForTask(
    grpc::ServerContext *context,
    const crane::grpc::ReleaseCgroupForTaskRequest *request,
    crane::grpc::ReleaseCgroupForTaskReply *response) {
  task_id_t task_id = request->task_id();
  uid_t uid = request->uid();

  bool ok = g_task_mgr->ReleaseCgroupAsync(task_id, uid);
  if (!ok) {
    CRANE_ERROR("Failed to release cgroup for task #{}, uid {}", task_id, uid);
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

    util::Cgroup *cg;
    if (g_task_mgr->QueryCgOfTaskIdAsync(reply_from_remote_service.task_id(),
                                         &cg)) {
      CRANE_TRACE(
          "ssh client with remote port {} belongs to task #{}. "
          "Moving this ssh session process into the task's cgroup",
          request->ssh_remote_port(), reply_from_remote_service.task_id());

      response->set_ok(true);
      response->set_task_id(reply_from_remote_service.task_id());
      response->set_cgroup_path(cg->GetCgroupString());
    } else {
      CRANE_TRACE(
          "ssh client with remote port {} belongs to task #{}. "
          "But the task's cgroup is not found. Reject this ssh request",
          request->ssh_remote_port(), reply_from_remote_service.task_id());
      response->set_ok(false);
    }

    return Status::OK;
  } else {
    TaskInfoOfUid info{};
    ok = g_task_mgr->QueryTaskInfoOfUidAsync(request->uid(), &info);
    if (ok) {
      CRANE_TRACE(
          "Found a task #{} belonging to uid {}. "
          "This ssh session process is going to be moved into the task's "
          "cgroup {}.",
          info.first_task_id, request->uid(), info.cgroup_path);
      response->set_task_id(info.first_task_id);
      response->set_cgroup_path(info.cgroup_path);
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
  CRANE_TRACE("Moving pid {} to cgroup {}", request->pid(),
              request->cgroup_path());
  bool ok = util::CgroupManager::Instance().MigrateProcTo(
      request->pid(), request->cgroup_path());

  if (!ok) {
    CRANE_ERROR("GrpcMigrateSshProcToCgroup failed on pid: {}, cgroup: {}",
                request->pid(), request->cgroup_path());
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

CraneErr CranedServer::CheckValidityOfResourceUuid(const uuid &resource_uuid,
                                                   uint32_t task_id) {
  LockGuard guard(m_mtx_);

  auto iter = m_resource_uuid_map_.find(resource_uuid);
  if (iter == m_resource_uuid_map_.end()) return CraneErr::kNonExistent;

  if (iter->second != task_id) return CraneErr::kInvalidParam;

  return CraneErr::kOk;
}

}  // namespace Craned
