#include "CranedServer.h"

#include <arpa/inet.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <boost/algorithm/string/join.hpp>
#include <filesystem>
#include <fstream>
#include <utility>

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
    const crane::grpc::ExecuteTaskRequest *request,
    crane::grpc::ExecuteTaskReply *response) {
  CRANE_TRACE("Received a task with id {}", request->task().task_id());

  g_task_mgr->ExecuteTaskAsync(request->task());

  response->set_ok(true);
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
    const crane::grpc::CreateCgroupForTaskRequest *request,
    crane::grpc::CreateCgroupForTaskReply *response) {
  CRANE_TRACE("Receive CreateCgroup for task #{}, uid {}", request->task_id(),
              request->uid());
  bool ok = g_task_mgr->CreateCgroupAsync(request->task_id(), request->uid());
  response->set_ok(ok);
  return Status::OK;
}

grpc::Status CranedServiceImpl::ReleaseCgroupForTask(
    grpc::ServerContext *context,
    const crane::grpc::ReleaseCgroupForTaskRequest *request,
    crane::grpc::ReleaseCgroupForTaskReply *response) {
  bool ok = g_task_mgr->ReleaseCgroupAsync(request->task_id(), request->uid());
  response->set_ok(ok);
  return Status::OK;
}

grpc::Status CranedServiceImpl::QueryTaskIdFromPortForward(
    grpc::ServerContext *context,
    const crane::grpc::QueryTaskIdFromPortForwardRequest *request,
    crane::grpc::QueryTaskIdFromPortForwardReply *response) {
  // Check whether the remote address is in the addresses of CraneD nodes.
  auto ip_iter =
      g_config.Ipv4ToNodesHostname.find(request->target_craned_address());
  if (ip_iter == g_config.Ipv4ToNodesHostname.end()) {
    // Not in the addresses of CraneD nodes. This ssh request comes from a user.
    // Check if the user's uid is running a task. If so, move it in to the
    // cgroup of his first task. If not so, reject this ssh request.
    CRANE_TRACE(
        "Receive QueryTaskIdFromPortForward from Pam module: "
        "ssh_remote_port: {}, craned_address: {}, craned_port: {}. "
        "This ssh comes from a user machine. uid: {}",
        request->ssh_remote_port(), request->target_craned_address(),
        request->target_craned_port(), request->uid());

    response->set_from_user(true);

    TaskInfoOfUid info{};
    bool ok = g_task_mgr->QueryTaskInfoOfUidAsync(request->uid(), &info);
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

  CRANE_TRACE(
      "Receive QueryTaskIdFromPortForward from Pam module: "
      "ssh_remote_port: {}, craned_address: {}, craned_port: {}. "
      "This ssh comes from a CraneD node. uid: {}",
      request->ssh_remote_port(), request->target_craned_address(),
      request->target_craned_port(), request->uid());
  // In the addresses of CraneD nodes. This ssh request comes from a CraneD
  // node. Check if the remote port belongs to a task. If so, move it in to the
  // cgroup of this task. If not so, check uid again or reject this ssh request.

  std::string target_craned = fmt::format(
      "{}:{}", request->target_craned_address(), request->target_craned_port());
  std::shared_ptr<Channel> channel_of_remote_craned =
      grpc::CreateChannel(target_craned, grpc::InsecureChannelCredentials());

  std::unique_ptr<crane::grpc::Craned::Stub> stub_of_remote_craned =
      crane::grpc::Craned::NewStub(channel_of_remote_craned);

  crane::grpc::QueryTaskIdFromPortRequest request_to_remote_craned;
  crane::grpc::QueryTaskIdFromPortReply reply_from_remote_craned;
  ClientContext context_of_remote_craned;
  Status status_remote_craned;

  request_to_remote_craned.set_port(request->ssh_remote_port());

  status_remote_craned = stub_of_remote_craned->QueryTaskIdFromPort(
      &context_of_remote_craned, request_to_remote_craned,
      &reply_from_remote_craned);
  if (!status_remote_craned.ok()) {
    CRANE_ERROR("QueryTaskIdFromPort gRPC call failed: {} | {}",
                status_remote_craned.error_message(),
                status_remote_craned.error_details());
    response->set_ok(false);
    return Status::OK;
  }

  if (reply_from_remote_craned.ok()) {
    util::Cgroup *cg;
    if (g_task_mgr->QueryCgOfTaskIdAsync(reply_from_remote_craned.task_id(),
                                         &cg)) {
      CRANE_TRACE(
          "ssh client with remote port {} belongs to task #{}. "
          "Moving this ssh session process into the task's cgroup",
          request->ssh_remote_port(), reply_from_remote_craned.task_id());

      response->set_ok(true);
      response->set_task_id(reply_from_remote_craned.task_id());
      response->set_cgroup_path(cg->GetCgroupString());
    } else {
      CRANE_TRACE(
          "ssh client with remote port {} belongs to task #{}. "
          "But the task's cgroup is not found. Reject this ssh request",
          request->ssh_remote_port(), reply_from_remote_craned.task_id());
      response->set_ok(false);
    }

    return Status::OK;
  } else {
    TaskInfoOfUid info{};
    bool ok = g_task_mgr->QueryTaskInfoOfUidAsync(request->uid(), &info);
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

CranedServer::CranedServer(const Config::CranedListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CranedServiceImpl>();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_conf.UnixSocketListenAddr,
                           grpc::InsecureServerCredentials());

  std::string listen_addr_port = fmt::format(
      "{}:{}", listen_conf.CranedListenAddr, listen_conf.CranedListenPort);
  if (listen_conf.UseTls) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair pem_key_cert_pair;
    pem_key_cert_pair.cert_chain = listen_conf.CertContent;
    pem_key_cert_pair.private_key = listen_conf.KeyContent;

    grpc::SslServerCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = listen_conf.CertContent;
    ssl_opts.pem_key_cert_pairs.emplace_back(std::move(pem_key_cert_pair));
    ssl_opts.force_client_auth = true;
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
