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
#include "SupervisorStub.h"

#include <protos/Supervisor.grpc.pb.h>

#include <fstream>
#include <sstream>

#include "absl/cleanup/cleanup.h"

namespace {

std::filesystem::path SupervisorUnixSockDir() {
  return Craned::g_config.CraneBaseDir / kDefaultSupervisorUnixSockDir;
}

}  // namespace

namespace Craned {
using grpc::ClientContext;

namespace {
// Find the listening socket without connecting. Frozen supervisors cannot
// drain their accept queue, including across repeated Craned restarts.
CraneExpected<pid_t> FrozenListenerPid(const std::filesystem::path& socket,
                                       const std::vector<pid_t>& members) {
  std::ifstream table("/proc/net/unix");
  std::string row;
  while (std::getline(table, row)) {
    std::istringstream fields(row);
    std::string addr, refs, proto, flags, type, state, inode, path;
    if (!(fields >> addr >> refs >> proto >> flags >> type >> state >> inode) ||
        flags != "00010000" || type != "0001")
      continue;
    std::getline(fields >> std::ws, path);
    if (std::filesystem::path(path) != socket) continue;
    const auto target = "socket:[" + inode + "]";
    for (pid_t pid : members) {
      std::error_code ec;
      const auto fds = std::filesystem::path(std::format("/proc/{}/fd", pid));
      std::filesystem::directory_iterator fd(fds, ec), end;
      for (; !ec && fd != end; fd.increment(ec)) {
        std::error_code link_error;
        if (std::filesystem::read_symlink(fd->path(), link_error) == target)
          return pid;
      }
    }
  }
  return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
}

using SupervisorStatus = std::tuple<job_id_t, step_id_t, pid_t, StepStatus>;

CraneExpected<std::optional<SupervisorStatus>> SuspendedSupervisorStatus(
    const std::filesystem::path& socket, const crane::grpc::JobStepsToD& config,
    step_id_t step, SupervisorStub& stub) {
  namespace cg = Common::CgConstant;
  const auto& job = config.job();
  auto job_path = CgroupManager::MakeCgroupPathInfo(
      job.job_id(), ResourceInNodeV3(job.res()).GetCpuSet());
  const auto system =
      cg::kSystemCgPathPrefix / cg::kRootCgNamePrefix /
      CgroupManager::CgroupStrByStepId(job_path.cg_str, step, true);
  const auto procs = CgroupManager::FreezerPath(system) / "cgroup.procs";
  std::error_code ec;
  if (!std::filesystem::exists(procs, ec) && !ec) return std::nullopt;
  std::ifstream members_file(procs);
  std::vector<pid_t> members{std::istream_iterator<pid_t>(members_file), {}};
  if (!members_file.eof()) return std::unexpected(CraneErrCode::ERR_CGROUP);
  if (members.empty()) return std::nullopt;

  const auto frozen = CgroupManager::IsFullyFrozen(system);
  if (!frozen) return std::unexpected(CraneErrCode::ERR_CGROUP);
  if (!*frozen) {
    auto status = stub.CheckStatus();
    if (!status) return std::unexpected(status.error());
    return *status;
  }
  auto pid = FrozenListenerPid(socket, members);
  if (!pid) return std::unexpected(pid.error());
  return SupervisorStatus{job.job_id(), step, *pid,
                          config.step_status().at(step)};
}
}  // namespace

SupervisorStub::SupervisorStub(job_id_t job_id, step_id_t step_id) {
  auto sock_path =
      SupervisorUnixSockDir() / fmt::format("step_{}.{}.sock", job_id, step_id);
  InitChannelAndStub_(fmt::format("unix://{}", sock_path.string()));
}

SupervisorStub::SupervisorStub(const std::string& endpoint) {
  InitChannelAndStub_(endpoint);
}

CraneExpected<absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                                  SupervisorStub::SupervisorRecoverInfo>>
SupervisorStub::InitAndGetRecoveredMap(
    const crane::grpc::ConfigureCranedRequest& config) {
  static constexpr LazyRE2 supervisor_sock_pattern(
      R"(step_(\d+)\.(\d+)\.sock$)");
  try {
    std::filesystem::path path = SupervisorUnixSockDir();
    if (!std::filesystem::exists(path) ||
        !std::filesystem::is_directory(path)) {
      CRANE_WARN("Supervisor socket dir does not exist. Skip recovery.");
      return {};
    }

    std::vector<std::filesystem::path> files;
    for (const auto& it : std::filesystem::directory_iterator(path)) {
      if (std::filesystem::is_socket(it.path()) &&
          RE2::PartialMatch(it.path().c_str(), *supervisor_sock_pattern))
        files.emplace_back(it.path());
    }

    absl::flat_hash_map<std::pair<job_id_t, step_id_t>, SupervisorRecoverInfo>
        supervisor_status_map;
    supervisor_status_map.reserve(files.size());
    std::atomic_bool retry_restore{false};
    std::latch latch(files.size());
    absl::Mutex mtx;
    for (const auto& file : files) {
      g_thread_pool->detach_task([file, &config, &latch, &mtx, &retry_restore,
                                  &supervisor_status_map] {
        absl::Cleanup finished = [&] { latch.count_down(); };
        auto sock_path = fmt::format("unix://{}", file.string());
        std::shared_ptr stub = std::make_shared<SupervisorStub>(sock_path);

        std::optional<SupervisorStatus> status;
        job_id_t job;
        step_id_t step;
        if (RE2::FullMatch(file.filename().string(), *supervisor_sock_pattern,
                           &job, &step)) {
          const auto alloc = config.job_steps().find(job);
          if (alloc != config.job_steps().end() &&
              JobConfigIsSuspended(alloc->second) &&
              alloc->second.step_status().contains(step)) {
            auto recovered =
                SuspendedSupervisorStatus(file, alloc->second, step, *stub);
            if (!recovered) {
              CRANE_WARN(
                  "Cannot verify suspended supervisor {}, retrying discovery",
                  file.string());
              retry_restore = true;
              return;
            }
            status = *recovered;
          }
        }
        if (!status) {
          auto reply = stub->CheckStatus();
          if (reply) status = *reply;
        }
        if (!status) {
          CRANE_ERROR("CheckStepStatus for {} failed, removing it.",
                      file.string());
          std::error_code ec;
          std::filesystem::remove(file, ec);
          if (ec) {
            CRANE_ERROR("Failed to remove socket file {}: {}", file.string(),
                        ec.message());
          }
          return;
        }
        auto [job_id, step_id, pid, step_status] = *status;
        CRANE_DEBUG("[Step #{}.{}] Supervisor socket {} recovered, pid: {}",
                    job_id, step_id, file.string(), pid);
        absl::MutexLock lock(&mtx);
        supervisor_status_map.emplace(
            std::make_pair(job_id, step_id),
            SupervisorRecoverInfo{pid, step_status, stub});
      });
    }

    latch.wait();
    if (retry_restore) return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
    return supervisor_status_map;

  } catch (const std::exception& e) {
    CRANE_ERROR("An error occurred when recovering supervisor: {}", e.what());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }
}

CraneErrCode SupervisorStub::ExecuteStep() {
  return ExecuteStepWithStatus().code;
}

ExecuteStepRpcResult SupervisorStub::ExecuteStepWithStatus() {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::StepExecutionRequest request;
  crane::grpc::supervisor::StepExecutionReply reply;

  auto ok = m_stub_->ExecuteStep(&context, request, &reply);
  if (!ok.ok()) {
    CRANE_ERROR("ExecuteStep failed: reply {},{}, code: {}", ok.ok(),
                ok.error_message(), static_cast<int>(ok.error_code()));
    return ExecuteStepRpcResult{.code = CraneErrCode::ERR_RPC_FAILURE,
                                .grpc_status = ok.error_code(),
                                .error_message = ok.error_message()};
  }

  return ExecuteStepRpcResult{.code = static_cast<CraneErrCode>(reply.code()),
                              .grpc_status = grpc::StatusCode::OK};
}

CraneExpected<EnvMap> SupervisorStub::QueryStepEnv() {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::QueryStepEnvRequest request;
  crane::grpc::supervisor::QueryStepEnvReply reply;

  auto ok = m_stub_->QueryEnvMap(&context, request, &reply);
  if (ok.ok()) {
    return std::unordered_map(reply.env().begin(), reply.env().end());
  }

  CRANE_ERROR("QueryStepEnv failed: reply {},{}", reply.ok(),
              ok.error_message());
  return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
}

CraneExpected<std::tuple<job_id_t, step_id_t, pid_t, StepStatus>>
SupervisorStub::CheckStatus() {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::CheckStatusRequest request;
  crane::grpc::supervisor::CheckStatusReply reply;

  auto ok = m_stub_->CheckStatus(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return std::make_tuple(reply.job_id(), reply.step_id(),
                           reply.supervisor_pid(), reply.status());
  }

  CRANE_WARN("CheckStatus failed: reply {},{}", reply.ok(), ok.error_message());
  return std::unexpected(CraneErrCode::ERR_RPC_FAILURE);
}

CraneErrCode SupervisorStub::TerminateStep(
    crane::grpc::TerminateSource terminate_source) {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::TerminateStepRequest request;
  crane::grpc::supervisor::TerminateStepReply reply;

  request.set_terminate_source(terminate_source);

  auto ok = m_stub_->TerminateStep(&context, request, &reply);
  if (ok.ok() && reply.ok()) {
    return CraneErrCode::SUCCESS;
  }
  CRANE_ERROR("TerminateStep failed: reply {},{}", reply.ok(),
              ok.error_message());

  CRANE_WARN("TerminateStep failed: reply {},{}", reply.ok(),
             ok.error_message());
  return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorStub::ChangeStepTimeConstraint(
    std::optional<int64_t> time_limit_seconds,
    std::optional<int64_t> deadline_time) {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::ChangeStepTimeConstraintRequest request;
  crane::grpc::supervisor::ChangeStepTimeConstraintReply reply;

  if (time_limit_seconds) {
    request.set_time_limit_seconds(time_limit_seconds.value());
  }

  if (deadline_time) {
    request.set_deadline_time(deadline_time.value());
  }

  auto ok = m_stub_->ChangeStepTimeConstraint(&context, request, &reply);
  if (ok.ok() && reply.ok()) return CraneErrCode::SUCCESS;

  CRANE_WARN("ChangeStepTimeConstraint failed: reply {},{}", reply.ok(),
             ok.error_message());
  return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorStub::MigrateSshProcToCg(pid_t pid) {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::MigrateSshProcToCgroupRequest request;
  crane::grpc::supervisor::MigrateSshProcToCgroupReply reply;
  request.set_pid(pid);
  auto ok = m_stub_->MigrateSshProcToCgroup(&context, request, &reply);
  if (ok.ok())
    return reply.err_code();
  else {
    CRANE_ERROR("MigrateSshProcToCg failed: {}", ok.error_message());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
}

CraneErrCode SupervisorStub::ShutdownSupervisor() {
  ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::supervisor::ShutdownSupervisorRequest request;
  crane::grpc::supervisor::ShutdownSupervisorReply reply;

  auto ok = m_stub_->ShutdownSupervisor(&context, request, &reply);
  if (ok.ok()) return CraneErrCode::SUCCESS;

  CRANE_WARN("ShutdownSupervisor failed: ok,{}", ok.error_message());
  return CraneErrCode::ERR_RPC_FAILURE;
}

CraneErrCode SupervisorStub::ReceivePmixPort(
    ClientContext& context,
    const std::vector<std::pair<CranedId, std::string>>& pmix_ports) {
  crane::grpc::supervisor::ReceivePmixPortRequest request;
  crane::grpc::supervisor::ReceivePmixPortReply reply;

  auto pmix_port_list = request.mutable_pmix_ports();
  for (const auto& pmix_port : pmix_ports) {
    auto pmix_port_req = pmix_port_list->Add();
    pmix_port_req->set_craned_id(pmix_port.first);
    pmix_port_req->set_port(pmix_port.second);
  }

  auto ok = m_stub_->ReceivePmixPort(&context, request, &reply);
  if (!ok.ok() || !reply.ok()) {
    CRANE_ERROR("ReceivePmixPort failed: reply {},{}, request size {}, ok {}",
                reply.ok(), ok.error_message(), request.pmix_ports().size(),
                ok.ok());
    return CraneErrCode::ERR_RPC_FAILURE;
  }

  return CraneErrCode::SUCCESS;
}

void SupervisorStub::InitChannelAndStub_(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::supervisor::Supervisor::NewStub(m_channel_);
}

}  // namespace Craned
