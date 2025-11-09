/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include "StepInstance.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
namespace Craned {
using namespace std::literals::chrono_literals;
StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      supv_pid(0),
      step_to_d(step_to_d),
      status(StepStatus::Configuring) {}

StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d,
                           pid_t supv_pid, StepStatus status,
                           std::shared_ptr<SupervisorStub> supervisor_stub)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      supv_pid(supv_pid),
      step_to_d(step_to_d),
      status(status),
      supervisor_stub(supervisor_stub) {}

void StepInstance::CleanUp() {
  if (this->status != StepStatus::Completed &&
      this->status != StepStatus::Failed &&
      this->status != StepStatus::Cancelled &&
      this->status != StepStatus::OutOfMemory &&
      this->status != StepStatus::ExceedTimeLimit) {
    CRANE_WARN(
        "[Step #{}.{}] Cleaning up a step which is not in finished status, "
        "current status: {}.",
        job_id, step_id, static_cast<int>(this->status));
  }
  constexpr auto destroy_cgroup = [](CgroupInterface* cgroup) {
    if (cgroup == nullptr) return;

    g_thread_pool->detach_task([cgroup]() {
      int cnt = 0;

      while (true) {
        if (cgroup->Empty()) break;

        if (cnt >= 5) {
          CRANE_ERROR(
              "Couldn't kill the processes in cgroup {} after {} times. "
              "Skipping it.",
              cgroup->CgroupName(), cnt);
          break;
        }

        cgroup->KillAllProcesses();
        ++cnt;
        std::this_thread::sleep_for(std::chrono::milliseconds{100ms});
      }
      // TODO: Plugin support step cgroup destroy hooks.
      cgroup->Destroy();

      delete cgroup;
    });
  };
  if (this->crane_cgroup != nullptr) {
    destroy_cgroup(this->crane_cgroup.release());
    this->crane_cgroup = nullptr;
  }
  if (this->user_cgroup != nullptr) {
    destroy_cgroup(this->user_cgroup.release());
    this->user_cgroup = nullptr;
  }
}

CraneErrCode StepInstance::CreateCg() {
  auto cg_expt = CgroupManager::AllocateAndGetCgroup(
      CgroupManager::CgroupStrByStepId(job_id, step_id, true), step_to_d.res(),
      false);
  if (!cg_expt) return cg_expt.error();
  this->crane_cgroup = std::move(cg_expt.value());
  return CraneErrCode::SUCCESS;
}

CraneErrCode StepInstance::SpawnSupervisor(const EnvMap& job_env_map) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  std::array<int, 2> supervisor_craned_pipe{};
  std::array<int, 2> craned_supervisor_pipe{};

  if (pipe(supervisor_craned_pipe.data()) == -1) {
    CRANE_ERROR("Pipe creation failed!");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (pipe(craned_supervisor_pipe.data()) == -1) {
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    CRANE_ERROR("Pipe creation failed!");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // The ResourceInNode structure should be copied here if being accessed in
  // the child process.
  // Note that CgroupManager acquires a lock for this.
  // If the lock is held in the parent process during fork, the forked thread
  // in the child proc will block forever.
  // auto res_in_node = job->job_spec.cgroup_spec.res_in_node;

  pid_t child_pid = fork();

  if (child_pid == -1) {
    CRANE_ERROR("[Step #{}.{}] fork() failed: {}", job_id, step_id,
                strerror(errno));

    close(craned_supervisor_pipe[0]);
    close(craned_supervisor_pipe[1]);
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("[Step #{}.{}] Subprocess was created, pid: {}", job_id,
                step_id, child_pid);

    bool ok;
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    int craned_supervisor_fd = craned_supervisor_pipe[1];
    close(craned_supervisor_pipe[0]);
    int supervisor_craned_fd = supervisor_craned_pipe[0];
    close(supervisor_craned_pipe[1]);

    FileInputStream istream(supervisor_craned_fd);
    FileOutputStream ostream(craned_supervisor_fd);

    // Before exec, we need to make sure that the cgroup is ready.
    if (!this->crane_cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "[Step #{}.{}] Terminate the subprocess due to failure of cgroup "
          "migration.",
          job_id, step_id);

      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_CGROUP;
    }

    // Do Supervisor Init
    crane::grpc::supervisor::InitSupervisorRequest init_req;
    init_req.set_job_id(job_id);
    init_req.set_step_id(step_id);
    init_req.set_debug_level(g_config.Supervisor.DebugLevel);
    init_req.set_craned_id(g_config.CranedIdOfThisNode);
    init_req.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_req.set_crane_base_dir(g_config.CraneBaseDir);
    init_req.set_crane_script_dir(g_config.CranedScriptDir);
    init_req.mutable_step_spec()->CopyFrom(step_to_d);
    init_req.set_log_dir(g_config.Supervisor.LogDir);
    auto* cfored_listen_conf = init_req.mutable_cfored_listen_conf();
    cfored_listen_conf->set_use_tls(g_config.ListenConf.TlsConfig.Enabled);
    cfored_listen_conf->set_domain_suffix(
        g_config.ListenConf.TlsConfig.DomainSuffix);
    auto* tls_certs = cfored_listen_conf->mutable_tls_certs();
    tls_certs->set_cert_content(
        g_config.ListenConf.TlsConfig.TlsCerts.CertContent);
    tls_certs->set_ca_content(g_config.ListenConf.TlsConfig.CaContent);
    tls_certs->set_key_content(
        g_config.ListenConf.TlsConfig.TlsCerts.KeyContent);

    // Pass job env to supervisor
    init_req.mutable_env()->clear();
    init_req.mutable_env()->insert(job_env_map.begin(), job_env_map.end());

    std::string cgroup_path_str = this->crane_cgroup->CgroupPath().string();
    init_req.set_cgroup_path(cgroup_path_str);
    CRANE_TRACE("[Step #{}.{}] Setting cgroup path: {}", job_id, step_id,
                cgroup_path_str);

    if (g_config.Container.Enabled) {
      auto* container_conf = init_req.mutable_container_config();
      container_conf->set_temp_dir(g_config.Container.TempDir);
      container_conf->set_runtime_endpoint(g_config.Container.RuntimeEndpoint);
      container_conf->set_image_endpoint(g_config.Container.ImageEndpoint);
    }

    if (g_config.Plugin.Enabled) {
      auto* plugin_conf = init_req.mutable_plugin_config();
      plugin_conf->set_socket_path(g_config.Plugin.PlugindSockPath);
    }

    ok = SerializeDelimitedToZeroCopyStream(init_req, &ostream);
    if (!ok) {
      CRANE_ERROR("[Step #{}.{}] Failed to serialize msg to ostream: {}",
                  job_id, step_id, strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Step #{}.{}] Failed to send init msg to supervisor: {}",
                  job_id, step_id, strerror(ostream.GetErrno()));

      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    CRANE_TRACE("[Step #{}.{}] Supervisor init msg send.", job_id, step_id);

    crane::grpc::supervisor::SupervisorReady supervisor_ready;
    bool clean_eof{false};
    ok = ParseDelimitedFromZeroCopyStream(&supervisor_ready, &istream,
                                          &clean_eof);
    if (!ok || !supervisor_ready.ok()) {
      if (!ok)
        CRANE_ERROR("[Step #{}.{}] Pipe child endpoint failed: {},{}", job_id,
                    step_id,
                    std::error_code(istream.GetErrno(), std::generic_category())
                        .message(),
                    clean_eof);
      if (!supervisor_ready.ok())
        CRANE_ERROR("[Step #{}.{}] False from subprocess {}.", job_id, step_id,
                    child_pid);

      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    CRANE_TRACE("[Step #{}.{}] Supervisor init msg received.", job_id, step_id);
    this->supervisor_stub = std::make_shared<SupervisorStub>(job_id, step_id);

    this->supv_pid = child_pid;
    return CraneErrCode::SUCCESS;
  } else {  // Child proc, NOLINT(readability-else-after-return)
    // Disable SIGABRT backtrace from child processes.
    signal(SIGABRT, SIG_DFL);

    int craned_supervisor_fd = craned_supervisor_pipe[0];
    close(craned_supervisor_pipe[1]);
    int supervisor_craned_fd = supervisor_craned_pipe[1];
    close(supervisor_craned_pipe[0]);

    // Message will send to stdin of Supervisor for its init.
    for (int retry_count{0}; retry_count < 5; ++retry_count) {
      if (-1 == dup2(craned_supervisor_fd, STDIN_FILENO)) {
        if (errno == EINTR) {
          fmt::print(
              stderr,
              "[Step #{}.{}] Retrying dup2 stdin: interrupted system call, "
              "retry count: {}",
              job_id, step_id, retry_count);
          continue;
        }
        std::error_code ec(errno, std::generic_category());
        fmt::print("[Step #{}.{}] Failed to dup2 stdin: {}.", job_id, step_id,
                   ec.message());
        std::abort();
      }

      break;
    }

    for (int retry_count{0}; retry_count < 5; ++retry_count) {
      if (-1 == dup2(supervisor_craned_fd, STDOUT_FILENO)) {
        if (errno == EINTR) {
          fmt::print(
              stderr,
              "[Step #{}.{}] Retrying dup2 stdout: interrupted system call, "
              "retry count: {}",
              job_id, step_id, retry_count);
          continue;
        }
        std::error_code ec(errno, std::generic_category());
        fmt::print("[Step #{}.{}] Failed to dup2 stdout: {}.", job_id, step_id,
                   ec.message());
        std::abort();
      }

      break;
    }
    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    util::os::CloseFdFrom(3);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    auto supv_name = fmt::format("csupervisor: [{}.{}]", job_id, step_id);
    argv.emplace_back(supv_name.c_str());
    argv.push_back(nullptr);

    // Use execvp to search the kSupervisorPath in the PATH.
    execvp(g_config.Supervisor.Path.c_str(),
           const_cast<char* const*>(argv.data()));

    // Error occurred since execvp returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess] Failed to execvp {}. Error: {}\n",
               g_config.Supervisor.Path.c_str(), strerror(errno));

    // TODO: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    // exit codes
    abort();
  }
}

void StepInstance::GotNewStatus(const StepStatus& new_status) {
  switch (new_status) {
  case StepStatus::Configuring:
  case StepStatus::Pending:
  case StepStatus::Invalid: {
    CRANE_ERROR("[Step #{}.{}] Invalid new status received: {}, ignored.",
                job_id, step_id, new_status);
    return;
  }

  case StepStatus::Running: {
    if (this->IsDaemonStep()) {
      if (status != StepStatus::Configuring)
        CRANE_WARN(
            "[Step {}.{}] Daemon step status is not 'Configuring' when "
            "receiving new status 'Running', current status: {}.",
            job_id, step_id, this->status);
    } else {
      if (status != StepStatus::Starting)
        CRANE_WARN(
            "[Step {}.{}] Step status is not 'Starting' when receiving new "
            "status 'Running', current status: {}.",
            job_id, step_id, this->status);
    }
    break;
  }
  case StepStatus::Starting: {
    if (this->IsDaemonStep()) {
      CRANE_WARN(
          "[Step {}.{}] Daemon step got invalid status 'Starting' current "
          "status: {}.",
          job_id, step_id, this->status);
    } else {
      if (status != StepStatus::Configuring)
        CRANE_WARN(
            "[Step {}.{}] Step status is not 'Configuring' when "
            "receiving new status 'Starting', current status: {}.",
            job_id, step_id, this->status);
    }
    break;
  }

  case StepStatus::Completing: {
    if (status != StepStatus::Running)
      CRANE_WARN(
          "[Step {}.{}] Step status is not 'Running' when receiving new "
          "status 'Completing', current status: {}.",
          job_id, step_id, this->status);
    break;
  }
  // Finished status
  case StepStatus::ExceedTimeLimit:
  case StepStatus::OutOfMemory:
  case StepStatus::Cancelled:
  case StepStatus::Failed:
  case StepStatus::Completed: {
    if (status != StepStatus::Running && status != StepStatus::Completing &&
        status != StepStatus::Starting && status != StepStatus::Configuring) {
      CRANE_WARN(
          "[Step {}.{}] Step status is not "
          "Running/Completing/Starting/Configuring when receiving new finished "
          "status {}, current status: {}.",
          job_id, step_id, new_status, this->status);
    }
    break;
  }
  default: {
    std::unreachable();
  }
  }

  status = new_status;
}

void StepInstance::ExecuteStepAsync() {
  this->GotNewStatus(StepStatus::Running);

  g_thread_pool->detach_task([job_id = job_id, step_id = step_id,
                              stub = supervisor_stub] {
    auto code = stub->ExecuteStep();
    if (code != CraneErrCode::SUCCESS) {
      CRANE_ERROR("[Step #{}.{}] Supervisor failed to execute task, code:{}.",
                  job_id, step_id, static_cast<int>(code));
    }
  });
}

CraneErrCode StepInstance::KillPid_(pid_t pid, int signum) {
  // TODO: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  CRANE_TRACE("Killing pid {} with signal {}", pid, signum);

  // Send the signal to the whole process group.
  int err = kill(-pid, signum);

  if (err == 0)
    return CraneErrCode::SUCCESS;
  else {
    std::error_code ec(errno, std::system_category());
    CRANE_TRACE("kill failed. error: {}", ec.message());
    return CraneErrCode::ERR_GENERIC_FAILURE;
  }
}

}  // namespace Craned
