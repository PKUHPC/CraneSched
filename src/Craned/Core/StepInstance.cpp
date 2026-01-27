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

#include "CtldClient.h"

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
  if (this->crane_cgroup != nullptr) {
    g_thread_pool->detach_task([job_id = job_id, step_id = step_id,
                                cgroup = crane_cgroup.release()] {
      // This is step_id/system cgroup
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

        cgroup->KillAllProcesses(SIGKILL);
        ++cnt;
        std::this_thread::sleep_for(std::chrono::milliseconds{100ms});
      }
      // TODO: Plugin support step cgroup destroy hooks.
      cgroup->Destroy();

      delete cgroup;

      auto step_cg_path =
          std::filesystem::path{Common::CgConstant::kSystemCgPathPrefix} /
          Common::CgConstant::kRootCgNamePrefix /
          CgroupManager::CgroupStrByJobId(job_id) /
          fmt::format("{}{}", Common::CgConstant::kStepCgNamePrefix, step_id);

      std::error_code ec;
      if (std::filesystem::exists(step_cg_path, ec)) {
        // Remove step cgroup directory
        std::filesystem::remove(step_cg_path, ec);
        if (ec) {
          CRANE_ERROR("[Step #{}.{}] Failed to remove step cgroup dir {}: {}",
                      job_id, step_id, step_cg_path, ec.message());
        } else {
          CRANE_DEBUG("[Step #{}.{}] Step cgroup dir {} removed.", job_id,
                      step_id, step_cg_path);
        }
      } else {
        if (ec) {
          CRANE_ERROR(
              "[Step #{}.{}] Failed to check existence of step cgroup dir {}: "
              "{}",
              job_id, step_id, step_cg_path, ec.message());
        } else {
          CRANE_DEBUG(
              "[Step #{}.{}] Step cgroup dir {} does not exist, skip clean.",
              job_id, step_id, step_cg_path);
        }
      }
    });
  }
}

CraneErrCode StepInstance::Prepare() {
  auto cg_expt = CgroupManager::CreateOrOpenCgroup(
      CgroupManager::CgroupStrByStepId(job_id, step_id, true), false);
  if (!cg_expt) return cg_expt.error();
  this->crane_cgroup = std::move(cg_expt.value());
  auto* cg = this->crane_cgroup.get();
  CraneErrCode err = CgroupManager::SetCgroupResource(cg, step_to_d.res());
  if (err != CraneErrCode::SUCCESS) {
    return err;
  }
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

    // Do Supervisor Init
    crane::grpc::supervisor::InitSupervisorRequest init_req;
    init_req.set_job_id(job_id);
    init_req.set_job_name(step_to_d.name());
    init_req.set_step_id(step_id);
    init_req.set_debug_level(g_config.Supervisor.DebugLevel);
    init_req.set_craned_id(g_config.CranedIdOfThisNode);
    init_req.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_req.set_crane_base_dir(g_config.CraneBaseDir);
    init_req.set_crane_script_dir(g_config.CranedScriptDir);
    init_req.mutable_step_spec()->CopyFrom(step_to_d);
    init_req.set_log_dir(g_config.Supervisor.LogDir);
    init_req.set_max_log_file_size(g_config.Supervisor.MaxLogFileSize);
    init_req.set_max_log_file_num(g_config.Supervisor.MaxLogFileNum);
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
      if (g_config.Container.BindFs.Enabled) {
        auto* bindfs_conf = container_conf->mutable_bindfs();
        bindfs_conf->set_bindfs_binary(
            g_config.Container.BindFs.BindfsBinary.string());
        bindfs_conf->set_fusermount_binary(
            g_config.Container.BindFs.FusermountBinary.string());
        bindfs_conf->set_mount_base_dir(
            g_config.Container.BindFs.MountBaseDir.string());
      }
      auto* subid_conf = container_conf->mutable_subid();
      subid_conf->set_managed(g_config.Container.SubId.Managed);
      subid_conf->set_range_size(g_config.Container.SubId.RangeSize);
      subid_conf->set_base_offset(g_config.Container.SubId.BaseOffset);
    }

    if (g_config.Plugin.Enabled) {
      auto* plugin_conf = init_req.mutable_plugin_config();
      plugin_conf->set_socket_path(g_config.Plugin.PlugindSockPath);
    }

    if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob ||
        !g_config.JobLifecycleHook.TaskPrologs.empty() ||
        !g_config.JobLifecycleHook.TaskEpilogs.empty()) {
      auto* job_lifecycle_hook_conf =
          init_req.mutable_job_lifecycle_hook_config();

      for (const auto& prolog : g_config.JobLifecycleHook.TaskPrologs) {
        job_lifecycle_hook_conf->add_task_prologs(prolog);
      }
      for (const auto& epilog : g_config.JobLifecycleHook.TaskEpilogs) {
        job_lifecycle_hook_conf->add_task_epilogs(epilog);
      }

      if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob &&
          IsDaemonStep()) {
        for (const auto& prolog : g_config.JobLifecycleHook.Prologs) {
          job_lifecycle_hook_conf->add_prologs(prolog);
        }
        for (const auto& epilog : g_config.JobLifecycleHook.Epilogs) {
          job_lifecycle_hook_conf->add_epilogs(epilog);
        }
      }

      job_lifecycle_hook_conf->set_prolog_timeout(
          g_config.JobLifecycleHook.PrologTimeout);
      job_lifecycle_hook_conf->set_epilog_timeout(
          g_config.JobLifecycleHook.EpilogTimeout);
      job_lifecycle_hook_conf->set_prolog_epilog_timeout(
          g_config.JobLifecycleHook.PrologEpilogTimeout);
      job_lifecycle_hook_conf->set_max_output_size(
          g_config.JobLifecycleHook.MaxOutputSize);
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

      crane_cgroup->KillAllProcesses(SIGKILL);

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

      crane_cgroup->KillAllProcesses(SIGKILL);

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

    // Before exec, we need to make sure that the cgroup is ready.
    if (!this->crane_cgroup->MigrateProcIn(getpid())) {
      fmt::print(
          stderr,
          "[Step #{}.{}] Terminate the subprocess due to failure of cgroup "
          "migration.",
          job_id, step_id);

      std::abort();
    }
    int craned_supervisor_fd = craned_supervisor_pipe[0];
    close(craned_supervisor_pipe[1]);
    int supervisor_craned_fd = supervisor_craned_pipe[1];
    close(supervisor_craned_pipe[0]);

    util::os::CloseFdFromExcept(3,
                                {craned_supervisor_fd, supervisor_craned_fd});

    // Prepare the command line arguments.
    std::vector<std::string> string_argv;
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    auto supv_name = fmt::format("csupervisor: [{}.{}]", job_id, step_id);
    string_argv.emplace_back(supv_name.c_str());
    string_argv.push_back("--input-fd");
    string_argv.push_back(std::to_string(craned_supervisor_fd));
    string_argv.push_back("--output-fd");
    string_argv.push_back(std::to_string(supervisor_craned_fd));
    argv.reserve(string_argv.size());
    for (auto& arg : string_argv) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);  // argv must be null-terminated.
    fmt::print(stderr,
               "[{:%Y-%m-%d %H:%M:%S}] [Step #{}.{}]: Executing supervisor\n",
               std::chrono::system_clock::now(), job_id, step_id);

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
      g_ctld_client->StepStatusChangeAsync(StepStatusChangeQueueElem{
          .job_id = job_id,
          .step_id = step_id,
          .new_status = StepStatus::Failed,
          .exit_code = ExitCode::EC_RPC_ERR,
          .reason = "Supervisor not responding when execute task",
          .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
      // Ctld will send ShutdownSupervisor after status change from
      // daemon supervisor, for common step, will shut down itself when all
      // task in local step finished.
    }
  });
}

}  // namespace Craned
