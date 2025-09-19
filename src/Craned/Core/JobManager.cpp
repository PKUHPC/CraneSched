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

#include "JobManager.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <pty.h>
#include <sys/wait.h>

#include "CranedPublicDefs.h"
#include "CtldClient.h"
#include "SupervisorKeeper.h"
#include "crane/PluginClient.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"
#include "protos/Supervisor.pb.h"

namespace Craned {

EnvMap JobInD::GetJobEnvMap() {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(job_to_d.res());

  // TODO: Move all job level env to here.
  env_map.emplace("CRANE_JOB_ID", std::to_string(job_to_d.job_id()));
  return env_map;
}

JobManager::JobManager() {
  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });

  if (int rc = m_sigchld_handle_->start(SIGCHLD); rc != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle: {}", uv_err_name(rc));
  }

  m_sigint_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigint_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigint_handle_->start(SIGINT); rc != 0) {
    CRANE_ERROR("Failed to start the SIGINT handle: {}", uv_err_name(rc));
  }

  m_sigterm_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigterm_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigterm_handle_->start(SIGTERM); rc != 0) {
    CRANE_ERROR("Failed to start the SIGTERM handle: {}", uv_err_name(rc));
  }

  m_check_supervisor_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_check_supervisor_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanCheckSupervisorQueueCb_();
      });

  m_check_supervisor_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_check_supervisor_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        if (EvCheckSupervisorRunning_()) handle.stop();
      });

  // gRPC Execute Task Event
  m_grpc_execute_step_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_execute_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcExecuteStepQueueCb_();
      });

  // Task Status Change Event
  m_task_status_change_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_task_status_change_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTaskStatusChangeQueueCb_();
      });

  m_change_task_time_limit_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_change_task_time_limit_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanChangeTaskTimeLimitQueueCb_();
      });

  m_terminate_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateTaskQueueCb_();
      });

  m_uvw_thread_ = std::thread([this]() {
    util::SetCurrentThreadName("JobMgrLoopThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_is_ending_now_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in JobManager loop.");
    }
    m_uvw_loop_->run();
  });
}

CraneErrCode JobManager::Recover(
    std::unordered_map<task_id_t, JobInD>&& job_map,
    std::unordered_map<task_id_t, std::unique_ptr<StepInstance>>&& step_map) {
  CRANE_INFO("Job allocation [{}] recovered.",
             absl::StrJoin(job_map | std::views::keys, ","));

  for (auto&& job : job_map | std::views::values) {
    job_id_t job_id = job.job_id;
    uid_t uid = job.Uid();
    m_job_map_.Emplace(job_id, std::move(job));

    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }

  for (auto&& [job_id, step_inst] : step_map) {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    CRANE_ASSERT(job);

    // TODO:replace this with step_id
    step_id_t step_id = 0;

    job->step_map.emplace(step_id, std::move(step_inst));
    CRANE_TRACE("Job #{} Step #{} was recovered.", job_id, step_id);
  }
  return CraneErrCode::SUCCESS;
}

JobManager::~JobManager() {
  CRANE_DEBUG("JobManager is being destroyed.");
  m_is_ending_now_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

bool JobManager::AllocJobs(std::vector<JobInD>&& jobs) {
  CRANE_DEBUG("Allocating {} job", jobs.size());

  auto begin = std::chrono::steady_clock::now();

  {
    auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
    auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    for (auto& job : jobs) {
      task_id_t job_id = job.job_id;
      uid_t uid = job.Uid();
      CRANE_TRACE("Create lazily allocated cgroups for job #{}, uid {}", job_id,
                  uid);
      job_map_ptr->emplace(job_id, std::move(job));

      if (uid_map_ptr->contains(uid)) {
        uid_map_ptr->at(uid).RawPtr()->emplace(job_id);
      } else {
        uid_map_ptr->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
      }
    }
  }

  auto end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());
  return true;
}

CgroupInterface* JobManager::GetCgForJob(task_id_t job_id) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_TRACE("Job #{} does not exist when querying its cgroup.", job_id);
    return nullptr;
  }
  if (job->cgroup) return job->cgroup.get();

  auto cg_expt = CgroupManager::AllocateAndGetCgroup(
      CgroupManager::CgroupStrByJobId(job->job_id), job->job_to_d.res(), false);
  if (cg_expt.has_value()) {
    job->cgroup = std::move(cg_expt.value());
    return job->cgroup.get();
  }

  CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
  return nullptr;
}

// Wait for supervisor exit and release cgroup
bool JobManager::FreeJobs(std::set<task_id_t>&& job_ids) {
  // We do noting here, just check if supervisor exist and remove its cgroup
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      if (!map_ptr->contains(job_id)) {
        CRANE_WARN("Try to free nonexistent job#{}", job_ids);
        return false;
      }
    }
  }

  m_check_supervisor_queue_.enqueue(job_ids | std::ranges::to<std::vector>());
  m_check_supervisor_async_handle_->send();
  return true;
}

void JobManager::EvCleanCheckSupervisorQueueCb_() {
  std::vector<task_id_t> job_ids;
  absl::MutexLock lk(&m_release_cg_mtx_);
  while (m_check_supervisor_queue_.try_dequeue(job_ids)) {
    for (task_id_t job_id : job_ids) {
      if (m_release_job_retry_map_.contains(job_id)) {
        CRANE_DEBUG("[Job #{}] already waiting to release, ignored.", job_id);
        continue;
      }
      m_release_job_retry_map_.emplace(job_id, 0);
    }
    if (!m_check_supervisor_timer_handle_->active())
      m_check_supervisor_timer_handle_->start(uvw::timer_handle::time{0},
                                              uvw::timer_handle::time{1000});
  }
}

bool JobManager::EvCheckSupervisorRunning_() {
  std::vector<task_id_t> job_ids;
  std::vector<job_id_t> missing_jobs;
  {
    absl::MutexLock lk(&m_release_cg_mtx_);
    for (auto& [job_id, retry_count] : m_release_job_retry_map_) {
      // TODO: replace following with step_id
      auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
      if (!job_ptr) {
        missing_jobs.emplace_back(job_id);
        continue;
      }
      if (job_ptr->step_map.empty()) {
        CRANE_DEBUG("[Job #{}] has no step, just clean up.", job_id);
        job_ids.push_back(job_id);
      }
      for (const auto& [step_id, step] : job_ptr->step_map) {
        bool exists = false;
        if (kill(step->supv_pid, 0) == 0) {
          exists = true;
        } else {
          if (errno == ESRCH) {
            exists = false;
          } else {
            CRANE_ERROR(
                "Failed to detect step supervisor pid #{}:{}, consider it "
                "exit.",
                step->supv_pid, strerror(errno));
            exists = false;
          }
        }
        if (!exists) {
          job_ids.emplace_back(job_id);
        } else {
          CRANE_TRACE("[Step #{}.{}] Step supervisor pid {} still exists.",
                      job_id, step_id, step->supv_pid);
          retry_count++;
        }
        if (retry_count > kMaxSupervisorCheckRetryCount) {
          job_ids.emplace_back(job_id);
        }
      }
    }
    for (task_id_t job_id : missing_jobs) {
      m_release_job_retry_map_.erase(job_id);
    }
    for (task_id_t job_id : job_ids) {
      m_release_job_retry_map_.erase(job_id);
    }
  }

  if (!missing_jobs.empty()) {
    CRANE_TRACE("Job [{}] does not exist, skip cgroup clean up.",
                absl::StrJoin(missing_jobs, ","));
  }

  if (!job_ids.empty()) {
    CRANE_TRACE("Supervisor for Job [{}] found to be exited",
                absl::StrJoin(job_ids, ","));

    g_thread_pool->detach_task(
        [this, jobs = std::move(job_ids)] { FreeJobAllocation_(jobs); });
  }

  return m_release_job_retry_map_.empty();
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
void JobManager::EvSigchldCb_() {
  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      CRANE_TRACE("Child pid {} exit", pid);
      // We do nothing now
    } else if (pid == 0) {
      // There's no child that needs reaping.
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void JobManager::EvSigintCb_() {
  CRANE_TRACE("Triggering exit event...");
  if (m_sigint_cb_) m_sigint_cb_();
  m_is_ending_now_ = true;
}

void JobManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

CraneErrCode JobManager::KillPid_(pid_t pid, int signum) {
  // TODO: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  CRANE_TRACE("Killing pid {} with signal {}", pid, signum);

  // Send the signal to the whole process group.
  int err = kill(-pid, signum);

  if (err == 0)
    return CraneErrCode::SUCCESS;
  else {
    CRANE_TRACE("kill failed. error: {}", strerror(errno));
    return CraneErrCode::ERR_GENERIC_FAILURE;
  }
}

void JobManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErrCode JobManager::SpawnSupervisor_(JobInD* job, StepInstance* step) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  task_id_t task_id = step->step_to_d.task_id();

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
    CRANE_ERROR("fork() failed for task #{}: {}", task_id, strerror(errno));

    close(craned_supervisor_pipe[0]);
    close(craned_supervisor_pipe[1]);
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}", task_id,
                child_pid);

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
    if (!job->cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "[Task #{}] Terminate the subprocess due to failure of cgroup "
          "migration.",
          step->step_to_d.task_id());

      job->err_before_supervisor_ready = CraneErrCode::ERR_CGROUP;

      // Ask child to suicide
      msg.set_ok(false);
      ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
      if (!ok) {
        CRANE_ERROR("[Task #{}] Failed to ask subprocess to suicide.",
                    child_pid, task_id);

        job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
        KillPid_(child_pid, SIGKILL);
      }
      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_CGROUP;
    }

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to serialize msg to ostream: {}", task_id,
                  strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to send ok=true to supervisor {}: {}",
                  task_id, child_pid, strerror(ostream.GetErrno()));

      job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    ok = ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream,
                                          nullptr);
    if (!ok || !child_process_ready.ok()) {
      if (!ok)
        CRANE_ERROR("[Task #{}] Pipe child endpoint failed: {}", task_id,
                    strerror(istream.GetErrno()));
      if (!child_process_ready.ok())
        CRANE_ERROR("[Task #{}] Received false from subprocess {}", task_id,
                    child_pid);

      job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    // Do Supervisor Init
    crane::grpc::supervisor::InitSupervisorRequest init_req;
    init_req.set_job_id(task_id);
    init_req.set_debug_level(g_config.Supervisor.DebugLevel);
    init_req.set_craned_id(g_config.CranedIdOfThisNode);
    init_req.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_req.set_crane_base_dir(g_config.CraneBaseDir);
    init_req.set_crane_script_dir(g_config.CranedScriptDir);
    init_req.mutable_step_spec()->CopyFrom(step->step_to_d);
    init_req.set_log_dir(g_config.Supervisor.LogDir);
    auto* cfored_listen_conf = init_req.mutable_cfored_listen_conf();
    cfored_listen_conf->set_use_tls(g_config.ListenConf.TlsConfig.Enabled);
    cfored_listen_conf->set_domain_suffix(
        g_config.ListenConf.TlsConfig.DomainSuffix);
    auto* tls_certs = cfored_listen_conf->mutable_tls_certs();
    tls_certs->set_cert_content(
        g_config.ListenConf.TlsConfig.TlsCerts.CertContent);
    tls_certs->set_ca_content(g_config.ListenConf.TlsConfig.TlsCerts.CaContent);
    tls_certs->set_key_content(
        g_config.ListenConf.TlsConfig.TlsCerts.KeyContent);

    // Pass job env to supervisor
    EnvMap res_env_map = job->GetJobEnvMap();
    init_req.mutable_env()->clear();
    init_req.mutable_env()->insert(res_env_map.begin(), res_env_map.end());

      std::string cgroup_path_str = job->cgroup->CgroupPath().string();
      init_req.set_cgroup_path(cgroup_path_str);
      CRANE_TRACE("[Task #{}] Setting cgroup path: {}", task_id,
                  cgroup_path_str);

    if (g_config.Container.Enabled) {
      auto* container_conf = init_req.mutable_container_config();
      container_conf->set_temp_dir(g_config.Container.TempDir);
      container_conf->set_runtime_bin(g_config.Container.RuntimeBin);
      container_conf->set_state_cmd(g_config.Container.RuntimeState);
      container_conf->set_run_cmd(g_config.Container.RuntimeRun);
      container_conf->set_kill_cmd(g_config.Container.RuntimeKill);
      container_conf->set_delete_cmd(g_config.Container.RuntimeDelete);
    }

    if (g_config.Plugin.Enabled) {
      auto* plugin_conf = init_req.mutable_plugin_config();
      plugin_conf->set_socket_path(g_config.Plugin.PlugindSockPath);
    }

    ok = SerializeDelimitedToZeroCopyStream(init_req, &ostream);
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to serialize msg to ostream: {}", task_id,
                  strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to send init msg to supervisor: {}",
                  child_pid, task_id, strerror(ostream.GetErrno()));

      job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    CRANE_TRACE("[Task #{}] Supervisor init msg send.", task_id);

    crane::grpc::supervisor::SupervisorReady supervisor_ready;
    ok = ParseDelimitedFromZeroCopyStream(&supervisor_ready, &istream, nullptr);
    if (!ok || !supervisor_ready.ok()) {
      if (!ok)
        CRANE_ERROR("[Task #{}] Pipe child endpoint failed: {}", task_id,
                    strerror(istream.GetErrno()));
      if (!supervisor_ready.ok())
        CRANE_ERROR("[Task #{}] False from subprocess {}.", child_pid, task_id);

      job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    CRANE_TRACE("[Task #{}] Supervisor init msg received.", task_id);
    g_supervisor_keeper->AddSupervisor(task_id);

    auto stub = g_supervisor_keeper->GetStub(task_id);
    auto code = stub->ExecuteTask();
    if (code != CraneErrCode::SUCCESS) {
      CRANE_ERROR("[Job #{}] Supervisor failed to execute task, code:{}.",
                  task_id, static_cast<int>(code));
      KillPid_(child_pid, SIGKILL);
      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_SUPERVISOR;
    }

    // TODO: replace this with step_id
    step->supv_pid = child_pid;
    return CraneErrCode::SUCCESS;
  } else {  // Child proc, NOLINT(readability-else-after-return)
    // Disable SIGABRT backtrace from child processes.
    signal(SIGABRT, SIG_DFL);

    int craned_supervisor_fd = craned_supervisor_pipe[0];
    close(craned_supervisor_pipe[1]);
    int supervisor_craned_fd = supervisor_craned_pipe[1];
    close(supervisor_craned_pipe[0]);

    // Message will send to stdin of Supervisor for its init.
    dup2(craned_supervisor_fd, STDIN_FILENO);
    dup2(supervisor_craned_fd, STDOUT_FILENO);
    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    FileInputStream istream(STDIN_FILENO);
    FileOutputStream ostream(STDOUT_FILENO);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;
    bool ok;

    ok = ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
    if (!ok || !msg.ok()) {
      if (!ok) {
        int err = istream.GetErrno();
        fmt::print(stderr, "Failed to read socket from parent: {}",
                   strerror(err));
      }

      if (!msg.ok())
        fmt::print(stderr, "Parent process ask not to start the subprocess.");

      std::abort();
    }

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      fmt::print(stderr, "[Child Process] Error: Failed to flush.");
      std::abort();
    }

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    util::os::CloseFdFrom(3);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    auto supervisor_name = fmt::format("csupervisor: [{}]", task_id);
    argv.emplace_back(supervisor_name.c_str());

    argv.push_back(nullptr);

    // Use execvp to search the kSupervisorPath in the PATH.
    execvp(g_config.Supervisor.Path.c_str(),
           const_cast<char* const*>(argv.data()));

    // Error occurred since execvp returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess] Failed to execvp {}. Error: {}\n",
               g_config.Supervisor.Path.c_str(), strerror(errno));

    // TODO: See https://tldp.org/LDP/abs/html/exitcodes.html, return
    // standard
    //  exit codes
    abort();
  }
}

CraneErrCode JobManager::ExecuteStepAsync(StepToD const& step) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    return CraneErrCode::ERR_SHUTTING_DOWN;
  }

  CRANE_INFO("Executing step #{} of job #{}", step.task_id(), step.task_id());
  if (!m_job_map_.Contains(step.task_id())) {
    CRANE_DEBUG("Task #{} without job allocation. Ignoring it.",
                step.task_id());
    return CraneErrCode::ERR_CGROUP;
  }

  // Simply wrap the Task structure within an Execution structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  auto step_inst = std::make_unique<StepInstance>();
  step_inst->step_to_d = step;
  EvQueueExecuteStepElem elem{.step_inst = std::move(step_inst)};

  m_grpc_execute_step_queue_.enqueue(std::move(elem));
  m_grpc_execute_step_async_handle_->send();

  return CraneErrCode::SUCCESS;
}

void JobManager::EvCleanGrpcExecuteStepQueueCb_() {
  EvQueueExecuteStepElem elem;

  while (m_grpc_execute_step_queue_.try_dequeue(elem)) {
    // Once ExecuteTask RPC is processed, the Execution goes into m_job_map_.
    std::unique_ptr execution = std::move(elem.step_inst);

    if (!m_job_map_.Contains(execution->step_to_d.task_id())) {
      CRANE_ERROR("Failed to find job #{} allocation",
                  execution->step_to_d.task_id());
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
      continue;
    }
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);

    g_thread_pool->detach_task([this, execution = execution.release()] {
      LaunchStepMt_(std::unique_ptr<StepInstance>(execution));
    });
  }
}

bool JobManager::FreeJobAllocation_(const std::vector<task_id_t>& job_ids) {
  CRANE_DEBUG("Freeing job [{}] allocation.", absl::StrJoin(job_ids, ","));
  std::unordered_map<task_id_t, CgroupInterface*> job_cg_map;
  std::vector<uid_t> uid_vec;
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      JobInD* job = map_ptr->at(job_id).RawPtr();

      job_cg_map[job_id] = job->cgroup.release();
      uid_vec.push_back(job->Uid());
      map_ptr->erase(job_id);
    }
  }
  {
    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    for (auto [idx, job_id] : job_ids | std::ranges::views::enumerate) {
      bool erase{false};
      {
        auto& value = uid_map->at(uid_vec[idx]);
        value.RawPtr()->erase(job_id);
        erase = value.RawPtr()->empty();
      }
      if (erase) {
        uid_map->erase(uid_vec[idx]);
      }
    }
  }

  for (auto [job_id, cgroup] : job_cg_map) {
    if (cgroup == nullptr) continue;

    if (g_config.Plugin.Enabled && g_plugin_client) {
      g_plugin_client->DestroyCgroupHookAsync(job_id, cgroup->CgroupName());
    }

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
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      cgroup->Destroy();

      delete cgroup;
    });
  }
  return true;
}

void JobManager::LaunchStepMt_(std::unique_ptr<StepInstance> step) {
  // This function runs in a multi-threading manner. Take care of thread
  // safety. JobInstance will not be free during this function. Take care of
  // data race for job instance.

  task_id_t job_id = step->step_to_d.task_id();
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_ERROR("Failed to get the allocation of job#{}", job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to get the allocation for job#{} ", job_id));
    return;
  }
  auto* job = job_ptr.get();

  // Check if the step is acceptable.
  if (!step->step_to_d.container().empty() && !g_config.Container.Enabled) {
    CRANE_ERROR("Container support is disabled but job #{} requires it.",
                job_id);
    ActivateTaskStatusChangeAsync_(step->step_to_d.task_id(),
                                   crane::grpc::TaskStatus::Failed,
                                   ExitCode::kExitCodeSpawnProcessFail,
                                   "Container is not enabled in this craned.");
    return;
  }

  if (!job->cgroup) {
    auto cg_expt = CgroupManager::AllocateAndGetCgroup(
        CgroupManager::CgroupStrByJobId(job->job_id), job->job_to_d.res(),
        false);
    if (cg_expt.has_value()) {
      job->cgroup = std::move(cg_expt.value());
    } else {
      CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
      ActivateTaskStatusChangeAsync_(
          job_id, crane::grpc::TaskStatus::Failed,
          ExitCode::kExitCodeCgroupError,
          fmt::format("Failed to get cgroup for job#{} ", job_id));
      return;
    }
  }

  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErrCode err = SpawnSupervisor_(job, step.get());
  if (err != CraneErrCode::SUCCESS) {
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeSpawnProcessFail,
        fmt::format("Cannot spawn a new process inside the instance of job #{}",
                    job_id));
  } else {
    // kOk means that SpawnSupervisor_ has successfully forked a child
    // process. Now we put the child pid into index maps. SIGCHLD sent just
    // after fork() and before putting pid into maps will repeatedly be sent
    // by timer and eventually be handled once the SIGCHLD processing callback
    // sees the pid in index maps. Now we do not support launch multiple tasks
    // in a job.
    CRANE_TRACE("[job #{}] Spawned successfully.", job->job_id);
    // TODO: replace this with step_id
    job->step_map.emplace(0, std::move(step));
  }
}

void JobManager::EvCleanTaskStatusChangeQueueCb_() {
  TaskStatusChangeQueueElem status_change;
  while (m_task_status_change_queue_.try_dequeue(status_change)) {
    auto job_ptr = m_job_map_.GetValueExclusivePtr(status_change.step_id);
    if (!job_ptr) {
      // When Ctrl+C is pressed for Craned, all tasks including just forked
      // tasks will be terminated.
      // In some error cases, a double TaskStatusChange might be triggered.
      // Just ignore it. See comments in SpawnProcessInInstance_().
      continue;
    }

    bool orphaned = job_ptr->orphaned;
    if (!orphaned)
      g_ctld_client->StepStatusChangeAsync(std::move(status_change));
  }
}

void JobManager::ActivateTaskStatusChangeAsync_(
    task_id_t task_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason) {
  TaskStatusChangeQueueElem status_change{task_id, new_status, exit_code};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  m_task_status_change_async_handle_->send();
}

/**
 * @brief Move process on craned to specify job's cgroup. Called from
 * CranedServer
 * @param pid process pid
 * @param job_id
 * @return True if job and cgroup exists
 */
bool JobManager::MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id) {
  CgroupInterface* cg = GetCgForJob(job_id);
  if (cg == nullptr) return false;

  return cg->MigrateProcIn(pid);
}

CraneExpected<JobInD*> JobManager::QueryJob(job_id_t job_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  return job_ptr.get();
}

std::set<task_id_t> JobManager::GetAllocatedJobs() {
  auto job_map_ptr = m_job_map_.GetMapConstSharedPtr();
  return *job_map_ptr | std::ranges::views::keys |
         std::ranges::to<std::set<task_id_t>>();
}

std::optional<TaskInfoOfUid> JobManager::QueryTaskInfoOfUid(uid_t uid) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  TaskInfoOfUid info;
  info.job_cnt = 0;
  info.cgroup_exists = false;

  if (auto task_ids = this->m_uid_to_job_ids_map_[uid]; task_ids) {
    info.cgroup_exists = true;
    info.job_cnt = task_ids->size();
    info.first_task_id = *task_ids->begin();
  } else {
    CRANE_WARN("Uid {} not found in uid_to_task_ids_map", uid);
    return std::nullopt;
  }
  return info;
}

void JobManager::EvCleanTerminateTaskQueueCb_() {
  StepTerminateQueueElem elem;
  while (m_step_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.step_id);

    auto job_instance = m_job_map_.GetValueExclusivePtr(elem.step_id);
    if (!job_instance || job_instance->step_map.empty()) {
      CRANE_DEBUG("Terminating a non-existent task #{}.", elem.step_id);

      // Note if Ctld wants to terminate some tasks that are not running,
      // it might indicate other nodes allocated to the task might have
      // crashed. We should mark the task as kind of not runnable by removing
      // its cgroup.
      //
      // Considering such a situation:
      // In Task Scheduler of Ctld,
      // the task index from node id to task id have just been added and
      // Ctld are sending CreateCgroupForTasks.
      // Right at the moment, one Craned allocated to this task and
      // designated as the executing node crashes,
      // but it has been sent a CreateCgroupForTasks and replied.
      // Then the CranedKeeper search the task index and
      // send TerminateTasksOnCraned to all Craned allocated to this task
      // including this node.
      // In order to give Ctld kind of feedback without adding complicated
      // synchronizing mechanism in ScheduleThread_(),
      // we just remove the cgroup for such task, Ctld will fail in the
      // following ExecuteTasks and the task will go to the right place as
      // well as the completed queue.

      if (job_instance)
        FreeJobAllocation_({job_instance->job_id});
      else {
        ActivateTaskStatusChangeAsync_(
            elem.step_id, crane::grpc::TaskStatus::Cancelled,
            ExitCode::kExitCodeTerminated, "Job not found.");
      }
      continue;
    }

    auto* instance = job_instance.get();
    instance->orphaned = elem.mark_as_orphaned;

    auto stub = g_supervisor_keeper->GetStub(elem.step_id);
    if (!stub) {
      CRANE_ERROR("Supervisor for task #{} not found", elem.step_id);
      continue;
    }
    auto err =
        stub->TerminateTask(elem.mark_as_orphaned, elem.terminated_by_user);
    if (err != CraneErrCode::SUCCESS) {
      CRANE_ERROR("Failed to terminate task #{}", elem.step_id);
      // Supervisor dead for some reason.
      g_supervisor_keeper->RemoveSupervisor(elem.step_id);
      ActivateTaskStatusChangeAsync_(
          elem.step_id, crane::grpc::TaskStatus::Cancelled,
          ExitCode::kExitCodeTerminated, "Terminated failed.");
    }
  }
}

void JobManager::TerminateStepAsync(step_id_t step_id) {
  StepTerminateQueueElem elem{.step_id = step_id, .terminated_by_user = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

void JobManager::MarkStepAsOrphanedAndTerminateAsync(step_id_t step_id) {
  StepTerminateQueueElem elem{.step_id = step_id, .mark_as_orphaned = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

bool JobManager::ChangeJobTimeLimitAsync(job_id_t job_id,
                                         absl::Duration time_limit) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    return false;
  }
  ChangeTaskTimeLimitQueueElem elem{.job_id = job_id, .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  m_change_task_time_limit_async_handle_->send();
  return ok_fut.get();
}

void JobManager::StepStopAndDoStatusChangeAsync(
    task_id_t job_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason) {
  if (!m_job_map_.Contains(job_id)) {
    CRANE_ERROR("Task #{} not found in TaskStopAndDoStatusChangeAsync.",
                job_id);
    return;
  }
  CRANE_INFO("Task #{} stopped and is doing TaskStatusChange...", job_id);
  ActivateTaskStatusChangeAsync_(job_id, new_status, exit_code,
                                 std::move(reason));
}

void JobManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  ChangeTaskTimeLimitQueueElem elem;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    if (auto job_ptr = m_job_map_.GetValueExclusivePtr(elem.job_id); job_ptr) {
      auto stub = g_supervisor_keeper->GetStub(elem.job_id);
      if (!stub) {
        CRANE_ERROR("Supervisor for task #{} not found", elem.job_id);
        continue;
      }
      auto err = stub->ChangeTaskTimeLimit(elem.time_limit);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("Failed to change task time limit for task #{}",
                    elem.job_id);
        continue;
      }
      elem.ok_prom.set_value(true);
    } else {
      CRANE_ERROR("Try to update the time limit of a non-existent task #{}.",
                  elem.job_id);
      elem.ok_prom.set_value(false);
    }
  }
}

}  // namespace Craned
