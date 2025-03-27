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

EnvMap JobSpec::GetJobEnvMap() const {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(
      this->cgroup_spec.res_in_node);

  // TODO: Move all job level env to here.
  env_map.emplace("CRANE_JOB_ID", std::to_string(this->cgroup_spec.job_id));
  return env_map;
};

JobInstance::JobInstance(JobSpec&& spec)
    : job_id(spec.cgroup_spec.job_id), job_spec(spec) {}

JobInstance::JobInstance(const JobSpec& spec)
    : job_id(spec.cgroup_spec.job_id), job_spec(spec) {}

JobManager::JobManager() {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });

  if (int rc = m_sigchld_handle_->start(SIGCLD); rc != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle: {}", uv_err_name(rc));
  }

  m_sigint_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigint_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigint_handle_->start(SIGINT); rc != 0) {
    CRANE_ERROR("Failed to start the SIGINT handle: {}", uv_err_name(rc));
  }

  m_check_supervisor_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_check_supervisor_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        if (EvCheckSupervisorRunning_()) handle.stop();
      });

  // gRPC Execute Task Event
  m_grpc_execute_task_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_execute_task_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcExecuteTaskQueueCb_();
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

  m_terminate_task_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_task_async_handle_->on<uvw::async_event>(
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
    std::unordered_map<task_id_t, JobStatusSpec>&& job_status_map) {
  for (auto& [job_id, step_status] : job_status_map) {
    CRANE_TRACE("[Job #{}] Recover from supervisor.", job_id);
    uid_t uid = step_status.job_spec.cgroup_spec.uid;
    step_status.job_spec.cgroup_spec.recovered = true;
    auto job_instance = std::make_unique<JobInstance>(step_status.job_spec);
    auto execution = std::make_unique<StepInstance>(
        StepInstance{.step_spec = step_status.step_spec});
    // TODO:replace this with step_id
    job_instance->step_map.emplace(0, std::move(execution));
    job_instance->cgroup =
        g_cg_mgr->AllocateAndGetJobCgroup(job_instance->job_spec.cgroup_spec);

    m_job_map_.Emplace(job_id, std::move(job_instance));
    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }
  return CraneErrCode::SUCCESS;
}

JobManager::~JobManager() {
  CRANE_DEBUG("JobManager is being destroyed.");
  m_is_ending_now_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
  m_pending_thread_pool_tasks.wait();
}

bool JobManager::AllocJobs(std::vector<JobSpec>&& job_specs) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  CRANE_DEBUG("Allocating {} tasks", job_specs.size());

  auto begin = std::chrono::steady_clock::now();

  for (auto& job_spec : job_specs) {
    auto& cg_spec = job_spec.cgroup_spec;
    task_id_t job_id = cg_spec.job_id;
    uid_t uid = cg_spec.uid;
    CRANE_TRACE("Create lazily allocated cgroups for task #{}, uid {}", job_id,
                uid);

    auto job_instance = std::make_unique<JobInstance>(std::move(job_spec));
    m_job_map_.Emplace(job_id, std::move(job_instance));

    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }

  auto end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());
  return true;
}

bool JobManager::FreeJobs(const std::vector<task_id_t>& job_ids) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      if (!map_ptr->contains(job_id)) {
        CRANE_WARN("Try to free nonexistent job#{}", job_ids);
        return false;
      }
    }
  }

  absl::MutexLock lk(&m_release_cg_mtx_);

  m_release_job_req_set_.insert(job_ids.begin(), job_ids.end());
  if (!m_check_supervisor_timer_handle_->active())
    m_check_supervisor_timer_handle_->start(uvw::timer_handle::time{1000},
                                            uvw::timer_handle::time{1000});

  return true;
}

bool JobManager::EvCheckSupervisorRunning_() {
  absl::MutexLock lk(&m_release_cg_mtx_);
  std::error_code ec;
  std::vector<task_id_t> job_ids;
  for (task_id_t job_id : m_release_job_req_set_) {
    // TODO: replace following with step_id
    auto exists = std::filesystem::exists(
        fmt::format("/proc/{}", m_job_map_.GetValueExclusivePtr(job_id)
                                    ->get()
                                    ->step_map[0]
                                    ->supervisor_pid),
        ec);
    if (ec) {
      CRANE_WARN("Failed to check supervisor for Job #{} is running.", job_id);
      continue;
    }
    if (!exists) {
      job_ids.emplace_back(job_id);
    }
  }
  for (task_id_t job_id : job_ids) {
    m_release_job_req_set_.erase(job_id);
  }

  if (!job_ids.empty()) {
    CRANE_TRACE("Supervisor for Job [{}] found to be exited",
                absl::StrJoin(job_ids, ","));

    m_pending_thread_pool_tasks.count_up();
    g_thread_pool->detach_task([this, jobs = std::move(job_ids)] {
      FreeJobInstanceAllocation_(jobs);
      m_pending_thread_pool_tasks.count_down();
    });
  }

  return m_release_job_req_set_.empty();
}

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
  m_sigint_cb_();
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

CraneErrCode JobManager::SpawnSupervisor_(JobInstance* job,
                                          StepInstance* step) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int supervisor_craned_pipe[2];
  int craned_supervisor_pipe[2];

  if (pipe(supervisor_craned_pipe) == -1) {
    CRANE_ERROR("Pipe creation failed!");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (pipe(craned_supervisor_pipe) == -1) {
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
    CRANE_ERROR("fork() failed for task #{}: {}", step->step_spec.task_id(),
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                step->step_spec.task_id(), child_pid);

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
          step->step_spec.task_id());

      job->err_before_exec = CraneErrCode::ERR_CGROUP;

      // Ask child to suicide
      msg.set_ok(false);
      ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
      if (!ok) {
        CRANE_ERROR("[Task #{}] Failed to ask subprocess to suicide.",
                    child_pid, step->step_spec.task_id());

        job->err_before_exec = CraneErrCode::ERR_PROTOBUF;
        KillPid_(child_pid, SIGKILL);
      }

      return CraneErrCode::ERR_CGROUP;
    }

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to serialize msg to ostream: {}",
                  step->step_spec.task_id(), strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to send ok=true to supervisor {}: {}",
                  step->step_spec.task_id(), child_pid,
                  strerror(ostream.GetErrno()));

      job->err_before_exec = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);
      return CraneErrCode::ERR_PROTOBUF;
    }

    ok = ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream,
                                          nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("[Task #{}] Pipe child endpoint failed: {}",
                    step->step_spec.task_id(), strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("[Task #{}] Received false from subprocess {}",
                    step->step_spec.task_id(), child_pid);

      job->err_before_exec = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);
      return CraneErrCode::ERR_PROTOBUF;
    }

    // Do Supervisor Init
    crane::grpc::supervisor::InitSupervisorRequest init_req;
    init_req.set_job_id(step->step_spec.task_id());
    init_req.set_debug_level(g_config.Supervisor.DebugLevel);
    init_req.set_craned_id(g_config.CranedIdOfThisNode);
    init_req.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_req.set_crane_base_dir(g_config.CraneBaseDir);
    init_req.set_crane_script_dir(g_config.CranedScriptDir);
    init_req.mutable_step_spec()->CopyFrom(step->step_spec);

    // Pass job env to supervisor
    EnvMap res_env_map = job->job_spec.GetJobEnvMap();
    init_req.mutable_env()->clear();
    init_req.mutable_env()->insert(res_env_map.begin(), res_env_map.end());

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
      CRANE_ERROR("[Task #{}] Failed to serialize msg to ostream: {}",
                  step->step_spec.task_id(), strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to send init msg to supervisor: {}",
                  child_pid, step->step_spec.task_id(),
                  strerror(ostream.GetErrno()));

      job->err_before_exec = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);
      return CraneErrCode::ERR_PROTOBUF;
    } else {
      CRANE_TRACE("[Job #{}] Supervisor init msg send.",
                  step->step_spec.task_id());
    }

    crane::grpc::supervisor::SupervisorReady supervisor_ready;
    ok = ParseDelimitedFromZeroCopyStream(&supervisor_ready, &istream, nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("[Task #{}] Pipe child endpoint failed: {}",
                    step->step_spec.task_id(), strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("[Task #{}] False from subprocess {}.", child_pid,
                    step->step_spec.task_id());

      job->err_before_exec = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);
      return CraneErrCode::ERR_PROTOBUF;
    }

    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    g_supervisor_keeper->AddSupervisor(step->step_spec.task_id());

    auto stub = g_supervisor_keeper->GetStub(step->step_spec.task_id());
    auto pid = stub->ExecuteTask(step->step_spec);
    if (!pid) {
      CRANE_ERROR("[Task #{}] Supervisor failed to execute task.",
                  step->step_spec.task_id());
      job->err_before_exec = CraneErrCode::ERR_SUPERVISOR;
      KillPid_(child_pid, SIGKILL);
      return CraneErrCode::ERR_SUPERVISOR;
    }

    // TODO: replace this with step_id
    step->supervisor_pid = child_pid;
    return CraneErrCode::SUCCESS;
  } else {  // Child proc
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
      // if (!ok) {
      // int err = istream.GetErrno();
      // CRANE_ERROR("Failed to read socket from parent: {}", strerror(err));
      // }

      // if (!msg.ok())
      // CRANE_ERROR("Parent process ask not to start the subprocess.");

      std::abort();
    }

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      // CRANE_ERROR("[Child Process] Error: Failed to flush.");
      std::abort();
    }

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    util::os::CloseFdFrom(3);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    argv.emplace_back("csupervisor");

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

CraneErrCode JobManager::ExecuteTaskAsync(
    crane::grpc::TaskToD const& task_spec) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  CRANE_INFO("Executing task #{} job #{}", task_spec.task_id(),
             task_spec.task_id());
  if (!m_job_map_.Contains(task_spec.task_id())) {
    CRANE_DEBUG("Task #{} without job allocation. Ignoring it.",
                task_spec.task_id());
    return CraneErrCode::ERR_CGROUP;
  }
  auto step = std::make_unique<StepInstance>();

  // Simply wrap the Task structure within an Execution structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  step->step_spec = task_spec;
  EvQueueExecuteTaskElem elem{.execution = std::move(step)};

  auto future = elem.ok_prom.get_future();
  m_grpc_execute_task_queue_.enqueue(std::move(elem));
  m_grpc_execute_task_async_handle_->send();

  return CraneErrCode::SUCCESS;
}

void JobManager::EvCleanGrpcExecuteTaskQueueCb_() {
  EvQueueExecuteTaskElem elem;

  while (m_grpc_execute_task_queue_.try_dequeue(elem)) {
    // Once ExecuteTask RPC is processed, the Exection goes into
    // m_job_map_.
    std::unique_ptr execution = std::move(elem.execution);

    if (!m_job_map_.Contains(execution->step_spec.task_id())) {
      CRANE_ERROR("Failed to find job #{} allocation",
                  execution->step_spec.task_id());
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
    }
    m_pending_thread_pool_tasks.count_up();
    g_thread_pool->detach_task([this, execution = execution.release()] {
      LaunchStepMt_(std::unique_ptr<StepInstance>(execution));
      m_pending_thread_pool_tasks.count_down();
    });
  }
}

bool JobManager::FreeJobInstanceAllocation_(
    const std::vector<task_id_t>& job_ids) {
  CRANE_DEBUG("Freeing job [{}] instance allocation",
              absl::StrJoin(job_ids, ","));
  std::vector<JobInstance*> job_ptr_vec;
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      job_ptr_vec.emplace_back(map_ptr->at(job_id).RawPtr()->release());
      map_ptr->erase(job_id);
    }
  }
  {
    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    for (auto* job : job_ptr_vec) {
      bool erase{false};
      {
        auto& value = uid_map->at(job->job_spec.cgroup_spec.uid);
        value.RawPtr()->erase(job->job_id);
        erase = value.RawPtr()->empty();
      }
      if (erase) {
        uid_map->erase(job->job_spec.cgroup_spec.uid);
      }
    }
  }
  for (auto* instance : job_ptr_vec) {
    auto* cgroup = instance->cgroup.release();
    if (g_config.Plugin.Enabled) {
      g_plugin_client->DestroyCgroupHookAsync(instance->job_id,
                                              cgroup->GetCgroupString());
    }

    if (cgroup != nullptr) {
      g_thread_pool->detach_task([cgroup]() {
        int cnt = 0;

        while (true) {
          if (cgroup->Empty()) break;

          if (cnt >= 5) {
            CRANE_ERROR(
                "Couldn't kill the processes in cgroup {} after {} times. "
                "Skipping it.",
                cgroup->GetCgroupString(), cnt);
            break;
          }

          cgroup->KillAllProcesses();
          ++cnt;
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        delete cgroup;
      });
    }
    delete instance;
  }
  return true;
}

void JobManager::LaunchStepMt_(std::unique_ptr<StepInstance> step) {
  // This function runs in a multi-threading manner. Take care of thread
  // safety. JobInstance will not be free during this function. Take care of
  // data race for job instance.

  task_id_t job_id = step->step_spec.task_id();
  auto job_it = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_it) {
    CRANE_ERROR("Failed to get the allocation of job#{}", job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to get the allocation for job#{} ", job_id));
    return;
  }
  auto* job = job_it.get()->get();

  if (!job->cgroup) {
    job->cgroup = g_cg_mgr->AllocateAndGetJobCgroup(job->job_spec.cgroup_spec);
  }
  if (!job->cgroup) {
    CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to get cgroup for job#{} ", job_id));
    return;
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
    auto job_pptr = m_job_map_.GetValueExclusivePtr(status_change.task_id);
    if (!job_pptr) {
      // When Ctrl+C is pressed for Craned, all tasks including just forked
      // tasks will be terminated.
      // In some error cases, a double TaskStatusChange might be triggered.
      // Just ignore it. See comments in SpawnProcessInInstance_().
      continue;
    }

    g_supervisor_keeper->RemoveSupervisor(job_pptr.get()->get()->job_id);
    bool orphaned = job_pptr->get()->orphaned;
    if (!orphaned)
      g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }
}

void JobManager::ActivateTaskStatusChangeAsync_(
    uint32_t task_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
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
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  auto job_instance = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_instance) return false;
  auto& cg = job_instance->get()->cgroup;
  if (!cg) {
    cg = g_cg_mgr->AllocateAndGetJobCgroup(
        job_instance->get()->job_spec.cgroup_spec);
  }
  if (!cg) return false;

  return cg->MigrateProcIn(pid);
}

CraneExpected<JobSpec> JobManager::QueryJobSpec(task_id_t job_id) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  auto instance = m_job_map_.GetValueExclusivePtr(job_id);
  if (!instance) return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  return instance->get()->job_spec;
}

std::unordered_set<task_id_t> JobManager::QueryExistentJobIds() {
  std::unordered_set<task_id_t> job_ids;
  auto job_map_ptr = m_job_map_.GetMapConstSharedPtr();
  job_ids.reserve(job_map_ptr->size());
  return *job_map_ptr | std::ranges::views::keys |
         std::ranges::to<std::unordered_set<task_id_t>>();
}

bool JobManager::QueryTaskInfoOfUid(uid_t uid, TaskInfoOfUid* info) {
  CRANE_DEBUG("Query task info for uid {}", uid);
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));

  info->job_cnt = 0;
  info->cgroup_exists = false;

  if (auto task_ids = this->m_uid_to_job_ids_map_[uid]; task_ids) {
    if (!task_ids) {
      CRANE_WARN("Uid {} not found in uid_to_task_ids_map", uid);
      return false;
    }
    info->job_cnt = task_ids->size();
    info->first_task_id = *task_ids->begin();
  }
  return info->job_cnt > 0;
}

void JobManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.task_id);

    auto job_instance = m_job_map_.GetValueExclusivePtr(elem.task_id);
    if (!job_instance || job_instance->get()->step_map.empty()) {
      CRANE_DEBUG("Terminating a non-existent task #{}.", elem.task_id);

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
      // FreeJobInstanceAllocation_(job_instance->release());
      continue;
    }

    auto* instance = job_instance.get()->get();
    instance->orphaned = elem.mark_as_orphaned;

    auto stub = g_supervisor_keeper->GetStub(elem.task_id);
    if (!stub) {
      CRANE_ERROR("Supervisor for task #{} not found", elem.task_id);
      continue;
    }
    auto err =
        stub->TerminateTask(elem.mark_as_orphaned, elem.terminated_by_user);
    if (err != CraneErrCode::SUCCESS) {
      CRANE_ERROR("Failed to terminate task #{}", elem.task_id);
    }
  }
}

void JobManager::TerminateTaskAsync(uint32_t task_id) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  TaskTerminateQueueElem elem{.task_id = task_id, .terminated_by_user = true};
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

void JobManager::MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  TaskTerminateQueueElem elem{.task_id = task_id, .mark_as_orphaned = true};
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

bool JobManager::ChangeTaskTimeLimitAsync(task_id_t task_id,
                                          absl::Duration time_limit) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
  ChangeTaskTimeLimitQueueElem elem{.job_id = task_id,
                                    .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  m_change_task_time_limit_async_handle_->send();
  return ok_fut.get();
}

void JobManager::TaskStopAndDoStatusChangeAsync(
    uint32_t job_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason) {
  CRANE_ASSERT(!m_is_ending_now_.load(std::memory_order_acquire));
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
