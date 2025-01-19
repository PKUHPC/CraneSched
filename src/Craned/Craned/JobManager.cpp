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

#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <pty.h>
#include <sys/wait.h>

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

  // todo: Move all job level env to here.
  env_map.emplace("CRANE_JOB_ID", std::to_string(this->cgroup_spec.job_id));
  return env_map;
};

JobInstance::JobInstance(JobSpec&& spec)
    : job_id(spec.cgroup_spec.job_id), job_spec(std::move(spec)) {}

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

  if (m_sigchld_handle_->start(SIGCLD) != 0) {
    CRANE_ERROR("Failed to start the SIGCLD handle");
  }

  m_sigint_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigint_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (m_sigint_handle_->start(SIGINT) != 0) {
    CRANE_ERROR("Failed to start the SIGINT handle");
  }

  // gRPC: QueryTaskIdFromPid
  m_query_task_id_from_pid_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_query_task_id_from_pid_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcQueryTaskIdFromPidQueueCb_();
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

  m_check_task_status_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_check_task_status_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanCheckTaskStatusQueueCb_();
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

CraneErr JobManager::Init(
    std::unordered_map<task_id_t, JobStatusSpce>&& job_status_map) {
  for (auto& [job_id, task_status] : job_status_map) {
    task_status.job_spec.cgroup_spec.recovered = true;
    auto* job_instance = new JobInstance(task_status.job_spec);
    auto* process =
        new TaskExecutionInstance{.task_spec = task_status.task_spec,
                                  .job_id = job_id,
                                  .pid = task_status.task_pid};
    job_instance->processes.emplace(task_status.task_pid, process);
    // When init,no need to lock.
    m_pid_job_map_.emplace(task_status.task_pid, job_instance);
    m_pid_proc_map_.emplace(task_status.task_pid, process);

    m_job_map_.Emplace(job_id, std::unique_ptr<JobInstance>(job_instance));
    uid_t uid = task_status.job_spec.cgroup_spec.uid;
    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }
  return CraneErr::kOk;
}

JobManager::~JobManager() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

bool JobManager::AllocJobs(std::vector<JobSpec>&& job_specs) {
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;
  CRANE_DEBUG("Allocating {} tasks", job_specs.size());

  begin = std::chrono::steady_clock::now();

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

  end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());
  return true;
}

bool JobManager::FreeJobAllocation(task_id_t job_id) {
  JobInstance* job_instance;
  {
    auto job_unique_ptr = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job_unique_ptr) {
      CRANE_WARN("Try to free nonexistent job#{}", job_id);
      return false;
    }
    job_instance = job_unique_ptr->release();
  }
  return FreeJobInstanceAllocation_(job_instance);
}

void JobManager::EvSigchldCb_() {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      CRANE_TRACE("Child pid #{} exit", pid);
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

CraneErr JobManager::KillPid_(pid_t pid, int signum) {
  // Todo: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  CRANE_TRACE("Killing pid {} with signal {}", pid, signum);

  // Send the signal to the whole process group.
  int err = kill(-pid, signum);

  if (err == 0)
    return CraneErr::kOk;
  else {
    CRANE_TRACE("kill failed. error: {}", strerror(errno));
    return CraneErr::kGenericFailure;
  }
}

void JobManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErr JobManager::SpawnSupervisor_(JobInstance* instance,
                                      TaskExecutionInstance* process) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  int supervisor_craned_pipe[2];
  int craned_supervisor_pipe[2];

  if (pipe(supervisor_craned_pipe) == -1) {
    CRANE_ERROR("Pipe creation failed!");
    return CraneErr::kSystemErr;
  }

  if (pipe(craned_supervisor_pipe) == -1) {
    CRANE_ERROR("Pipe creation failed!");
    return CraneErr::kSystemErr;
  }

  // The ResourceInNode structure should be copied here for being accessed in
  // the child process.
  // Note that CgroupManager acquires a lock for this.
  // If the lock is held in the parent process during fork, the forked thread
  // in the child proc will block forever. That's why we should copy it here
  // and the child proc should not hold any lock.
  auto res_in_node = instance->job_spec.cgroup_spec.res_in_node;

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid = fork();

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", process->task_spec.task_id(),
                strerror(errno));
    return CraneErr::kSystemErr;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                process->task_spec.task_id(), child_pid);

    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    if (!instance->cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "[Task #{}] Terminate the subprocess due to failure of cgroup "
          "migration.",
          process->task_spec.task_id());

      instance->err_before_exec = CraneErr::kCgroupError;
      // Ask child to suicide
      msg.set_ok(false);

      ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
      close(ctrl_fd);
      if (!ok) {
        CRANE_ERROR("[Task #{}] Failed to ask subprocess to suicide.",
                    child_pid, process->task_spec.task_id());

        instance->err_before_exec = CraneErr::kProtobufError;
        KillPid_(child_pid, SIGKILL);
      }

      return CraneErr::kCgroupError;
    }

    CRANE_TRACE("[Task #{}] Task is ready. Asking subprocess to execv...",
                process->task_spec.task_id());

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to serialize msg to ostream: {}",
                  process->task_spec.task_id(), strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to send ok=true to supervisor {}: {}",
                  process->task_spec.task_id(), child_pid,
                  strerror(ostream.GetErrno()));
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kProtobufError;
    }

    ok = ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream,
                                          nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("[Task #{}] Socket child endpoint failed: {}",
                    process->task_spec.task_id(), strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("[Task #{}] Received false from subprocess {}",
                    process->task_spec.task_id(), child_pid);
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kProtobufError;
    }

    int craned_supervisor_fd = craned_supervisor_pipe[1];
    close(craned_supervisor_pipe[0]);
    int supervisor_craned_fd = supervisor_craned_pipe[0];
    close(supervisor_craned_pipe[1]);

    FileInputStream supervisor_os(supervisor_craned_fd);
    FileOutputStream supervisor_is(craned_supervisor_fd);

    // Do Supervisor Init
    crane::grpc::supervisor::InitSupervisorRequest init_request;
    init_request.set_job_id(process->task_spec.task_id());
    init_request.set_debug_level("trace");
    init_request.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_request.set_crane_base_dir(g_config.CraneBaseDir);
    init_request.set_crane_script_dir(g_config.CranedScriptDir);
    auto* plugin_conf = init_request.mutable_plugin_config();
    plugin_conf->set_enabled(g_config.Plugin.Enabled);
    plugin_conf->set_plugindsockpath(g_config.Plugin.PlugindSockPath);

    ok = SerializeDelimitedToZeroCopyStream(init_request, &supervisor_is);
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to serialize msg to ostream: {}",
                  process->task_spec.task_id(),
                  strerror(supervisor_is.GetErrno()));
    }

    if (ok) ok &= supervisor_is.Flush();
    if (!ok) {
      CRANE_ERROR("[Task #{}] Failed to send init msg to supervisor: {}",
                  child_pid, process->task_spec.task_id(),
                  strerror(supervisor_is.GetErrno()));
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kProtobufError;
    } else {
      CRANE_TRACE("[Job #{}] Supervisor init msg send.",
                  process->task_spec.task_id());
    }

    crane::grpc::supervisor::SupervisorReady supervisor_ready;
    ok = ParseDelimitedFromZeroCopyStream(&supervisor_ready, &supervisor_os,
                                          nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("[Task #{}] Pipe child endpoint failed: {}",
                    process->task_spec.task_id(),
                    strerror(supervisor_os.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("[Task #{}] False from subprocess {}.", child_pid,
                    process->task_spec.task_id());
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kProtobufError;
    }

    close(craned_supervisor_fd);
    close(supervisor_craned_fd);
    close(ctrl_fd);

    g_supervisor_keeper->AddSupervisor(process->task_spec.task_id());

    auto stub = g_supervisor_keeper->GetStub(process->task_spec.task_id());
    auto pid = stub->ExecuteTask(process->task_spec);
    if (!pid) {
      CRANE_ERROR("[Task #{}] Supervisor failed to execute task.",
                  process->task_spec.task_id());
      instance->err_before_exec = CraneErr::kSupervisorError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kSupervisorError;
    }
    process->pid = pid.value();

    return CraneErr::kOk;

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

    close(ctrl_sock_pair[0]);
    int ctrl_fd = ctrl_sock_pair[1];

    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
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

    // Set job level resource env
    EnvMap res_env_map =
        CgroupManager::GetResourceEnvMapByResInNode(res_in_node);

    if (clearenv()) {
      // fmt::print("clearenv() failed!\n");
    }

    for (const auto& [name, value] : res_env_map)
      if (setenv(name.c_str(), value.c_str(), 1)) {
        // fmt::print("setenv for {}={} failed!\n", name, value);
      }

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    argv.emplace_back("Supervisor");

    argv.push_back(nullptr);

    execv(g_config.SupervisorPath.c_str(),
          const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess Error] Failed to execv. Error: {}\n",
               strerror(errno));
    // Todo: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }
}

CraneErr JobManager::ExecuteTaskAsync(crane::grpc::TaskToD const& task) {
  CRANE_INFO("Executing job #{}", task.task_id());
  if (!m_job_map_.Contains(task.task_id())) {
    CRANE_DEBUG("Executing job #{} without job allocation. Ignoring it.",
                task.task_id());
    return CraneErr::kCgroupError;
  }
  auto instance = std::make_unique<TaskExecutionInstance>();

  // Simply wrap the Task structure within a JobInstance structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  instance->task_spec = task;
  instance->job_id = task.task_id();
  EvQueueExecuteTaskElem elem{.task_execution_instance = std::move(instance)};
  auto future = elem.ok_prom.get_future();
  m_grpc_execute_task_queue_.enqueue(std::move(elem));
  m_grpc_execute_task_async_handle_->send();

  return CraneErr::kOk;
}

void JobManager::EvCleanGrpcExecuteTaskQueueCb_() {
  EvQueueExecuteTaskElem elem;

  while (m_grpc_execute_task_queue_.try_dequeue(elem)) {
    // Once ExecuteTask RPC is processed, the JobInstance goes into
    // m_job_map_.
    TaskExecutionInstance* task_exec_instance =
        elem.task_execution_instance.release();
    task_id_t job_id = task_exec_instance->task_spec.task_id();

    if (!m_job_map_.Contains(job_id)) {
      CRANE_ERROR("Failed to find job #{} allocation", job_id);
      elem.ok_prom.set_value(CraneErr::kCgroupError);
    }

    g_thread_pool->detach_task([this, task_exec_instance]() mutable {
      LaunchTaskInstanceMt_(task_exec_instance);
    });
  }
}

bool JobManager::FreeJobInstanceAllocation_(JobInstance* job_instance) {
  auto job_id = job_instance->job_id;
  this->m_job_map_.Erase(job_id);
  auto* cgroup = job_instance->cgroup.release();

  if (g_config.Plugin.Enabled) {
    g_plugin_client->DestroyCgroupHookAsync(job_id, cgroup->GetCgroupString());
  }

  if (cgroup != nullptr) {
    g_thread_pool->detach_task([cgroup]() {
      bool rc;
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
  return true;
}

void JobManager::LaunchTaskInstanceMt_(TaskExecutionInstance* process) {
  // This function runs in a multi-threading manner. Take care of thread
  // safety. JobInstance will not be free during this function. Take care of
  // data race for job instance.

  task_id_t job_id = process->job_id;
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  auto* job_instance = job.get()->get();
  if (!job_instance) {
    CRANE_ERROR("Failed to get job#{} allocation", job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to get allocation for job#{} ", job_id));
    return;
  }

  if (!(job_instance->cgroup)) {
    job_instance->cgroup =
        g_cg_mgr->AllocateAndGetJobCgroup(job_instance->job_spec.cgroup_spec);
  }
  if (!job_instance->cgroup) {
    CRANE_ERROR("Failed to get job#{} cgroup", job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to get cgroup for job#{} ", job_id));
    return;
  }

  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErr err = SpawnSupervisor_(job_instance, process);
  if (err != CraneErr::kOk) {
    ActivateTaskStatusChangeAsync_(
        job_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeSpawnProcessFail,
        fmt::format(
            "Cannot spawn a new process inside the instance of task #{}",
            job_id));
  } else {
    CRANE_TRACE("[job #{}] Spawned success.", process->task_spec.task_id());
    // kOk means that SpawnProcessInInstance_ has successfully forked a child
    // process.
    // Now we put the child pid into index maps.
    // SIGCHLD sent just after fork() and before putting pid into maps
    // will repeatedly be sent by timer and eventually be handled once the
    // SIGCHLD processing callback sees the pid in index maps.
    // Now we do not support launch multiprocess task.
    job_instance->processes.emplace(
        process->pid, std::unique_ptr<TaskExecutionInstance>(process));
    absl::MutexLock lk(&m_mtx_);
    m_pid_job_map_.emplace(process->pid, job_instance);
    m_pid_proc_map_.emplace(process->pid, process);
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

bool JobManager::MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id) {
  auto job_instance = m_job_map_.GetValueExclusivePtr(job_id);
  CgroupInterface* cg = job_instance->get()->cgroup.get();
  if (!cg) {
    cg =
        g_cg_mgr
            ->AllocateAndGetJobCgroup(job_instance->get()->job_spec.cgroup_spec)
            .release();
    job_instance->get()->cgroup = std::unique_ptr<CgroupInterface>(cg);
  }
  if (!cg) return false;

  return cg->MigrateProcIn(pid);
}

CraneExpected<JobSpec> JobManager::QueryJobSpec(task_id_t job_id) {
  auto instance = m_job_map_.GetValueExclusivePtr(job_id);
  if (!instance) return std::unexpected(CraneErr::kNonExistent);
  return instance->get()->job_spec;
}

std::future<CraneExpected<task_id_t>> JobManager::QueryTaskIdFromPidAsync(
    pid_t pid) {
  EvQueueQueryTaskIdFromPid elem{.pid = pid};
  auto task_id_opt_future = elem.task_id_prom.get_future();
  m_query_task_id_from_pid_queue_.enqueue(std::move(elem));
  m_query_task_id_from_pid_async_handle_->send();
  return task_id_opt_future;
}

bool JobManager::QueryTaskInfoOfUid(uid_t uid, TaskInfoOfUid* info) {
  CRANE_DEBUG("Query task info for uid {}", uid);

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

void JobManager::EvCleanGrpcQueryTaskIdFromPidQueueCb_() {
  EvQueueQueryTaskIdFromPid elem;
  while (m_query_task_id_from_pid_queue_.try_dequeue(elem)) {
    m_mtx_.Lock();

    auto task_iter = m_pid_job_map_.find(elem.pid);
    if (task_iter == m_pid_job_map_.end())
      elem.task_id_prom.set_value(std::unexpected(CraneErr::kSystemErr));
    else {
      JobInstance* instance = task_iter->second;
      uint32_t task_id = instance->job_id;
      elem.task_id_prom.set_value(task_id);
    }

    m_mtx_.Unlock();
  }
}

void JobManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.task_id);

    auto job_instance = m_job_map_.GetValueExclusivePtr(elem.task_id);
    if (job_instance->get()->processes.empty()) {
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
      FreeJobInstanceAllocation_(job_instance->release());
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
    if (err != CraneErr::kOk) {
      CRANE_ERROR("Failed to terminate task #{}", elem.task_id);
    }
  }
}

void JobManager::TerminateTaskAsync(uint32_t task_id) {
  TaskTerminateQueueElem elem{.task_id = task_id, .terminated_by_user = true};
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

void JobManager::MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id) {
  TaskTerminateQueueElem elem{.task_id = task_id, .mark_as_orphaned = true};
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

bool JobManager::CheckTaskStatusAsync(task_id_t task_id,
                                      crane::grpc::TaskStatus* status) {
  CheckTaskStatusQueueElem elem{.task_id = task_id};

  std::future<std::pair<bool, crane::grpc::TaskStatus>> res{
      elem.status_prom.get_future()};

  m_check_task_status_queue_.enqueue(std::move(elem));
  m_check_task_status_async_handle_->send();

  auto [ok, task_status] = res.get();
  if (!ok) return false;

  *status = task_status;
  return true;
}

void JobManager::EvCleanCheckTaskStatusQueueCb_() {
  CheckTaskStatusQueueElem elem;
  while (m_check_task_status_queue_.try_dequeue(elem)) {
    task_id_t task_id = elem.task_id;
    auto job_instance = m_job_map_.GetValueExclusivePtr(task_id);
    // todo: use process id
    if (job_instance && job_instance->get()->processes.contains(task_id)) {
      // Found in task map. The task must be running.
      elem.status_prom.set_value({true, crane::grpc::TaskStatus::Running});
      continue;
    }

    // If a task id can be found in g_ctld_client, the task has ended.
    //  Now if CraneCtld check the status of these tasks, there is no need to
    //  send to TaskStatusChange again. Just cancel them.
    crane::grpc::TaskStatus status;
    bool exist =
        g_ctld_client->CancelTaskStatusChangeByTaskId(task_id, &status);
    if (exist) {
      elem.status_prom.set_value({true, status});
      continue;
    }

    elem.status_prom.set_value(
        {false, /* Invalid Value*/ crane::grpc::Pending});
  }
}

bool JobManager::ChangeTaskTimeLimitAsync(task_id_t task_id,
                                          absl::Duration time_limit) {
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
  if (!m_job_map_.Contains(job_id)) {
    CRANE_ERROR("Task #{} not found in TaskStopAndDoStatusChangeAsync.",
                job_id);
    return;
  }
  CRANE_INFO("Task #{} stopped and is doing TaskStatusChange...", job_id);
  ActivateTaskStatusChangeAsync_(job_id, new_status, exit_code, reason);
}

void JobManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  absl::Time now = absl::Now();

  ChangeTaskTimeLimitQueueElem elem;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    auto job_ptr = m_job_map_.GetValueExclusivePtr(elem.job_id);
    if (job_ptr) {
      auto stub = g_supervisor_keeper->GetStub(elem.job_id);
      if (!stub) {
        CRANE_ERROR("Supervisor for task #{} not found", elem.job_id);
        continue;
      }
      auto err = stub->ChangeTaskTimeLimit(elem.time_limit);
      if (err != CraneErr::kOk) {
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
