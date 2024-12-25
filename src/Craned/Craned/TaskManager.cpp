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

#include "TaskManager.h"

#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <pty.h>
#include <sys/wait.h>

#include "CtldClient.h"
#include "SupervisorKeeper.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"
#include "protos/Supervisor.pb.h"

namespace Craned {

TaskManager::TaskManager() {
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

  // gRPC: QueryTaskEnvironmentVariable
  m_query_task_environment_variables_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_query_task_environment_variables_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcQueryTaskEnvQueueCb_();
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

TaskManager::~TaskManager() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

const TaskInstance* TaskManager::FindInstanceByTaskId_(uint32_t task_id) {
  auto iter = m_task_map_.find(task_id);
  if (iter == m_task_map_.end()) return nullptr;
  return iter->second.get();
}

void TaskManager::EvSigchldCb_() {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
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

void TaskManager::EvSigintCb_() { m_is_ending_now_ = true; }

void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

CraneErr TaskManager::KillPid_(pid_t pid, int signum) {
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

void TaskManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}
void TaskManager::AddRecoveredTask_(crane::grpc::TaskToD task) {}

CraneErr TaskManager::SpawnSupervisor_(TaskInstance* instance,
                                       ProcessInstance* process) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::CanStartMessage;
  using crane::grpc::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  // int supervisor_ctrl_pipe[2];  // for init Supervisor
  //
  // // 创建管道
  // if (pipe(supervisor_ctrl_pipe) == -1) {
  //   CRANE_ERROR("Pipe creation failed!");
  //   return CraneErr::kSystemErr;
  // }

  // The ResourceInNode structure should be copied here for being accessed in
  // the child process.
  // Note that CgroupManager acquires a lock for this.
  // If the lock is held in the parent process during fork, the forked thread in
  // the child proc will block forever.
  // That's why we should copy it here and the child proc should not hold any
  // lock.
  auto res_in_node = g_cg_mgr->GetTaskResourceInNode(instance->task.task_id());
  if (!res_in_node.has_value()) {
    CRANE_ERROR("Failed to get resource info for task #{}",
                instance->task.task_id());
    return CraneErr::kCgroupError;
  }

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid = fork();

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", instance->task.task_id(),
                strerror(errno));
    return CraneErr::kSystemErr;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                instance->task.task_id(), child_pid);

    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    if (!instance->cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          instance->task.task_id());

      instance->err_before_exec = CraneErr::kCgroupError;
      // Ask child to suicide
      msg.set_ok(false);

      ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
      close(ctrl_fd);
      if (!ok) {
        CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                    child_pid, instance->task.task_id());

        instance->err_before_exec = CraneErr::kProtobufError;
        KillPid_(child_pid, SIGKILL);
      }

      return CraneErr::kCgroupError;
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                instance->task.task_id());

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      CRANE_ERROR("Failed to serialize msg to ostream: {}",
                  strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("Failed to send ok=true to subprocess {} for task #{}: {}",
                  child_pid, instance->task.task_id(),
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
        CRANE_ERROR("Socket child endpoint failed: {}",
                    strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("False from subprocess {} of task #{}", child_pid,
                    instance->task.task_id());
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kProtobufError;
    }

    // Do Supervisor Init
    crane::grpc::InitSupervisorRequest init_request;
    init_request.set_debug_level("off");
    init_request.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_request.set_task_id(instance->task.task_id());

    ok = SerializeDelimitedToZeroCopyStream(init_request, &ostream);
    if (!ok) {
      CRANE_ERROR("Failed to serialize msg to ostream: {}",
                  strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("Failed to send init msg to supervisor for task #{}: {}",
                  child_pid, instance->task.task_id(),
                  strerror(ostream.GetErrno()));
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kProtobufError;
    }

    crane::grpc::SupervisorReady supervisor_ready;
    ok = ParseDelimitedFromZeroCopyStream(&supervisor_ready, &istream, nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("Socket child endpoint failed: {}",
                    strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("False from subprocess {} of task #{}", child_pid,
                    instance->task.task_id());
      close(ctrl_fd);

      instance->err_before_exec = CraneErr::kProtobufError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kOk;
    }

    close(ctrl_fd);

    // todo: Create Supervisor stub,request task execution
    g_supervisor_keeper->AddSupervisor(instance->task.task_id());

    auto stub = g_supervisor_keeper->GetStub(instance->task.task_id());
    auto task_id = stub->ExecuteTask(process);
    if (!task_id) {
      CRANE_ERROR("Supervisor failed to execute task #{}",
                  instance->task.task_id());
      instance->err_before_exec = CraneErr::kSupervisorError;
      KillPid_(child_pid, SIGKILL);
      return CraneErr::kSupervisorError;
    }
    process->pid = task_id.value();

    return CraneErr::kOk;

  } else {  // Child proc
    // Disable SIGABRT backtrace from child processes.
    signal(SIGABRT, SIG_DFL);

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

    // Message will send to stdin of Supervisor for its init.
    dup2(ctrl_fd, STDIN_FILENO);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    util::os::CloseFdFrom(3);

    // Just set job resource env
    EnvMap res_env_map =
        CgroupManager::GetResourceEnvMapByResInNode(res_in_node.value());

    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    for (const auto& [name, value] : res_env_map)
      if (setenv(name.c_str(), value.c_str(), 1))
        fmt::print("setenv for {}={} failed!\n", name, value);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    argv.emplace_back("Supervisor");

    if (instance->task.get_user_env()) {
      // If --get-user-env is specified,
      // we need to use --login option of bash to load settings from the user's
      // settings.
      argv.emplace_back("--login");
    }

    argv.emplace_back(g_config.SupervisorPath.c_str());
    argv.push_back(nullptr);

    execv("/bin/bash", const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess Error] Failed to execv. Error: {}\n",
               strerror(errno));
    // Todo: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }
}

CraneErr TaskManager::ExecuteTaskAsync(crane::grpc::TaskToD const& task) {
  if (!g_cg_mgr->CheckIfCgroupForTasksExists(task.task_id())) {
    CRANE_DEBUG("Executing task #{} without an allocated cgroup. Ignoring it.",
                task.task_id());
    return CraneErr::kCgroupError;
  }
  CRANE_INFO("Executing task #{}", task.task_id());

  auto instance = std::make_unique<TaskInstance>();

  // Simply wrap the Task structure within a TaskInstance structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  instance->task = task;

  m_grpc_execute_task_queue_.enqueue(std::move(instance));
  m_grpc_execute_task_async_handle_->send();

  return CraneErr::kOk;
}

void TaskManager::EvCleanGrpcExecuteTaskQueueCb_() {
  std::unique_ptr<TaskInstance> popped_instance;

  while (m_grpc_execute_task_queue_.try_dequeue(popped_instance)) {
    // Once ExecuteTask RPC is processed, the TaskInstance goes into
    // m_task_map_.
    TaskInstance* instance = popped_instance.get();
    task_id_t task_id = instance->task.task_id();

    auto [iter, ok] = m_task_map_.emplace(task_id, std::move(popped_instance));
    if (!ok) {
      CRANE_ERROR("Duplicated ExecuteTask request for task #{}. Ignore it.",
                  task_id);
      continue;
    }

    g_thread_pool->detach_task(
        [this, instance]() { LaunchTaskInstanceMt_(instance); });
  }
}

void TaskManager::LaunchTaskInstanceMt_(TaskInstance* instance) {
  // This function runs in a multi-threading manner. Take care of thread safety.
  task_id_t task_id = instance->task.task_id();

  if (!g_cg_mgr->CheckIfCgroupForTasksExists(task_id)) {
    CRANE_ERROR("Failed to find created cgroup for task #{}", task_id);
    ActivateTaskStatusChangeAsync_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to find created cgroup for task #{}", task_id));
    return;
  }

  CgroupInterface* cg;
  bool ok = g_cg_mgr->AllocateAndGetCgroup(task_id, &cg);
  if (!ok) {
    CRANE_ERROR("Failed to allocate cgroup for task #{}", task_id);
    ActivateTaskStatusChangeAsync_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to allocate cgroup for task #{}", task_id));
    return;
  }
  instance->cgroup = cg;
  instance->cgroup_path = cg->GetCgroupString();

  auto process = std::make_unique<ProcessInstance>(instance->task);

  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErr err = SpawnSupervisor_(instance, process.get());
  if (err != CraneErr::kOk) {
    ActivateTaskStatusChangeAsync_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeSpawnProcessFail,
        fmt::format(
            "Cannot spawn a new process inside the instance of task #{}",
            task_id));
  } else {
    // kOk means that SpawnProcessInInstance_ has successfully forked a child
    // process.
    // Now we put the child pid into index maps.
    // SIGCHLD sent just after fork() and before putting pid into maps
    // will repeatedly be sent by timer and eventually be handled once the
    // SIGCHLD processing callback sees the pid in index maps.
    instance->processes.emplace(process->pid, std::move(process));
  }
}

void TaskManager::EvCleanTaskStatusChangeQueueCb_() {
  TaskStatusChangeQueueElem status_change;
  while (m_task_status_change_queue_.try_dequeue(status_change)) {
    auto iter = m_task_map_.find(status_change.task_id);
    if (iter == m_task_map_.end()) {
      // When Ctrl+C is pressed for Craned, all tasks including just forked
      // tasks will be terminated.
      // In some error cases, a double TaskStatusChange might be triggered.
      // Just ignore it. See comments in SpawnProcessInInstance_().
      continue;
    }

    TaskInstance* instance = iter->second.get();

    bool orphaned = instance->orphaned;

    // Free the TaskInstance structure
    m_task_map_.erase(status_change.task_id);

    if (!orphaned)
      g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }
}

void TaskManager::ActivateTaskStatusChangeAsync_(
    uint32_t task_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason) {
  TaskStatusChangeQueueElem status_change{task_id, new_status, exit_code};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  m_task_status_change_async_handle_->send();
}

CraneExpected<EnvMap> TaskManager::QueryTaskEnvMapAsync(task_id_t task_id) {
  EvQueueQueryTaskEnvMap elem{.task_id = task_id};
  std::future<CraneExpected<EnvMap>> env_future = elem.env_prom.get_future();
  m_query_task_environment_variables_queue.enqueue(std::move(elem));
  m_query_task_environment_variables_async_handle_->send();
  return env_future.get();
}

void TaskManager::EvCleanGrpcQueryTaskEnvQueueCb_() {
  EvQueueQueryTaskEnvMap elem;
  while (m_query_task_environment_variables_queue.try_dequeue(elem)) {
    auto task_iter = m_task_map_.find(elem.task_id);
    if (task_iter == m_task_map_.end())
      elem.env_prom.set_value(std::unexpected(CraneErr::kSystemErr));
    else {
      auto& instance = task_iter->second;
      auto stub = g_supervisor_keeper->GetStub(instance->task.task_id());
      if (!stub) {
        CRANE_ERROR("Supervisor for task #{} not found",
                    instance->task.task_id());
        elem.env_prom.set_value(std::unexpected(CraneErr::kSupervisorError));
        continue;
      }
      auto env_map = stub->QueryTaskEnv();
      if (!env_map) {
        CRANE_ERROR("Failed to query env map for task #{}",
                    instance->task.task_id());
        elem.env_prom.set_value(std::unexpected(CraneErr::kSupervisorError));
        continue;
      }
      elem.env_prom.set_value(env_map);
    }
  }
}

CraneExpected<task_id_t> TaskManager::QueryTaskIdFromPidAsync(pid_t pid) {
  EvQueueQueryTaskIdFromPid elem{.pid = pid};
  std::future<CraneExpected<task_id_t>> task_id_opt_future =
      elem.task_id_prom.get_future();
  m_query_task_id_from_pid_queue_.enqueue(std::move(elem));
  m_query_task_environment_variables_async_handle_->send();
  return task_id_opt_future.get();
}

void TaskManager::EvCleanGrpcQueryTaskIdFromPidQueueCb_() {
  EvQueueQueryTaskIdFromPid elem;
  while (m_query_task_id_from_pid_queue_.try_dequeue(elem)) {
    m_mtx_.Lock();

    auto task_iter = m_pid_task_map_.find(elem.pid);
    if (task_iter == m_pid_task_map_.end())
      elem.task_id_prom.set_value(std::unexpected(CraneErr::kSystemErr));
    else {
      TaskInstance* instance = task_iter->second;
      uint32_t task_id = instance->task.task_id();
      elem.task_id_prom.set_value(task_id);
    }

    m_mtx_.Unlock();
  }
}

void TaskManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.task_id);

    auto iter = m_task_map_.find(elem.task_id);
    if (iter == m_task_map_.end()) {
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
      g_cg_mgr->ReleaseCgroupByTaskIdOnly(elem.task_id);
      continue;
    }
    auto* instance = iter->second.get();
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

void TaskManager::TerminateTaskAsync(uint32_t task_id) {
  TaskTerminateQueueElem elem{.task_id = task_id, .terminated_by_user = true};
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

void TaskManager::MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id) {
  TaskTerminateQueueElem elem{.task_id = task_id, .mark_as_orphaned = true};
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

bool TaskManager::CheckTaskStatusAsync(task_id_t task_id,
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

void TaskManager::EvCleanCheckTaskStatusQueueCb_() {
  CheckTaskStatusQueueElem elem;
  while (m_check_task_status_queue_.try_dequeue(elem)) {
    task_id_t task_id = elem.task_id;
    if (m_task_map_.contains(task_id)) {
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

bool TaskManager::ChangeTaskTimeLimitAsync(task_id_t task_id,
                                           absl::Duration time_limit) {
  ChangeTaskTimeLimitQueueElem elem{.task_id = task_id,
                                    .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  m_change_task_time_limit_async_handle_->send();
  return ok_fut.get();
}

void TaskManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  absl::Time now = absl::Now();

  ChangeTaskTimeLimitQueueElem elem;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    auto iter = m_task_map_.find(elem.task_id);
    if (iter != m_task_map_.end()) {
      auto stub = g_supervisor_keeper->GetStub(elem.task_id);
      if (!stub) {
        CRANE_ERROR("Supervisor for task #{} not found", elem.task_id);
        continue;
      }
      auto err = stub->ChangeTaskTimeLimit(elem.time_limit);
      if (err != CraneErr::kOk) {
        CRANE_ERROR("Failed to change task time limit for task #{}",
                    elem.task_id);
        continue;
      }
      elem.ok_prom.set_value(true);
    } else {
      CRANE_ERROR("Try to update the time limit of a non-existent task #{}.",
                  elem.task_id);
      elem.ok_prom.set_value(false);
    }
  }
}

}  // namespace Craned
