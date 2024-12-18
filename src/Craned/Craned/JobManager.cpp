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

#include "CforedClient.h"
#include "CtldClient.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"
#include "protos/Supervisor.pb.h"

namespace Craned {

bool TaskInstance::IsCrun() const {
  return this->task.type() == crane::grpc::Interactive &&
         this->task.interactive_meta().interactive_type() == crane::grpc::Crun;
}
bool TaskInstance::IsCalloc() const {
  return this->task.type() == crane::grpc::Interactive &&
         this->task.interactive_meta().interactive_type() ==
             crane::grpc::Calloc;
}

EnvMap TaskInstance::GetTaskEnvMap() const {
  std::unordered_map<std::string, std::string> env_map;
  // Crane Env will override user task env;
  for (auto& [name, value] : this->task.env()) {
    env_map.emplace(name, value);
  }

  if (this->task.get_user_env()) {
    // If --get-user-env is set, the new environment is inherited
    // from the execution CraneD rather than the submitting node.
    //
    // Since we want to reinitialize the environment variables of the user
    // by reloading the settings in something like .bashrc or /etc/profile,
    // we are actually performing two steps: login -> start shell.
    // Shell starting is done by calling "bash --login".
    //
    // During shell starting step, the settings in
    // /etc/profile, ~/.bash_profile, ... are loaded.
    //
    // During login step, "HOME" and "SHELL" are set.
    // Here we are just mimicking the login module.

    // Slurm uses `su <username> -c /usr/bin/env` to retrieve
    // all the environment variables.
    // We use a more tidy way.
    env_map.emplace("HOME", this->pwd_entry.HomeDir());
    env_map.emplace("SHELL", this->pwd_entry.Shell());
  }

  env_map.emplace("CRANE_JOB_NODELIST",
                  absl::StrJoin(this->task.allocated_nodes(), ";"));
  env_map.emplace("CRANE_EXCLUDES", absl::StrJoin(this->task.excludes(), ";"));
  env_map.emplace("CRANE_JOB_NAME", this->task.name());
  env_map.emplace("CRANE_ACCOUNT", this->task.account());
  env_map.emplace("CRANE_PARTITION", this->task.partition());
  env_map.emplace("CRANE_QOS", this->task.qos());

  env_map.emplace("CRANE_JOB_ID", std::to_string(this->task.task_id()));

  if (this->IsCrun() && !this->task.interactive_meta().term_env().empty()) {
    env_map.emplace("TERM", this->task.interactive_meta().term_env());
  }

  int64_t time_limit_sec = this->task.time_limit().seconds();
  int hours = time_limit_sec / 3600;
  int minutes = (time_limit_sec % 3600) / 60;
  int seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_map.emplace("CRANE_TIMELIMIT", time_limit);
  return env_map;
}

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

  m_process_sigchld_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_process_sigchld_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanSigchldQueueCb_();
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
          if (m_task_cleared_) {
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

JobManager::~JobManager() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

const TaskInstance* JobManager::FindInstanceByTaskId_(uint32_t task_id) {
  auto iter = m_task_map_.find(task_id);
  if (iter == m_task_map_.end()) return nullptr;
  return iter->second.get();
}

void JobManager::TaskStopAndDoStatusChangeAsync(uint32_t task_id) {
  auto it = m_task_map_.find(task_id);
  if (it == m_task_map_.end()) {
    CRANE_ERROR("Task #{} not found in TaskStopAndDoStatusChangeAsync.",
                task_id);
    return;
  }
  TaskInstance* instance = it->second.get();

  CRANE_INFO("Task #{} stopped and is doing TaskStatusChange...", task_id);

  switch (instance->err_before_exec) {
  case CraneErr::kProtobufError:
    ActivateTaskStatusChangeAsync_(task_id, crane::grpc::TaskStatus::Failed,
                                   ExitCode::kExitCodeSpawnProcessFail,
                                   std::nullopt);
    break;

  case CraneErr::kCgroupError:
    ActivateTaskStatusChangeAsync_(task_id, crane::grpc::TaskStatus::Failed,
                                   ExitCode::kExitCodeCgroupError,
                                   std::nullopt);
    break;

  default:
    break;
  }

  ProcSigchldInfo& sigchld_info = instance->sigchld_info;
  if (instance->task.type() == crane::grpc::Batch || instance->IsCrun()) {
    // For a Batch task, the end of the process means it is done.
    if (sigchld_info.is_terminated_by_signal) {
      if (instance->cancelled_by_user)
        ActivateTaskStatusChangeAsync_(
            task_id, crane::grpc::TaskStatus::Cancelled,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else if (instance->terminated_by_timeout)
        ActivateTaskStatusChangeAsync_(
            task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else
        ActivateTaskStatusChangeAsync_(
            task_id, crane::grpc::TaskStatus::Failed,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
    } else
      ActivateTaskStatusChangeAsync_(task_id,
                                     crane::grpc::TaskStatus::Completed,
                                     sigchld_info.value, std::nullopt);
  } else /* Calloc */ {
    // For a COMPLETING Calloc task with a process running,
    // the end of this process means that this task is done.
    if (sigchld_info.is_terminated_by_signal)
      ActivateTaskStatusChangeAsync_(
          task_id, crane::grpc::TaskStatus::Completed,
          sigchld_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
    else
      ActivateTaskStatusChangeAsync_(task_id,
                                     crane::grpc::TaskStatus::Completed,
                                     sigchld_info.value, std::nullopt);
  }
}

void JobManager::EvSigchldCb_() {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      auto sigchld_info = std::make_unique<ProcSigchldInfo>();

      if (WIFEXITED(status)) {
        // Exited with status WEXITSTATUS(status)
        sigchld_info->pid = pid;
        sigchld_info->is_terminated_by_signal = false;
        sigchld_info->value = WEXITSTATUS(status);

        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: false, Status: {}",
                    pid, WEXITSTATUS(status));
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        sigchld_info->pid = pid;
        sigchld_info->is_terminated_by_signal = true;
        sigchld_info->value = WTERMSIG(status);

        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: true, Signal: {}",
                    pid, WTERMSIG(status));
      }
      /* Todo(More status tracing):
       else if (WIFSTOPPED(status)) {
        printf("stopped by signal %d\n", WSTOPSIG(status));
      } else if (WIFCONTINUED(status)) {
        printf("continued\n");
      } */
      m_sigchld_queue_.enqueue(std::move(sigchld_info));
      m_process_sigchld_async_handle_->send();
    } else if (pid == 0) {
      // There's no child that needs reaping.
      // If Craned is exiting, check if there's any task remaining.
      // If there's no task running, just stop the loop of JobManager.
      if (m_is_ending_now_) {
        if (m_task_map_.empty()) {
          ActivateShutdownAsync_();
        }
      }
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void JobManager::EvCleanSigchldQueueCb_() {
  std::unique_ptr<ProcSigchldInfo> sigchld_info;
  while (m_sigchld_queue_.try_dequeue(sigchld_info)) {
    auto pid = sigchld_info->pid;

    if (sigchld_info->resend_timer != nullptr) {
      sigchld_info->resend_timer->close();
      sigchld_info->resend_timer.reset();
    }

    m_mtx_.Lock();
    auto task_iter = m_pid_task_map_.find(pid);
    auto proc_iter = m_pid_proc_map_.find(pid);

    if (task_iter == m_pid_task_map_.end() ||
        proc_iter == m_pid_proc_map_.end()) {
      m_mtx_.Unlock();

      auto* sigchld_info_raw_ptr = sigchld_info.release();
      sigchld_info_raw_ptr->resend_timer =
          m_uvw_loop_->resource<uvw::timer_handle>();
      sigchld_info_raw_ptr->resend_timer->on<uvw::timer_event>(
          [this, sigchld_info_raw_ptr](const uvw::timer_event&,
                                       uvw::timer_handle&) {
            EvSigchldTimerCb_(sigchld_info_raw_ptr);
          });
      sigchld_info_raw_ptr->resend_timer->start(
          std::chrono::milliseconds(kEvSigChldResendMs),
          std::chrono::milliseconds(0));
      CRANE_TRACE("Child Process {} exit too early, will do SigchldCb later",
                  pid);
      continue;
    }

    TaskInstance* instance = task_iter->second;
    ProcessInstance* proc = proc_iter->second;
    uint32_t task_id = instance->task.task_id();

    // Remove indexes from pid to ProcessInstance*
    m_pid_proc_map_.erase(proc_iter);
    m_pid_task_map_.erase(task_iter);

    m_mtx_.Unlock();

    instance->sigchld_info = *sigchld_info;

    // Free the ProcessInstance. ITask struct is not freed here because
    // the ITask for an Interactive task can have no ProcessInstance.
    auto pr_it = instance->processes.find(pid);
    if (pr_it == instance->processes.end()) {
      CRANE_ERROR("Failed to find pid {} in task #{}'s ProcessInstances", pid,
                  task_id);
    } else {
      instance->processes.erase(pr_it);

      if (!instance->processes.empty()) {
        if (sigchld_info->is_terminated_by_signal) {
          // If a task is terminated by a signal and there are other
          //  running processes belonging to this task, kill them.
          TerminateTaskAsync(task_id);
        }
      } else {
        if (instance->IsCrun())
          // TaskStatusChange of a crun task is triggered in
          // CforedManager.
          g_cfored_manager->TaskProcOnCforedStopped(
              instance->task.interactive_meta().cfored_name(),
              instance->task.task_id());
        else /* Batch / Calloc */ {
          // If the ProcessInstance has no process left,
          // send TaskStatusChange for this task.
          // See the comment of EvActivateTaskStatusChange_.
          TaskStopAndDoStatusChangeAsync(task_id);
        }
      }
    }
  }
}

void JobManager::EvSigchldTimerCb_(ProcSigchldInfo* sigchld_info) {
  m_sigchld_queue_.enqueue(std::unique_ptr<ProcSigchldInfo>(sigchld_info));
  m_process_sigchld_async_handle_->send();
}

void JobManager::EvSigintCb_() {
  if (!m_is_ending_now_) {
    // SIGINT has been sent once. If SIGINT are captured twice, it indicates
    // the signal sender can't wait to stop Craned and Craned just send SIGTERM
    // to all tasks to kill them immediately.

    CRANE_INFO("Caught SIGINT. Send SIGTERM to all running tasks...");

    m_is_ending_now_ = true;

    if (m_sigint_cb_) m_sigint_cb_();

    for (auto task_it = m_task_map_.begin(); task_it != m_task_map_.end();) {
      task_id_t task_id = task_it->first;
      TaskInstance* task_instance = task_it->second.get();

      if (task_instance->task.type() == crane::grpc::Batch ||
          task_instance->IsCrun()) {
        for (auto&& [pid, pr_instance] : task_instance->processes) {
          CRANE_INFO(
              "Sending SIGINT to the process group of task #{} with root "
              "process pid {}",
              task_id, pr_instance->GetPid());
          KillProcessInstance_(pr_instance.get(), SIGKILL);
        }
        task_it++;
      } else {
        // Kill all process of a calloc task and just remove it from the
        // task map.
        CRANE_DEBUG("Cleaning Calloc task #{}...",
                    task_instance->task.task_id());

        // Todo: Performance issue!
        task_instance->cgroup->KillAllProcesses();

        auto to_remove_it = task_it++;
        m_task_map_.erase(to_remove_it);
      }
    }

    if (m_task_map_.empty()) {
      // If there is not any batch task to wait for, stop the loop directly.
      ActivateShutdownAsync_();
    }
  } else {
    CRANE_INFO(
        "SIGINT has been triggered already. Sending SIGKILL to all process "
        "groups instead.");
    if (m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      ActivateShutdownAsync_();
    } else {
      for (auto&& [task_id, task_instance] : m_task_map_) {
        for (auto&& [pid, pr_instance] : task_instance->processes) {
          CRANE_INFO(
              "Sending SIGKILL to the process group of task #{} with root "
              "process pid {}",
              task_id, pr_instance->GetPid());
          KillProcessInstance_(pr_instance.get(), SIGKILL);
        }
      }
    }
  }
}

void JobManager::ActivateShutdownAsync_() {
  CRANE_TRACE("Triggering exit event...");
  CRANE_ASSERT(m_is_ending_now_ == true);
  m_task_cleared_ = true;
}

void JobManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

CraneErr JobManager::KillProcessInstance_(const ProcessInstance* proc,
                                          int signum) {
  // Todo: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  if (proc) {
    CRANE_TRACE("Killing pid {} with signal {}", proc->GetPid(), signum);

    // Send the signal to the whole process group.
    int err = kill(-proc->GetPid(), signum);

    if (err == 0)
      return CraneErr::kOk;
    else {
      CRANE_TRACE("kill failed. error: {}", strerror(errno));
      return CraneErr::kGenericFailure;
    }
  }

  return CraneErr::kNonExistent;
}

void JobManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErr JobManager::SpawnProcessInInstance_(TaskInstance* instance,
                                             ProcessInstance* process) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::CanStartMessage;
  using crane::grpc::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  // Socket pair for forwarding IO of crun tasks. Craned read from index 0.
  int crun_io_sock_pair[2];

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

  // save the current uid/gid
  SavedPrivilege saved_priv{getuid(), getgid()};

  int rc = setegid(instance->pwd_entry.Gid());
  if (rc == -1) {
    CRANE_ERROR("error: setegid. {}", strerror(errno));
    return CraneErr::kSystemErr;
  }
  __gid_t gid_a[1] = {instance->pwd_entry.Gid()};
  setgroups(1, gid_a);
  rc = seteuid(instance->pwd_entry.Uid());
  if (rc == -1) {
    CRANE_ERROR("error: seteuid. {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid;
  bool launch_pty{false};

  if (instance->IsCrun()) {
    auto* crun_meta =
        dynamic_cast<CrunMetaInTaskInstance*>(instance->meta.get());
    launch_pty = instance->task.interactive_meta().pty();
    CRANE_DEBUG("Launch crun task #{} pty:{}", instance->task.task_id(),
                launch_pty);

    if (launch_pty) {
      child_pid = forkpty(&crun_meta->msg_fd, nullptr, nullptr, nullptr);
    } else {
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, crun_io_sock_pair) != 0) {
        CRANE_ERROR("Failed to create socket pair for task io forward: {}",
                    strerror(errno));
        return CraneErr::kSystemErr;
      }
      crun_meta->msg_fd = crun_io_sock_pair[0];
      child_pid = fork();
    }
  } else {
    child_pid = fork();
  }

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", instance->task.task_id(),
                strerror(errno));
    return CraneErr::kSystemErr;
  }

  if (child_pid > 0) {  // Parent proc
    process->SetPid(child_pid);
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                instance->task.task_id(), child_pid);

    if (instance->IsCrun()) {
      auto* meta = dynamic_cast<CrunMetaInTaskInstance*>(instance->meta.get());
      g_cfored_manager->RegisterIOForward(
          instance->task.interactive_meta().cfored_name(),
          instance->task.task_id(), meta->msg_fd, launch_pty);
    }

    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);
    if (instance->IsCrun() && !launch_pty) {
      close(crun_io_sock_pair[1]);
    }

    setegid(saved_priv.gid);
    seteuid(saved_priv.uid);
    setgroups(0, nullptr);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    // Add event for stdout/stderr of the new subprocess
    // struct bufferevent* ev_buf_event;
    // ev_buf_event =
    //     bufferevent_socket_new(m_ev_base_, fd, BEV_OPT_CLOSE_ON_FREE);
    // if (!ev_buf_event) {
    //   CRANE_ERROR(
    //       "Error constructing bufferevent for the subprocess of task #!",
    //       instance->task.task_id());
    //   err = CraneErr::kLibEventError;
    //   goto AskChildToSuicide;
    // }
    // bufferevent_setcb(ev_buf_event, EvSubprocessReadCb_, nullptr, nullptr,
    //                   (void*)process.get());
    // bufferevent_enable(ev_buf_event, EV_READ);
    // bufferevent_disable(ev_buf_event, EV_WRITE);
    // process->SetEvBufEvent(ev_buf_event);

    // Migrate the new subprocess to newly created cgroup
    if (!instance->cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          instance->task.task_id());

      instance->err_before_exec = CraneErr::kCgroupError;
      goto AskChildToSuicide;
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

      // Communication failure caused by process crash or grpc error.
      // Since now the parent cannot ask the child
      // process to commit suicide, kill child process here and just return.
      // The child process will be reaped in SIGCHLD handler and
      // thus only ONE TaskStatusChange will be triggered!
      instance->err_before_exec = CraneErr::kProtobufError;
      KillProcessInstance_(process, SIGKILL);
      return CraneErr::kOk;
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

      // See comments above.
      instance->err_before_exec = CraneErr::kProtobufError;
      KillProcessInstance_(process, SIGKILL);
      return CraneErr::kOk;
    }

    close(ctrl_fd);
    return CraneErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    close(ctrl_fd);
    if (!ok) {
      CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                  child_pid, instance->task.task_id());

      // See comments above.
      instance->err_before_exec = CraneErr::kProtobufError;
      KillProcessInstance_(process, SIGKILL);
    }

    // See comments above.
    // As long as fork() is done and the grpc channel to the child process is
    // healthy, we should return kOk, not trigger a manual TaskStatusChange, and
    // reap the child process by SIGCHLD after it commits suicide.
    return CraneErr::kOk;
  } else {  // Child proc
    // Disable SIGABRT backtrace from child processes.
    signal(SIGABRT, SIG_DFL);

    const std::string& cwd = instance->task.cwd();
    rc = chdir(cwd.c_str());
    if (rc == -1) {
      // CRANE_ERROR("[Child Process] Error: chdir to {}. {}", cwd.c_str(),
      //             strerror(errno));
      std::abort();
    }

    setreuid(instance->pwd_entry.Uid(), instance->pwd_entry.Uid());
    setregid(instance->pwd_entry.Gid(), instance->pwd_entry.Gid());

    // Set pgid to the pid of task root process.
    setpgid(0, 0);

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

    if (instance->task.type() == crane::grpc::Batch) {
      int stdout_fd, stderr_fd;

      const std::string& stdout_file_path =
          process->batch_meta.parsed_output_file_pattern;
      const std::string& stderr_file_path =
          process->batch_meta.parsed_error_file_pattern;

      stdout_fd =
          open(stdout_file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
      if (stdout_fd == -1) {
        // CRANE_ERROR("[Child Process] Error: open {}. {}", stdout_file_path,
        // strerror(errno));
        std::abort();
      }
      dup2(stdout_fd, 1);

      if (stderr_file_path.empty()) {
        dup2(stdout_fd, 2);
      } else {
        stderr_fd =
            open(stderr_file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (stderr_fd == -1) {
          // CRANE_ERROR("[Child Process] Error: open {}. {}", stderr_file_path,
          //             strerror(errno));
          std::abort();
        }
        dup2(stderr_fd, 2);  // stderr -> error file
        close(stderr_fd);
      }
      close(stdout_fd);

    } else if (instance->IsCrun() && !launch_pty) {
      close(crun_io_sock_pair[0]);

      dup2(crun_io_sock_pair[1], 0);
      dup2(crun_io_sock_pair[1], 1);
      dup2(crun_io_sock_pair[1], 2);
      close(crun_io_sock_pair[1]);
    }

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      // CRANE_ERROR("[Child Process] Error: Failed to flush.");
      std::abort();
    }

    close(ctrl_fd);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    if (instance->task.type() == crane::grpc::Batch) close(0);
    util::os::CloseFdFrom(3);

    EnvMap task_env_map = instance->GetTaskEnvMap();
    EnvMap res_env_map =
        CgroupManager::GetResourceEnvMapByResInNode(res_in_node.value());

    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    auto FuncSetEnv =
        [](const std::unordered_map<std::string, std::string>& v) {
          for (const auto& [name, value] : v)
            if (setenv(name.c_str(), value.c_str(), 1))
              fmt::print("setenv for {}={} failed!\n", name, value);
        };

    FuncSetEnv(task_env_map);
    FuncSetEnv(res_env_map);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    argv.emplace_back("CraneScript");

    if (instance->task.get_user_env()) {
      // If --get-user-env is specified,
      // we need to use --login option of bash to load settings from the user's
      // settings.
      argv.emplace_back("--login");
    }

    argv.emplace_back(process->GetExecPath().c_str());
    for (auto&& arg : process->GetArgList()) {
      argv.push_back(arg.c_str());
    }
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

CraneErr JobManager::ExecuteTaskAsync(crane::grpc::TaskToD const& task) {
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

  // Create meta for batch or crun tasks.
  if (instance->task.type() == crane::grpc::Batch)
    instance->meta = std::make_unique<BatchMetaInTaskInstance>();
  else
    instance->meta = std::make_unique<CrunMetaInTaskInstance>();

  m_grpc_execute_task_queue_.enqueue(std::move(instance));
  m_grpc_execute_task_async_handle_->send();

  return CraneErr::kOk;
}

void JobManager::EvCleanGrpcExecuteTaskQueueCb_() {
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

    // Add a timer to limit the execution time of a task.
    // Note: event_new and event_add in this function is not thread safe,
    //       so we move it outside the multithreading part.
    int64_t sec = instance->task.time_limit().seconds();
    AddTerminationTimer_(instance, sec);
    CRANE_TRACE("Add a timer of {} seconds for task #{}", sec, task_id);

    g_thread_pool->detach_task(
        [this, instance]() { LaunchTaskInstanceMt_(instance); });
  }
}

void JobManager::LaunchTaskInstanceMt_(TaskInstance* instance) {
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

  instance->pwd_entry.Init(instance->task.uid());
  if (!instance->pwd_entry.Valid()) {
    CRANE_DEBUG("Failed to look up password entry for uid {} of task #{}",
                instance->task.uid(), task_id);
    ActivateTaskStatusChangeAsync_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodePermissionDenied,
        fmt::format("Failed to look up password entry for uid {} of task #{}",
                    instance->task.uid(), task_id));
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

  // Calloc tasks have no scripts to run. Just return.
  if (instance->IsCalloc()) return;

  instance->meta->parsed_sh_script_path =
      fmt::format("{}/Crane-{}.sh", g_config.CranedScriptDir, task_id);
  auto& sh_path = instance->meta->parsed_sh_script_path;

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}", task_id);
    ActivateTaskStatusChangeAsync_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeFileNotFound,
        fmt::format("Cannot write shell script for batch task #{}", task_id));
    return;
  }

  if (instance->task.type() == crane::grpc::Batch)
    fputs(instance->task.batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(instance->task.interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

  auto process =
      std::make_unique<ProcessInstance>(sh_path, std::list<std::string>());

  // Prepare file output name for batch tasks.
  if (instance->task.type() == crane::grpc::Batch) {
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    process->batch_meta.parsed_output_file_pattern =
        ParseFilePathPattern_(instance->task.batch_meta().output_file_pattern(),
                              instance->task.cwd(), task_id);
    absl::StrReplaceAll({{"%j", std::to_string(task_id)},
                         {"%u", instance->pwd_entry.Username()},
                         {"%x", instance->task.name()}},
                        &process->batch_meta.parsed_output_file_pattern);

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!instance->task.batch_meta().error_file_pattern().empty()) {
      process->batch_meta.parsed_error_file_pattern = ParseFilePathPattern_(
          instance->task.batch_meta().error_file_pattern(),
          instance->task.cwd(), task_id);
      absl::StrReplaceAll({{"%j", std::to_string(task_id)},
                           {"%u", instance->pwd_entry.Username()},
                           {"%x", instance->task.name()}},
                          &process->batch_meta.parsed_error_file_pattern);
    }
  }

  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErr err = SpawnProcessInInstance_(instance, process.get());
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
    m_mtx_.Lock();
    m_pid_task_map_.emplace(process->GetPid(), instance);
    m_pid_proc_map_.emplace(process->GetPid(), process.get());

    // Move the ownership of ProcessInstance into the TaskInstance.
    // Make sure existing process can be found when handling SIGCHLD.
    instance->processes.emplace(process->GetPid(), std::move(process));

    m_mtx_.Unlock();
  }
}

std::string JobManager::ParseFilePathPattern_(const std::string& path_pattern,
                                              const std::string& cwd,
                                              task_id_t task_id) {
  std::string resolved_path_pattern;

  if (path_pattern.empty()) {
    // If file path is not specified, first set it to cwd.
    resolved_path_pattern = fmt::format("{}/", cwd);
  } else {
    if (path_pattern[0] == '/')
      // If output file path is an absolute path, do nothing.
      resolved_path_pattern = path_pattern;
    else
      // If output file path is a relative path, prepend cwd to the path.
      resolved_path_pattern = fmt::format("{}/{}", cwd, path_pattern);
  }

  // Path ends with a directory, append default stdout file name
  // `Crane-<Job ID>.out` to the path.
  if (absl::EndsWith(resolved_path_pattern, "/"))
    resolved_path_pattern += fmt::format("Crane-{}.out", task_id);

  return resolved_path_pattern;
}

void JobManager::EvCleanTaskStatusChangeQueueCb_() {
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
    if (instance->task.type() == crane::grpc::Batch || instance->IsCrun()) {
      const std::string& path = instance->meta->parsed_sh_script_path;
      if (!path.empty())
        g_thread_pool->detach_task([p = path]() { util::os::DeleteFile(p); });
    }

    bool orphaned = instance->orphaned;

    // Free the TaskInstance structure
    m_task_map_.erase(status_change.task_id);

    if (!orphaned)
      g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }

  // Todo: Add additional timer to check periodically whether all children
  //  have exited.
  if (m_is_ending_now_ && m_task_map_.empty()) {
    CRANE_TRACE(
        "Craned is ending and all tasks have been reaped. "
        "Stop event loop.");
    ActivateShutdownAsync_();
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

CraneExpected<EnvMap> JobManager::QueryTaskEnvMapAsync(task_id_t task_id) {
  EvQueueQueryTaskEnvMap elem{.task_id = task_id};
  std::future<CraneExpected<EnvMap>> env_future = elem.env_prom.get_future();
  m_query_task_environment_variables_queue.enqueue(std::move(elem));
  m_query_task_environment_variables_async_handle_->send();
  return env_future.get();
}

void JobManager::EvCleanGrpcQueryTaskEnvQueueCb_() {
  EvQueueQueryTaskEnvMap elem;
  while (m_query_task_environment_variables_queue.try_dequeue(elem)) {
    auto task_iter = m_task_map_.find(elem.task_id);
    if (task_iter == m_task_map_.end())
      elem.env_prom.set_value(std::unexpected(CraneErr::kSystemErr));
    else {
      auto& instance = task_iter->second;

      std::unordered_map<std::string, std::string> env_map;
      for (const auto& [name, value] : instance->GetTaskEnvMap()) {
        env_map.emplace(name, value);
      }
      elem.env_prom.set_value(env_map);
    }
  }
}

CraneExpected<task_id_t> JobManager::QueryTaskIdFromPidAsync(pid_t pid) {
  EvQueueQueryTaskIdFromPid elem{.pid = pid};
  std::future<CraneExpected<task_id_t>> task_id_opt_future =
      elem.task_id_prom.get_future();
  m_query_task_id_from_pid_queue_.enqueue(std::move(elem));
  m_query_task_environment_variables_async_handle_->send();
  return task_id_opt_future.get();
}

void JobManager::EvCleanGrpcQueryTaskIdFromPidQueueCb_() {
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

void JobManager::EvTaskTimerCb_(task_id_t task_id) {
  CRANE_TRACE("Task #{} exceeded its time limit. Terminating it...", task_id);

  // Sometimes, task finishes just before time limit.
  // After the execution of SIGCHLD callback where the task has been erased,
  // the timer is triggered immediately.
  // That's why we need to check the existence of the task again in timer
  // callback, otherwise a segmentation fault will occur.
  auto task_it = m_task_map_.find(task_id);
  if (task_it == m_task_map_.end()) {
    CRANE_TRACE("Task #{} has already been removed.");
    return;
  }

  TaskInstance* task_instance = task_it->second.get();
  DelTerminationTimer_(task_instance);

  if (task_instance->task.type() == crane::grpc::Batch) {
    TaskTerminateQueueElem ev_task_terminate{
        .task_id = task_id,
        .terminated_by_timeout = true,
    };
    m_task_terminate_queue_.enqueue(ev_task_terminate);
    m_terminate_task_async_handle_->send();
  } else {
    ActivateTaskStatusChangeAsync_(
        task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
        ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
  }
}

void JobManager::EvCleanTerminateTaskQueueCb_() {
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

    TaskInstance* task_instance = iter->second.get();

    if (elem.terminated_by_user) task_instance->cancelled_by_user = true;
    if (elem.mark_as_orphaned) task_instance->orphaned = true;
    if (elem.terminated_by_timeout) task_instance->terminated_by_timeout = true;

    int sig = SIGTERM;  // For BatchTask
    if (task_instance->IsCrun()) sig = SIGHUP;

    if (!task_instance->processes.empty()) {
      // For an Interactive task with a process running or a Batch task, we
      // just send a kill signal here.
      for (auto&& [pid, pr_instance] : task_instance->processes)
        KillProcessInstance_(pr_instance.get(), sig);
    } else if (task_instance->task.type() == crane::grpc::Interactive) {
      // For an Interactive task with no process running, it ends immediately.
      ActivateTaskStatusChangeAsync_(elem.task_id, crane::grpc::Completed,
                                     ExitCode::kExitCodeTerminated,
                                     std::nullopt);
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

bool JobManager::ChangeTaskTimeLimitAsync(task_id_t task_id,
                                          absl::Duration time_limit) {
  ChangeTaskTimeLimitQueueElem elem{.task_id = task_id,
                                    .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  m_change_task_time_limit_async_handle_->send();
  return ok_fut.get();
}

void JobManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  absl::Time now = absl::Now();

  ChangeTaskTimeLimitQueueElem elem;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    auto iter = m_task_map_.find(elem.task_id);
    if (iter != m_task_map_.end()) {
      TaskInstance* task_instance = iter->second.get();
      DelTerminationTimer_(task_instance);

      absl::Time start_time =
          absl::FromUnixSeconds(task_instance->task.start_time().seconds());
      absl::Duration const& new_time_limit = elem.time_limit;

      if (now - start_time >= new_time_limit) {
        // If the task times out, terminate it.
        TaskTerminateQueueElem ev_task_terminate{.task_id = elem.task_id,
                                                 .terminated_by_timeout = true};
        m_task_terminate_queue_.enqueue(ev_task_terminate);
        m_terminate_task_async_handle_->send();

      } else {
        // If the task haven't timed out, set up a new timer.
        AddTerminationTimer_(
            task_instance,
            ToInt64Seconds((new_time_limit - (absl::Now() - start_time))));
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
