/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "TaskManager.h"

#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>

#include "CforedClient.h"
#include "crane/OS.h"
#include "crane/String.h"
#include "protos/CraneSubprocess.pb.h"

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

std::vector<EnvPair> TaskInstance::GetEnvList() const {
  std::vector<EnvPair> env_vec;
  for (auto& [name, value] : this->task.env()) {
    env_vec.emplace_back(name, value);
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
    env_vec.emplace_back("HOME", this->pwd_entry.HomeDir());
    env_vec.emplace_back("SHELL", this->pwd_entry.Shell());
  }

  env_vec.emplace_back("CRANE_JOB_NODELIST",
                       absl::StrJoin(this->task.allocated_nodes(), ";"));
  env_vec.emplace_back("CRANE_EXCLUDES",
                       absl::StrJoin(this->task.excludes(), ";"));
  env_vec.emplace_back("CRANE_JOB_NAME", this->task.name());
  env_vec.emplace_back("CRANE_ACCOUNT", this->task.account());
  env_vec.emplace_back("CRANE_PARTITION", this->task.partition());
  env_vec.emplace_back("CRANE_QOS", this->task.qos());

  env_vec.emplace_back("CRANE_JOB_ID", std::to_string(this->task.task_id()));

  if (this->IsCrun() && !this->task.interactive_meta().term_env().empty()) {
    env_vec.emplace_back("TERM", this->task.interactive_meta().term_env());
  }

  int64_t time_limit_sec = this->task.time_limit().seconds();
  int hours = time_limit_sec / 3600;
  int minutes = (time_limit_sec % 3600) / 60;
  int seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_vec.emplace_back("CRANE_TIMELIMIT", time_limit);
  return env_vec;
}

TaskManager::TaskManager() {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_ev_base_ = event_base_new();
  if (m_ev_base_ == nullptr) {
    CRANE_ERROR("Could not initialize libevent!");
    std::terminate();
  }
  {  // SIGCHLD
    m_ev_sigchld_ = evsignal_new(m_ev_base_, SIGCHLD, EvSigchldCb_, this);
    if (!m_ev_sigchld_) {
      CRANE_ERROR("Failed to create the SIGCHLD event!");
      std::terminate();
    }

    if (event_add(m_ev_sigchld_, nullptr) < 0) {
      CRANE_ERROR("Could not add the SIGCHLD event to base!");
      std::terminate();
    }
  }
  {  // SIGINT
    m_ev_sigint_ = evsignal_new(m_ev_base_, SIGINT, EvSigintCb_, this);
    if (!m_ev_sigint_) {
      CRANE_ERROR("Failed to create the SIGCHLD event!");
      std::terminate();
    }

    if (event_add(m_ev_sigint_, nullptr) < 0) {
      CRANE_ERROR("Could not add the SIGINT event to base!");
      std::terminate();
    }
  }
  {  // gRPC: QueryTaskIdFromPid
    m_ev_query_task_id_from_pid_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcQueryTaskIdFromPidCb_, this);
    if (!m_ev_query_task_id_from_pid_) {
      CRANE_ERROR("Failed to create the query task id event!");
      std::terminate();
    }

    if (event_add(m_ev_query_task_id_from_pid_, nullptr) < 0) {
      CRANE_ERROR("Could not add the query task id event to base!");
      std::terminate();
    }
  }
  {  // gRPC: QueryTaskEnvironmentVariable
    m_ev_query_task_environment_variables_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcQueryTaskEnvironmentVariableCb_, this);
    if (!m_ev_query_task_environment_variables_) {
      CRANE_ERROR("Failed to create the query task env event!");
      std::terminate();
    }

    if (event_add(m_ev_query_task_environment_variables_, nullptr) < 0) {
      CRANE_ERROR("Could not add the query task env to base!");
      std::terminate();
    }
  }
  {  // Exit Event
    m_ev_exit_event_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ, EvExitEventCb_, this);
    if (!m_ev_exit_event_) {
      CRANE_ERROR("Failed to create the exit event!");
      std::terminate();
    }

    if (event_add(m_ev_exit_event_, nullptr) < 0) {
      CRANE_ERROR("Could not add the exit event to base!");
      std::terminate();
    }
  }
  {  // Grpc Execute Task Event
    m_ev_grpc_execute_task_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                        EvGrpcExecuteTaskCb_, this);
    if (!m_ev_grpc_execute_task_) {
      CRANE_ERROR("Failed to create the grpc_execute_task event!");
      std::terminate();
    }
    if (event_add(m_ev_grpc_execute_task_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_grpc_execute_task_ to base!");
      std::terminate();
    }
  }
  {  // Task Status Change Event
    m_ev_task_status_change_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                         EvTaskStatusChangeCb_, this);
    if (!m_ev_task_status_change_) {
      CRANE_ERROR("Failed to create the task_status_change event!");
      std::terminate();
    }
    if (event_add(m_ev_task_status_change_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_task_status_change_event_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_task_time_limit_change_ = event_new(
        m_ev_base_, -1, EV_READ | EV_PERSIST, EvChangeTaskTimeLimitCb_, this);
    if (!m_ev_task_time_limit_change_) {
      CRANE_ERROR("Failed to create the task_time_limit_change event!");
      std::terminate();
    }
    if (event_add(m_ev_task_time_limit_change_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_task_time_limit_change_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_task_terminate_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                     EvTerminateTaskCb_, this);
    if (!m_ev_task_terminate_) {
      CRANE_ERROR("Failed to create the task_terminate event!");
      std::terminate();
    }
    if (event_add(m_ev_task_terminate_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_task_terminate_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_check_task_status_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                        EvCheckTaskStatusCb_, this);
    if (!m_ev_check_task_status_) {
      CRANE_ERROR("Failed to create the check_task_status event!");
      std::terminate();
    }
    if (event_add(m_ev_check_task_status_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_check_task_status_ to base!");
      std::terminate();
    }
  }

  m_ev_loop_thread_ =
      std::thread([this]() { event_base_dispatch(m_ev_base_); });
}

TaskManager::~TaskManager() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();

  if (m_ev_sigchld_) event_free(m_ev_sigchld_);
  if (m_ev_sigint_) event_free(m_ev_sigint_);

  if (m_ev_query_task_id_from_pid_) event_free(m_ev_query_task_id_from_pid_);
  if (m_ev_query_task_environment_variables_)
    event_free(m_ev_query_task_environment_variables_);
  if (m_ev_grpc_execute_task_) event_free(m_ev_grpc_execute_task_);
  if (m_ev_exit_event_) event_free(m_ev_exit_event_);
  if (m_ev_task_status_change_) event_free(m_ev_task_status_change_);
  if (m_ev_task_time_limit_change_) event_free(m_ev_task_time_limit_change_);
  if (m_ev_task_terminate_) event_free(m_ev_task_terminate_);
  if (m_ev_check_task_status_) event_free(m_ev_check_task_status_);

  if (m_ev_base_) event_base_free(m_ev_base_);
}

const TaskInstance* TaskManager::FindInstanceByTaskId_(uint32_t task_id) {
  auto iter = m_task_map_.find(task_id);
  if (iter == m_task_map_.end()) return nullptr;
  return iter->second.get();
}

void TaskManager::TaskStopAndDoStatusChangeAsync(uint32_t task_id) {
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
    EvActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Cancelled,
                                ExitCode::kExitCodeSpawnProcessFail,
                                std::nullopt);
    break;

  case CraneErr::kCgroupError:
    EvActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Cancelled,
                                ExitCode::kExitCodeCgroupError, std::nullopt);
    break;

  default:
    break;
  }

  ProcSigchldInfo& sigchld_info = instance->sigchld_info;
  if (instance->task.type() == crane::grpc::Batch || instance->IsCrun()) {
    // For a Batch task, the end of the process means it is done.
    if (sigchld_info.is_terminated_by_signal) {
      if (instance->cancelled_by_user)
        EvActivateTaskStatusChange_(
            task_id, crane::grpc::TaskStatus::Cancelled,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else if (instance->terminated_by_timeout)
        EvActivateTaskStatusChange_(
            task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else
        EvActivateTaskStatusChange_(
            task_id, crane::grpc::TaskStatus::Failed,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
    } else
      EvActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Completed,
                                  sigchld_info.value, std::nullopt);
  } else /* Calloc */ {
    // For a COMPLETING Calloc task with a process running,
    // the end of this process means that this task is done.
    if (sigchld_info.is_terminated_by_signal)
      EvActivateTaskStatusChange_(
          task_id, crane::grpc::TaskStatus::Completed,
          sigchld_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
    else
      EvActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Completed,
                                  sigchld_info.value, std::nullopt);
  }
}

void TaskManager::EvSigchldCb_(evutil_socket_t sig, short events,
                               void* user_data) {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  ProcSigchldInfo sigchld_info{};

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      if (WIFEXITED(status)) {
        // Exited with status WEXITSTATUS(status)
        sigchld_info = {pid, false, WEXITSTATUS(status)};
        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: false, Status: {}",
                    pid, WEXITSTATUS(status));
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        sigchld_info = {pid, true, WTERMSIG(status)};
        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: true, Signal: {}",
                    pid, WTERMSIG(status));
      }
      /* Todo(More status tracing):
       else if (WIFSTOPPED(status)) {
        printf("stopped by signal %d\n", WSTOPSIG(status));
      } else if (WIFCONTINUED(status)) {
        printf("continued\n");
      } */

      this_->m_mtx_.Lock();

      auto task_iter = this_->m_pid_task_map_.find(pid);
      auto proc_iter = this_->m_pid_proc_map_.find(pid);
      if (task_iter == this_->m_pid_task_map_.end() ||
          proc_iter == this_->m_pid_proc_map_.end()) {
        CRANE_WARN("Failed to find task id for pid {}.", pid);
        this_->m_mtx_.Unlock();
      } else {
        TaskInstance* instance = task_iter->second;
        ProcessInstance* proc = proc_iter->second;
        uint32_t task_id = instance->task.task_id();

        // Remove indexes from pid to ProcessInstance*
        this_->m_pid_proc_map_.erase(proc_iter);
        this_->m_pid_task_map_.erase(task_iter);

        this_->m_mtx_.Unlock();

        instance->sigchld_info = sigchld_info;
        proc->Finish(sigchld_info.is_terminated_by_signal, sigchld_info.value);

        // Free the ProcessInstance. ITask struct is not freed here because
        // the ITask for an Interactive task can have no ProcessInstance.
        auto pr_it = instance->processes.find(pid);
        if (pr_it == instance->processes.end()) {
          CRANE_ERROR("Failed to find pid {} in task #{}'s ProcessInstances",
                      pid, task_id);
        } else {
          instance->processes.erase(pr_it);

          if (!instance->processes.empty()) {
            if (sigchld_info.is_terminated_by_signal) {
              // If a task is terminated by a signal and there are other
              //  running processes belonging to this task, kill them.
              this_->TerminateTaskAsync(task_id);
            }
          } else {
            if (instance->task.interactive_meta().interactive_type() ==
                crane::grpc::Crun)
              // TaskStatusChange of a crun task is triggered in
              // CforedManager.
              g_cfored_manager->TaskProcOnCforedStopped(
                  instance->task.interactive_meta().cfored_name(),
                  instance->task.task_id());
            else /* Batch / Calloc */ {
              // If the ProcessInstance has no process left,
              // send TaskStatusChange for this task.
              // See the comment of EvActivateTaskStatusChange_.
              this_->TaskStopAndDoStatusChangeAsync(task_id);
            }
          }
        }
      }
    } else if (pid == 0) {
      // There's no child that needs reaping.
      // If Craned is exiting, check if there's any task remaining.
      // If there's no task running, just stop the loop of TaskManager.
      if (this_->m_is_ending_now_) {
        if (this_->m_task_map_.empty()) {
          this_->EvActivateShutdown_();
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

void TaskManager::EvSubprocessReadCb_(struct bufferevent* bev, void* process) {
  auto* proc = reinterpret_cast<ProcessInstance*>(process);

  size_t buf_len = evbuffer_get_length(bev->input);

  std::string str;
  str.resize(buf_len);
  int n_copy = evbuffer_remove(bev->input, str.data(), buf_len);

  CRANE_TRACE("Read {:>4} bytes from subprocess (pid: {}): {}", n_copy,
              proc->GetPid(), str);

  proc->Output(std::move(str));
}

void TaskManager::EvSigintCb_(int sig, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  if (!this_->m_is_ending_now_) {
    // SIGINT has been sent once. If SIGINT are captured twice, it indicates
    // the signal sender can't wait to stop Craned and Craned just send SIGTERM
    // to all tasks to kill them immediately.

    CRANE_INFO("Caught SIGINT. Send SIGTERM to all running tasks...");

    this_->m_is_ending_now_ = true;

    if (this_->m_sigint_cb_) this_->m_sigint_cb_();

    for (auto task_it = this_->m_task_map_.begin();
         task_it != this_->m_task_map_.end();) {
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
        this_->m_task_map_.erase(to_remove_it);
      }
    }

    if (this_->m_task_map_.empty()) {
      // If there is not any batch task to wait for, stop the loop directly.
      this_->EvActivateShutdown_();
    }
  } else {
    CRANE_INFO(
        "SIGINT has been triggered already. Sending SIGKILL to all process "
        "groups instead.");
    if (this_->m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      this_->EvActivateShutdown_();
    } else {
      for (auto&& [task_id, task_instance] : this_->m_task_map_) {
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

void TaskManager::EvExitEventCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  CRANE_TRACE("Exit event triggered. Stop event loop.");

  struct timeval delay = {0, 0};
  event_base_loopexit(this_->m_ev_base_, &delay);
}

void TaskManager::EvActivateShutdown_() {
  CRANE_TRACE("Triggering exit event...");
  m_is_ending_now_ = true;
  event_active(m_ev_exit_event_, 0, 0);
}

void TaskManager::Wait() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
}

CraneErr TaskManager::KillProcessInstance_(const ProcessInstance* proc,
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

void TaskManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErr TaskManager::SpawnProcessInInstance_(
    TaskInstance* instance, std::unique_ptr<ProcessInstance> process) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::subprocess::CanStartMessage;
  using crane::grpc::subprocess::ChildProcessReady;

  int ctrl_sock_pair[2];    // Socket pair for passing control messages.
  int io_in_sock_pair[2];   // Socket pair for forwarding IO of crun tasks.
  int io_out_sock_pair[2];  // Socket pair for forwarding IO of crun tasks.

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  // Create IO socket pair for crun tasks.
  if (instance->IsCrun()) {
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, io_in_sock_pair) != 0) {
      CRANE_ERROR("Failed to create socket pair for task io forward: {}",
                  strerror(errno));
      return CraneErr::kSystemErr;
    }

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, io_out_sock_pair) != 0) {
      CRANE_ERROR("Failed to create socket pair for task io forward: {}",
                  strerror(errno));
      return CraneErr::kSystemErr;
    }

    auto* crun_meta =
        dynamic_cast<CrunMetaInTaskInstance*>(instance->meta.get());
    crun_meta->proc_in_fd = io_in_sock_pair[0];
    crun_meta->proc_out_fd = io_out_sock_pair[0];
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

  pid_t child_pid = fork();
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
          instance->task.task_id(), meta->proc_in_fd, meta->proc_out_fd);
    }

    // Note that the following code will move the child process into cgroup.
    // Once the child process is moved into cgroup, it might be killed due to
    // memory limitation.
    // Since the task status change is triggered by SIGCHLD,
    // we should put the child pid into the index map IMMEDIATELY when fork() is
    // done.
    // Otherwise, SIGCHLD handler will not find the pid if the child process
    // stops really fast, for example, due to being killed by oom killer.
    // In such case, the task status change for this quickly dying job will not
    // be triggered, and it will cause infinitely running jobs which are
    // actually dead.
    m_mtx_.Lock();
    m_pid_task_map_.emplace(child_pid, instance);
    m_pid_proc_map_.emplace(child_pid, process.get());
    m_mtx_.Unlock();

    // Move the ownership of ProcessInstance into the TaskInstance.
    instance->processes.emplace(child_pid, std::move(process));

    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);
    if (instance->IsCrun()) {
      close(io_in_sock_pair[1]);
      close(io_out_sock_pair[1]);
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
      KillProcessInstance_(process.get(), SIGKILL);
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
      KillProcessInstance_(process.get(), SIGKILL);
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
      KillProcessInstance_(process.get(), SIGKILL);
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
      CRANE_ERROR("[Child Process] Error: chdir to {}. {}", cwd.c_str(),
                  strerror(errno));
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
      if (!ok) {
        int err = istream.GetErrno();
        CRANE_ERROR("Failed to read socket from parent: {}", strerror(err));
      }

      if (!msg.ok())
        CRANE_ERROR("Parent process ask not to start the subprocess.");

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
        CRANE_ERROR("[Child Process] Error: open {}. {}", stdout_file_path,
                    strerror(errno));
        std::abort();
      }
      dup2(stdout_fd, 1);

      if (stderr_file_path.empty()) {
        dup2(stdout_fd, 2);
      } else {
        stderr_fd =
            open(stderr_file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (stderr_fd == -1) {
          CRANE_ERROR("[Child Process] Error: open {}. {}", stderr_file_path,
                      strerror(errno));
          std::abort();
        }
        dup2(stderr_fd, 2);  // stderr -> error file
        close(stderr_fd);
      }
      close(stdout_fd);

    } else if (instance->IsCrun()) {
      close(io_in_sock_pair[0]);
      close(io_out_sock_pair[0]);

      dup2(io_in_sock_pair[1], 0);
      close(io_in_sock_pair[1]);

      dup2(io_out_sock_pair[1], 1);
      dup2(io_out_sock_pair[1], 2);
      close(io_out_sock_pair[1]);
    }

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Child Process] Error: Failed to flush.");
      std::abort();
    }

    close(ctrl_fd);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    if (instance->task.type() == crane::grpc::Batch) close(0);
    util::os::CloseFdFrom(3);

    std::vector<EnvPair> task_env_vec = instance->GetEnvList();
    std::vector<EnvPair> res_env_vec =
        g_cg_mgr->GetResourceEnvListOfTaskNoLock(instance->task.task_id());

    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    auto FuncSetEnv = [](const std::vector<EnvPair>& v) {
      for (const auto& [name, value] : v)
        if (setenv(name.c_str(), value.c_str(), 1))
          fmt::print("setenv for {}={} failed!\n", name, value);
    };

    FuncSetEnv(task_env_vec);
    FuncSetEnv(res_env_vec);

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

  // Create meta for batch or crun tasks.
  if (instance->task.type() == crane::grpc::Batch)
    instance->meta = std::make_unique<BatchMetaInTaskInstance>();
  else
    instance->meta = std::make_unique<CrunMetaInTaskInstance>();

  m_grpc_execute_task_queue_.enqueue(std::move(instance));
  event_active(m_ev_grpc_execute_task_, 0, 0);

  return CraneErr::kOk;
}

void TaskManager::EvGrpcExecuteTaskCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);
  std::unique_ptr<TaskInstance> popped_instance;

  while (this_->m_grpc_execute_task_queue_.try_dequeue(popped_instance)) {
    // Once ExecuteTask RPC is processed, the TaskInstance goes into
    // m_task_map_.
    TaskInstance* instance = popped_instance.get();
    task_id_t task_id = instance->task.task_id();

    auto [iter, ok] =
        this_->m_task_map_.emplace(task_id, std::move(popped_instance));
    if (!ok) {
      CRANE_ERROR("Duplicated ExecuteTask request for task #{}. Ignore it.",
                  task_id);
      continue;
    }

    // Add a timer to limit the execution time of a task.
    // Note: event_new and event_add in this function is not thread safe,
    //       so we move it outside the multithreading part.
    int64_t sec = instance->task.time_limit().seconds();
    this_->EvAddTerminationTimer_(instance, sec);
    CRANE_TRACE("Add a timer of {} seconds for task #{}", sec, task_id);

    g_thread_pool->detach_task(
        [this_, instance]() { this_->LaunchTaskInstanceMt_(instance); });
  }
}

void TaskManager::LaunchTaskInstanceMt_(TaskInstance* instance) {
  // This function runs in a multi-threading manner. Take care of thread safety.
  task_id_t task_id = instance->task.task_id();

  if (!g_cg_mgr->CheckIfCgroupForTasksExists(task_id)) {
    CRANE_ERROR("Failed to find created cgroup for task #{}", task_id);
    EvActivateTaskStatusChange_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to find created cgroup for task #{}", task_id));
    return;
  }

  instance->pwd_entry.Init(instance->task.uid());
  if (!instance->pwd_entry.Valid()) {
    CRANE_DEBUG("Failed to look up password entry for uid {} of task #{}",
                instance->task.uid(), task_id);
    EvActivateTaskStatusChange_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodePermissionDenied,
        fmt::format("Failed to look up password entry for uid {} of task #{}",
                    instance->task.uid(), task_id));
    return;
  }

  Cgroup* cg;
  bool ok = g_cg_mgr->AllocateAndGetCgroup(task_id, &cg);
  if (!ok) {
    CRANE_ERROR("Failed to allocate cgroup for task #{}", task_id);
    EvActivateTaskStatusChange_(
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
    EvActivateTaskStatusChange_(
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
  CraneErr err = SpawnProcessInInstance_(instance, std::move(process));
  if (err != CraneErr::kOk) {
    EvActivateTaskStatusChange_(
        task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeSpawnProcessFail,
        fmt::format(
            "Cannot spawn a new process inside the instance of task #{}",
            task_id));
  }
}

std::string TaskManager::ParseFilePathPattern_(const std::string& path_pattern,
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

void TaskManager::EvTaskStatusChangeCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  TaskStatusChange status_change;
  while (this_->m_task_status_change_queue_.try_dequeue(status_change)) {
    auto iter = this_->m_task_map_.find(status_change.task_id);
    if (iter == this_->m_task_map_.end()) {
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
    this_->m_task_map_.erase(status_change.task_id);

    if (!orphaned)
      g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }

  // Todo: Add additional timer to check periodically whether all children
  //  have exited.
  if (this_->m_is_ending_now_ && this_->m_task_map_.empty()) {
    CRANE_TRACE(
        "Craned is ending and all tasks have been reaped. "
        "Stop event loop.");
    this_->EvActivateShutdown_();
  }
}

void TaskManager::EvActivateTaskStatusChange_(
    uint32_t task_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason) {
  TaskStatusChange status_change{task_id, new_status, exit_code};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  event_active(m_ev_task_status_change_, 0, 0);
}

std::optional<std::vector<std::pair<std::string, std::string>>>
TaskManager::QueryTaskEnvironmentVariablesAsync(task_id_t task_id) {
  EvQueueQueryTaskEnvironmentVariables elem{.task_id = task_id};
  std::future<std::optional<std::vector<std::pair<std::string, std::string>>>>
      env_future = elem.env_prom.get_future();
  m_query_task_environment_variables_queue.enqueue(std::move(elem));
  event_active(m_ev_query_task_environment_variables_, 0, 0);
  return env_future.get();
}

void TaskManager::EvGrpcQueryTaskEnvironmentVariableCb_(int efd, short events,
                                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueQueryTaskEnvironmentVariables elem;
  while (this_->m_query_task_environment_variables_queue.try_dequeue(elem)) {
    auto task_iter = this_->m_task_map_.find(elem.task_id);
    if (task_iter == this_->m_task_map_.end())
      elem.env_prom.set_value(std::nullopt);
    else {
      auto& instance = task_iter->second;

      std::vector<std::pair<std::string, std::string>> env_opt;
      for (const auto& [name, value] : instance->GetEnvList()) {
        env_opt.emplace_back(name, value);
      }
      elem.env_prom.set_value(env_opt);
    }
  }
}

std::optional<uint32_t> TaskManager::QueryTaskIdFromPidAsync(pid_t pid) {
  EvQueueQueryTaskIdFromPid elem{.pid = pid};
  std::future<std::optional<uint32_t>> task_id_opt_future =
      elem.task_id_prom.get_future();
  m_query_task_id_from_pid_queue_.enqueue(std::move(elem));
  event_active(m_ev_query_task_id_from_pid_, 0, 0);

  return task_id_opt_future.get();
}

void TaskManager::EvGrpcQueryTaskIdFromPidCb_(int efd, short events,
                                              void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueQueryTaskIdFromPid elem;
  while (this_->m_query_task_id_from_pid_queue_.try_dequeue(elem)) {
    this_->m_mtx_.Lock();

    auto task_iter = this_->m_pid_task_map_.find(elem.pid);
    if (task_iter == this_->m_pid_task_map_.end())
      elem.task_id_prom.set_value(std::nullopt);
    else {
      TaskInstance* instance = task_iter->second;
      uint32_t task_id = instance->task.task_id();
      elem.task_id_prom.set_value(task_id);
    }

    this_->m_mtx_.Unlock();
  }
}

void TaskManager::EvOnTimerCb_(int, short, void* arg_) {
  auto* arg = reinterpret_cast<EvTimerCbArg*>(arg_);
  TaskManager* this_ = arg->task_manager;
  task_id_t task_id = arg->task_id;

  CRANE_TRACE("Task #{} exceeded its time limit. Terminating it...", task_id);

  // Sometimes, task finishes just before time limit.
  // After the execution of SIGCHLD callback where the task has been erased,
  // the timer is triggered immediately.
  // That's why we need to check the existence of the task again in timer
  // callback, otherwise a segmentation fault will occur.
  auto task_it = this_->m_task_map_.find(task_id);
  if (task_it == this_->m_task_map_.end()) {
    CRANE_TRACE("Task #{} has already been removed.");
    return;
  }

  TaskInstance* task_instance = task_it->second.get();
  this_->EvDelTerminationTimer_(task_instance);

  if (task_instance->task.type() == crane::grpc::Batch) {
    EvQueueTaskTerminate ev_task_terminate{
        .task_id = task_id,
        .terminated_by_timeout = true,
    };
    this_->m_task_terminate_queue_.enqueue(ev_task_terminate);
    event_active(this_->m_ev_task_terminate_, 0, 0);
  } else {
    this_->EvActivateTaskStatusChange_(
        task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
        ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
  }
}

void TaskManager::EvTerminateTaskCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueTaskTerminate elem;
  while (this_->m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.task_id);

    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter == this_->m_task_map_.end()) {
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
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Completed,
                                         ExitCode::kExitCodeTerminated,
                                         std::nullopt);
    }
  }
}

void TaskManager::TerminateTaskAsync(uint32_t task_id) {
  EvQueueTaskTerminate elem{.task_id = task_id, .terminated_by_user = true};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

void TaskManager::MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id) {
  EvQueueTaskTerminate elem{.task_id = task_id, .mark_as_orphaned = true};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

bool TaskManager::CheckTaskStatusAsync(task_id_t task_id,
                                       crane::grpc::TaskStatus* status) {
  EvQueueCheckTaskStatus elem{.task_id = task_id};

  std::future<std::pair<bool, crane::grpc::TaskStatus>> res{
      elem.status_prom.get_future()};

  m_check_task_status_queue_.enqueue(std::move(elem));
  event_active(m_ev_check_task_status_, 0, 0);

  auto [ok, task_status] = res.get();
  if (!ok) return false;

  *status = task_status;
  return true;
}

void TaskManager::EvCheckTaskStatusCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueCheckTaskStatus elem;
  while (this_->m_check_task_status_queue_.try_dequeue(elem)) {
    task_id_t task_id = elem.task_id;
    if (this_->m_task_map_.contains(task_id)) {
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
  EvQueueChangeTaskTimeLimit elem{.task_id = task_id, .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  event_active(m_ev_task_time_limit_change_, 0, 0);
  return ok_fut.get();
}

void TaskManager::EvChangeTaskTimeLimitCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);
  absl::Time now = absl::Now();

  EvQueueChangeTaskTimeLimit elem;
  while (this_->m_task_time_limit_change_queue_.try_dequeue(elem)) {
    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter != this_->m_task_map_.end()) {
      TaskInstance* task_instance = iter->second.get();
      this_->EvDelTerminationTimer_(task_instance);

      absl::Time start_time =
          absl::FromUnixSeconds(task_instance->task.start_time().seconds());
      absl::Duration const& new_time_limit = elem.time_limit;

      if (now - start_time >= new_time_limit) {
        // If the task times out, terminate it.
        EvQueueTaskTerminate ev_task_terminate{.task_id = elem.task_id,
                                               .terminated_by_timeout = true};
        this_->m_task_terminate_queue_.enqueue(ev_task_terminate);
        event_active(this_->m_ev_task_terminate_, 0, 0);

      } else {
        // If the task haven't timed out, set up a new timer.
        this_->EvAddTerminationTimer_(
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
