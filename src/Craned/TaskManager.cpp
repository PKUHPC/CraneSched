#include "TaskManager.h"

#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>
#include <bit>


#include "ResourceAllocators.h"
#include "crane/FdFunctions.h"
#include "crane/String.h"
#include "protos/CraneSubprocess.pb.h"

namespace Craned {

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
  {  // gRPC: SpawnInteractiveTask
    m_ev_grpc_interactive_task_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcSpawnInteractiveTaskCb_, this);
    if (!m_ev_grpc_interactive_task_) {
      CRANE_ERROR("Failed to create the grpc event!");
      std::terminate();
    }

    if (event_add(m_ev_grpc_interactive_task_, nullptr) < 0) {
      CRANE_ERROR("Could not add the grpc event to base!");
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

  if (m_ev_grpc_interactive_task_) event_free(m_ev_grpc_interactive_task_);
  if (m_ev_query_task_id_from_pid_) event_free(m_ev_query_task_id_from_pid_);
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

std::string TaskManager::CgroupStrByTaskId_(uint32_t task_id) {
  return fmt::format("Crane_Task_{}", task_id);
}

void TaskManager::EvSigchldCb_(evutil_socket_t sig, short events,
                               void* user_data) {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  SigchldInfo sigchld_info{};

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

        proc->Finish(sigchld_info.is_terminated_by_signal, sigchld_info.value);

        // Free the ProcessInstance. ITask struct is not freed here because
        // the ITask for an Interactive task can have no ProcessInstance.
        auto pr_it = instance->processes.find(pid);
        if (pr_it == instance->processes.end()) {
          CRANE_ERROR("Failed to find pid {} in task #{}'s ProcessInstances",
                      task_id, pid);
        } else {
          instance->processes.erase(pr_it);

          if (!instance->processes.empty()) {
            if (sigchld_info.is_terminated_by_signal) {
              // If a task is terminated by a signal and there are other
              //  running processes belonging to this task, kill them.
              this_->TerminateTaskAsync(task_id);
            }
          } else {
            if (!instance->orphaned) {
              // If the ProcessInstance has no process left and the task was not
              // marked as an orphaned task, send TaskStatusChange for this
              // task. See the comment of EvActivateTaskStatusChange_.
              if (instance->task.type() == crane::grpc::Batch) {
                // For a Batch task, the end of the process means it is done.
                if (sigchld_info.is_terminated_by_signal) {
                  if (instance->cancelled_by_user)
                    this_->EvActivateTaskStatusChange_(
                        task_id, crane::grpc::TaskStatus::Cancelled,
                        sigchld_info.value + ExitCode::kTerminationSignalBase,
                        std::nullopt);
                  else if (instance->terminated_by_timeout)
                    this_->EvActivateTaskStatusChange_(
                        task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
                        sigchld_info.value + ExitCode::kTerminationSignalBase,
                        std::nullopt);
                  else
                    this_->EvActivateTaskStatusChange_(
                        task_id, crane::grpc::TaskStatus::Failed,
                        sigchld_info.value + ExitCode::kTerminationSignalBase,
                        std::nullopt);
                } else
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Completed,
                      sigchld_info.value, std::nullopt);
              } else {
                // For a COMPLETING Interactive task with a process running, the
                // end of this process means that this task is done.
                if (sigchld_info.is_terminated_by_signal) {
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Completed,
                      sigchld_info.value + ExitCode::kTerminationSignalBase,
                      std::nullopt);
                } else {
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Completed,
                      sigchld_info.value, std::nullopt);
                }
              }
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
          this_->ShutdownAsync();
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

    if (this_->m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      this_->ShutdownAsync();
    } else {
      // Send SIGINT to all tasks and the event loop will stop
      // when the ev_sigchld_cb_ of the last task is called.
      for (auto&& [task_id, task_instance] : this_->m_task_map_) {
        for (auto&& [pid, pr_instance] : task_instance->processes) {
          CRANE_INFO(
              "Sending SIGTERM to the process group of task #{} with root "
              "process pid {}",
              task_id, pr_instance->GetPid());
          KillProcessInstance_(pr_instance.get(), SIGTERM);
        }
      }
    }
  } else {
    if (this_->m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      this_->ShutdownAsync();
    } else {
      CRANE_INFO(
          "SIGINT has been triggered already. Sending SIGKILL to all process "
          "groups instead.");
      for (auto&& [task_id, task_instance] : this_->m_task_map_) {
        for (auto&& [pid, pr_instance] : task_instance->processes) {
          CRANE_INFO(
              "Sending SIGINT to the process group of task #{} with root "
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

void TaskManager::ShutdownAsync() {
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

CraneErr TaskManager::SpawnProcessInInstance_(TaskInstance* instance,
                                              ProcessInstance* process) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::subprocess::CanStartMessage;
  using crane::grpc::subprocess::ChildProcessReady;

  int socket_pair[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, socket_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  // save the current uid/gid
  savedPrivilege saved_priv{getuid(), getgid()};

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
  if (child_pid > 0) {  // Parent proc
    close(socket_pair[1]);
    int fd = socket_pair[0];
    bool ok;
    CraneErr err;

    setegid(saved_priv.gid);
    seteuid(saved_priv.uid);
    setgroups(0, nullptr);

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                instance->task.task_id(), child_pid);

    process->SetPid(child_pid);

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
    if (!instance->cgroup->MigrateProcIn(process->GetPid())) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          instance->task.task_id());

      err = CraneErr::kCgroupError;
      goto AskChildToSuicide;
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                instance->task.task_id());

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("Failed to send ok=true to subprocess {} for task #{}",
                  child_pid, instance->task.task_id());
      close(fd);
      return CraneErr::kProtobufError;
    }

    ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream, nullptr);
    if (!msg.ok()) {
      CRANE_ERROR("Failed to read protobuf from subprocess {} of task #{}",
                  child_pid, instance->task.task_id());
      close(fd);
      return CraneErr::kProtobufError;
    }

    close(fd);
    return CraneErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    close(fd);
    if (!ok) {
      CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                  child_pid, instance->task.task_id());
      return CraneErr::kProtobufError;
    }
    return err;
  } else {  // Child proc
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

    close(socket_pair[0]);
    int fd = socket_pair[1];

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;
    bool ok;

    ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
    if (!msg.ok()) std::abort();

    const std::string& out_file_path =
        process->batch_meta.parsed_output_file_pattern;
    int out_fd = open(out_file_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (out_fd == -1) {
      CRANE_ERROR("[Child Process] Error: open {}. {}", out_file_path,
                  strerror(errno));
      std::abort();
    }

    dup2(out_fd, 1);  // stdout -> output file
    dup2(out_fd, 2);  // stderr -> output file
    close(out_fd);

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Child Process] Error: Failed to flush.");
      std::abort();
    }

    close(fd);

    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    close(0);  // close stdin
    util::CloseFdFrom(3);

    // std::vector<std::string> env_vec =
    //     absl::StrSplit(instance->task.env(), "||");
    std::vector<std::pair<std::string, std::string>> env_vec;

    // Since we want to reinitialize the environment variables of the user,
    // we are actually performing two steps: login -> start shell.
    // Shell starting is done by calling "bash --login".
    // During shell starting step, /etc/profile, ~/.bash_profile, ... are
    // loaded.
    // During login step, "HOME" and "SHELL" are set. Here we are just
    // performing the role of login module.
    env_vec.emplace_back("HOME", instance->pwd_entry.HomeDir());
    env_vec.emplace_back("SHELL", instance->pwd_entry.Shell());

    env_vec.emplace_back("CRANE_JOB_NODELIST",
                         absl::StrJoin(instance->task.allocated_nodes(), ";"));
    env_vec.emplace_back("CRANE_EXCLUDES",
                         absl::StrJoin(instance->task.excludes(), ";"));
    env_vec.emplace_back("CRANE_JOB_NAME", instance->task.name());
    env_vec.emplace_back("CRANE_ACCOUNT", instance->task.account());
    env_vec.emplace_back("CRANE_PARTITION", instance->task.partition());
    env_vec.emplace_back("CRANE_QOS", instance->task.qos());
    env_vec.emplace_back("CRANE_MEM_PER_NODE",
                         std::to_string(instance->task.resources()
                                            .allocatable_resource()
                                            .memory_limit_bytes() /
                                        (1024 * 1024)));
    env_vec.emplace_back("CRANE_JOB_ID",
                         std::to_string(instance->task.task_id()));;
    for (const auto& resource:instance->task.resources().dedicated_resource().devices())
    {
      if(util::CgroupUtil::getDeviceType(resource.first)==DedicatedResource::DeviceType::NVIDIA_GRAPHICS_CARD){
        env_vec.emplace_back("CUDA_VISIABLE_DEVICE",
                             util::GenerateCudaVisiableDeviceStr(std::popcount(resource.second)));
      }
    }
    int64_t time_limit_sec = instance->task.time_limit().seconds();
    int hours = time_limit_sec / 3600;
    int minutes = (time_limit_sec % 3600) / 60;
    int seconds = time_limit_sec % 60;
    std::string time_limit =
        fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
    env_vec.emplace_back("CRANE_TIMELIMIT", time_limit);

    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    for (const auto& [name, value] : env_vec) {
      if (setenv(name.c_str(), value.c_str(), 1)) {
        fmt::print("setenv for {}={} failed!\n", name, value);
      }
    }

    // Prepare the command line arguments.
    std::vector<const char*> argv;
    argv.emplace_back("--login");
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
  CRANE_INFO("Executing task #{}", task.task_id());

  auto instance = std::make_unique<TaskInstance>();

  // Simply wrap the Task structure within a TaskInstance structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  instance->task = task;

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

    auto [iter, _] = this_->m_task_map_.emplace(instance->task.task_id(),
                                                std::move(popped_instance));

    g_thread_pool->push_task([this_, instance]() {
      this_->m_mtx_.Lock();

      auto cg_iter = this_->m_task_id_to_cg_map_.find(instance->task.task_id());
      if (cg_iter == this_->m_task_id_to_cg_map_.end()) {
        this_->m_mtx_.Unlock();
        CRANE_ERROR("Failed to find created cgroup for task #{}",
                    instance->task.task_id());
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            ExitCode::kExitCodeCgroupError,
            fmt::format("Failed to find created cgroup for task #{}",
                        instance->task.task_id()));
        return;
      }

      // Note: cg_iter is in an absl::node_hash_map. Pointer stability of
      // iterator is guaranteed. We can modify the value even if mutex is
      // unlocked.
      this_->m_mtx_.Unlock();

      // Lazy creation of cgroup
      // Todo: Lock on iterator may be required here!
      if (!cg_iter->second) {
        instance->cgroup_path = CgroupStrByTaskId_(instance->task.task_id());
        cg_iter->second = util::CgroupUtil::CreateOrOpen(
            instance->cgroup_path,
            util::NO_CONTROLLER_FLAG |
                util::CgroupConstant::Controller::CPU_CONTROLLER |
                util::CgroupConstant::Controller::MEMORY_CONTROLLER |
                util::CgroupConstant::Controller::DEVICES_CONTROLLER,
            util::NO_CONTROLLER_FLAG, false);
      }
      instance->cgroup = cg_iter->second.get();

      instance->pwd_entry.Init(instance->task.uid());
      if (!instance->pwd_entry.Valid()) {
        CRANE_DEBUG("Failed to look up password entry for uid {} of task #{}",
                    instance->task.uid(), instance->task.task_id());
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            ExitCode::kExitCodePermissionDenied,
            fmt::format(
                "Failed to look up password entry for uid {} of task #{}",
                instance->task.uid(), instance->task.task_id()));
        return;
      }

      bool ok = AllocatableResourceAllocator::Allocate(
          instance->task.resources().allocatable_resource(), instance->cgroup);

      if (!ok) {
        CRANE_ERROR(
            "Failed to allocate allocatable resource in cgroup for task #{}",
            instance->task.task_id());
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            ExitCode::kExitCodeCgroupError,
            fmt::format(
                "Cannot allocate resources for the instance of task #{}",
                instance->task.task_id()));
        return;
      }
      
      if(!DedicatedResourceAllocator::Allocate(instance->task.resources().dedicated_resource(), instance->cgroup)){
          CRANE_ERROR(
          "Failed to allocate dedicated resource in cgroup for task #{}",
          instance->task.task_id());
      this_->EvActivateTaskStatusChange_(
          instance->task.task_id(), crane::grpc::TaskStatus::Failed,
          ExitCode::kExitCodeCgroupError,
          fmt::format(
              "Cannot dedicated resources for the instance of task #{}",
              instance->task.task_id()));
      return;
      }  

      // If this is a batch task, run it now.
      if (instance->task.type() == crane::grpc::Batch) {
        instance->batch_meta.parsed_sh_script_path =
            fmt::format("{}/Crane-{}.sh", kDefaultCranedScriptDir,
                        instance->task.task_id());
        auto& sh_path = instance->batch_meta.parsed_sh_script_path;

        FILE* fptr = fopen(sh_path.c_str(), "w");
        if (fptr == nullptr) {
          CRANE_ERROR("Cannot write shell script for batch task #{}",
                      instance->task.task_id());
          this_->EvActivateTaskStatusChange_(
              instance->task.task_id(), crane::grpc::TaskStatus::Failed,
              ExitCode::kExitCodeFileNotFound,
              fmt::format("Cannot write shell script for batch task #{}",
                          instance->task.task_id()));
          return;
        }
        fputs(instance->task.batch_meta().sh_script().c_str(), fptr);
        fclose(fptr);

        chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

        CraneErr err = CraneErr::kOk;
        auto process = std::make_unique<ProcessInstance>(
            sh_path, std::list<std::string>());

        /* Perform file name substitutions
         * %j - Job ID
         * %u - User name
         * %x - Job name
         */
        if (instance->task.batch_meta().output_file_pattern().empty()) {
          // If output file path is not specified, first set it to cwd.
          process->batch_meta.parsed_output_file_pattern =
              fmt::format("{}/", instance->task.cwd());
        } else {
          if (instance->task.batch_meta().output_file_pattern()[0] == '/') {
            // If output file path is an absolute path, do nothing.
            process->batch_meta.parsed_output_file_pattern =
                instance->task.batch_meta().output_file_pattern();
          } else {
            // If output file path is a relative path, prepend cwd to the path.
            process->batch_meta.parsed_output_file_pattern =
                fmt::format("{}/{}", instance->task.cwd(),
                            instance->task.batch_meta().output_file_pattern());
          }
        }

        // Path ends with a directory, append default output file name
        // `Crane-<Job ID>.out` to the path.
        if (absl::EndsWith(process->batch_meta.parsed_output_file_pattern,
                           "/")) {
          process->batch_meta.parsed_output_file_pattern +=
              fmt::format("Crane-{}.out", g_ctld_client->GetCranedId());
        }

        // Replace the format strings.
        absl::StrReplaceAll({{"%j", std::to_string(instance->task.task_id())},
                             {"%u", instance->pwd_entry.Username()},
                             {"%x", instance->task.name()}},
                            &process->batch_meta.parsed_output_file_pattern);

        // auto output_cb = [](std::string&& buf, void* data) {
        //   CRANE_TRACE("Read output from subprocess: {}", buf);
        // };
        //
        // process->SetOutputCb(std::move(output_cb));

        err = SpawnProcessInInstance_(instance, process.get());

        if (err == CraneErr::kOk) {
          this_->m_mtx_.Lock();

          // Child process may finish or abort before we put its pid into maps.
          // However, it doesn't matter because SIGCHLD will be handled after
          // this function or event ends.
          // Add indexes from pid to TaskInstance*, ProcessInstance*
          this_->m_pid_task_map_.emplace(process->GetPid(), instance);
          this_->m_pid_proc_map_.emplace(process->GetPid(), process.get());

          this_->m_mtx_.Unlock();

          // Move the ownership of ProcessInstance into the TaskInstance.
          instance->processes.emplace(process->GetPid(), std::move(process));
        } else {
          this_->EvActivateTaskStatusChange_(
              instance->task.task_id(), crane::grpc::TaskStatus::Failed,
              ExitCode::kExitCodeSpawnProcessFail,
              fmt::format(
                  "Cannot spawn a new process inside the instance of task #{}",
                  instance->task.task_id()));
        }
      }

      // Add a timer to limit the execution time of a task.
      this_->EvAddTerminationTimer_(instance,
                                    instance->task.time_limit().seconds());
    });
  }
}

void TaskManager::EvTaskStatusChangeCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  TaskStatusChange status_change;
  while (this_->m_task_status_change_queue_.try_dequeue(status_change)) {
    auto iter = this_->m_task_map_.find(status_change.task_id);
    CRANE_ASSERT_MSG(iter != this_->m_task_map_.end(),
                     "Task should be found here.");

    TaskInstance* task_instance = iter->second.get();

    if (task_instance->termination_timer) {
      event_del(task_instance->termination_timer);
      event_free(task_instance->termination_timer);
    }

    // Free the TaskInstance structure
    this_->m_task_map_.erase(status_change.task_id);
    g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }

  // Todo: Add additional timer to check periodically whether all children
  //  have exited.
  if (this_->m_is_ending_now_ && this_->m_task_map_.empty()) {
    CRANE_TRACE(
        "Craned is ending and all tasks have been reaped. "
        "Stop event loop.");
    this_->ShutdownAsync();
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

CraneErr TaskManager::SpawnInteractiveTaskAsync(
    uint32_t task_id, std::string executive_path,
    std::list<std::string> arguments,
    std::function<void(std::string&&, void*)> output_cb,
    std::function<void(bool, int, void*)> finish_cb) {
  EvQueueGrpcInteractiveTask elem{
      .task_id = task_id,
      .executive_path = std::move(executive_path),
      .arguments = std::move(arguments),
      .output_cb = std::move(output_cb),
      .finish_cb = std::move(finish_cb),
  };
  std::future<CraneErr> err_future = elem.err_promise.get_future();

  m_grpc_interactive_task_queue_.enqueue(std::move(elem));
  event_active(m_ev_grpc_interactive_task_, 0, 0);

  return err_future.get();
}

std::optional<uint32_t> TaskManager::QueryTaskIdFromPidAsync(pid_t pid) {
  EvQueueQueryTaskIdFromPid elem{.pid = pid};
  std::future<std::optional<uint32_t>> task_id_opt_future =
      elem.task_id_prom.get_future();
  m_query_task_id_from_pid_queue_.enqueue(std::move(elem));
  event_active(m_ev_query_task_id_from_pid_, 0, 0);

  return task_id_opt_future.get();
}

void TaskManager::EvGrpcSpawnInteractiveTaskCb_(int efd, short events,
                                                void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueGrpcInteractiveTask elem;
  while (this_->m_grpc_interactive_task_queue_.try_dequeue(elem)) {
    CRANE_TRACE("Receive one GrpcSpawnInteractiveTask for task #{}",
                elem.task_id);

    auto task_iter = this_->m_task_map_.find(elem.task_id);
    if (task_iter == this_->m_task_map_.end()) {
      CRANE_ERROR("Cannot find task #{}", elem.task_id);
      elem.err_promise.set_value(CraneErr::kNonExistent);
      return;
    }

    if (task_iter->second->task.type() != crane::grpc::Interactive) {
      CRANE_ERROR("Try spawning a new process in non-interactive task #{}!",
                  elem.task_id);
      elem.err_promise.set_value(CraneErr::kInvalidParam);
      return;
    }

    auto process = std::make_unique<ProcessInstance>(
        std::move(elem.executive_path), std::move(elem.arguments));

    process->SetOutputCb(std::move(elem.output_cb));
    process->SetFinishCb(std::move(elem.finish_cb));

    CraneErr err;
    err =
        this_->SpawnProcessInInstance_(task_iter->second.get(), process.get());
    elem.err_promise.set_value(err);

    if (err != CraneErr::kOk)
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Failed,
                                         ExitCode::kExitCodeSpawnProcessFail,
                                         std::string(CraneErrStr(err)));
  }
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
  task_id_t task_id = arg->task_instance->task.task_id();

  CRANE_TRACE("Task #{} exceeded its time limit. Terminating it...",
              arg->task_instance->task.task_id());

  this_->EvDelTerminationTimer_(arg->task_instance);

  if (arg->task_instance->task.type() == crane::grpc::Batch) {
    EvQueueTaskTerminate ev_task_terminate{
        .task_id = arg->task_instance->task.task_id(),
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

  EvQueueTaskTerminate elem;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  while (this_->m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.task_id);

    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter == this_->m_task_map_.end()) {
      CRANE_DEBUG("Trying terminating unknown task #{}", elem.task_id);
      return;
    }

    const auto& task_instance = iter->second;

    if (elem.terminated_by_user) task_instance->cancelled_by_user = true;
    if (elem.mark_as_orphaned) task_instance->orphaned = true;
    if (elem.terminated_by_timeout) task_instance->terminated_by_timeout = true;

    int sig = SIGTERM;  // For BatchTask
    if (task_instance->task.type() == crane::grpc::Interactive) sig = SIGHUP;

    if (!task_instance->processes.empty()) {
      // For an Interactive task with a process running or a Batch task, we just
      // send a kill signal here.
      for (auto&& [pid, pr_instance] : task_instance->processes)
        KillProcessInstance_(pr_instance.get(), sig);
    } else if (task_instance->task.type() == crane::grpc::Interactive) {
      // For an Interactive task with no process running, it ends immediately.
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Completed,
                                         ExitCode::kExitCodeTerminal,
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

bool TaskManager::CreateCgroupsAsync(
    std::vector<std::pair<task_id_t, uid_t>>&& task_id_uid_pairs) {
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  CRANE_DEBUG("Creating cgroups for {} tasks", task_id_uid_pairs.size());

  begin = std::chrono::steady_clock::now();

  this->m_mtx_.Lock();
  for (int i = 0; i < task_id_uid_pairs.size(); i++) {
    auto [task_id, uid] = task_id_uid_pairs[i];

    CRANE_TRACE("Create lazily allocated cgroups for task #{}, uid {}", task_id,
                uid);

    this->m_task_id_to_cg_map_.emplace(task_id, nullptr);
    this->m_uid_to_task_ids_map_[uid].emplace(task_id);
  }
  this->m_mtx_.Unlock();

  end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());

  return true;
}

bool TaskManager::ReleaseCgroupAsync(uint32_t task_id, uid_t uid) {
  this->m_mtx_.Lock();

  this->m_uid_to_task_ids_map_[uid].erase(task_id);
  if (this->m_uid_to_task_ids_map_[uid].empty()) {
    this->m_uid_to_task_ids_map_.erase(uid);
  }

  auto iter = this->m_task_id_to_cg_map_.find(task_id);
  if (iter == this->m_task_id_to_cg_map_.end()) {
    this->m_mtx_.Unlock();
    CRANE_DEBUG(
        "Trying to release a non-existent cgroup for task #{}. Ignoring it...",
        task_id);

    return false;
  } else {
    // The termination of all processes in a cgroup is a time-consuming work.
    // Therefore, once we are sure that the cgroup for this task exists, we
    // let gRPC call return and put the termination work into the thread pool
    // to avoid blocking the event loop of TaskManager.
    // Kind of async behavior.
    std::shared_ptr<util::Cgroup> cgroup = iter->second;
    this->m_task_id_to_cg_map_.erase(iter);
    this->m_mtx_.Unlock();

    if (cgroup) {
      g_thread_pool->push_task([cgroup] {
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
      });
    }
    return true;
  }
}

bool TaskManager::QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid* info) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  info->job_cnt = 0;
  info->cgroup_exists = false;

  this->m_mtx_.Lock();
  auto iter = this->m_uid_to_task_ids_map_.find(uid);
  if (iter != this->m_uid_to_task_ids_map_.end()) {
    info->job_cnt = iter->second.size();
    info->first_task_id = *iter->second.begin();

    auto cg_iter = this->m_task_id_to_cg_map_.find(info->first_task_id);
    if (cg_iter != this->m_task_id_to_cg_map_.end()) {
      this->m_mtx_.Unlock();
      if (cg_iter->second == nullptr) {
        // Lazy creation of cgroup
        // Todo: Lock on iterator may be required here!
        cg_iter->second = util::CgroupUtil::CreateOrOpen(
            CgroupStrByTaskId_(info->first_task_id),
            util::NO_CONTROLLER_FLAG |
                util::CgroupConstant::Controller::CPU_CONTROLLER |
                util::CgroupConstant::Controller::MEMORY_CONTROLLER,
            util::NO_CONTROLLER_FLAG, false);
      }

      info->cgroup_path = cg_iter->second->GetCgroupString();
      info->cgroup_exists = true;
    } else {
      CRANE_ERROR("Cannot find Cgroup* for uid {}'s task #{}!", uid,
                  info->first_task_id);
    }
  } else {
    this->m_mtx_.Unlock();
  }

  return info->job_cnt > 0 && info->cgroup_exists;
}

bool TaskManager::QueryCgOfTaskIdAsync(uint32_t task_id, util::Cgroup** cg) {
  CRANE_DEBUG("Query Cgroup* task #{}", task_id);

  this->m_mtx_.Lock();

  auto iter = this->m_task_id_to_cg_map_.find(task_id);
  if (iter != this->m_task_id_to_cg_map_.end()) {
    if (iter->second == nullptr) {
      // Lazy creation of cgroup
      // Todo: Lock on iterator may be required here!
      iter->second = util::CgroupUtil::CreateOrOpen(
          CgroupStrByTaskId_(task_id),
          util::NO_CONTROLLER_FLAG |
              util::CgroupConstant::Controller::CPU_CONTROLLER |
              util::CgroupConstant::Controller::MEMORY_CONTROLLER,
          util::NO_CONTROLLER_FLAG, false);
    }

    *cg = iter->second.get();
  } else {
    *cg = nullptr;
  }

  this->m_mtx_.Unlock();

  return *cg != nullptr;
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

  EvQueueChangeTaskTimeLimit elem;
  while (this_->m_task_time_limit_change_queue_.try_dequeue(elem)) {
    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter != this_->m_task_map_.end()) {
      TaskInstance* task_instance = iter->second.get();
      this_->EvDelTerminationTimer_(task_instance);

      absl::Time start_time =
          absl::FromUnixSeconds(task_instance->task.start_time().seconds());
      absl::Duration const& new_time_limit = elem.time_limit;

      if (absl::Now() - start_time >= new_time_limit) {
        // If the task times out, terminate it.
        EvQueueTaskTerminate ev_task_terminate{elem.task_id};
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

bool TaskManager::MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id) {
  this->m_mtx_.Lock();

  auto iter = this->m_task_id_to_cg_map_.find(task_id);
  if (iter == this->m_task_id_to_cg_map_.end()) {
    this->m_mtx_.Unlock();
    return false;
  }

  this->m_mtx_.Unlock();

  return iter->second->MigrateProcIn(pid);
}

}  // namespace Craned
