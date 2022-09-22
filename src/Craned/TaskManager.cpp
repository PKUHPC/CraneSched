#include "TaskManager.h"

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>

#include <utility>

#include "ResourceAllocators.h"
#include "crane/FdFunctions.h"
#include "protos/CraneSubprocess.pb.h"

namespace Craned {

TaskManager::TaskManager()
    : m_cg_mgr_(util::CgroupManager::Instance()),
      m_ev_sigchld_(nullptr),
      m_ev_base_(nullptr),
      m_ev_grpc_interactive_task_(nullptr),
      m_ev_grpc_create_cg_(nullptr),
      m_ev_grpc_release_cg_(nullptr),
      m_ev_query_task_id_from_pid_(nullptr),
      m_ev_exit_event_(nullptr),
      m_ev_task_status_change_(nullptr),
      m_is_ending_now_(false) {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_ev_base_ = event_base_new();
  if (!m_ev_base_) {
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
  {  // gRPC: CreateCgroup
    m_ev_grpc_create_cg_ = event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                                     EvGrpcCreateCgroupCb_, this);
    if (!m_ev_grpc_create_cg_) {
      CRANE_ERROR("Failed to create the create cgroup event!");
      std::terminate();
    }

    if (event_add(m_ev_grpc_create_cg_, nullptr) < 0) {
      CRANE_ERROR("Could not add the create cgroup event to base!");
      std::terminate();
    }
  }
  {  // gRPC: ReleaseCgroup
    m_ev_grpc_release_cg_ = event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                                      EvGrpcReleaseCgroupCb_, this);
    if (!m_ev_grpc_release_cg_) {
      CRANE_ERROR("Failed to create the release cgroup event!");
      std::terminate();
    }

    if (event_add(m_ev_grpc_release_cg_, nullptr) < 0) {
      CRANE_ERROR("Could not add the release cgroup event to base!");
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
  {  // gRPC: QueryTaskInfoOfUid
    m_ev_query_task_info_of_uid_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcQueryTaskInfoOfUidCb_, this);
    if (!m_ev_query_task_info_of_uid_) {
      CRANE_ERROR("Failed to create the query info of uid event!");
      std::terminate();
    }

    if (event_add(m_ev_query_task_info_of_uid_, nullptr) < 0) {
      CRANE_ERROR("Could not add the query info of uid event to base!");
      std::terminate();
    }
  }
  {  // gRPC: QueryCgOfTaskId
    m_ev_query_cg_of_task_id_ = event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                                          EvGrpcQueryCgOfTaskIdCb_, this);
    if (!m_ev_query_cg_of_task_id_) {
      CRANE_ERROR("Failed to create the query cg of task id event!");
      std::terminate();
    }

    if (event_add(m_ev_query_cg_of_task_id_, nullptr) < 0) {
      CRANE_ERROR("Could not add the query cg of task id event to base!");
      std::terminate();
    }
  }
  {  // Exit Event
    if ((m_ev_exit_fd_ = eventfd(0, EFD_CLOEXEC)) < 0) {
      CRANE_ERROR("Failed to init the eventfd!");
      std::terminate();
    }

    m_ev_exit_event_ = event_new(m_ev_base_, m_ev_exit_fd_,
                                 EV_PERSIST | EV_READ, EvExitEventCb_, this);
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

  m_ev_loop_thread_ =
      std::thread([this]() { event_base_dispatch(m_ev_base_); });
}

TaskManager::~TaskManager() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();

  if (m_ev_sigchld_) event_free(m_ev_sigchld_);
  if (m_ev_sigint_) event_free(m_ev_sigint_);

  if (m_ev_grpc_interactive_task_) event_free(m_ev_grpc_interactive_task_);
  if (m_ev_query_task_info_of_uid_) event_free(m_ev_query_task_info_of_uid_);
  if (m_ev_query_cg_of_task_id_) event_free(m_ev_query_cg_of_task_id_);
  if (m_ev_grpc_create_cg_) event_free(m_ev_grpc_create_cg_);
  if (m_ev_grpc_release_cg_) event_free(m_ev_grpc_release_cg_);
  if (m_ev_query_task_id_from_pid_) event_free(m_ev_query_task_id_from_pid_);

  if (m_ev_exit_event_) event_free(m_ev_exit_event_);
  close(m_ev_exit_fd_);

  if (m_ev_grpc_execute_task_) event_free(m_ev_grpc_execute_task_);

  if (m_ev_task_status_change_) event_free(m_ev_task_status_change_);

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

      uint32_t task_id;
      TaskInstance* instance;
      ProcessInstance* proc;

      auto task_iter = this_->m_pid_task_map_.find(pid);
      auto proc_iter = this_->m_pid_proc_map_.find(pid);
      if (task_iter == this_->m_pid_task_map_.end() ||
          proc_iter == this_->m_pid_proc_map_.end())
        CRANE_ERROR("Failed to find task id for pid {}.", pid);
      else {
        instance = task_iter->second;
        proc = proc_iter->second;
        task_id = instance->task.task_id();

        proc->Finish(sigchld_info.is_terminated_by_signal, sigchld_info.value);

        // Free the ProcessInstance. ITask struct is not freed here because
        // the ITask for an Interactive task can have no ProcessInstance.
        auto pr_it = instance->processes.find(pid);
        if (pr_it == instance->processes.end()) {
          CRANE_ERROR("Failed to find pid {} in task #{}'s ProcessInstances",
                      task_id, pid);
        } else {
          instance->processes.erase(pr_it);

          // Remove indexes from pid to ProcessInstance*
          this_->m_pid_proc_map_.erase(proc_iter);

          if (!instance->processes.empty()) {
            if (sigchld_info.is_terminated_by_signal &&
                !instance->already_failed) {
              instance->already_failed = true;
              this_->TerminateTaskAsync(task_id);
            }
          } else {  // ProcessInstance has no process left.
            // See the comment of EvActivateTaskStatusChange_.
            if (instance->task.type() == crane::grpc::Batch) {
              // For a Batch task, the end of the process means it is done.
              if (sigchld_info.is_terminated_by_signal) {
                if (instance->cancelled_by_user)
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Cancelled,
                      std::nullopt);
                else
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Failed, std::nullopt);
              } else
                this_->EvActivateTaskStatusChange_(
                    task_id, crane::grpc::TaskStatus::Finished, std::nullopt);
            } else {
              // For a COMPLETING Interactive task with a process running, the
              // end of this process means that this task is done.
              this_->EvActivateTaskStatusChange_(
                  task_id, crane::grpc::TaskStatus::Finished, std::nullopt);
            }
          }
        }
      }
    } else if (pid == 0)  // There's no child that needs reaping.
      break;
    else if (pid < 0) {
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
    CRANE_INFO("Caught SIGINT. Send SIGTERM to all running tasks...");

    this_->m_is_ending_now_ = true;

    if (this_->m_sigint_cb_) this_->m_sigint_cb_();

    if (this_->m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      this_->Shutdown();
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
      this_->Shutdown();
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

  uint64_t u;
  ssize_t s;
  s = read(efd, &u, sizeof(uint64_t));
  if (s != sizeof(uint64_t)) {
    if (errno != EAGAIN) {
      CRANE_ERROR("Failed to read exit_fd: errno {}, {}", errno,
                  strerror(errno));
    }
    return;
  }

  struct timeval delay = {0, 0};
  event_base_loopexit(this_->m_ev_base_, &delay);
}

void TaskManager::Shutdown() {
  CRANE_TRACE("Triggering exit event...");
  m_is_ending_now_ = true;
  eventfd_t u = 1;
  ssize_t s = eventfd_write(m_ev_exit_fd_, u);
  if (s < 0) {
    CRANE_ERROR("Failed to write to grpc event fd: {}", strerror(errno));
  }
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

CraneErr TaskManager::SpawnProcessInInstance_(
    TaskInstance* instance, std::unique_ptr<ProcessInstance> process) {
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
  savedPrivilege saved_priv;
  saved_priv.uid = getuid();
  saved_priv.gid = getgid();
  saved_priv.cwd = get_current_dir_name();

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
  const std::string& cwd = instance->task.cwd();
  rc = chdir(cwd.c_str());
  if (rc == -1) {
    CRANE_ERROR("error: chdir to {}. {}", cwd.c_str(), strerror(errno));
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
    chdir(saved_priv.cwd.c_str());

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                instance->task.task_id(), child_pid);

    process->SetPid(child_pid);

    // Add event for stdout/stderr of the new subprocess
    struct bufferevent* ev_buf_event;
    ev_buf_event =
        bufferevent_socket_new(m_ev_base_, fd, BEV_OPT_CLOSE_ON_FREE);
    if (!ev_buf_event) {
      CRANE_ERROR(
          "Error constructing bufferevent for the subprocess of task #!",
          instance->task.task_id());
      err = CraneErr::kLibEventError;
      goto AskChildToSuicide;
    }
    bufferevent_setcb(ev_buf_event, EvSubprocessReadCb_, nullptr, nullptr,
                      (void*)process.get());
    bufferevent_enable(ev_buf_event, EV_READ);
    bufferevent_disable(ev_buf_event, EV_WRITE);

    process->SetEvBufEvent(ev_buf_event);

    // Migrate the new subprocess to newly created cgroup
    if (!m_cg_mgr_.MigrateProcTo(process->GetPid(), instance->cg_path)) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          instance->task.task_id());

      m_cg_mgr_.Release(instance->cg_path);
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
      CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                  child_pid, instance->task.task_id());
      return CraneErr::kProtobufError;
    }

    ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream, nullptr);
    if (!msg.ok()) {
      CRANE_ERROR("Failed to read protobuf from subprocess {} of task #{}",
                  child_pid, instance->task.task_id());
      return CraneErr::kProtobufError;
    }

    // Add indexes from pid to TaskInstance*, ProcessInstance*
    m_pid_task_map_.emplace(child_pid, instance);
    m_pid_proc_map_.emplace(child_pid, process.get());

    // Move the ownership of ProcessInstance into the TaskInstance.
    instance->processes.emplace(child_pid, std::move(process));

    return CraneErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                  child_pid, instance->task.task_id());
      return CraneErr::kProtobufError;
    }
    return err;
  } else {  // Child proc
    // CRANE_TRACE("Set reuid to {}, regid to {}", instance->pwd_entry.Uid(),
    //              instance->pwd_entry.Gid());
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

    int out_fd = open(process->batch_meta.parsed_output_file_pattern.c_str(),
                      O_RDWR | O_CREAT | O_APPEND, 0644);
    if (out_fd == -1) {
      std::abort();
    }

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      std::abort();
    }

    close(fd);

    dup2(out_fd, 1);  // stdout -> output file
    dup2(out_fd, 2);  // stderr -> output file
    close(out_fd);

    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    close(0);  // close stdin
    util::CloseFdFrom(3);

    std::vector<std::string> env_vec =
        absl::StrSplit(instance->task.env(), "||");

    std::string nodelist = absl::StrJoin(instance->task.allocated_nodes(), ";");
    std::string nodelist_env = fmt::format("CRANE_JOB_NODELIST={}", nodelist);

    env_vec.emplace_back(nodelist_env);

    for (const auto& str : env_vec) {
      auto pos = str.find_first_of('=');
      if (std::string::npos != pos) {
        std::string name = str.substr(0, pos);
        std::string value = str.substr(pos + 1);
        if (setenv(name.c_str(), value.c_str(), 1)) {
          // fmt::print("set environ failed!\n");
        }
      }
    }

    // Prepare the command line arguments.
    std::vector<const char*> argv;
    argv.push_back(process->GetExecPath().c_str());
    for (auto&& arg : process->GetArgList()) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    execv(process->GetExecPath().c_str(),
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

CraneErr TaskManager::ExecuteTaskAsync(crane::grpc::TaskToD task) {
  auto instance = std::make_unique<TaskInstance>();

  // Simply wrap the Task structure within a TaskInstance structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  instance->task = std::move(task);

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
    auto [iter, ok] = this_->m_task_map_.emplace(
        popped_instance->task.task_id(), std::move(popped_instance));

    TaskInstance* instance = iter->second.get();

    // Add task id to the running task set of the UID.
    // Pam module need it.
    this_->m_uid_to_task_ids_map_[instance->task.uid()].emplace(
        instance->task.task_id());

    instance->pwd_entry.Init(instance->task.uid());
    if (!instance->pwd_entry.Valid()) {
      CRANE_DEBUG("Failed to look up password entry for uid {} of task #{}",
                  instance->task.uid(), instance->task.task_id());
      this_->EvActivateTaskStatusChange_(
          instance->task.task_id(), crane::grpc::TaskStatus::Failed,
          fmt::format("Failed to look up password entry for uid {} of task #{}",
                      instance->task.uid(), instance->task.task_id()));
      return;
    }

    auto cg_iter = this_->m_task_id_to_cg_map_.find(instance->task.task_id());
    if (cg_iter != this_->m_task_id_to_cg_map_.end()) {
      util::Cgroup* cgroup = cg_iter->second;
      instance->cg_path = cgroup->GetCgroupString();
      if (!AllocatableResourceAllocator::Allocate(
              instance->task.resources().allocatable_resource(), cgroup)) {
        CRANE_ERROR(
            "Failed to allocate allocatable resource in cgroup for task #{}",
            instance->task.task_id());
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            fmt::format(
                "Cannot allocate resources for the instance of task #{}",
                instance->task.task_id()));
        return;
      }
    } else {
      CRANE_ERROR("Failed to find created cgroup for task #{}",
                  instance->task.task_id());
      this_->EvActivateTaskStatusChange_(
          instance->task.task_id(), crane::grpc::TaskStatus::Failed,
          fmt::format("Failed to find created cgroup for task #{}",
                      instance->task.task_id()));
      return;
    }

    // If this is a batch task, run it now.
    if (instance->task.type() == crane::grpc::Batch) {
      instance->batch_meta.parsed_sh_script_path = fmt::format(
          "{}/Crane-{}.sh", kDefaultCranedScriptDir, instance->task.task_id());
      auto& sh_path = instance->batch_meta.parsed_sh_script_path;

      FILE* fptr = fopen(sh_path.c_str(), "w");
      if (fptr == nullptr) {
        CRANE_ERROR("Cannot write shell script for batch task #{}",
                    instance->task.task_id());
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            fmt::format("Cannot write shell script for batch task #{}",
                        instance->task.task_id()));
        return;
      }
      fputs(instance->task.batch_meta().sh_script().c_str(), fptr);
      fclose(fptr);

      chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

      CraneErr err = CraneErr::kOk;
      auto process =
          std::make_unique<ProcessInstance>(sh_path, std::list<std::string>());

      /* Perform file name substitutions
       * %A - Job array's master job allocation number.
       * %j - Job ID
       * %u - User name
       * %x - Job name
       */
      if (instance->task.batch_meta().output_file_pattern().empty()) {
        process->batch_meta.parsed_output_file_pattern =
            fmt::format("{}/", instance->task.cwd());
      } else {
        if (instance->task.batch_meta().output_file_pattern()[0] == '/') {
          process->batch_meta.parsed_output_file_pattern =
              instance->task.batch_meta().output_file_pattern();
        } else {
          process->batch_meta.parsed_output_file_pattern =
              fmt::format("{}/{}", instance->task.cwd(),
                          instance->task.batch_meta().output_file_pattern());
        }
      }

      if (boost::ends_with(process->batch_meta.parsed_output_file_pattern,
                           "/")) {
        process->batch_meta.parsed_output_file_pattern += fmt::format(
            "Crane-{}.out", g_ctld_client->GetNodeId().craned_index);
      }

      boost::replace_all(process->batch_meta.parsed_output_file_pattern, "%j",
                         std::to_string(instance->task.task_id()));
      boost::replace_all(process->batch_meta.parsed_output_file_pattern, "%u",
                         instance->pwd_entry.Username());

      if (!boost::ends_with(process->batch_meta.parsed_output_file_pattern,
                            ".out")) {
        process->batch_meta.parsed_output_file_pattern += ".out";
      }

      auto output_cb = [](std::string&& buf, void* data) {
        CRANE_TRACE("Read output from subprocess: {}", buf);
      };

      process->SetOutputCb(std::move(output_cb));

      err = this_->SpawnProcessInInstance_(instance, std::move(process));

      if (err != CraneErr::kOk) {
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            fmt::format(
                "Cannot spawn a new process inside the instance of task #{}",
                instance->task.task_id()));
      }
    }

    // Add a timer to limit the execution time of a task.
    this_->EvAddTerminationTimer_(instance,
                                  instance->task.time_limit().seconds());
  }
}

void TaskManager::EvTaskStatusChangeCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  TaskStatusChange status_change;
  while (this_->m_task_status_change_queue_.try_dequeue(status_change)) {
    CRANE_ASSERT_MSG_VA(
        status_change.new_status == crane::grpc::TaskStatus::Finished ||
            status_change.new_status == crane::grpc::TaskStatus::Failed ||
            status_change.new_status == crane::grpc::TaskStatus::Cancelled,
        "Task #{}: When TaskStatusChange event is triggered, the task should "
        "either be Finished, Failed or Cancelled. new_status = {}",
        status_change.task_id, status_change.new_status);

    CRANE_DEBUG(R"(Destroying Task structure for "{}".)"
                R"(Task new status: {})",
                status_change.task_id, status_change.new_status);

    auto iter = this_->m_task_map_.find(status_change.task_id);
    CRANE_ASSERT_MSG(iter != this_->m_task_map_.end(),
                     "Task should be found here.");

    TaskInstance* task_instance = iter->second.get();
    uid_t uid = task_instance->task.uid();

    if (task_instance->termination_timer) {
      event_del(task_instance->termination_timer);
      event_free(task_instance->termination_timer);
    }

    // Free the TaskInstance structure
    this_->m_task_map_.erase(status_change.task_id);

    CRANE_TRACE("Put TaskStatusChange for task #{} into queue.",
                status_change.task_id);
    g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }

  // Todo: Add additional timer to check periodically whether all children
  //  have exited.
  if (this_->m_is_ending_now_ && this_->m_task_map_.empty()) {
    CRANE_TRACE(
        "Craned is ending and all tasks have been reaped. "
        "Stop event loop.");
    this_->Shutdown();
  }
}

void TaskManager::EvActivateTaskStatusChange_(
    uint32_t task_id, crane::grpc::TaskStatus new_status,
    std::optional<std::string> reason) {
  TaskStatusChange status_change{task_id, new_status};
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
    err = this_->SpawnProcessInInstance_(task_iter->second.get(),
                                         std::move(process));
    elem.err_promise.set_value(err);

    if (err != CraneErr::kOk)
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Failed,
                                         std::string(CraneErrStr(err)));
  }
}

void TaskManager::EvGrpcQueryTaskIdFromPidCb_(int efd, short events,
                                              void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueQueryTaskIdFromPid elem;
  while (this_->m_query_task_id_from_pid_queue_.try_dequeue(elem)) {
    auto task_iter = this_->m_pid_task_map_.find(elem.pid);
    if (task_iter == this_->m_pid_task_map_.end())
      elem.task_id_prom.set_value(std::nullopt);
    else {
      TaskInstance* instance = task_iter->second;
      uint32_t task_id = instance->task.task_id();
      elem.task_id_prom.set_value(task_id);
    }
  }
}

void TaskManager::EvOnTimerCb_(int, short, void* arg_) {
  auto* arg = reinterpret_cast<EvTimerCbArg*>(arg_);
  TaskManager* this_ = arg->task_manager;

  CRANE_TRACE("Task #{} exceeded its time limit. Terminating it...",
              arg->task_instance->task.task_id());

  EvQueueTaskTerminate ev_task_terminate{arg->task_instance->task.task_id()};
  this_->m_task_terminate_queue_.enqueue(ev_task_terminate);
  event_active(this_->m_ev_task_terminate_, 0, 0);

  event_del(arg->timer_ev);
  event_free(arg->timer_ev);
  arg->task_instance->termination_timer = nullptr;
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
      CRANE_ERROR("Trying terminating unknown task #{}", elem.task_id);
      return;
    }

    const auto& task_instance = iter->second;

    if (elem.comes_from_grpc) task_instance->cancelled_by_user = true;

    int sig = SIGTERM;  // For BatchTask
    if (task_instance->task.type() == crane::grpc::Interactive) sig = SIGHUP;

    if (!task_instance->processes.empty()) {
      // For an Interactive task with a process running or a Batch task, we just
      // send a kill signal here.
      for (auto&& [pid, pr_instance] : task_instance->processes)
        KillProcessInstance_(pr_instance.get(), sig);
    } else if (task_instance->task.type() == crane::grpc::Interactive) {
      // For an Interactive task with no process running, it ends immediately.
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Finished,
                                         std::nullopt);
    }
  }
}

void TaskManager::TerminateTaskAsync(uint32_t task_id) {
  EvQueueTaskTerminate elem{.task_id = task_id, .comes_from_grpc = true};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

bool TaskManager::CreateCgroupAsync(uint32_t task_id, uid_t uid) {
  EvQueueCreateCg elem{.task_id = task_id, .uid = uid};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_grpc_create_cg_queue_.enqueue(std::move(elem));
  event_active(m_ev_grpc_create_cg_, 0, 0);
  return ok_fut.get();
}

bool TaskManager::ReleaseCgroupAsync(uint32_t task_id, uid_t uid) {
  EvQueueReleaseCg elem{.task_id = task_id, .uid = uid};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_grpc_release_cg_queue_.enqueue(std::move(elem));
  event_active(m_ev_grpc_release_cg_, 0, 0);
  return ok_fut.get();
}

void TaskManager::EvGrpcCreateCgroupCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueCreateCg create_cg;
  while (this_->m_grpc_create_cg_queue_.try_dequeue(create_cg)) {
    CRANE_DEBUG("Creating Cgroup for task #{}", create_cg.task_id);

    std::string cg_path = CgroupStrByTaskId_(create_cg.task_id);
    util::Cgroup* cgroup;
    cgroup = this_->m_cg_mgr_.CreateOrOpen(cg_path, util::ALL_CONTROLLER_FLAG,
                                           util::NO_CONTROLLER_FLAG, false);
    // Create cgroup for the new subprocess
    if (!cgroup) {
      CRANE_ERROR("Failed to create cgroup for task #{}", create_cg.task_id);
      create_cg.ok_prom.set_value(false);
    } else {
      create_cg.ok_prom.set_value(true);
      this_->m_task_id_to_cg_map_.emplace(create_cg.task_id, cgroup);
      this_->m_uid_to_task_ids_map_[create_cg.uid].emplace(create_cg.task_id);
    }
  }
}

void TaskManager::EvGrpcReleaseCgroupCb_(int efd, short events,
                                         void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueReleaseCg release_cg;
  while (this_->m_grpc_release_cg_queue_.try_dequeue(release_cg)) {
    CRANE_DEBUG("Destroying Cgroup for task #{}", release_cg.task_id);

    this_->m_uid_to_task_ids_map_[release_cg.uid].erase(release_cg.task_id);
    if (this_->m_uid_to_task_ids_map_[release_cg.uid].empty()) {
      this_->m_uid_to_task_ids_map_.erase(release_cg.uid);
    }

    auto iter = this_->m_task_id_to_cg_map_.find(release_cg.task_id);
    if (iter == this_->m_task_id_to_cg_map_.end()) {
      CRANE_ERROR("Failed to find cgroup for task #{}", release_cg.task_id);
      release_cg.ok_prom.set_value(false);
      return;
    } else {
      // The termination of all processes in a cgroup is a time-consuming work.
      // Therefore, once we are sure that the cgroup for this task exists, we
      // let gRPC call return and put the termination work into the thread pool
      // to avoid blocking the event loop of TaskManager.
      // Kind of async behavior.
      release_cg.ok_prom.set_value(true);

      util::Cgroup* cg = iter->second;
      this_->m_task_id_to_cg_map_.erase(iter);

      g_thread_pool->push_task(
          [cg, cg_mgr = &this_->m_cg_mgr_, task_id = release_cg.task_id] {
            bool rc;
            int cnt = 0;

            while (true) {
              if (cg->Empty()) {
                CRANE_TRACE("Cgroup {} now has no process inside.",
                            cg->GetCgroupString());
                break;
              }

              if (cnt >= 5) {
                CRANE_ERROR(
                    "Couldn't kill the processes in cgroup {} after {} times. "
                    "Skipping it.",
                    cg->GetCgroupString(), cnt);
                break;
              }

              cg->KillAllProcesses();
              ++cnt;
              std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }

            rc = cg_mgr->Release(cg->GetCgroupString());
            // Release cgroup for the new subprocess
            if (!rc) {
              CRANE_ERROR("Failed to Release cgroup for task #{}", task_id);
            }
          });
    }
  }
}

bool TaskManager::QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid* info) {
  EvQueueQueryTaskInfoOfUid elem{.uid = uid};

  std::future<TaskInfoOfUid> info_fut = elem.info_prom.get_future();
  m_query_task_info_of_uid_queue_.enqueue(std::move(elem));
  event_active(m_ev_query_task_info_of_uid_, 0, 0);

  *info = info_fut.get();
  return info->job_cnt > 0 && info->cgroup_exists;
}

void TaskManager::EvGrpcQueryTaskInfoOfUidCb_(int efd, short events,
                                              void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueQueryTaskInfoOfUid query;
  while (this_->m_query_task_info_of_uid_queue_.try_dequeue(query)) {
    CRANE_DEBUG("Query task info for uid {}", query.uid);

    TaskInfoOfUid info{.job_cnt = 0, .cgroup_exists = false};

    auto iter = this_->m_uid_to_task_ids_map_.find(query.uid);
    if (iter != this_->m_uid_to_task_ids_map_.end()) {
      info.job_cnt = iter->second.size();
      info.first_task_id = *iter->second.begin();

      try {
        util::Cgroup* cg = this_->m_task_id_to_cg_map_.at(info.first_task_id);
        info.cgroup_path = cg->GetCgroupString();
        info.cgroup_exists = true;
      } catch (const std::out_of_range& e) {
        CRANE_ERROR("Cannot find Cgroup* for uid {}'s task #{}!", query.uid,
                    info.first_task_id);
      }
    }

    query.info_prom.set_value(info);
  }
}

bool TaskManager::QueryCgOfTaskIdAsync(uint32_t task_id, util::Cgroup** cg) {
  EvQueueQueryCgOfTaskId elem{.task_id = task_id};

  std::future<util::Cgroup*> cg_fut = elem.cg_prom.get_future();
  m_query_cg_of_task_id_queue_.enqueue(std::move(elem));
  event_active(m_ev_query_cg_of_task_id_, 0, 0);

  *cg = cg_fut.get();
  return *cg != nullptr;
}

void TaskManager::EvGrpcQueryCgOfTaskIdCb_(int efd, short events,
                                           void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueQueryCgOfTaskId query;
  while (this_->m_query_cg_of_task_id_queue_.try_dequeue(query)) {
    CRANE_DEBUG("Query Cgroup* task #{}", query.task_id);

    auto iter = this_->m_task_id_to_cg_map_.find(query.task_id);
    if (iter != this_->m_task_id_to_cg_map_.end()) {
      query.cg_prom.set_value(iter->second);
    } else {
      query.cg_prom.set_value(nullptr);
    }
  }
}

}  // namespace Craned
