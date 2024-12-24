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

#include <google/protobuf/util/delimited_message_util.h>
#include <grp.h>
#include <pty.h>
#include <sys/wait.h>

#include "CforedClient.h"
#include "CranedClient.h"
#include "crane/String.h"
#include "protos/Supervisor.grpc.pb.h"

namespace Supervisor {

bool TaskInstance::IsBatch() const { return task.type() == crane::grpc::Batch; }

bool TaskInstance::IsCrun() const {
  return task.type() == crane::grpc::Interactive &&
         task.interactive_meta().interactive_type() == crane::grpc::Crun;
}

bool TaskInstance::IsCalloc() const {
  return task.type() == crane::grpc::Interactive &&
         task.interactive_meta().interactive_type() == crane::grpc::Calloc;
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

TaskManager::TaskManager() : m_supervisor_exit_(false) {
  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });
  if (m_sigchld_handle_->start(SIGCHLD) != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle");
  }

  m_terminate_task_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_task_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateTaskQueueCb_();
      });

  m_change_task_time_limit_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_change_task_time_limit_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanChangeTaskTimeLimitQueueCb_();
      });

  m_grpc_execute_task_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_execute_task_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvGrpcExecuteTaskCb_();
      });

  m_uvw_thread_ = std::thread([this]() {
    util::SetCurrentThreadName("TaskMgrLoopThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_supervisor_exit_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in TaskManager loop.");
    }
    m_uvw_loop_->run();
  });
}

TaskManager::~TaskManager() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}
void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::TaskStopAndDoStatusChange() {
  CRANE_INFO("Task #{} stopped and is doing TaskStatusChange...",
             m_task_->task.task_id());

  switch (m_task_->err_before_exec) {
  case CraneErr::kProtobufError:
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeSpawnProcessFail,
                              std::nullopt);
    break;

  case CraneErr::kCgroupError:
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeCgroupError, std::nullopt);
    break;

  default:
    break;
  }

  ProcSigchldInfo& sigchld_info = m_task_->sigchld_info;
  if (m_task_->IsBatch() || m_task_->IsCrun()) {
    // For a Batch task, the end of the process means it is done.
    if (sigchld_info.is_terminated_by_signal) {
      if (m_task_->cancelled_by_user)
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::Cancelled,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else if (m_task_->terminated_by_timeout)
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::ExceedTimeLimit,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::Failed,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
    } else
      ActivateTaskStatusChange_(crane::grpc::TaskStatus::Completed,
                                sigchld_info.value, std::nullopt);
  } else /* Calloc */ {
    // For a COMPLETING Calloc task with a process running,
    // the end of this process means that this task is done.
    if (sigchld_info.is_terminated_by_signal)
      ActivateTaskStatusChange_(
          crane::grpc::TaskStatus::Completed,
          sigchld_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
    else
      ActivateTaskStatusChange_(crane::grpc::TaskStatus::Completed,
                                sigchld_info.value, std::nullopt);
  }
}

void TaskManager::ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                            uint32_t exit_code,
                                            std::optional<std::string> reason) {
  TaskInstance* instance = m_task_.get();
  if (instance->IsBatch() || instance->IsCrun()) {
    const std::string& path = instance->process->meta->parsed_sh_script_path;
    if (!path.empty())
      g_thread_pool->detach_task([p = path]() { util::os::DeleteFile(p); });
  }

  bool orphaned = instance->orphaned;
  // Free the TaskInstance structure
  m_task_.release();
  if (!orphaned)
    g_craned_client->TaskStatusChange(new_status, exit_code, reason);
}

std::string TaskManager::ParseFilePathPattern_(const std::string& path_pattern,
                                               const std::string& cwd) {
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
    resolved_path_pattern += fmt::format("Crane-{}.out", g_config.TaskId);

  return resolved_path_pattern;
}

void TaskManager::LaunchTaskInstance_() {
  auto* instance = m_task_.get();
  instance->pwd_entry.Init(instance->task.uid());
  if (!instance->pwd_entry.Valid()) {
    CRANE_DEBUG("Failed to look up password entry for uid {} of task",
                instance->task.uid());
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodePermissionDenied,
        fmt::format("Failed to look up password entry for uid {} of task",
                    instance->task.uid()));
    return;
  }
  // Calloc tasks have no scripts to run. Just return.
  if (instance->IsCalloc()) return;
  auto& sh_path = instance->process->meta->parsed_sh_script_path =
      fmt::format("{}/Crane-{}.sh", g_config.CraneScriptDir, g_config.TaskId);

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task");
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeFileNotFound,
        fmt::format("Cannot write shell script for batch task"));
    return;
  }

  if (instance->task.type() == crane::grpc::Batch)
    fputs(instance->task.batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(instance->task.interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

  // TaskManager is single threaded, no need to worry data race of process
  // in task instance.
  instance->process =
      std::make_unique<ProcessInstance>(sh_path, std::list<std::string>());

  auto process = instance->process.get();
  // Prepare file output name for batch tasks.
  if (instance->task.type() == crane::grpc::Batch) {
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto* meta = dynamic_cast<BatchMetaInTaskInstance*>(process->meta.get());
    meta->parsed_output_file_pattern =
        ParseFilePathPattern_(instance->task.batch_meta().output_file_pattern(),
                              instance->task.cwd());
    absl::StrReplaceAll({{"%j", std::to_string(g_config.TaskId)},
                         {"%u", instance->pwd_entry.Username()},
                         {"%x", instance->task.name()}},
                        &meta->parsed_output_file_pattern);

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!instance->task.batch_meta().error_file_pattern().empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          instance->task.batch_meta().error_file_pattern(),
          instance->task.cwd());
      absl::StrReplaceAll({{"%j", std::to_string(g_config.TaskId)},
                           {"%u", instance->pwd_entry.Username()},
                           {"%x", instance->task.name()}},
                          &meta->parsed_error_file_pattern);
    }
  }

  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErr err = SpawnTaskInstance_();
  if (err != CraneErr::kOk) {
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeSpawnProcessFail,
        fmt::format("Cannot spawn a new process inside the instance of task"));
  }
}

CraneErr TaskManager::SpawnTaskInstance_() {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::CanStartMessage;
  using crane::grpc::ChildProcessReady;

  auto* instance = m_task_.get();

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
        dynamic_cast<CrunMetaInTaskInstance*>(instance->process->meta.get());
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
    auto process = instance->process.get();
    process->SetPid(child_pid);
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                instance->task.task_id(), child_pid);

    if (instance->IsCrun()) {
      auto* meta =
          dynamic_cast<CrunMetaInTaskInstance*>(instance->process->meta.get());
      g_cfored_manager->RegisterIOForward(
          instance->task.interactive_meta().cfored_name(), meta->msg_fd,
          launch_pty);
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

    // Migrate the new subprocess to newly created cgroup

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
      KillTaskInstance_(SIGKILL);
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
      KillTaskInstance_(SIGKILL);
      return CraneErr::kOk;
    }

    close(ctrl_fd);
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

    if (instance->IsBatch()) {
      int stdout_fd, stderr_fd;

      auto* meta =
          dynamic_cast<BatchMetaInTaskInstance*>(instance->process->meta.get());
      const std::string& stdout_file_path = meta->parsed_output_file_pattern;
      const std::string& stderr_file_path = meta->parsed_error_file_pattern;

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
    if (instance->IsBatch()) close(0);
    util::os::CloseFdFrom(3);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    EnvMap task_env_map = instance->GetTaskEnvMap();
    for (const auto& [name, value] : task_env_map)
      if (setenv(name.c_str(), value.c_str(), 1))
        fmt::print("setenv for {}={} failed!\n", name, value);

    // Argv[0] is the program name which can be anything.
    argv.emplace_back("CraneScript");

    if (instance->task.get_user_env()) {
      // If --get-user-env is specified,
      // we need to use --login option of bash to load settings from the user's
      // settings.
      argv.emplace_back("--login");
    }

    argv.emplace_back(instance->process->GetExecPath().c_str());
    for (auto&& arg : instance->process->GetArgList()) {
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

CraneErr TaskManager::KillTaskInstance_(int signum) {
  if (m_task_ && m_task_->process) {
    CRANE_TRACE("Killing pid {} with signal {}", m_task_->process->GetPid(),
                signum);

    // Send the signal to the whole process group.
    int err = kill(m_task_->process->GetPid(), signum);

    if (err == 0)
      return CraneErr::kOk;
    else {
      CRANE_TRACE("kill failed. error: {}", strerror(errno));
      return CraneErr::kGenericFailure;
    }
  }

  return CraneErr::kNonExistent;
}

void TaskManager::EvSigchldCb_() {
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

      m_task_->sigchld_info = *sigchld_info;

      if (m_task_->IsCrun())
        // TaskStatusChange of a crun task is triggered in
        // CforedManager.
        g_cfored_manager->TaskProcStopped();
      else /* Batch / Calloc */ {
        // If the TaskInstance has no process left,
        // send TaskStatusChange for this task.
        // See the comment of EvActivateTaskStatusChange_.
        TaskStopAndDoStatusChange();
      }
    } else if (pid == 0) {
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void TaskManager::EvTaskTimerCb_() {
  CRANE_TRACE("Task #{} exceeded its time limit. Terminating it...",
              g_config.TaskId);

  // Sometimes, task finishes just before time limit.
  // After the execution of SIGCHLD callback where the task has been erased,
  // the timer is triggered immediately.
  // That's why we need to check the existence of the task again in timer
  // callback, otherwise a segmentation fault will occur.
  if (!m_task_) {
    CRANE_TRACE("Task #{} has already been removed.", g_config.TaskId);
    return;
  }

  TaskInstance* task_instance = m_task_.get();
  DelTerminationTimer_(task_instance);

  if (task_instance->IsBatch()) {
    m_task_terminate_queue_.enqueue(
        TaskTerminateQueueElem{.terminated_by_timeout = true});
    m_terminate_task_async_handle_->send();
  } else {
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::ExceedTimeLimit,
                              ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
  }
}

void TaskManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        g_config.TaskId);

    if (!m_task_) {
      CRANE_DEBUG("Terminating a non-existent task #{}.", g_config.TaskId);

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
      continue;
    }

    TaskInstance* task_instance = m_task_.get();

    if (elem.terminated_by_user) task_instance->cancelled_by_user = true;
    if (elem.mark_as_orphaned) task_instance->orphaned = true;
    if (elem.terminated_by_timeout) task_instance->terminated_by_timeout = true;

    int sig = SIGTERM;  // For BatchTask
    if (task_instance->IsCrun()) sig = SIGHUP;

    if (task_instance->process) {
      // For an Interactive task with a process running or a Batch task, we
      // just send a kill signal here.
      KillTaskInstance_(sig);
    } else if (task_instance->task.type() == crane::grpc::Interactive) {
      // For an Interactive task with no process running, it ends immediately.
      ActivateTaskStatusChange_(crane::grpc::Completed,
                                ExitCode::kExitCodeTerminated, std::nullopt);
    }
  }
}
void TaskManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  absl::Time now = absl::Now();

  ChangeTaskTimeLimitQueueElem elem;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    if (!m_task_) {
      CRANE_ERROR("Try to update the time limit of a non-existent task #{}.",
                  g_config.TaskId);
      elem.ok_prom.set_value(false);
      continue;
    }
    TaskInstance* task_instance = m_task_.get();
    DelTerminationTimer_(task_instance);

    absl::Time start_time =
        absl::FromUnixSeconds(task_instance->task.start_time().seconds());
    absl::Duration const& new_time_limit = elem.time_limit;

    if (now - start_time >= new_time_limit) {
      // If the task times out, terminate it.
      TaskTerminateQueueElem ev_task_terminate{.terminated_by_timeout = true};
      m_task_terminate_queue_.enqueue(ev_task_terminate);
      m_terminate_task_async_handle_->send();

    } else {
      // If the task haven't timed out, set up a new timer.
      AddTerminationTimer_(
          task_instance,
          ToInt64Seconds((new_time_limit - (absl::Now() - start_time))));
    }
    elem.ok_prom.set_value(true);
  }
}
void TaskManager::EvGrpcExecuteTaskCb_() {
  if (m_task_ != nullptr) {
    CRANE_ERROR("Duplicated ExecuteTask request for task #{}. Ignore it.",
                g_config.TaskId);
    return;
  }
  std::unique_ptr<TaskInstance> instance(
      m_grpc_execute_task_elem_.exchange(nullptr, std::memory_order_acquire));
  // Add a timer to limit the execution time of a task.
  // Note: event_new and event_add in this function is not thread safe,
  //       so we move it outside the multithreading part.
  int64_t sec = instance->task.time_limit().seconds();
  AddTerminationTimer_(instance.get(), sec);
  CRANE_TRACE("Add a timer of {} seconds", sec);
}

}  // namespace Supervisor