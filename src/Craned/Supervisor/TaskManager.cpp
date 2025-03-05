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

#include <absl/strings/str_split.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <grp.h>
#include <pty.h>
#include <spdlog/fmt/bundled/core.h>
#include <sys/wait.h>

#include <nlohmann/json.hpp>

#include "CforedClient.h"
#include "CranedClient.h"
#include "SupervisorPublicDefs.h"
#include "SupervisorServer.h"
#include "crane/OS.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"

namespace Supervisor {

inline bool ExecutionInterface::IsBatch() const {
  return task.type() == crane::grpc::Batch;
}

inline bool ExecutionInterface::IsCrun() const {
  return task.type() == crane::grpc::Interactive &&
         task.interactive_meta().interactive_type() == crane::grpc::Crun;
}

inline bool ExecutionInterface::IsCalloc() const {
  return task.type() == crane::grpc::Interactive &&
         task.interactive_meta().interactive_type() == crane::grpc::Calloc;
}

std::string ExecutionInterface::ParseFilePathPattern_(
    const std::string& pattern, const std::string& cwd) const {
  std::filesystem::path resolved_path(pattern);

  // If not specified, use cwd.
  if (resolved_path.empty())
    resolved_path = cwd;
  else if (resolved_path.is_relative())
    resolved_path = cwd / resolved_path;

  // Path ends with a directory, append default stdout/err file name
  // `Crane-<Job ID>.out` to the path.
  if (std::filesystem::is_directory(resolved_path))
    resolved_path = resolved_path / fmt::format("Crane-{}.out", task.task_id());

  std::string resolved_path_pattern = std::move(resolved_path.string());
  absl::StrReplaceAll({{"%j", std::to_string(task.task_id())},
                       {"%u", pwd.Username()},
                       {"%x", task.name()}},
                      &resolved_path_pattern);

  return resolved_path_pattern;
}

// TODO: Refactor this into TaskManager, instead of in ExecutionInterface.
EnvMap ExecutionInterface::GetChildProcessEnv_() const {
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
    env_map.emplace("HOME", this->pwd.HomeDir());
    env_map.emplace("SHELL", this->pwd.Shell());
  }

  env_map.emplace("CRANE_JOB_NODELIST",
                  absl::StrJoin(this->task.allocated_nodes(), ";"));
  env_map.emplace("CRANE_EXCLUDES", absl::StrJoin(this->task.excludes(), ";"));
  env_map.emplace("CRANE_JOB_NAME", this->task.name());
  env_map.emplace("CRANE_ACCOUNT", this->task.account());
  env_map.emplace("CRANE_PARTITION", this->task.partition());
  env_map.emplace("CRANE_QOS", this->task.qos());

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

void ExecutionInterface::SetChildProcessSignalHandler_() {
  // Disable SIGABRT backtrace from child processes.
  signal(SIGABRT, SIG_DFL);
  // Reset the following signal handlers to default.
  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);
  signal(SIGTSTP, SIG_DFL);
  signal(SIGQUIT, SIG_DFL);
  signal(SIGUSR1, SIG_DFL);
  signal(SIGUSR2, SIG_DFL);
  signal(SIGALRM, SIG_DFL);
  signal(SIGHUP, SIG_DFL);
}

CraneErr ExecutionInterface::SetChildProcessProperty_() {
  // TODO: Add all other supplementary groups.
  // Currently we only set the primary gid and the egid when task was
  // submitted.
  std::vector<gid_t> gids;
  if (task.gid() != pwd.Gid()) gids.emplace_back(task.gid());
  gids.emplace_back(pwd.Gid());

  int rc = setgroups(gids.size(), gids.data());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setgroups() failed: {}\n",
               task.task_id(), strerror(errno));
    return CraneErr::kSystemErr;
  }

  rc = setresgid(task.gid(), task.gid(), task.gid());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setegid() failed: {}\n",
               task.task_id(), strerror(errno));
    return CraneErr::kSystemErr;
  }

  rc = setresuid(pwd.Uid(), pwd.Uid(), pwd.Uid());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: seteuid() failed: {}\n",
               task.task_id(), strerror(errno));
    return CraneErr::kSystemErr;
  }

  const std::string& cwd = task.cwd();
  rc = chdir(cwd.c_str());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: chdir to {}. {}\n", cwd.c_str(),
               strerror(errno));
    return CraneErr::kSystemErr;
  }

  // Set pgid to the pid of task root process.
  setpgid(0, 0);

  return CraneErr::kOk;
}

CraneErr ExecutionInterface::SetChildProcessBatchFd_() {
  int stdout_fd, stderr_fd;

  auto* meta = dynamic_cast<BatchMetaInExecution*>(m_meta_.get());
  const std::string& stdout_file_path = meta->parsed_output_file_pattern;
  const std::string& stderr_file_path = meta->parsed_error_file_pattern;

  stdout_fd = open(stdout_file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (stdout_fd == -1) {
    fmt::print(stderr, "[Subprocess] Error: open {}. {}\n", stdout_file_path,
               strerror(errno));
    return CraneErr::kSystemErr;
  }
  dup2(stdout_fd, 1);  // stdout -> output file

  if (stderr_file_path.empty()) {
    dup2(stdout_fd, 2);  // if errfile not set, redirect stderr to stdout
  } else {
    stderr_fd =
        open(stderr_file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (stderr_fd == -1) {
      fmt::print(stderr, "[Subprocess] Error: open {}. {}\n", stderr_file_path,
                 strerror(errno));
      return CraneErr::kSystemErr;
    }
    dup2(stderr_fd, 2);  // stderr -> error file
    close(stderr_fd);
  }
  close(stdout_fd);

  return CraneErr::kOk;
}

CraneErr ExecutionInterface::SetChildProcessEnv_() const {
  if (clearenv())
    fmt::print(stderr, "[Subprocess] Warning: clearenv() failed.\n");

  for (const auto& [name, value] : m_env_)
    if (setenv(name.c_str(), value.c_str(), 1))
      fmt::print(stderr, "[Subprocess] Warning: setenv() for {}={} failed.\n",
                 name, value);

  return CraneErr::kOk;
}

std::vector<std::string> ExecutionInterface::GetChildProcessExecArgv_() const {
  // "bash (--login) -c 'm_executable_ [m_arguments_...]'"
  std::vector<std::string> argv;
  argv.emplace_back("/bin/bash");
  if (task.get_user_env()) {
    // Use --login to load user's shell settings
    argv.emplace_back("--login");
  }
  argv.emplace_back("-c");

  // Build the command string as: "m_executable_ [m_arguments_...]"
  std::string cmd = m_executable_;
  for (const auto& arg : m_arguments_) {
    cmd += " " + arg;
  }
  argv.emplace_back(std::move(cmd));

  return argv;
}

std::string ContainerInstance::ParseOCICmdPattern_(
    const std::string& cmd) const {
  std::string parsed_cmd(cmd);
  // NOTE: Using m_temp_path_ as the bundle is modified and stored here
  absl::StrReplaceAll({{"%b", m_temp_path_.string()},
                       {"%j", std::to_string(task.task_id())},
                       {"%x", task.name()},
                       {"%u", pwd.Username()},
                       {"%U", std::to_string(pwd.Uid())}},
                      &parsed_cmd);
  return parsed_cmd;
}

CraneErr ContainerInstance::ModifyOCIBundleConfig_(
    const std::string& src, const std::string& dst) const {
  using json = nlohmann::json;

  // Check if bundle is valid
  auto src_config = std::filesystem::path(src) / "config.json";
  auto src_rootfs = std::filesystem::path(src) / "rootfs";
  if (!std::filesystem::exists(src_config) ||
      !std::filesystem::exists(src_rootfs)) {
    CRANE_ERROR("Bundle provided by task #{} not exists : {}", task.task_id(),
                src_config.string());
    return CraneErr::kInvalidParam;
  }

  std::ifstream fin{src_config};
  if (!fin) {
    CRANE_ERROR("Failed to open bundle config provided by task #{}: {}",
                task.task_id(), src_config.string());
    return CraneErr::kSystemErr;
  }

  json config = json::parse(fin, nullptr, false);
  if (config.is_discarded()) {
    CRANE_ERROR("Bundle config provided by task #{} is invalid: {}",
                task.task_id(), src_config.string());
    return CraneErr::kInvalidParam;
  }

  try {
    // Set root object, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/config.md#root
    // Set real rootfs path in the modified config.
    config["root"]["path"] = src_rootfs;

    // Set mounts array, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/config.md#mounts
    // Bind mount script into the container
    auto mounts = config["mounts"].get<std::vector<json>>();
    std::string parsed_sh_mounted_path = "/tmp/crane/script.sh";
    mounts.emplace_back(json::object({
        {"destination", parsed_sh_mounted_path},
        {"source", m_meta_->parsed_sh_script_path},
        {"options", json::array({"bind", "ro"})},
    }));
    config["mounts"] = mounts;

    // Set process object, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/config.md#process
    auto& process = config["process"];

    // Set pass-through mode.
    // TODO: Check if interactive task is supported.
    process["terminal"] = false;

    // Write environment variables in IEEE format.
    std::vector<std::string> env_list;
    for (const auto& [name, value] : m_env_)
      env_list.emplace_back(fmt::format("{}={}", name, value));
    process["env"] = env_list;

    // For container, we use `/bin/sh` as the default interpreter.
    std::string real_exe = "/bin/sh";
    if (IsBatch() && !task.batch_meta().interpreter().empty())
      real_exe = task.batch_meta().interpreter();

    std::vector<std::string> args = absl::StrSplit(real_exe, ' ');
    args.emplace_back(std::move(parsed_sh_mounted_path));
    process["args"] = args;
  } catch (json::exception& e) {
    CRANE_ERROR("Failed to generate bundle config for task #{}: {}",
                task.task_id(), e.what());
    return CraneErr::kInvalidParam;
  }

  // Write the modified config
  auto dst_config = std::filesystem::path(dst) / "config.json";
  std::ofstream fout{dst_config};
  if (!fout) {
    CRANE_ERROR("Failed to write bundle config for task #{}: {}",
                task.task_id(), dst_config.string());
    return CraneErr::kSystemErr;
  }
  fout << config.dump(4);
  fout.flush();

  return CraneErr::kOk;
}

CraneErr ContainerInstance::Prepare() {
  // Generate path and params.
  m_temp_path_ = g_config.Container.TempDir / fmt::format("{}", task.task_id());
  m_bundle_path_ = task.container();
  m_executable_ = ParseOCICmdPattern_(g_config.Container.RuntimeRun);
  m_env_ = GetChildProcessEnv_();
  // m_arguments_ is not applicable for container tasks.

  // Write script into the temp folder
  auto sh_path = m_temp_path_ / fmt::format("Crane-{}.sh", task.task_id());
  if (!util::os::CreateFoldersForFile(sh_path)) {
    return CraneErr::kSystemErr;
  }

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}", task.task_id());
    return CraneErr::kSystemErr;
  }

  if (IsBatch())
    fputs(task.batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(task.interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

  // Write meta
  if (IsBatch()) {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchMetaInExecution>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        task.batch_meta().output_file_pattern(), task.cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!task.batch_meta().error_file_pattern().empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          task.batch_meta().error_file_pattern(), task.cwd());
    }

    m_meta_ = std::move(meta);
  } else {
    m_meta_ = std::make_unique<CrunMetaInExecution>();
  }
  m_meta_->parsed_sh_script_path = sh_path;

  // Modify bundle
  auto err = ModifyOCIBundleConfig_(m_bundle_path_, m_temp_path_);
  if (err != CraneErr::kOk) {
    CRANE_ERROR("Failed to modify OCI bundle config for task #{}",
                task.task_id());
    return err;
  }

  return CraneErr::kOk;
}

CraneErr ContainerInstance::Spawn() {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  // Socket pair for forwarding IO of crun tasks. Craned read from index 0.
  int crun_io_sock_pair[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid;
  bool launch_pty{false};

  if (IsCrun()) {
    auto* meta = dynamic_cast<CrunMetaInExecution*>(m_meta_.get());
    launch_pty = task.interactive_meta().pty();
    CRANE_DEBUG("Launch crun task #{} pty: {}", task.task_id(), launch_pty);

    if (launch_pty) {
      child_pid = forkpty(&meta->msg_fd, nullptr, nullptr, nullptr);
    } else {
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, crun_io_sock_pair) != 0) {
        CRANE_ERROR("Failed to create socket pair for task io forward: {}",
                    strerror(errno));
        return CraneErr::kSystemErr;
      }
      meta->msg_fd = crun_io_sock_pair[0];
      child_pid = fork();
    }
  } else {
    child_pid = fork();
  }

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", task.task_id(),
                strerror(errno));
    return CraneErr::kSystemErr;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}", task.task_id(),
                m_pid_);

    if (IsCrun()) {
      auto* meta = dynamic_cast<CrunMetaInExecution*>(m_meta_.get());
      g_cfored_manager->RegisterIOForward(task.interactive_meta().cfored_name(),
                                          meta->msg_fd, launch_pty);
    }

    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);
    if (IsCrun() && !launch_pty) {
      close(crun_io_sock_pair[1]);
    }

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                task.task_id());

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
                  child_pid, task.task_id(), strerror(ostream.GetErrno()));
      close(ctrl_fd);

      // Communication failure caused by process crash or grpc error.
      // Since now the parent cannot ask the child
      // process to commit suicide, kill child process here and just return.
      // The child process will be reaped in SIGCHLD handler and
      // thus only ONE TaskStatusChange will be triggered!
      err_before_exec = CraneErr::kProtobufError;
      Kill(SIGKILL);
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
                    task.task_id());
      close(ctrl_fd);

      // See comments above.
      err_before_exec = CraneErr::kProtobufError;
      Kill(SIGKILL);
      return CraneErr::kOk;
    }

    close(ctrl_fd);
    return CraneErr::kOk;

  } else {  // Child proc
    SetChildProcessSignalHandler_();

    CraneErr err = SetChildProcessProperty_();
    if (err != CraneErr::kOk) std::abort();

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
        fmt::print(stderr,
                   "[Subprocess] Error: Failed to read socket from "
                   "parent: {}\n",
                   strerror(err));
      }

      if (!msg.ok()) {
        fmt::print(stderr,
                   "[Subprocess] Error: Parent process ask to suicide.\n");
      }

      std::abort();
    }

    if (IsBatch()) {
      SetChildProcessBatchFd_();
    } else if (IsCrun() && !launch_pty) {
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
      fmt::print(stderr, "[Subprocess] Error: Failed to flush.\n");
      std::abort();
    }

    close(ctrl_fd);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    if (IsBatch()) close(0);
    util::os::CloseFdFrom(3);

    // Set env just in case OCI requires some.
    // Real env in container is handled in ModifyOCIBundle_
    SetChildProcessEnv_();

    // Prepare the command line arguments.
    auto argv = GetChildProcessExecArgv_();
    auto argv_ptrs = argv |
                     std::ranges::views::transform(
                         [](const std::string& s) { return s.c_str(); }) |
                     std::ranges::to<std::vector<const char*>>();
    argv_ptrs.push_back(nullptr);

    // Exec
    // fmt::print(stderr, "DEBUG\n");
    // for (const auto& arg : argv) fmt::print(stderr, "\"{}\" ", arg);
    execv(argv_ptrs[0], const_cast<char* const*>(argv_ptrs.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Subprocess] Error: execv failed: {}\n",
               strerror(errno));
    // TODO: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }
}

CraneErr ContainerInstance::Kill(int signum) {
  using json = nlohmann::json;
  if (m_pid_) {
    // If m_pid_ not exists, no further operation.
    int rc = 0;
    std::array<char, 256> buffer;
    std::string cmd, ret, status;
    json jret;

    // Check the state of the container
    cmd = ParseOCICmdPattern_(g_config.Container.RuntimeState);

    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                  pclose);
    if (!pipe) {
      CRANE_TRACE(
          "[Subprocess] Error in getting container status: popen() failed.");
      goto ProcessKill;
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) !=
           nullptr) {
      ret += buffer.data();
    }

    jret = json::parse(std::move(ret), nullptr, false);
    if (jret.is_discarded() || !jret.contains("status") ||
        !jret["status"].is_string()) {
      CRANE_TRACE("[Subprocess] Error in parsing container status: {}", cmd);
      goto ProcessKill;
    }

    // Take action according to OCI container states, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/runtime.md#state
    status = std::move(jret["status"]);
    if (status == "creating") {
      goto ProcessKill;
    } else if (status == "created") {
      goto ContainerDelete;
    } else if (status == "running") {
      goto ContainerKill;
    } else if (status == "stopped") {
      goto ContainerDelete;
    } else {
      CRANE_WARN("[Subprocess] Unknown container status received: {}", status);
      goto ProcessKill;
    }

  ContainerKill:
    // Try to stop gracefully.
    // Note: Signum is configured in config.yaml instead of in the param.
    cmd = ParseOCICmdPattern_(g_config.Container.RuntimeKill);
    rc = system(cmd.c_str());
    if (rc) {
      CRANE_TRACE(
          "[Subprocess] Failed to kill container for task #{}: error in {}",
          task.task_id(), cmd);
    }

  ContainerDelete:
    // Delete the container
    // Note: Admin could choose if --force is configured or not.
    cmd = ParseOCICmdPattern_(g_config.Container.RuntimeDelete);
    rc = system(cmd.c_str());
    if (rc) {
      CRANE_TRACE(
          "[Subprocess] Failed to delete container for task #{}: error in {}",
          task.task_id(), cmd);
    }

  ProcessKill:
    // Kill runc process as the last resort.
    // Note: If runc is launched in `detached` mode, this will not work.
    rc = kill(-m_pid_, signum);
    if (rc && (errno != ESRCH)) {
      CRANE_TRACE("[Subprocess] Failed to kill pid {}. error: {}", m_pid_,
                  strerror(errno));
      return CraneErr::kSystemErr;
    }

    return CraneErr::kOk;
  }

  return CraneErr::kNonExistent;
}

CraneErr ContainerInstance::Cleanup() {
  if (IsBatch() || IsCrun()) {
    if (!m_temp_path_.empty())
      g_thread_pool->detach_task(
          [p = m_temp_path_]() { util::os::DeleteFolders(p); });
  }

  // Dummy return
  return CraneErr::kOk;
}

CraneErr ProcessInstance::Prepare() {
  // Write script content into file
  auto sh_path =
      g_config.CraneScriptDir / fmt::format("Crane-{}.sh", g_config.JobId);

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}", task.task_id());
    return CraneErr::kSystemErr;
  }

  if (IsBatch())
    fputs(task.batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(task.interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

  // Write m_meta_
  if (IsBatch()) {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchMetaInExecution>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        task.batch_meta().output_file_pattern(), task.cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!task.batch_meta().error_file_pattern().empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          task.batch_meta().error_file_pattern(), task.cwd());
    }

    m_meta_ = std::move(meta);
  } else {
    m_meta_ = std::make_unique<CrunMetaInExecution>();
  }

  m_meta_->parsed_sh_script_path = sh_path;

  // If interpreter is not set, let system decide.
  m_executable_ = sh_path.string();
  if (IsBatch() && !task.batch_meta().interpreter().empty()) {
    m_executable_ =
        fmt::format("{} {}", task.batch_meta().interpreter(), sh_path.string());
  }

  m_env_ = GetChildProcessEnv_();
  // TODO: Currently we don't support arguments in batch scripts.
  // m_arguments_ = std::list<std::string>{};

  return CraneErr::kOk;
}

CraneErr ProcessInstance::Spawn() {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  // Socket pair for forwarding IO of crun tasks. Craned read from index 0.
  int crun_io_sock_pair[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid;
  bool launch_pty{false};

  if (IsCrun()) {
    auto* meta = dynamic_cast<CrunMetaInExecution*>(m_meta_.get());
    launch_pty = task.interactive_meta().pty();
    CRANE_DEBUG("Launch crun task #{} pty: {}", task.task_id(), launch_pty);

    if (launch_pty) {
      child_pid = forkpty(&meta->msg_fd, nullptr, nullptr, nullptr);
    } else {
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, crun_io_sock_pair) != 0) {
        CRANE_ERROR("Failed to create socket pair for task io forward: {}",
                    strerror(errno));
        return CraneErr::kSystemErr;
      }
      meta->msg_fd = crun_io_sock_pair[0];
      child_pid = fork();
    }
  } else {
    child_pid = fork();
  }

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", task.task_id(),
                strerror(errno));
    return CraneErr::kSystemErr;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}", task.task_id(),
                m_pid_);

    if (IsCrun()) {
      auto* meta = dynamic_cast<CrunMetaInExecution*>(m_meta_.get());
      g_cfored_manager->RegisterIOForward(task.interactive_meta().cfored_name(),
                                          meta->msg_fd, launch_pty);
    }

    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);
    if (IsCrun() && !launch_pty) {
      close(crun_io_sock_pair[1]);
    }

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                task.task_id());

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
                  child_pid, task.task_id(), strerror(ostream.GetErrno()));
      close(ctrl_fd);

      // Communication failure caused by process crash or grpc error.
      // Since now the parent cannot ask the child
      // process to commit suicide, kill child process here and just return.
      // The child process will be reaped in SIGCHLD handler and
      // thus only ONE TaskStatusChange will be triggered!
      err_before_exec = CraneErr::kProtobufError;
      Kill(SIGKILL);
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
                    task.task_id());
      close(ctrl_fd);

      // See comments above.
      err_before_exec = CraneErr::kProtobufError;
      Kill(SIGKILL);
      return CraneErr::kOk;
    }

    close(ctrl_fd);
    return CraneErr::kOk;

  } else {  // Child proc
    SetChildProcessSignalHandler_();

    CraneErr err = SetChildProcessProperty_();
    if (err != CraneErr::kOk) std::abort();

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
        fmt::print(stderr,
                   "[Subprocess] Error: Failed to read socket from "
                   "parent: {}\n",
                   strerror(err));
      }

      if (!msg.ok()) {
        fmt::print(stderr,
                   "[Subprocess] Error: Parent process ask to suicide.\n");
      }

      std::abort();
    }

    if (IsBatch()) {
      SetChildProcessBatchFd_();
    } else if (IsCrun() && !launch_pty) {
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
      fmt::print(stderr, "[Subprocess] Error: Failed to flush.\n");
      std::abort();
    }

    close(ctrl_fd);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    if (IsBatch()) close(0);
    util::os::CloseFdFrom(3);

    // Apply environment variables
    SetChildProcessEnv_();

    // Prepare the command line arguments.
    auto argv = GetChildProcessExecArgv_();
    auto argv_ptrs = argv |
                     std::ranges::views::transform(
                         [](const std::string& s) { return s.c_str(); }) |
                     std::ranges::to<std::vector<const char*>>();
    argv_ptrs.push_back(nullptr);

    // Exec
    // fmt::print(stderr, "DEBUG\n");
    // for (const auto& arg : argv) fmt::print(stderr, "\"{}\" ", arg);
    execv(argv_ptrs[0], const_cast<char* const*>(argv_ptrs.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Subprocess] Error: execv failed: {}\n",
               strerror(errno));
    // TODO: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }
}

CraneErr ProcessInstance::Kill(int signum) {
  if (m_pid_) {
    CRANE_TRACE("Killing pid {} with signal {}", m_pid_, signum);

    // Send the signal to the whole process group.
    int err = kill(-m_pid_, signum);

    if (err == 0)
      return CraneErr::kOk;
    else {
      CRANE_TRACE("kill failed. error: {}", strerror(errno));
      return CraneErr::kGenericFailure;
    }
  }

  return CraneErr::kNonExistent;
}

CraneErr ProcessInstance::Cleanup() {
  if (IsBatch() || IsCrun()) {
    const std::string& path = m_meta_->parsed_sh_script_path;
    if (!path.empty())
      g_thread_pool->detach_task([p = path]() { util::os::DeleteFile(p); });
  }

  // Dummy return
  return CraneErr::kOk;
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

  m_check_task_status_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_check_task_status_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanCheckTaskStatusQueueCb_();
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
            CRANE_TRACE("Stopping supervisor.");
            g_server->Shutdown();
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
  CRANE_TRACE("Shutting down task manager.");
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::TaskStopAndDoStatusChange() {
  CRANE_INFO("task #{} stopped and is doing TaskStatusChange...",
             m_instance_->task.task_id());

  switch (m_instance_->err_before_exec) {
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

  ProcSigchldInfo& sigchld_info = m_instance_->sigchld_info;
  if (m_instance_->IsBatch() || m_instance_->IsCrun()) {
    // For a Batch task, the end of the process means it is done.
    if (sigchld_info.is_terminated_by_signal) {
      if (m_instance_->cancelled_by_user)
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::Cancelled,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else if (m_instance_->terminated_by_timeout)
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
  m_instance_->Cleanup();
  bool orphaned = m_instance_->orphaned;
  // Free the TaskInstance structure
  m_instance_.reset();
  if (!orphaned)
    g_craned_client->TaskStatusChangeAsync(new_status, exit_code, reason);
}

std::future<CraneExpected<pid_t>> TaskManager::ExecuteTaskAsync(
    const TaskSpec& spec) {
  std::promise<CraneExpected<pid_t>> pid_promise;
  auto pid_future = pid_promise.get_future();

  auto elem = ExecuteTaskElem{.pid_prom = std::move(pid_promise)};

  if (spec.container().length() != 0) {
    // Container
    elem.instance = std::make_unique<ContainerInstance>(spec);
  } else {
    // Process
    elem.instance = std::make_unique<ProcessInstance>(spec);
  }

  m_grpc_execute_task_queue_.enqueue(std::move(elem));
  m_grpc_execute_task_async_handle_->send();
  return pid_future;
}

void TaskManager::LaunchExecution_() {
  m_instance_->pwd.Init(m_instance_->task.uid());
  if (!m_instance_->pwd.Valid()) {
    CRANE_DEBUG("[Job #{}] Failed to look up password entry for uid {} of task",
                m_instance_->task.task_id(), m_instance_->task.uid());
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodePermissionDenied,
        fmt::format(
            "[Job #{}] Failed to look up password entry for uid {} of task",
            m_instance_->task.task_id(), m_instance_->task.uid()));
    return;
  }

  // Calloc tasks have no scripts to run. Just return.
  if (m_instance_->IsCalloc()) return;

  // Prepare for execution
  CraneErr err = m_instance_->Prepare();
  if (err != CraneErr::kOk) {
    CRANE_DEBUG("[Job #{}] Failed to prepare task",
                m_instance_->task.task_id());
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeFileNotFound,
                              fmt::format("Failed to prepare task"));
    return;
  }

  CRANE_TRACE("[Job #{}] Spawning process in task",
              m_instance_->task.task_id());
  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  err = m_instance_->Spawn();
  if (err != CraneErr::kOk) {
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeSpawnProcessFail,
        fmt::format("Cannot spawn child process in task"));
  }
}

std::future<CraneExpected<pid_t>> TaskManager::CheckTaskStatusAsync() {
  std::promise<CraneExpected<pid_t>> status_promise;
  auto status_future = status_promise.get_future();
  m_check_task_status_queue_.enqueue(std::move(status_promise));
  m_check_task_status_async_handle_->send();
  return status_future;
}

std::future<CraneErr> TaskManager::ChangeTaskTimeLimitAsync(
    absl::Duration time_limit) {
  std::promise<CraneErr> ok_promise;
  auto ok_future = ok_promise.get_future();
  ChangeTaskTimeLimitQueueElem elem;
  elem.time_limit = time_limit;
  elem.ok_prom = std::move(ok_promise);
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  m_change_task_time_limit_async_handle_->send();
  return ok_future;
}

void TaskManager::TerminateTaskAsync(bool mark_as_orphaned,
                                     bool terminated_by_user) {
  TaskTerminateQueueElem elem;
  elem.mark_as_orphaned = mark_as_orphaned;
  elem.terminated_by_user = terminated_by_user;
  m_task_terminate_queue_.enqueue(std::move(elem));
  m_terminate_task_async_handle_->send();
  return;
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

      m_instance_->sigchld_info = *sigchld_info;

      if (m_instance_->IsCrun())
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
  CRANE_TRACE("task #{} exceeded its time limit. Terminating it...",
              g_config.JobId);

  // Sometimes, task finishes just before time limit.
  // After the execution of SIGCHLD callback where the task has been erased,
  // the timer is triggered immediately.
  // That's why we need to check the existence of the task again in timer
  // callback, otherwise a segmentation fault will occur.
  if (!m_instance_) {
    CRANE_TRACE("task #{} has already been removed.", g_config.JobId);
    return;
  }

  ExecutionInterface* instance = m_instance_.get();
  DelTerminationTimer_(instance);

  if (instance->IsBatch()) {
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
        g_config.JobId);

    if (!m_instance_) {
      CRANE_DEBUG("Terminating a non-existent task #{}.", g_config.JobId);

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

    if (elem.terminated_by_user) m_instance_->cancelled_by_user = true;
    if (elem.mark_as_orphaned) m_instance_->orphaned = true;
    if (elem.terminated_by_timeout) m_instance_->terminated_by_timeout = true;

    int sig = SIGTERM;  // For BatchTask
    if (m_instance_->IsCrun()) sig = SIGHUP;

    if (m_instance_->GetPid()) {
      // For an Interactive task with a process running or a Batch task, we
      // just send a kill signal here.
      m_instance_->Kill(sig);
    } else if (m_instance_->task.type() == crane::grpc::Interactive) {
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
    if (!m_instance_) {
      CRANE_ERROR("Try to update the time limit of a non-existent task #{}.",
                  g_config.JobId);
      elem.ok_prom.set_value(CraneErr::kNonExistent);
      continue;
    }

    // Delete the old timer.
    DelTerminationTimer_(m_instance_.get());

    absl::Time start_time =
        absl::FromUnixSeconds(m_instance_->task.start_time().seconds());
    absl::Duration const& new_time_limit = elem.time_limit;

    if (now - start_time >= new_time_limit) {
      // If the task times out, terminate it.
      TaskTerminateQueueElem ev_task_terminate{.terminated_by_timeout = true};
      m_task_terminate_queue_.enqueue(ev_task_terminate);
      m_terminate_task_async_handle_->send();

    } else {
      // If the task haven't timed out, set up a new timer.
      AddTerminationTimer_(
          m_instance_.get(),
          ToInt64Seconds((new_time_limit - (absl::Now() - start_time))));
    }
    elem.ok_prom.set_value(CraneErr::kOk);
  }
}

void TaskManager::EvCleanCheckTaskStatusQueueCb_() {
  std::promise<CraneExpected<pid_t>> elem;
  while (m_check_task_status_queue_.try_dequeue(elem)) {
    if (m_instance_) {
      if (!m_instance_->GetPid())
        // Launch failed
        elem.set_value(std::unexpected(CraneErr::kNonExistent));
      else {
        elem.set_value(m_instance_->GetPid());
      }
    } else {
      elem.set_value(std::unexpected(CraneErr::kNonExistent));
    }
  }
}

void TaskManager::EvGrpcExecuteTaskCb_() {
  if (m_instance_ != nullptr) {
    CRANE_ERROR("Duplicated ExecuteTask request for task #{}. Ignore it.",
                g_config.JobId);
    return;
  }
  struct ExecuteTaskElem elem;
  while (m_grpc_execute_task_queue_.try_dequeue(elem)) {
    m_instance_ = std::move(elem.instance);

    // Add a timer to limit the execution time of a task.
    // Note: event_new and event_add in this function is not thread safe,
    //       so we move it outside the multithreading part.
    int64_t sec = m_instance_->task.time_limit().seconds();
    AddTerminationTimer_(m_instance_.get(), sec);
    CRANE_TRACE("Add a timer of {} seconds", sec);

    LaunchExecution_();
    if (!m_instance_->GetPid()) {
      CRANE_WARN("[task #{}] Failed to launch process.]",
                 m_instance_->task.task_id());
      elem.pid_prom.set_value(std::unexpected(CraneErr::kGenericFailure));
    } else {
      elem.pid_prom.set_value(m_instance_->GetPid());
    }
  }
}

}  // namespace Supervisor