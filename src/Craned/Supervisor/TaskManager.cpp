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

bool StepSpec::IsBatch() const { return spec.type() == crane::grpc::Batch; }

bool StepSpec::IsCrun() const {
  return spec.type() == crane::grpc::Interactive &&
         spec.interactive_meta().interactive_type() == crane::grpc::Crun;
}

bool StepSpec::IsCalloc() const {
  return spec.type() == crane::grpc::Interactive &&
         spec.interactive_meta().interactive_type() == crane::grpc::Calloc;
}

ExecutionInterface::~ExecutionInterface() {
  if (termination_timer) {
    termination_timer->close();
  }

  if (step_spec->IsCrun()) {
    auto* crun_meta = GetCrunMeta();

    close(crun_meta->task_input_fd);
    // For crun pty job, avoid close same fd twice
    if (crun_meta->task_output_fd != crun_meta->task_input_fd)
      close(crun_meta->task_output_fd);

    if (!crun_meta->x11_auth_path.empty() &&
        !absl::EndsWith(crun_meta->x11_auth_path, "XXXXXX")) {
      std::error_code ec;
      bool ok = std::filesystem::remove(crun_meta->x11_auth_path, ec);
      if (!ok)
        CRANE_ERROR("Failed to remove x11 auth {} for task #{}: {}",
                    crun_meta->x11_auth_path, this->step_spec->spec.task_id(),
                    ec.message());
    }
  }
}

void ExecutionInterface::TaskProcStopped() {
  auto ok_to_free = step_spec->cfored_client->TaskProcessStop(m_pid_);
  if (ok_to_free) {
    CRANE_TRACE("It's ok to unregister task #{}", g_config.JobId);
    step_spec->cfored_client->TaskEnd(m_pid_);
  }
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
    resolved_path =
        resolved_path / fmt::format("Crane-{}.out", step_spec->spec.task_id());

  std::string resolved_path_pattern = std::move(resolved_path.string());
  absl::StrReplaceAll({{"%j", std::to_string(step_spec->spec.task_id())},
                       {"%u", pwd.Username()},
                       {"%x", step_spec->spec.name()}},
                      &resolved_path_pattern);

  return resolved_path_pattern;
}

EnvMap ExecutionInterface::GetChildProcessEnv_() const {
  std::unordered_map<std::string, std::string> env_map;

  // Job env from CraneD
  for (auto& [name, value] : g_config.JobEnv) {
    env_map.emplace(name, value);
  }

  // Crane env will override user task env;
  for (auto& [name, value] : this->step_spec->spec.env()) {
    env_map.emplace(name, value);
  }

  if (this->step_spec->spec.get_user_env()) {
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
                  absl::StrJoin(this->step_spec->spec.allocated_nodes(), ";"));
  env_map.emplace("CRANE_EXCLUDES",
                  absl::StrJoin(this->step_spec->spec.excludes(), ";"));
  env_map.emplace("CRANE_JOB_NAME", this->step_spec->spec.name());
  env_map.emplace("CRANE_ACCOUNT", this->step_spec->spec.account());
  env_map.emplace("CRANE_PARTITION", this->step_spec->spec.partition());
  env_map.emplace("CRANE_QOS", this->step_spec->spec.qos());

  if (this->step_spec->IsCrun()) {
    auto const& ia_meta = this->step_spec->spec.interactive_meta();
    if (!ia_meta.term_env().empty())
      env_map.emplace("TERM", ia_meta.term_env());

    if (ia_meta.x11()) {
      auto const& x11_meta = ia_meta.x11_meta();

      std::string target =
          ia_meta.x11_meta().enable_forwarding() ? "" : x11_meta.target();
      env_map["DISPLAY"] =
          fmt::format("{}:{}", target, this->GetCrunMeta()->x11_port - 6000);
      env_map["XAUTHORITY"] = this->GetCrunMeta()->x11_auth_path;
    }
  }

  int64_t time_limit_sec = this->step_spec->spec.time_limit().seconds();
  int64_t hours = time_limit_sec / 3600;
  int64_t minutes = (time_limit_sec % 3600) / 60;
  int64_t seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_map.emplace("CRANE_TIMELIMIT", time_limit);
  return env_map;
}

CraneErrCode ExecutionInterface::SetupCrunX11_() {
  auto* inst_crun_meta = this->GetCrunMeta();
  const PasswordEntry& pwd_entry = this->pwd;

  inst_crun_meta->x11_auth_path =
      fmt::sprintf("%s/.crane/xauth/.Xauthority-XXXXXX", pwd_entry.HomeDir());

  bool ok = util::os::CreateFoldersForFileEx(
      inst_crun_meta->x11_auth_path, pwd_entry.Uid(), pwd_entry.Gid(), 0700);
  if (!ok) {
    CRANE_ERROR("Failed to create xauth source file for task #{}",
                this->step_spec->spec.task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Default file permission is 0600.
  int xauth_fd = mkstemp(inst_crun_meta->x11_auth_path.data());
  if (xauth_fd == -1) {
    CRANE_ERROR("mkstemp() for xauth file failed: {}\n", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  int ret = fchown(xauth_fd, this->pwd.Uid(), this->pwd.Gid());
  if (ret == -1) {
    CRANE_ERROR("fchown() for xauth file failed: {}\n", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  return CraneErrCode::SUCCESS;
}

CraneExpected<pid_t> ExecutionInterface::Fork_(bool* launch_pty,
                                               std::vector<int>* to_crun_pipe,
                                               std::vector<int>* from_crun_pipe,
                                               int* crun_pty_fd) {
  if (step_spec->IsCrun()) {
    auto* meta = dynamic_cast<CrunMetaInExecution*>(m_meta_.get());
    *launch_pty = step_spec->spec.interactive_meta().pty();
    CRANE_DEBUG("Launch crun task #{} pty: {}", step_spec->spec.task_id(),
                *launch_pty);

    if (pipe(to_crun_pipe->data()) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  step_spec->spec.task_id(), strerror(errno));
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }
    if (pipe(from_crun_pipe->data()) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  step_spec->spec.task_id(), strerror(errno));
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }
    meta->task_input_fd = to_crun_pipe->at(1);
    meta->task_output_fd = from_crun_pipe->at(0);
    if (*launch_pty) {
      return forkpty(crun_pty_fd, nullptr, nullptr, nullptr);
    } else {
      return fork();
    }
  } else {
    return fork();
  }
}

uint16_t ExecutionInterface::SetupCrunMsgFwd_(
    bool launch_pty, const std::vector<int>& to_crun_pipe,
    const std::vector<int>& from_crun_pipe, int crun_pty_fd) {
  auto* meta = dynamic_cast<CrunMetaInExecution*>(m_meta_.get());
  if (launch_pty) {
    close(meta->task_input_fd);
    close(meta->task_output_fd);
    meta->task_input_fd = crun_pty_fd;
    meta->task_output_fd = crun_pty_fd;
  }
  bool x11_enable_fwd =
      step_spec->spec.interactive_meta().x11() &&
      step_spec->spec.interactive_meta().x11_meta().enable_forwarding();
  auto port = step_spec->cfored_client->SetUpTaskFwd(
      m_pid_, meta->task_input_fd, meta->task_output_fd, launch_pty,
      x11_enable_fwd);
  close(to_crun_pipe[0]);
  close(from_crun_pipe[1]);
  meta->x11_port = x11_enable_fwd
                       ? port
                       : step_spec->spec.interactive_meta().x11_meta().port();
  return meta->x11_port;
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

CraneErrCode ExecutionInterface::SetChildProcessProperty_() {
  // TODO: Add all other supplementary groups.
  // Currently we only set the primary gid and the egid when task was
  // submitted.
  std::vector<gid_t> gids;
  if (step_spec->spec.gid() != pwd.Gid())
    gids.emplace_back(step_spec->spec.gid());
  gids.emplace_back(pwd.Gid());

  int rc = setgroups(gids.size(), gids.data());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setgroups() failed: {}\n",
               step_spec->spec.task_id(), strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  rc = setresgid(step_spec->spec.gid(), step_spec->spec.gid(),
                 step_spec->spec.gid());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setegid() failed: {}\n",
               step_spec->spec.task_id(), strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  rc = setresuid(pwd.Uid(), pwd.Uid(), pwd.Uid());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: seteuid() failed: {}\n",
               step_spec->spec.task_id(), strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  const std::string& cwd = step_spec->spec.cwd();
  rc = chdir(cwd.c_str());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: chdir to {}. {}\n", cwd.c_str(),
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Set pgid to the pid of task root process.
  setpgid(0, 0);

  return CraneErrCode::SUCCESS;
}

CraneErrCode ExecutionInterface::SetChildProcessBatchFd_() {
  int stdout_fd, stderr_fd;

  auto* meta = dynamic_cast<BatchMetaInExecution*>(m_meta_.get());
  const std::string& stdout_file_path = meta->parsed_output_file_pattern;
  const std::string& stderr_file_path = meta->parsed_error_file_pattern;

  stdout_fd = open(stdout_file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (stdout_fd == -1) {
    fmt::print(stderr, "[Subprocess] Error: open {}. {}\n", stdout_file_path,
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
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
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
    dup2(stderr_fd, 2);  // stderr -> error file
    close(stderr_fd);
  }
  close(stdout_fd);

  return CraneErrCode::SUCCESS;
}

void ExecutionInterface::SetupChildProcessCrunFd_(
    bool launch_pty, const std::vector<int>& to_crun_pipe,
    const std::vector<int>& from_crun_pipe, int crun_pty_fd) {
  if (!launch_pty) {
    dup2(to_crun_pipe[0], STDIN_FILENO);
    dup2(from_crun_pipe[1], STDOUT_FILENO);
    dup2(from_crun_pipe[1], STDERR_FILENO);
  }
  close(to_crun_pipe[0]);
  close(to_crun_pipe[1]);
  close(from_crun_pipe[0]);
  close(from_crun_pipe[1]);
}

void ExecutionInterface::SetupChildProcessCrunX11_(uint16_t port) {
  auto* inst_crun_meta = this->GetCrunMeta();
  const auto& proto_x11_meta =
      this->step_spec->spec.interactive_meta().x11_meta();

  // Overwrite x11_port with real value from parent process.
  inst_crun_meta->x11_port = port;

  std::string x11_target = proto_x11_meta.enable_forwarding()
                               ? g_config.CranedIdOfThisNode
                               : proto_x11_meta.target();
  std::string x11_disp_fmt =
      proto_x11_meta.enable_forwarding() ? "%s/unix:%u" : "%s:%u";

  std::string display =
      fmt::sprintf(x11_disp_fmt, x11_target, inst_crun_meta->x11_port - 6000);

  std::vector<const char*> xauth_argv{
      "/usr/bin/xauth",
      "-v",
      "-f",
      inst_crun_meta->x11_auth_path.c_str(),
      "add",
      display.c_str(),
      "MIT-MAGIC-COOKIE-1",
      proto_x11_meta.cookie().c_str(),
  };
  std::string xauth_cmd = absl::StrJoin(xauth_argv, ",");

  xauth_argv.push_back(nullptr);

  subprocess_s subprocess{};
  int result = subprocess_create(xauth_argv.data(), 0, &subprocess);
  if (0 != result) {
    fmt::print(stderr,
               "[Craned Subprocess] xauth subprocess creation failed: {}.\n",
               strerror(errno));
    std::abort();
  }

  auto buf = std::make_unique<char[]>(4096);
  std::string xauth_stdout_str, xauth_stderr_str;

  std::FILE* cmd_fd = subprocess_stdout(&subprocess);
  while (std::fgets(buf.get(), 4096, cmd_fd) != nullptr)
    xauth_stdout_str.append(buf.get());

  cmd_fd = subprocess_stderr(&subprocess);
  while (std::fgets(buf.get(), 4096, cmd_fd) != nullptr)
    xauth_stderr_str.append(buf.get());

  if (0 != subprocess_join(&subprocess, &result))
    fmt::print(stderr, "[Craned Subprocess] xauth join failed.\n");

  if (0 != subprocess_destroy(&subprocess))
    fmt::print(stderr, "[Craned Subprocess] xauth destroy failed.\n");

  if (result != 0) {
    fmt::print(stderr, "[Craned Subprocess] xauth return with {}.\n", result);
    fmt::print(stderr, "[Craned Subprocess] xauth stdout: {}\n",
               xauth_stdout_str);
    fmt::print(stderr, "[Craned Subprocess] xauth stderr: {}\n",
               xauth_stderr_str);
  }
}

CraneErrCode ExecutionInterface::SetChildProcessEnv_() const {
  if (clearenv())
    fmt::print(stderr, "[Subprocess] Warning: clearenv() failed.\n");

  for (const auto& [name, value] : m_env_)
    if (setenv(name.c_str(), value.c_str(), 1))
      fmt::print(stderr, "[Subprocess] Warning: setenv() for {}={} failed.\n",
                 name, value);

  return CraneErrCode::SUCCESS;
}

std::vector<std::string> ExecutionInterface::GetChildProcessExecArgv_() const {
  // "bash (--login) -c 'm_executable_ [m_arguments_...]'"
  std::vector<std::string> argv;
  argv.emplace_back("/bin/bash");
  if (step_spec->spec.get_user_env()) {
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
                       {"%j", std::to_string(step_spec->spec.task_id())},
                       {"%x", step_spec->spec.name()},
                       {"%u", pwd.Username()},
                       {"%U", std::to_string(pwd.Uid())}},
                      &parsed_cmd);
  return parsed_cmd;
}

CraneErrCode ContainerInstance::ModifyOCIBundleConfig_(
    const std::string& src, const std::string& dst) const {
  using json = nlohmann::json;

  // Check if bundle is valid
  auto src_config = std::filesystem::path(src) / "config.json";
  auto src_rootfs = std::filesystem::path(src) / "rootfs";
  if (!std::filesystem::exists(src_config) ||
      !std::filesystem::exists(src_rootfs)) {
    CRANE_ERROR("Bundle provided by task #{} not exists : {}",
                step_spec->spec.task_id(), src_config.string());
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  std::ifstream fin{src_config};
  if (!fin) {
    CRANE_ERROR("Failed to open bundle config provided by task #{}: {}",
                step_spec->spec.task_id(), src_config.string());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  json config = json::parse(fin, nullptr, false);
  if (config.is_discarded()) {
    CRANE_ERROR("Bundle config provided by task #{} is invalid: {}",
                step_spec->spec.task_id(), src_config.string());
    return CraneErrCode::ERR_INVALID_PARAM;
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
    if (step_spec->IsBatch() &&
        !step_spec->spec.batch_meta().interpreter().empty())
      real_exe = step_spec->spec.batch_meta().interpreter();

    std::vector<std::string> args = absl::StrSplit(real_exe, ' ');
    args.emplace_back(std::move(parsed_sh_mounted_path));
    process["args"] = args;
  } catch (json::exception& e) {
    CRANE_ERROR("Failed to generate bundle config for task #{}: {}",
                step_spec->spec.task_id(), e.what());
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  // Write the modified config
  auto dst_config = std::filesystem::path(dst) / "config.json";
  std::ofstream fout{dst_config};
  if (!fout) {
    CRANE_ERROR("Failed to write bundle config for task #{}: {}",
                step_spec->spec.task_id(), dst_config.string());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  fout << config.dump(4);
  fout.flush();

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Prepare() {
  // Generate path and params.
  m_temp_path_ =
      g_config.Container.TempDir / fmt::format("{}", step_spec->spec.task_id());
  m_bundle_path_ = step_spec->spec.container();
  m_executable_ = ParseOCICmdPattern_(g_config.Container.RuntimeRun);
  m_env_ = GetChildProcessEnv_();
  // m_arguments_ is not applicable for container tasks.

  // Check relative path
  if (m_bundle_path_.is_relative())
    m_bundle_path_ = step_spec->spec.cwd() / m_bundle_path_;

  // Write script into the temp folder
  auto sh_path =
      m_temp_path_ / fmt::format("Crane-{}.sh", step_spec->spec.task_id());
  if (!util::os::CreateFoldersForFile(sh_path)) {
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}",
                step_spec->spec.task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (step_spec->IsBatch())
    fputs(step_spec->spec.batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(step_spec->spec.interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

  // Write meta
  if (step_spec->IsBatch()) {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchMetaInExecution>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        step_spec->spec.batch_meta().output_file_pattern(),
        step_spec->spec.cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!step_spec->spec.batch_meta().error_file_pattern().empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          step_spec->spec.batch_meta().error_file_pattern(),
          step_spec->spec.cwd());
    }

    m_meta_ = std::move(meta);
  } else {
    m_meta_ = std::make_unique<CrunMetaInExecution>();
  }
  m_meta_->parsed_sh_script_path = sh_path;

  // Modify bundle
  auto err = ModifyOCIBundleConfig_(m_bundle_path_, m_temp_path_);
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR("Failed to modify OCI bundle config for task #{}",
                step_spec->spec.task_id());
    return err;
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Spawn() {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  std::vector<int> to_crun_pipe(2, -1);
  std::vector<int> from_crun_pipe(2, -1);

  if (this->step_spec->IsCrun() &&
      this->step_spec->spec.interactive_meta().x11()) {
    auto err = SetupCrunX11_();
    if (err != CraneErrCode::SUCCESS) {
      CRANE_WARN("Failed to setup crun X11 forwarding.");
      return err;
    }
  }

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  bool launch_pty{false};
  int crun_pty_fd;
  auto pid_expt =
      Fork_(&launch_pty, &to_crun_pipe, &from_crun_pipe, &crun_pty_fd);
  if (!pid_expt) return pid_expt.error();

  pid_t child_pid = pid_expt.value();

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", step_spec->spec.task_id(),
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    if (step_spec->IsCrun()) {
      auto& proto_ia_meta = step_spec->spec.interactive_meta();
      auto port = SetupCrunMsgFwd_(launch_pty, to_crun_pipe, from_crun_pipe,
                                   crun_pty_fd);
      msg.set_x11_port(port);
      CRANE_TRACE("Crun task x11 enabled: {}, forwarding: {}, port: {}",
                  proto_ia_meta.x11(),
                  proto_ia_meta.x11_meta().enable_forwarding(),
                  this->GetCrunMeta()->x11_port);
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                step_spec->spec.task_id());

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
                  child_pid, step_spec->spec.task_id(),
                  strerror(ostream.GetErrno()));
      close(ctrl_fd);

      // Communication failure caused by process crash or grpc error.
      // Since now the parent cannot ask the child
      // process to commit suicide, kill child process here and just return.
      // The child process will be reaped in SIGCHLD handler and
      // thus only ONE TaskStatusChange will be triggered!
      err_before_exec = CraneErrCode::ERR_PROTOBUF;
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    ok = ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream,
                                          nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("Socket child endpoint failed: {}",
                    strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("False from subprocess {} of task #{}", child_pid,
                    step_spec->spec.task_id());
      close(ctrl_fd);

      // See comments above.
      err_before_exec = CraneErrCode::ERR_PROTOBUF;
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    close(ctrl_fd);
    return CraneErrCode::SUCCESS;

  } else {  // Child proc
    SetChildProcessSignalHandler_();

    CraneErrCode err = SetChildProcessProperty_();
    if (err != CraneErrCode::SUCCESS) std::abort();

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

    if (step_spec->IsBatch()) {
      SetChildProcessBatchFd_();
    } else if (step_spec->IsCrun()) {
      SetupChildProcessCrunFd_(launch_pty, to_crun_pipe, from_crun_pipe,
                               crun_pty_fd);
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
    if (step_spec->IsBatch()) close(0);
    util::os::CloseFdFrom(3);

    // TODO: X11 in Container (feasible?)
    // SetupChildProcessCrunX11_(GetCrunMeta()->x11_port);

    // Set env just in case OCI requires some.
    // Real env in container is handled in ModifyOCIBundle_
    err = SetChildProcessEnv_();
    if (err != CraneErrCode::SUCCESS) {
      fmt::print(stderr, "[Subprocess] Error: Failed to set environment ");
    }

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

CraneErrCode ContainerInstance::Kill(int signum) {
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
          step_spec->spec.task_id(), cmd);
    }

  ContainerDelete:
    // Delete the container
    // Note: Admin could choose if --force is configured or not.
    cmd = ParseOCICmdPattern_(g_config.Container.RuntimeDelete);
    rc = system(cmd.c_str());
    if (rc) {
      CRANE_TRACE(
          "[Subprocess] Failed to delete container for task #{}: error in {}",
          step_spec->spec.task_id(), cmd);
    }

  ProcessKill:
    // Kill runc process as the last resort.
    // Note: If runc is launched in `detached` mode, this will not work.
    rc = kill(-m_pid_, signum);
    if (rc && (errno != ESRCH)) {
      CRANE_TRACE("[Subprocess] Failed to kill pid {}. error: {}", m_pid_,
                  strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }

    return CraneErrCode::SUCCESS;
  }

  return CraneErrCode::ERR_NON_EXISTENT;
}

CraneErrCode ContainerInstance::Cleanup() {
  if (step_spec->IsBatch() || step_spec->IsCrun()) {
    if (!m_temp_path_.empty())
      g_thread_pool->detach_task(
          [p = m_temp_path_]() { util::os::DeleteFolders(p); });
  }

  // Dummy return
  return CraneErrCode::SUCCESS;
}

CraneErrCode ProcessInstance::Prepare() {
  // Write script content into file
  auto sh_path =
      g_config.CraneScriptDir / fmt::format("Crane-{}.sh", g_config.JobId);

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}",
                step_spec->spec.task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (step_spec->IsBatch())
    fputs(step_spec->spec.batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(step_spec->spec.interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));

  // Write m_meta_
  if (step_spec->IsBatch()) {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchMetaInExecution>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        step_spec->spec.batch_meta().output_file_pattern(),
        step_spec->spec.cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!step_spec->spec.batch_meta().error_file_pattern().empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          step_spec->spec.batch_meta().error_file_pattern(),
          step_spec->spec.cwd());
    }

    m_meta_ = std::move(meta);
  } else {
    m_meta_ = std::make_unique<CrunMetaInExecution>();
  }

  m_meta_->parsed_sh_script_path = sh_path;

  // If interpreter is not set, let system decide.
  m_executable_ = sh_path.string();
  if (step_spec->IsBatch() &&
      !step_spec->spec.batch_meta().interpreter().empty()) {
    m_executable_ = fmt::format(
        "{} {}", step_spec->spec.batch_meta().interpreter(), sh_path.string());
  }

  m_env_ = GetChildProcessEnv_();
  // TODO: Currently we don't support arguments in batch scripts.
  // m_arguments_ = std::list<std::string>{};

  return CraneErrCode::SUCCESS;
}

CraneErrCode ProcessInstance::Spawn() {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  std::vector<int> to_crun_pipe(2, -1);
  std::vector<int> from_crun_pipe(2, -1);

  if (this->step_spec->IsCrun() &&
      this->step_spec->spec.interactive_meta().x11()) {
    auto err = SetupCrunX11_();
    if (err != CraneErrCode::SUCCESS) {
      CRANE_WARN("Failed to setup crun X11 forwarding.");
      return err;
    }
  }

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  bool launch_pty{false};
  int crun_pty_fd;
  auto pid_expt =
      Fork_(&launch_pty, &to_crun_pipe, &from_crun_pipe, &crun_pty_fd);
  if (!pid_expt) return pid_expt.error();

  pid_t child_pid = pid_expt.value();

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", step_spec->spec.task_id(),
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                step_spec->spec.task_id(), m_pid_);
    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    if (step_spec->IsCrun()) {
      auto& proto_ia_meta = step_spec->spec.interactive_meta();
      auto port = SetupCrunMsgFwd_(launch_pty, to_crun_pipe, from_crun_pipe,
                                   crun_pty_fd);
      msg.set_x11_port(port);
      CRANE_TRACE("Crun task x11 enabled: {}, forwarding: {}, port: {}",
                  proto_ia_meta.x11(),
                  proto_ia_meta.x11_meta().enable_forwarding(),
                  this->GetCrunMeta()->x11_port);
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                step_spec->spec.task_id());

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
                  child_pid, step_spec->spec.task_id(),
                  strerror(ostream.GetErrno()));
      close(ctrl_fd);

      // Communication failure caused by process crash or grpc error.
      // Since now the parent cannot ask the child
      // process to commit suicide, kill child process here and just return.
      // The child process will be reaped in SIGCHLD handler and
      // thus only ONE TaskStatusChange will be triggered!
      err_before_exec = CraneErrCode::ERR_PROTOBUF;
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    ok = ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream,
                                          nullptr);
    if (!ok || !msg.ok()) {
      if (!ok)
        CRANE_ERROR("Socket child endpoint failed: {}",
                    strerror(istream.GetErrno()));
      if (!msg.ok())
        CRANE_ERROR("False from subprocess {} of task #{}", child_pid,
                    step_spec->spec.task_id());
      close(ctrl_fd);

      // See comments above.
      err_before_exec = CraneErrCode::ERR_PROTOBUF;
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    close(ctrl_fd);
    return CraneErrCode::SUCCESS;

  } else {  // Child proc
    SetChildProcessSignalHandler_();

    CraneErrCode err = SetChildProcessProperty_();
    if (err != CraneErrCode::SUCCESS) std::abort();

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

    if (step_spec->IsBatch()) {
      SetChildProcessBatchFd_();
    } else if (step_spec->IsCrun()) {
      SetupChildProcessCrunFd_(launch_pty, to_crun_pipe, from_crun_pipe,
                               crun_pty_fd);
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
    if (step_spec->IsBatch()) close(0);
    util::os::CloseFdFrom(3);

    if (step_spec->IsCrun() && step_spec->spec.interactive_meta().x11())
      SetupChildProcessCrunX11_(GetCrunMeta()->x11_port);

    // Apply environment variables
    err = SetChildProcessEnv_();
    if (err != CraneErrCode::SUCCESS) {
      fmt::print(stderr, "[Subprocess] Error: Failed to set environment ");
    }

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

CraneErrCode ProcessInstance::Kill(int signum) {
  if (m_pid_) {
    CRANE_TRACE("Killing pid {} with signal {}", m_pid_, signum);

    // Send the signal to the whole process group.
    int err = kill(-m_pid_, signum);

    if (err == 0)
      return CraneErrCode::SUCCESS;
    else {
      CRANE_TRACE("kill failed. error: {}", strerror(errno));
      return CraneErrCode::ERR_GENERIC_FAILURE;
    }
  }

  return CraneErrCode::ERR_NON_EXISTENT;
}

CraneErrCode ProcessInstance::Cleanup() {
  if (step_spec->IsBatch() || step_spec->IsCrun()) {
    const std::string& path = m_meta_->parsed_sh_script_path;
    if (!path.empty())
      g_thread_pool->detach_task([p = path]() { util::os::DeleteFile(p); });
  }

  // Dummy return
  return CraneErrCode::SUCCESS;
}

TaskManager::TaskManager()
    : m_supervisor_exit_(false), m_step_spec_(g_config.StepSpec, nullptr) {
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
             m_instance_->step_spec->spec.task_id());

  switch (m_instance_->err_before_exec) {
  case CraneErrCode::ERR_PROTOBUF:
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeSpawnProcessFail,
                              std::nullopt);
    break;

  case CraneErrCode::ERR_CGROUP:
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeCgroupError, std::nullopt);
    break;

  default:
    break;
  }

  ProcSigchldInfo& sigchld_info = m_instance_->sigchld_info;
  if (m_step_spec_.IsBatch() || m_step_spec_.IsCrun()) {
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
  // No need to free the TaskInstance structure,will destruct with TaskMgr.
  if (!orphaned)
    g_craned_client->TaskStatusChangeAsync(new_status, exit_code,
                                           std::move(reason));
}

std::future<CraneExpected<pid_t>> TaskManager::ExecuteTaskAsync(
    const StepToSuper& spec) {
  std::promise<CraneExpected<pid_t>> pid_promise;
  auto pid_future = pid_promise.get_future();
  auto elem = ExecuteTaskElem{.pid_prom = std::move(pid_promise)};

  if (spec.container().length() != 0) {
    // Container
    if (!g_config.Container.Enabled) {
      // Container job is not supported in this node.
      elem.pid_prom.set_value(CraneErrCode::ERR_INVALID_PARAM);
      return pid_future;
    }

    elem.instance = std::make_unique<ContainerInstance>(&m_step_spec_);
  } else {
    // Process
    elem.instance = std::make_unique<ProcessInstance>(&m_step_spec_);
  }

  m_grpc_execute_task_queue_.enqueue(std::move(elem));
  m_grpc_execute_task_async_handle_->send();
  return pid_future;
}

void TaskManager::LaunchExecution_() {
  m_instance_->pwd.Init(m_instance_->step_spec->spec.uid());
  if (!m_instance_->pwd.Valid()) {
    CRANE_DEBUG("[Job #{}] Failed to look up password entry for uid {} of task",
                m_instance_->step_spec->spec.task_id(),
                m_instance_->step_spec->spec.uid());
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodePermissionDenied,
        fmt::format(
            "[Job #{}] Failed to look up password entry for uid {} of task",
            m_instance_->step_spec->spec.task_id(),
            m_instance_->step_spec->spec.uid()));
    return;
  }

  // Calloc tasks have no scripts to run. Just return.
  if (m_step_spec_.IsCalloc()) return;

  // Prepare for execution
  CraneErrCode err = m_instance_->Prepare();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_DEBUG("[Job #{}] Failed to prepare task",
                m_step_spec_.spec.task_id());
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeFileNotFound,
                              fmt::format("Failed to prepare task"));
    return;
  }

  CRANE_TRACE("[Job #{}] Spawning process in task",
              m_step_spec_.spec.task_id());
  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  if (m_step_spec_.IsCrun()) {
    auto cfored_client = std::make_unique<CforedClient>();
    cfored_client->InitChannelAndStub(
        m_step_spec_.spec.interactive_meta().cfored_name());
    m_step_spec_.cfored_client = std::move(cfored_client);
  }
  err = m_instance_->Spawn();
  if (err != CraneErrCode::SUCCESS) {
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

std::future<CraneErrCode> TaskManager::ChangeTaskTimeLimitAsync(
    absl::Duration time_limit) {
  std::promise<CraneErrCode> ok_promise;
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

      if (m_step_spec_.IsCrun())
        // TaskStatusChange of a crun task is triggered in
        // CforedManager.
        m_instance_->TaskProcStopped();
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

  if (m_step_spec_.IsBatch()) {
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

    int sig = SIGTERM;  // For BatchTask
    if (m_step_spec_.IsCrun()) sig = SIGHUP;

    if (elem.mark_as_orphaned) m_instance_->orphaned = true;
    if (elem.terminated_by_timeout) m_instance_->terminated_by_timeout = true;
    if (elem.terminated_by_user) {
      // If termination request is sent by user, send SIGKILL to ensure that
      // even freezing processes will be terminated immediately.
      sig = SIGKILL;
      m_instance_->cancelled_by_user = true;
    }

    if (m_instance_->GetPid()) {
      // For an Interactive task with a process running or a Batch task, we
      // just send a kill signal here.
      m_instance_->Kill(sig);
    } else if (m_step_spec_.spec.type() == crane::grpc::Interactive) {
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
      elem.ok_prom.set_value(CraneErrCode::ERR_NON_EXISTENT);
      continue;
    }

    // Delete the old timer.
    DelTerminationTimer_(m_instance_.get());

    absl::Time start_time =
        absl::FromUnixSeconds(m_step_spec_.spec.start_time().seconds());
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
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);
  }
}

void TaskManager::EvCleanCheckTaskStatusQueueCb_() {
  std::promise<CraneExpected<pid_t>> elem;
  while (m_check_task_status_queue_.try_dequeue(elem)) {
    if (m_instance_) {
      if (!m_instance_->GetPid()) {
        CRANE_DEBUG("CheckTaskStatus: task launch failed.");
        // Launch failed
        elem.set_value(std::unexpected(CraneErrCode::ERR_NON_EXISTENT));
      } else {
        elem.set_value(m_instance_->GetPid());
      }
    } else {
      CRANE_DEBUG("CheckTaskStatus: task #{} does not exist.", g_config.JobId);
      elem.set_value(std::unexpected(CraneErrCode::ERR_NON_EXISTENT));
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
    int64_t sec = m_step_spec_.spec.time_limit().seconds();
    AddTerminationTimer_(m_instance_.get(), sec);
    CRANE_TRACE("Add a timer of {} seconds", sec);

    LaunchExecution_();
    if (!m_instance_->GetPid()) {
      CRANE_WARN("[task #{}] Failed to launch process.",
                 m_step_spec_.spec.task_id());
      elem.pid_prom.set_value(
          std::unexpected(CraneErrCode::ERR_GENERIC_FAILURE));
    } else {
      elem.pid_prom.set_value(m_instance_->GetPid());
    }
  }
}

}  // namespace Supervisor