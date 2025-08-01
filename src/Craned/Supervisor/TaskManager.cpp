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

bool StepInstance::IsBatch() const {
  return GetStep().type() == crane::grpc::Batch;
}

bool StepInstance::IsCrun() const {
  return GetStep().type() == crane::grpc::Interactive &&
         GetStep().interactive_meta().interactive_type() == crane::grpc::Crun;
}

bool StepInstance::IsCalloc() const {
  return GetStep().type() == crane::grpc::Interactive &&
         GetStep().interactive_meta().interactive_type() == crane::grpc::Calloc;
}

ITaskInstance::~ITaskInstance() {
  if (termination_timer) {
    termination_timer->close();
  }

  if (m_parent_step_inst_->IsCrun()) {
    auto* crun_meta = GetCrunMeta();

    close(crun_meta->stdin_read);
    // For crun pty job, avoid close same fd twice
    if (crun_meta->stdout_read != crun_meta->stdin_read)
      close(crun_meta->stdout_read);

    if (!crun_meta->x11_auth_path.empty() &&
        !absl::EndsWith(crun_meta->x11_auth_path, "XXXXXX")) {
      std::error_code ec;
      bool ok = std::filesystem::remove(crun_meta->x11_auth_path, ec);
      if (!ok)
        CRANE_ERROR("Failed to remove x11 auth {} for task #{}: {}",
                    crun_meta->x11_auth_path,
                    this->m_parent_step_inst_->GetStep().task_id(),
                    ec.message());
    }
  }
}

EnvMap ITaskInstance::GetChildProcessEnv() const {
  std::unordered_map<std::string, std::string> env_map;

  // Job env from CraneD
  for (auto& [name, value] : g_config.JobEnv) {
    env_map.emplace(name, value);
  }

  // Crane env will override user task env;
  for (auto& [name, value] : this->m_parent_step_inst_->GetStep().env()) {
    env_map.emplace(name, value);
  }

  if (this->m_parent_step_inst_->GetStep().get_user_env()) {
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

  env_map.emplace(
      "CRANE_JOB_NODELIST",
      absl::StrJoin(this->m_parent_step_inst_->GetStep().allocated_nodes(),
                    ";"));
  env_map.emplace(
      "CRANE_EXCLUDES",
      absl::StrJoin(this->m_parent_step_inst_->GetStep().excludes(), ";"));
  env_map.emplace("CRANE_JOB_NAME",
                  this->m_parent_step_inst_->GetStep().name());
  env_map.emplace("CRANE_ACCOUNT",
                  this->m_parent_step_inst_->GetStep().account());
  env_map.emplace("CRANE_PARTITION",
                  this->m_parent_step_inst_->GetStep().partition());
  env_map.emplace("CRANE_QOS", this->m_parent_step_inst_->GetStep().qos());

  if (this->m_parent_step_inst_->IsCrun()) {
    auto const& ia_meta =
        this->m_parent_step_inst_->GetStep().interactive_meta();
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

  int64_t time_limit_sec =
      this->m_parent_step_inst_->GetStep().time_limit().seconds();
  int64_t hours = time_limit_sec / 3600;
  int64_t minutes = (time_limit_sec % 3600) / 60;
  int64_t seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_map.emplace("CRANE_TIMELIMIT", time_limit);
  return env_map;
}

std::string ITaskInstance::ParseFilePathPattern_(const std::string& pattern,
                                                 const std::string& cwd) const {
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
        resolved_path /
        fmt::format("Crane-{}.out", m_parent_step_inst_->GetStep().task_id());

  std::string resolved_path_pattern = std::move(resolved_path.string());
  absl::StrReplaceAll(
      {{"%j", std::to_string(m_parent_step_inst_->GetStep().task_id())},
       {"%u", pwd.Username()},
       {"%x", m_parent_step_inst_->GetStep().name()}},
      &resolved_path_pattern);

  return resolved_path_pattern;
}

CraneErrCode ITaskInstance::SetupCrunX11_() {
  auto* inst_crun_meta = this->GetCrunMeta();
  const PasswordEntry& pwd_entry = this->pwd;

  inst_crun_meta->x11_auth_path =
      fmt::sprintf("%s/.crane/xauth/.Xauthority-XXXXXX", pwd_entry.HomeDir());

  bool ok = util::os::CreateFoldersForFileEx(
      inst_crun_meta->x11_auth_path, pwd_entry.Uid(), pwd_entry.Gid(), 0700);
  if (!ok) {
    CRANE_ERROR("Failed to create xauth source file for task #{}",
                this->m_parent_step_inst_->GetStep().task_id());
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

CraneExpected<pid_t> ITaskInstance::ForkCrunAndInitMeta_() {
  if (!m_parent_step_inst_->IsCrun()) return fork();

  auto* meta = GetCrunInstanceMeta();
  meta->pty = m_parent_step_inst_->GetStep().interactive_meta().pty();
  CRANE_DEBUG("Launch crun task #{} pty: {}",
              m_parent_step_inst_->GetStep().task_id(), meta->pty);

  int to_crun_pipe[2], from_crun_pipe[2];
  int crun_pty_fd = -1;

  if (!meta->pty) {
    if (pipe(to_crun_pipe) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  m_parent_step_inst_->GetStep().task_id(), strerror(errno));
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }

    if (pipe(from_crun_pipe) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  m_parent_step_inst_->GetStep().task_id(), strerror(errno));
      close(to_crun_pipe[0]);
      close(to_crun_pipe[1]);
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }
  }

  pid_t pid = -1;
  if (meta->pty) {
    pid = forkpty(&crun_pty_fd, nullptr, nullptr, nullptr);

    if (pid > 0) {
      meta->stdin_read = crun_pty_fd;
      meta->stdout_read = crun_pty_fd;
      meta->stdin_write = crun_pty_fd;
      meta->stdout_write = crun_pty_fd;
    }
  } else {
    pid = fork();

    meta->stdin_write = to_crun_pipe[1];
    meta->stdin_read = to_crun_pipe[0];
    meta->stdout_write = from_crun_pipe[1];
    meta->stdout_read = from_crun_pipe[0];

    if (pid == -1) {
      close(to_crun_pipe[0]);
      close(to_crun_pipe[1]);
      close(from_crun_pipe[0]);
      close(from_crun_pipe[1]);
    }
  }

  return pid;
}

void ITaskInstance::SetupCrunFwdAtParent_(uint16_t* x11_port) {
  auto* meta = dynamic_cast<CrunInstanceMeta*>(m_meta_.get());

  if (!meta->pty) {
    // For non-pty tasks, pipe is used for stdin/stdout and one end should be
    // closed.
    close(meta->stdin_read);
    close(meta->stdout_write);
  }
  // For pty tasks, stdin_read and stdout_write are the same fd and should not
  // be closed.

  const auto& parent_step = m_parent_step_inst_->GetStep();
  auto* parent_cfored_client = m_parent_step_inst_->GetCforedClient();

  parent_cfored_client->InitFwdMetaAndUvStdoutFwdHandler(
      m_pid_, meta->stdin_write, meta->stdout_read, meta->pty);

  if (m_parent_step_inst_->RequiresX11()) {
    if (m_parent_step_inst_->RequiresX11Fwd())
      meta->x11_port = parent_cfored_client->InitUvX11FwdHandler(m_pid_);
    else
      meta->x11_port = parent_step.interactive_meta().x11_meta().port();

    if (x11_port != nullptr) *x11_port = meta->x11_port;

    CRANE_TRACE("Crun task x11 enabled. Forwarding: {}, X11 Port: {}",
                m_parent_step_inst_->RequiresX11Fwd(), meta->x11_port);
  }

  // TODO: It's ok here to start the uv loop thread, since currently 1 task only
  //  corresponds to 1 step.
  parent_cfored_client->StartUvLoopThread();
}

void ITaskInstance::ResetChildProcSigHandler_() {
  // Recover SIGPIPE default handler in from child processes.
  signal(SIGPIPE, SIG_DFL);
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

CraneErrCode ITaskInstance::SetChildProcessProperty_() {
  int ngroups = 0;
  // We should not check rc here. It must be -1.
  getgrouplist(pwd.Username().c_str(), m_parent_step_inst_->GetStep().gid(),
               nullptr, &ngroups);

  std::vector<gid_t> gids(ngroups);
  int rc =
      getgrouplist(pwd.Username().c_str(), m_parent_step_inst_->GetStep().gid(),
                   gids.data(), &ngroups);
  if (rc == -1) {
    fmt::print(stderr, "[Subproc] Error: getgrouplist() for user '{}'\n",
               pwd.Username());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (auto it = std::ranges::find(gids, m_parent_step_inst_->GetStep().gid());
      it != gids.begin()) {
    gids.erase(it);
    gids.insert(gids.begin(), m_parent_step_inst_->GetStep().gid());
  }

  if (!std::ranges::contains(gids, pwd.Gid())) gids.emplace_back(pwd.Gid());

  rc = setgroups(gids.size(), gids.data());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setgroups() failed: {}\n",
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  rc = setresgid(m_parent_step_inst_->GetStep().gid(),
                 m_parent_step_inst_->GetStep().gid(),
                 m_parent_step_inst_->GetStep().gid());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setegid() failed: {}\n",
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  rc = setresuid(pwd.Uid(), pwd.Uid(), pwd.Uid());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: seteuid() failed: {}\n",
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  const std::string& cwd = m_parent_step_inst_->GetStep().cwd();
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

CraneErrCode ITaskInstance::SetChildProcessBatchFd_() {
  int stdout_fd, stderr_fd;

  auto* meta = dynamic_cast<BatchInstanceMeta*>(m_meta_.get());
  const std::string& stdout_file_path = meta->parsed_output_file_pattern;
  const std::string& stderr_file_path = meta->parsed_error_file_pattern;

  int open_mode = m_parent_step_inst_->GetStep().batch_meta().open_mode_append()
                      ? O_APPEND
                      : O_TRUNC;
  stdout_fd =
      open(stdout_file_path.c_str(), O_RDWR | O_CREAT | open_mode, 0644);
  if (stdout_fd == -1) {
    fmt::print(stderr, "[Subprocess] Error: open {}. {}\n", stdout_file_path,
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  dup2(stdout_fd, 1);  // stdout -> output file

  if (stderr_file_path.empty()) {
    // if stderr file is not set, redirect stderr to stdout
    dup2(stdout_fd, 2);
  } else {
    stderr_fd =
        open(stderr_file_path.c_str(), O_RDWR | O_CREAT | open_mode, 0644);
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

void ITaskInstance::SetupCrunFwdAtChild_() {
  const auto* meta = GetCrunInstanceMeta();

  if (!meta->pty) {
    dup2(meta->stdin_read, STDIN_FILENO);
    dup2(meta->stdout_write, STDOUT_FILENO);
    dup2(meta->stdout_write, STDERR_FILENO);
  }
}

void ITaskInstance::SetupChildProcessCrunX11_() {
  auto* inst_crun_meta = this->GetCrunMeta();
  const auto& proto_x11_meta =
      this->m_parent_step_inst_->GetStep().interactive_meta().x11_meta();

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

CraneErrCode ITaskInstance::SetChildProcessEnv_() const {
  if (clearenv())
    fmt::print(stderr, "[Subprocess] Warning: clearenv() failed.\n");

  for (const auto& [name, value] : m_env_)
    if (setenv(name.c_str(), value.c_str(), 1))
      fmt::print(stderr, "[Subprocess] Warning: setenv() for {}={} failed.\n",
                 name, value);

  return CraneErrCode::SUCCESS;
}

std::vector<std::string> ITaskInstance::GetChildProcessExecArgv_() const {
  // "bash (--login) -c 'm_executable_ [m_arguments_...]'"
  std::vector<std::string> argv;
  argv.emplace_back("/bin/bash");
  if (m_parent_step_inst_->GetStep().get_user_env()) {
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
  absl::StrReplaceAll(
      {{"%b", m_temp_path_.string()},
       {"%j", std::to_string(m_parent_step_inst_->GetStep().task_id())},
       {"%x", m_parent_step_inst_->GetStep().name()},
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
                m_parent_step_inst_->GetStep().task_id(), src_config.string());
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  std::ifstream fin{src_config};
  if (!fin) {
    CRANE_ERROR("Failed to open bundle config provided by task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), src_config.string());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  json config = json::parse(fin, nullptr, false);
  if (config.is_discarded()) {
    CRANE_ERROR("Bundle config provided by task #{} is invalid: {}",
                m_parent_step_inst_->GetStep().task_id(), src_config.string());
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
    if (m_parent_step_inst_->IsBatch() &&
        !m_parent_step_inst_->GetStep().batch_meta().interpreter().empty())
      real_exe = m_parent_step_inst_->GetStep().batch_meta().interpreter();

    std::vector<std::string> args = absl::StrSplit(real_exe, ' ');
    args.emplace_back(std::move(parsed_sh_mounted_path));
    process["args"] = args;
  } catch (json::exception& e) {
    CRANE_ERROR("Failed to generate bundle config for task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), e.what());
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  // Write the modified config
  auto dst_config = std::filesystem::path(dst) / "config.json";
  std::ofstream fout{dst_config};
  if (!fout) {
    CRANE_ERROR("Failed to write bundle config for task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), dst_config.string());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  fout << config.dump(4);
  fout.flush();

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Prepare() {
  // Generate path and params.
  m_temp_path_ = g_config.Container.TempDir /
                 fmt::format("{}", m_parent_step_inst_->GetStep().task_id());
  m_bundle_path_ = m_parent_step_inst_->GetStep().container();
  m_executable_ = ParseOCICmdPattern_(g_config.Container.RuntimeRun);
  // FIXME: Some env like x11 port are assigned later in spawn.
  m_env_ = GetChildProcessEnv();
  // m_arguments_ is not applicable for container tasks.

  // Check relative path
  if (m_bundle_path_.is_relative())
    m_bundle_path_ = m_parent_step_inst_->GetStep().cwd() / m_bundle_path_;

  // Write script into the temp folder
  auto sh_path =
      m_temp_path_ /
      fmt::format("Crane-{}.sh", m_parent_step_inst_->GetStep().task_id());
  if (!util::os::CreateFoldersForFile(sh_path)) {
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}",
                m_parent_step_inst_->GetStep().task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (m_parent_step_inst_->IsBatch())
    fputs(m_parent_step_inst_->GetStep().batch_meta().sh_script().c_str(),
          fptr);
  else  // Crun
    fputs(m_parent_step_inst_->GetStep().interactive_meta().sh_script().c_str(),
          fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));
  if (chown(sh_path.c_str(), m_parent_step_inst_->GetStep().uid(),
            m_parent_step_inst_->GetStep().gid()) != 0) {
    CRANE_ERROR("Failed to change ownership of script file for task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), strerror(errno));
  }

  // Write meta
  if (m_parent_step_inst_->IsBatch()) {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchInstanceMeta>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        m_parent_step_inst_->GetStep().batch_meta().output_file_pattern(),
        m_parent_step_inst_->GetStep().cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!m_parent_step_inst_->GetStep()
             .batch_meta()
             .error_file_pattern()
             .empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          m_parent_step_inst_->GetStep().batch_meta().error_file_pattern(),
          m_parent_step_inst_->GetStep().cwd());
    }

    m_meta_ = std::move(meta);
  } else {
    m_meta_ = std::make_unique<CrunInstanceMeta>();
  }
  m_meta_->parsed_sh_script_path = sh_path;

  // Modify bundle
  auto err = ModifyOCIBundleConfig_(m_bundle_path_, m_temp_path_);
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR("Failed to modify OCI bundle config for task #{}",
                m_parent_step_inst_->GetStep().task_id());
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

  std::array<int, 2> ctrl_sock_pair{};  // Socket pair for passing control msg

  if (this->m_parent_step_inst_->IsCrun() &&
      this->m_parent_step_inst_->GetStep().interactive_meta().x11()) {
    auto err = SetupCrunX11_();
    if (err != CraneErrCode::SUCCESS) {
      CRANE_WARN("Failed to setup crun X11 forwarding.");
      return err;
    }
  }

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair.data()) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  pid_t child_pid = -1;
  if (m_parent_step_inst_->IsBatch())
    child_pid = fork();
  else {
    auto pid_expt = ForkCrunAndInitMeta_();
    if (!pid_expt) return pid_expt.error();

    child_pid = pid_expt.value();
  }

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), strerror(errno));
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

    if (m_parent_step_inst_->IsCrun()) {
      if (m_parent_step_inst_->RequiresX11()) {
        uint16_t x11_port;
        SetupCrunFwdAtParent_(&x11_port);
        msg.set_x11_port(x11_port);
      } else
        SetupCrunFwdAtParent_(nullptr);

      CRANE_DEBUG("Task #{} has initialized crun forwarding.",
                  m_parent_step_inst_->GetStep().task_id());
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                m_parent_step_inst_->GetStep().task_id());

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
                  child_pid, m_parent_step_inst_->GetStep().task_id(),
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
                    m_parent_step_inst_->GetStep().task_id());
      close(ctrl_fd);

      // See comments above.
      err_before_exec = CraneErrCode::ERR_PROTOBUF;
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    close(ctrl_fd);
    return CraneErrCode::SUCCESS;

  } else {  // Child proc
    ResetChildProcSigHandler_();

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

    if (m_parent_step_inst_->IsBatch()) {
      SetChildProcessBatchFd_();
    } else if (m_parent_step_inst_->IsCrun()) {
      SetupCrunFwdAtChild_();
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
    if (m_parent_step_inst_->IsBatch()) close(0);
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
  if (m_pid_ != 0) {
    // If m_pid_ not exists, no further operation.
    int rc = 0;
    std::array<char, 256> buffer{};
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
    // NOTE: It's admin's responsibility to ensure the command is safe.
    // Note: Signal is configured in config.yaml instead of in the param.
    cmd = ParseOCICmdPattern_(g_config.Container.RuntimeKill);
    rc = system(cmd.c_str());
    if (rc != 0) {
      CRANE_TRACE(
          "[Subprocess] Failed to kill container for task #{}: error in {}",
          m_parent_step_inst_->GetStep().task_id(), cmd);
    }

  ContainerDelete:
    // Delete the container
    // NOTE: Admin could choose if --force is configured or not.
    cmd = ParseOCICmdPattern_(g_config.Container.RuntimeDelete);
    rc = system(cmd.c_str());
    if (rc != 0) {
      CRANE_TRACE(
          "[Subprocess] Failed to delete container for task #{}: error in {}",
          m_parent_step_inst_->GetStep().task_id(), cmd);
    }

  ProcessKill:
    // Kill runc process as the last resort.
    // Note: If runc is launched in `detached` mode, this will not work.
    rc = kill(-m_pid_, signum);
    if ((rc != 0) && (errno != ESRCH)) {
      CRANE_TRACE("[Subprocess] Failed to kill pid {}. error: {}", m_pid_,
                  strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }

    return CraneErrCode::SUCCESS;
  }

  return CraneErrCode::ERR_NON_EXISTENT;
}

CraneErrCode ContainerInstance::Cleanup() {
  if (m_parent_step_inst_->IsBatch() || m_parent_step_inst_->IsCrun()) {
    if (!m_temp_path_.empty())
      g_thread_pool->detach_task(
          [p = m_temp_path_]() { util::os::DeleteFolders(p); });
  }

  // Dummy return
  return CraneErrCode::SUCCESS;
}

std::optional<const TaskExitInfo> ContainerInstance::HandleSigchld(pid_t pid,
                                                                   int status) {
  m_exit_info_.pid = pid;

  if (WIFEXITED(status)) {
    // Exited with status WEXITSTATUS(status)
    m_exit_info_.value = WEXITSTATUS(status);
    if (m_exit_info_.value > 128) {
      // OCI runtime may return 128 + signal number
      // See: https://tldp.org/LDP/abs/html/exitcodes.html
      m_exit_info_.is_terminated_by_signal = true;
      m_exit_info_.value -= 128;
    } else {
      // OCI runtime normal exiting
      m_exit_info_.is_terminated_by_signal = false;
    }

  } else if (WIFSIGNALED(status)) {
    // OCI runtime is forced to exit by signal
    // This is a undesired situation, but we should handle it gracefully.
    m_exit_info_.is_terminated_by_signal = true;
    m_exit_info_.value = WTERMSIG(status);
    CRANE_WARN("OCI runtime for task #{} is killed by signal {}.",
               m_parent_step_inst_->GetStep().task_id(), m_exit_info_.value);
  } else {
    CRANE_TRACE("Received SIGCHLD with status {} for task #{} but ignored.",
                status, m_parent_step_inst_->GetStep().task_id());
    return std::nullopt;
  }

  return std::optional<TaskExitInfo>{m_exit_info_};
}

CraneErrCode ProcInstance::Prepare() {
  // Write script content into file
  auto sh_path =
      g_config.CraneScriptDir / fmt::format("Crane-{}.sh", g_config.JobId);

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}",
                m_parent_step_inst_->GetStep().task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (m_parent_step_inst_->IsBatch())
    fputs(m_parent_step_inst_->GetStep().batch_meta().sh_script().c_str(),
          fptr);
  else  // Crun
    fputs(m_parent_step_inst_->GetStep().interactive_meta().sh_script().c_str(),
          fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));
  if (chown(sh_path.c_str(), m_parent_step_inst_->GetStep().uid(),
            m_parent_step_inst_->GetStep().gid()) != 0) {
    CRANE_ERROR("Failed to change ownership of script file for task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), strerror(errno));
  }

  // Write m_meta_
  if (m_parent_step_inst_->IsBatch()) {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchInstanceMeta>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        m_parent_step_inst_->GetStep().batch_meta().output_file_pattern(),
        m_parent_step_inst_->GetStep().cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!m_parent_step_inst_->GetStep()
             .batch_meta()
             .error_file_pattern()
             .empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          m_parent_step_inst_->GetStep().batch_meta().error_file_pattern(),
          m_parent_step_inst_->GetStep().cwd());
    }

    m_meta_ = std::move(meta);
  } else {
    m_meta_ = std::make_unique<CrunInstanceMeta>();
  }

  m_meta_->parsed_sh_script_path = sh_path;

  // If interpreter is not set, let system decide.
  m_executable_ = sh_path.string();
  if (m_parent_step_inst_->IsBatch() &&
      !m_parent_step_inst_->GetStep().batch_meta().interpreter().empty()) {
    m_executable_ = fmt::format(
        "{} {}", m_parent_step_inst_->GetStep().batch_meta().interpreter(),
        sh_path.string());
  }
  // TODO: Currently we don't support arguments in batch scripts.
  // m_arguments_ = std::list<std::string>{};

  return CraneErrCode::SUCCESS;
}

CraneErrCode ProcInstance::Spawn() {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  std::vector<int> to_crun_pipe(2, -1);
  std::vector<int> from_crun_pipe(2, -1);

  if (this->m_parent_step_inst_->IsCrun() &&
      this->m_parent_step_inst_->GetStep().interactive_meta().x11()) {
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

  pid_t child_pid = -1;
  if (m_parent_step_inst_->IsBatch())
    child_pid = fork();
  else {
    auto pid_expt = ForkCrunAndInitMeta_();
    if (!pid_expt) return pid_expt.error();

    child_pid = pid_expt.value();
  }

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}",
                m_parent_step_inst_->GetStep().task_id(), strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                m_parent_step_inst_->GetStep().task_id(), m_pid_);
    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    if (m_parent_step_inst_->IsCrun()) {
      if (m_parent_step_inst_->RequiresX11()) {
        uint16_t x11_port;
        SetupCrunFwdAtParent_(&x11_port);
        msg.set_x11_port(x11_port);
      } else
        SetupCrunFwdAtParent_(nullptr);

      CRANE_DEBUG("Task #{} has initialized crun forwarding.",
                  m_parent_step_inst_->GetStep().task_id());
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                m_parent_step_inst_->GetStep().task_id());

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
                  child_pid, m_parent_step_inst_->GetStep().task_id(),
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
                    m_parent_step_inst_->GetStep().task_id());
      close(ctrl_fd);

      // See comments above.
      err_before_exec = CraneErrCode::ERR_PROTOBUF;
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    close(ctrl_fd);
    return CraneErrCode::SUCCESS;

  } else {  // Child proc
    ResetChildProcSigHandler_();

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

    if (m_parent_step_inst_->IsBatch()) {
      if (SetChildProcessBatchFd_() != CraneErrCode::SUCCESS) std::abort();
    } else if (m_parent_step_inst_->IsCrun()) {
      SetupCrunFwdAtChild_();
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
    if (m_parent_step_inst_->IsBatch()) close(0);
    util::os::CloseFdFrom(3);

    if (m_parent_step_inst_->IsCrun() &&
        m_parent_step_inst_->GetStep().interactive_meta().x11()) {
      this->GetCrunMeta()->x11_port = msg.x11_port();
      SetupChildProcessCrunX11_();
    }

    m_env_ = GetChildProcessEnv();

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

CraneErrCode ProcInstance::Kill(int signum) {
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

CraneErrCode ProcInstance::Cleanup() {
  if (m_parent_step_inst_->IsBatch() || m_parent_step_inst_->IsCrun()) {
    const std::string& path = m_meta_->parsed_sh_script_path;
    if (!path.empty())
      g_thread_pool->detach_task([p = path]() { util::os::DeleteFile(p); });
  }

  // Dummy return
  return CraneErrCode::SUCCESS;
}

std::optional<const TaskExitInfo> ProcInstance::HandleSigchld(pid_t pid,
                                                              int status) {
  m_exit_info_.pid = pid;

  if (WIFEXITED(status)) {
    // Exited with status WEXITSTATUS(status)
    m_exit_info_.is_terminated_by_signal = false;
    m_exit_info_.value = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    // Killed by signal WTERMSIG(status)
    m_exit_info_.is_terminated_by_signal = true;
    m_exit_info_.value = WTERMSIG(status);
  } else {
    CRANE_TRACE("Received SIGCHLD with status {} for task #{} but ignored.",
                status, m_parent_step_inst_->GetStep().task_id());
    return std::nullopt;
  }

  return std::optional<TaskExitInfo>{m_exit_info_};
}

TaskManager::TaskManager()
    : m_supervisor_exit_(false), m_step_(g_config.StepSpec) {
  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });
  if (m_sigchld_handle_->start(SIGCHLD) != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle");
  }

  m_task_stop_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_task_stop_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTaskStopQueueCb_();
      });

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

  m_grpc_query_step_env_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_query_step_env_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvGrpcQueryStepEnvCb_();
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

void TaskManager::TaskStopAndDoStatusChange(task_id_t task_id) {
  m_task_stop_queue_.enqueue(task_id);
  m_task_stop_async_handle_->send();
}

void TaskManager::ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                            uint32_t exit_code,
                                            std::optional<std::string> reason) {
  m_task_->Cleanup();
  bool orphaned = m_task_->orphaned;
  // No need to free the TaskInstance structure,will destruct with TaskMgr.
  if (!orphaned)
    g_craned_client->TaskStatusChangeAsync(new_status, exit_code,
                                           std::move(reason));
}

std::future<CraneErrCode> TaskManager::ExecuteTaskAsync() {
  CRANE_INFO("[Job #{}] Executing task.", m_step_.GetStep().task_id());

  std::promise<CraneErrCode> ok_promise;
  std::future ok_future = ok_promise.get_future();

  auto elem = ExecuteTaskElem{.ok_prom = std::move(ok_promise)};

  if (m_step_.GetStep().container().length() != 0) {
    // Container
    elem.instance = std::make_unique<ContainerInstance>(&m_step_);
  } else {
    // Process
    elem.instance = std::make_unique<ProcInstance>(&m_step_);
  }

  m_grpc_execute_task_queue_.enqueue(std::move(elem));
  m_grpc_execute_task_async_handle_->send();
  return ok_future;
}

void TaskManager::LaunchExecution_() {
  // Prepare for execution
  CraneErrCode err = m_task_->Prepare();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_DEBUG("[Job #{}] Failed to prepare task",
                m_step_.GetStep().task_id());
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeFileNotFound,
                              fmt::format("Failed to prepare task"));
    return;
  }

  CRANE_TRACE("[Job #{}] Spawning process in task",
              m_step_.GetStep().task_id());
  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.

  if (m_step_.IsCrun()) m_step_.InitCforedClient();

  err = m_task_->Spawn();
  if (err != CraneErrCode::SUCCESS) {
    ActivateTaskStatusChange_(
        crane::grpc::TaskStatus::Failed, ExitCode::kExitCodeSpawnProcessFail,
        fmt::format("Cannot spawn child process in task"));
  }
}

std::future<CraneExpected<EnvMap>> TaskManager::QueryStepEnvAsync() {
  std::promise<CraneExpected<EnvMap>> env_promise;
  auto env_future = env_promise.get_future();
  m_grpc_query_step_env_queue_.enqueue(std::move(env_promise));
  m_grpc_query_step_env_async_handle_->send();
  return env_future;
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
  m_task_terminate_queue_.enqueue(elem);
  m_terminate_task_async_handle_->send();
}

void TaskManager::EvSigchldCb_() {
  int status;
  pid_t pid;

  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      const auto exit_info = m_task_->HandleSigchld(pid, status);
      if (!exit_info.has_value()) continue;

      CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: {}, Value: {}", pid,
                  exit_info->is_terminated_by_signal, exit_info->value);

      if (m_step_.IsCrun()) {
        // TaskStatusChange of a crun task is triggered in
        // CforedManager.
        auto ok_to_free = m_step_.GetCforedClient()->TaskProcessStop(pid);
        if (ok_to_free) {
          CRANE_TRACE("It's ok to unregister task #{}", g_config.JobId);
          m_step_.GetCforedClient()->TaskEnd(pid);
        }
      } else /* Batch / Calloc */ {
        // If the TaskInstance has no process left,
        // send TaskStatusChange for this task.
        // See the comment of EvActivateTaskStatusChange_.
        TaskStopAndDoStatusChange(g_config.JobId);
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
  if (!m_task_) {
    CRANE_TRACE("task #{} has already been removed.", g_config.JobId);
    return;
  }

  ITaskInstance* instance = m_task_.get();
  DelTerminationTimer_(instance);

  if (m_step_.IsBatch()) {
    m_task_terminate_queue_.enqueue(
        TaskTerminateQueueElem{.terminated_by_timeout = true});
    m_terminate_task_async_handle_->send();
  } else {
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::ExceedTimeLimit,
                              ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
  }
}

void TaskManager::EvCleanTaskStopQueueCb_() {
  task_id_t task_id;
  while (m_task_stop_queue_.try_dequeue(task_id)) {
    CRANE_INFO("[Job #{}.{}] Stopped and is doing TaskStatusChange...",
               m_task_->GetParentStep().task_id(), 0);

    switch (m_task_->err_before_exec) {
    case CraneErrCode::ERR_PROTOBUF:
      ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                                ExitCode::kExitCodeSpawnProcessFail,
                                std::nullopt);
      return;

    case CraneErrCode::ERR_CGROUP:
      ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                                ExitCode::kExitCodeCgroupError, std::nullopt);
      return;

    default:
      break;
    }

    const auto& exit_info = m_task_->GetExitInfo();
    if (m_step_.IsBatch() || m_step_.IsCrun()) {
      // For a Batch task, the end of the process means it is done.
      if (exit_info.is_terminated_by_signal) {
        switch (m_task_->terminated_by) {
        case TerminatedBy::CANCELLED_BY_USER: {
          ActivateTaskStatusChange_(
              crane::grpc::TaskStatus::Cancelled,
              exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
          break;
        }
        case TerminatedBy::TERMINATION_BY_TIMEOUT: {
          ActivateTaskStatusChange_(
              crane::grpc::TaskStatus::ExceedTimeLimit,
              exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
          break;
        }
        default:
          ActivateTaskStatusChange_(
              crane::grpc::TaskStatus::Failed,
              exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
        }
      } else
        ActivateTaskStatusChange_(crane::grpc::TaskStatus::Completed,
                                  exit_info.value, std::nullopt);
    } else /* Calloc */ {
      // For a COMPLETING Calloc task with a process running,
      // the end of this process means that this task is done.
      if (exit_info.is_terminated_by_signal)
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::Completed,
            exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
      else
        ActivateTaskStatusChange_(crane::grpc::TaskStatus::Completed,
                                  exit_info.value, std::nullopt);
    }
  }
}

void TaskManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        g_config.JobId);

    if (!m_task_) {
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
    if (m_step_.IsCrun()) sig = SIGHUP;

    if (elem.mark_as_orphaned) m_task_->orphaned = true;
    if (elem.terminated_by_timeout)
      m_task_->terminated_by = TerminatedBy::TERMINATION_BY_TIMEOUT;
    if (elem.terminated_by_user) {
      // If termination request is sent by user, send SIGKILL to ensure that
      // even freezing processes will be terminated immediately.
      sig = SIGKILL;
      m_task_->terminated_by = TerminatedBy::CANCELLED_BY_USER;
    }

    if (m_task_->GetPid()) {
      // For an Interactive task with a process running or a Batch task, we
      // just send a kill signal here.
      m_task_->Kill(sig);
    } else if (m_step_.GetStep().type() == crane::grpc::Interactive) {
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
                  g_config.JobId);
      elem.ok_prom.set_value(CraneErrCode::ERR_NON_EXISTENT);
      continue;
    }

    // Delete the old timer.
    DelTerminationTimer_(m_task_.get());

    absl::Time start_time =
        absl::FromUnixSeconds(m_step_.GetStep().start_time().seconds());
    absl::Duration const& new_time_limit = elem.time_limit;

    if (now - start_time >= new_time_limit) {
      // If the task times out, terminate it.
      TaskTerminateQueueElem ev_task_terminate{.terminated_by_timeout = true};
      m_task_terminate_queue_.enqueue(ev_task_terminate);
      m_terminate_task_async_handle_->send();

    } else {
      // If the task haven't timed out, set up a new timer.
      AddTerminationTimer_(
          m_task_.get(),
          ToInt64Seconds((new_time_limit - (absl::Now() - start_time))));
    }
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);
  }
}

void TaskManager::EvCleanCheckTaskStatusQueueCb_() {
  std::promise<CraneExpected<pid_t>> elem;
  while (m_check_task_status_queue_.try_dequeue(elem)) {
    if (m_task_) {
      if (!m_task_->GetPid()) {
        CRANE_DEBUG("CheckTaskStatus: task launch failed.");
        // Launch failed
        elem.set_value(std::unexpected(CraneErrCode::ERR_NON_EXISTENT));
      } else {
        elem.set_value(m_task_->GetPid());
      }
    } else {
      CRANE_DEBUG("CheckTaskStatus: task #{} does not exist.", g_config.JobId);
      elem.set_value(std::unexpected(CraneErrCode::ERR_NON_EXISTENT));
    }
  }
}

void TaskManager::EvGrpcExecuteTaskCb_() {
  if (m_task_ != nullptr) {
    CRANE_ERROR("Duplicated ExecuteTask request for task #{}. Ignore it.",
                g_config.JobId);
    return;
  }
  struct ExecuteTaskElem elem;
  while (m_grpc_execute_task_queue_.try_dequeue(elem)) {
    m_task_ = std::move(elem.instance);

    // Add a timer to limit the execution time of a task.
    // Note: event_new and event_add in this function is not thread safe,
    //       so we move it outside the multithreading part.
    int64_t sec = m_step_.GetStep().time_limit().seconds();
    AddTerminationTimer_(m_task_.get(), sec);
    CRANE_TRACE("Add a timer of {} seconds", sec);

    m_task_->pwd.Init(m_task_->GetParentStep().uid());
    if (!m_task_->pwd.Valid()) {
      CRANE_DEBUG(
          "[Job #{}] Failed to look up password entry for uid {} of task",
          m_task_->GetParentStep().task_id(), m_task_->GetParentStep().uid());
      ActivateTaskStatusChange_(
          crane::grpc::TaskStatus::Failed, ExitCode::kExitCodePermissionDenied,
          fmt::format(
              "[Job #{}] Failed to look up password entry for uid {} of task",
              m_task_->GetParentStep().task_id(),
              m_task_->GetParentStep().uid()));
      elem.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      return;
    }

    // Calloc tasks have no scripts to run. Just return.
    if (m_step_.IsCalloc()) {
      elem.ok_prom.set_value(CraneErrCode::SUCCESS);
      return;
    }

    LaunchExecution_();
    if (!m_task_->GetPid()) {
      CRANE_WARN("[task #{}] Failed to launch process.",
                 m_step_.GetStep().task_id());
      elem.ok_prom.set_value(CraneErrCode::ERR_GENERIC_FAILURE);
    } else {
      elem.ok_prom.set_value(CraneErrCode::SUCCESS);
    }
  }
}

void TaskManager::EvGrpcQueryStepEnvCb_() {
  std::promise<CraneExpected<EnvMap>> elem;
  while (m_grpc_query_step_env_queue_.try_dequeue(elem)) {
    if (!m_task_) {
      CRANE_ERROR("Try to query step env of a non-existent step #{}.",
                  g_config.JobId);
      elem.set_value(std::unexpected(CraneErrCode::ERR_NON_EXISTENT));
      continue;
    }
    elem.set_value(m_task_->GetChildProcessEnv());
  }
}

}  // namespace Supervisor