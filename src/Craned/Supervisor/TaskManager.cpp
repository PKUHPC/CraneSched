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
#include <sys/types.h>
#include <sys/wait.h>

#include <limits>

#include "CforedClient.h"
#include "CgroupManager.h"
#include "CranedClient.h"
#include "SupervisorPublicDefs.h"
#include "SupervisorServer.h"
#include "crane/CriClient.h"
#include "crane/OS.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"

namespace Craned::Supervisor {

using Common::kStepRequestCheckIntervalMs;

CraneErrCode StepInstance::Prepare() {
  // No need to write script for calloc or container steps
  if (IsCalloc() || IsContainer() || IsPod()) return CraneErrCode::SUCCESS;

  auto sh_path = g_config.CraneScriptDir /
                 fmt::format("Crane-{}.{}.sh", g_config.JobId, g_config.StepId);

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (this->IsCrun())
    fputs(m_step_to_supv_.interactive_meta().sh_script().c_str(), fptr);
  else
    fputs(m_step_to_supv_.batch_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));
  if (chown(sh_path.c_str(), m_step_to_supv_.uid(), gids[0]) != 0) {
    CRANE_ERROR("Failed to change ownership of script file:{}",
                strerror(errno));
  }
  script_path = sh_path;
  return CraneErrCode::SUCCESS;
}

void StepInstance::CleanUp() {
  if (script_path.has_value()) util::os::DeleteFile(script_path.value());
}

bool StepInstance::IsInteractive() const noexcept {
  return interactive_type.has_value();
}

bool StepInstance::IsCrun() const noexcept {
  return interactive_type.has_value() &&
         interactive_type.value() == crane::grpc::Crun;
}

bool StepInstance::IsCalloc() const noexcept {
  return interactive_type.has_value() &&
         interactive_type.value() == crane::grpc::Calloc;
}

bool StepInstance::IsPod() const noexcept {
  return IsDaemon() && m_step_to_supv_.has_pod_meta();
}

bool StepInstance::IsContainer() const noexcept {
  return m_step_to_supv_.has_container_meta();
}

bool StepInstance::IsDaemon() const noexcept {
  return m_step_to_supv_.step_type() == crane::grpc::StepType::DAEMON;
}

EnvMap StepInstance::GetStepProcessEnv() const {
  std::unordered_map<std::string, std::string> env_map;

  // Job env from CraneD
  for (auto& [name, value] : g_config.JobEnv) {
    env_map.emplace(name, value);
  }

  // Crane env will override user task env;
  for (const auto& [name, value] : m_step_to_supv_.env()) {
    env_map.emplace(name, value);
  }

  if (m_step_to_supv_.get_user_env()) {
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
    env_map.emplace("HOME", pwd.HomeDir());
    env_map.emplace("SHELL", pwd.Shell());
  }

  int64_t time_limit_sec = m_step_to_supv_.time_limit().seconds();
  int64_t hours = time_limit_sec / 3600;
  int64_t minutes = (time_limit_sec % 3600) / 60;
  int64_t seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_map.emplace("CRANE_TIMELIMIT", time_limit);
  return env_map;
}

void StepInstance::AddTaskInstance(task_id_t task_id,
                                   std::unique_ptr<ITaskInstance>&& task) {
  m_task_map_.emplace(task_id, std::move(task));
}

std::unique_ptr<ITaskInstance> StepInstance::RemoveTaskInstance(
    task_id_t task_id) {
  auto it = m_task_map_.find(task_id);
  if (it == m_task_map_.end()) {
    return nullptr;
  }
  std::unique_ptr task = std::move(it->second);
  m_task_map_.erase(it);
  return task;
}

bool StepInstance::AllTaskFinished() const { return m_task_map_.empty(); }

void StepInstance::InitOomBaseline() {
  // Use cgroup path passed from Craned
  if (g_config.CgroupPath.empty()) {
    CRANE_ERROR("[OOM] No cgroup path provided from Craned");
    return;
  }
  cgroup_path = g_config.CgroupPath;

  uint64_t oom_kill = 0, oom = 0;
  if (!Common::CgroupManager::ReadOomCountsFromCgroupPath(cgroup_path, oom_kill,
                                                          oom)) {
    CRANE_ERROR("[OOM] Failed to read OOM counts from cgroup path , path={}",
                cgroup_path);
    return;
  }
  baseline_oom_kill_count = oom_kill;
  if (Common::CgroupManager::IsCgV2()) baseline_oom_count = oom;
  oom_baseline_inited = true;
  CRANE_DEBUG("[OOM] Baseline inited: v2={}, path={}, oom={}, oom_kill={}",
              Common::CgroupManager::IsCgV2(), cgroup_path, baseline_oom_count,
              baseline_oom_kill_count);
}

bool StepInstance::EvaluateOomOnExit() {
  if (!oom_baseline_inited || cgroup_path.empty()) {
    CRANE_TRACE("[OOM] Skip evaluation: baseline_inited={} path='{}'",
                oom_baseline_inited, cgroup_path);
    return false;
  }
  uint64_t oom_kill = 0, oom = 0;
  if (!Common::CgroupManager::ReadOomCountsFromCgroupPath(cgroup_path, oom_kill,
                                                          oom))
    return false;
  if (Common::CgroupManager::IsCgV2()) {
    CRANE_DEBUG(
        "[OOM] Evaluate v2 path={} baseline(oom={},oom_kill={}) "
        "current(oom={},oom_kill={})",
        cgroup_path, baseline_oom_count, baseline_oom_kill_count, oom,
        oom_kill);
    if (oom_kill > baseline_oom_kill_count) return true;
    if (oom > baseline_oom_count && oom_kill == baseline_oom_kill_count)
      CRANE_DEBUG(
          "[OOM] v2: detected oom growth (delta_oom={}) but no oom_kill growth "
          "(delta_kill=0) -> not classified as OOM",
          oom - baseline_oom_count);
    return false;
  }
  CRANE_DEBUG(
      "[OOM] Evaluate v1 path={} baseline(oom_kill={}) current(oom_kill={})",
      cgroup_path, baseline_oom_kill_count, oom_kill);
  return oom_kill > baseline_oom_kill_count;
}

void ITaskInstance::InitEnvMap() {
  // Job env from CraneD
  for (const auto& [name, value] : g_config.JobEnv) {
    m_env_.emplace(name, value);
  }
  // Crane env will override user task env;
  for (const auto& [name, value] : m_parent_step_inst_->GetStep().env()) {
    m_env_.emplace(name, value);
  }
  for (const auto& [name, value] : m_parent_step_inst_->GetStepProcessEnv()) {
    m_env_.emplace(name, value);
  }
}

ProcInstance::~ProcInstance() {
  if (m_parent_step_inst_->IsCrun()) {
    auto* crun_meta = GetCrunMeta_();

    // For crun non pty job, close out fd in CforedClient
    // For crun pty job, close tty fd in CforedClient
    if (!m_parent_step_inst_->pty) close(crun_meta->stdin_write);

    if (!crun_meta->x11_auth_path.empty() &&
        !absl::EndsWith(crun_meta->x11_auth_path, "XXXXXX")) {
      std::error_code ec;
      bool ok = std::filesystem::remove(crun_meta->x11_auth_path, ec);
      if (!ok)
        CRANE_ERROR("Failed to remove x11 auth {} for task #{}: {}",
                    crun_meta->x11_auth_path, task_id, ec.message());
    }
  }
}

void ProcInstance::InitEnvMap() {
  ITaskInstance::InitEnvMap();
  if (m_parent_step_inst_->IsCrun()) {
    const auto& ia_meta = m_parent_step_inst_->GetStep().interactive_meta();
    if (!ia_meta.term_env().empty()) m_env_.emplace("TERM", ia_meta.term_env());
  }
}

std::string ProcInstance::ParseFilePathPattern_(const std::string& pattern,
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
    resolved_path = resolved_path / fmt::format("Crane-{}.out", g_config.JobId);

  std::string resolved_path_pattern = resolved_path.string();
  absl::StrReplaceAll({{"%j", std::to_string(g_config.JobId)},
                       {"%u", m_parent_step_inst_->pwd.Username()},
                       {"%x", m_parent_step_inst_->GetStep().name()}},
                      &resolved_path_pattern);

  return resolved_path_pattern;
}

CraneErrCode ProcInstance::PrepareXauthFiles_() {
  auto* inst_crun_meta = this->GetCrunMeta_();
  const PasswordEntry& pwd_entry = m_parent_step_inst_->pwd;

  inst_crun_meta->x11_auth_path =
      fmt::sprintf("%s/.crane/xauth/.Xauthority-XXXXXX", pwd_entry.HomeDir());

  bool ok = util::os::CreateFoldersForFileEx(
      inst_crun_meta->x11_auth_path, pwd_entry.Uid(), pwd_entry.Gid(), 0700);
  if (!ok) {
    CRANE_ERROR("Failed to create xauth source file for task #{}", task_id);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Default file permission is 0600.
  int xauth_fd = mkstemp(inst_crun_meta->x11_auth_path.data());
  if (xauth_fd == -1) {
    CRANE_ERROR("mkstemp() for xauth file failed: {}\n", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  int ret = fchown(xauth_fd, m_parent_step_inst_->pwd.Uid(),
                   m_parent_step_inst_->pwd.Gid());
  if (ret == -1) {
    CRANE_ERROR("fchown() for xauth file failed: {}\n", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  return CraneErrCode::SUCCESS;
}

CraneExpected<pid_t> ProcInstance::ForkCrunAndInitMeta_() {
  if (!m_parent_step_inst_->IsCrun()) return fork();

  auto* meta = GetCrunMeta_();
  CRANE_DEBUG("Launch crun task #{} pty: {}", task_id,
              m_parent_step_inst_->pty);

  int to_crun_pipe[2];
  int from_crun_pipe[2];
  int crun_pty_fd = -1;

  if (!m_parent_step_inst_->pty) {
    if (pipe(to_crun_pipe) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  task_id, strerror(errno));
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }

    if (pipe(from_crun_pipe) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  task_id, strerror(errno));
      close(to_crun_pipe[0]);
      close(to_crun_pipe[1]);
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }
  }

  pid_t pid = -1;
  if (m_parent_step_inst_->pty) {
    pid = forkpty(&crun_pty_fd, nullptr, nullptr, nullptr);

    if (pid > 0) {
      meta->stdin_read = -1;
      meta->stdout_read = crun_pty_fd;
      meta->stdin_write = crun_pty_fd;
      meta->stdout_write = -1;
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

bool ProcInstance::SetupCrunFwdAtParent_(uint16_t* x11_port) {
  auto* meta = GetCrunMeta_();
  pid_t pid = std::get<pid_t>(*GetExecId());

  if (!m_parent_step_inst_->pty) {
    // For non-pty tasks, pipe is used for stdin/stdout and one end should be
    // closed.
    close(meta->stdin_read);
    close(meta->stdout_write);
  }
  // For pty tasks, stdin_read and stdout_write are the same fd and should not
  // be closed.

  const auto& parent_step = GetParentStep();
  auto* parent_cfored_client = m_parent_step_inst_->GetCforedClient();

  auto ok = parent_cfored_client->InitFwdMetaAndUvStdoutFwdHandler(
      task_id, meta->stdin_write, meta->stdout_read, m_parent_step_inst_->pty);
  if (!ok) return false;

  if (m_parent_step_inst_->x11) {
    if (m_parent_step_inst_->x11_fwd)
      meta->x11_port = parent_cfored_client->InitUvX11FwdHandler(task_id);
    else
      meta->x11_port = parent_step.interactive_meta().x11_meta().port();

    if (x11_port != nullptr) *x11_port = meta->x11_port;

    CRANE_TRACE("Crun task x11 enabled. Forwarding: {}, X11 Port: {}",
                m_parent_step_inst_->x11_fwd, meta->x11_port);
  }
  CRANE_INFO("Task #{} fwd ready.", task_id);
  return true;
}

void ProcInstance::ResetChildProcSigHandler_() {
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

CraneErrCode ProcInstance::SetChildProcProperty_() {
  // Reset OOM score adjustment for child process
  std::filesystem::path oom_score_adj_file =
      fmt::format("/proc/{}/oom_score_adj", getpid());
  std::ofstream oom_score_adj_stream(oom_score_adj_file);
  if (oom_score_adj_stream.is_open()) {
    oom_score_adj_stream << "0";
  } else {
    fmt::print(stderr,
               "[Subprocess] Error: Failed to open oom_score_adj file: pid #{}",
               getpid());
  }
  oom_score_adj_stream.close();

  auto& pwd = m_parent_step_inst_->pwd;
  std::vector<gid_t> gids = m_parent_step_inst_->gids;

  if (!std::ranges::contains(gids, pwd.Gid())) gids.emplace_back(pwd.Gid());

  int rc = setgroups(gids.size(), gids.data());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setgroups() failed: {}\n",
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // FIXME: gids[0] or pwd.Gid()
  rc = setresgid(gids[0], gids[0], gids[0]);
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

  const std::string& cwd = GetParentStep().cwd();
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

CraneErrCode ProcInstance::SetChildProcBatchFd_() {
  int stdout_fd, stderr_fd;

  auto* meta = dynamic_cast<BatchInstanceMeta*>(m_meta_.get());
  const std::string& stdout_file_path = meta->parsed_output_file_pattern;
  const std::string& stderr_file_path = meta->parsed_error_file_pattern;

  int open_mode =
      GetParentStep().batch_meta().open_mode_append() ? O_APPEND : O_TRUNC;
  stdout_fd =
      open(stdout_file_path.c_str(), O_RDWR | O_CREAT | open_mode, 0644);
  if (stdout_fd == -1) {
    fmt::print(stderr, "[Subprocess] Error: open {}. {}\n", stdout_file_path,
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  if (dup2(stdout_fd, 1) == -1) std::exit(1);  // stdout -> output file

  if (stderr_file_path.empty()) {
    // if stderr file is not set, redirect stderr to stdout
    if (dup2(stdout_fd, 2) == -1) std::exit(1);
  } else {
    stderr_fd =
        open(stderr_file_path.c_str(), O_RDWR | O_CREAT | open_mode, 0644);
    if (stderr_fd == -1) {
      fmt::print(stderr, "[Subprocess] Error: open {}. {}\n", stderr_file_path,
                 strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
    if (dup2(stderr_fd, 2) == -1) std::exit(1);  // stderr -> error file
    close(stderr_fd);
  }
  close(stdout_fd);

  return CraneErrCode::SUCCESS;
}

void ProcInstance::SetupCrunFwdAtChild_() {
  const auto* meta = GetCrunMeta_();

  if (!m_parent_step_inst_->pty) {
    if (dup2(meta->stdin_read, STDIN_FILENO) == -1) std::exit(1);
    if (dup2(meta->stdout_write, STDOUT_FILENO) == -1) std::exit(1);
    if (dup2(meta->stdout_write, STDERR_FILENO) == -1) std::exit(1);
    close(meta->stdin_read);
    close(meta->stdout_write);
  }
}

void ProcInstance::SetupChildProcCrunX11_() {
  auto* inst_crun_meta = this->GetCrunMeta_();
  const auto& proto_x11_meta =
      this->GetParentStep().interactive_meta().x11_meta();

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

  xauth_argv.push_back(nullptr);

  subprocess_s subprocess{};
  int result = subprocess_create(xauth_argv.data(), 0, &subprocess);
  if (0 != result) {
    fmt::print(stderr,
               "[Craned Subprocess] xauth subprocess creation failed: {}.\n",
               strerror(errno));
    std::abort();
  }

  auto buf = std::array<char, 4096>();
  std::string xauth_stdout_str;
  std::string xauth_stderr_str;

  std::FILE* cmd_fd = subprocess_stdout(&subprocess);
  while (std::fgets(buf.data(), 4096, cmd_fd) != nullptr)
    xauth_stdout_str.append(buf.data());

  cmd_fd = subprocess_stderr(&subprocess);
  while (std::fgets(buf.data(), 4096, cmd_fd) != nullptr)
    xauth_stderr_str.append(buf.data());

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

  // After xauth is set up, set DISPLAY and XAUTHORITY envs
  m_env_["DISPLAY"] = display;
  m_env_["XAUTHORITY"] = inst_crun_meta->x11_auth_path;
}

CraneErrCode ProcInstance::SetChildProcEnv_() const {
  if (clearenv() != 0)
    fmt::print(stderr, "[Subprocess] Warning: clearenv() failed.\n");

  for (const auto& [name, value] : m_env_)
    if (setenv(name.c_str(), value.c_str(), 1) != 0)
      fmt::print(stderr, "[Subprocess] Warning: setenv() for {}={} failed.\n",
                 name, value);

  return CraneErrCode::SUCCESS;
}

std::vector<std::string> ProcInstance::GetChildProcExecArgv_() const {
  // "bash (--login) -c 'm_executable_ [m_arguments_...]'"
  std::vector<std::string> argv;
  argv.emplace_back("/bin/bash");
  if (GetParentStep().get_user_env()) {
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

std::string PodInstance::MakeHashId_(job_id_t job_id,
                                     const std::string& job_name,
                                     const std::string& node_name) {
  std::string h_msg =
      std::format("{}\x1f{}\x1f{}", job_id, node_name, job_name);
  uint64_t h64 =
      (static_cast<uint64_t>(util::Crc32Of(h_msg, 0xA5A5A5A5U)) << 32) |
      util::Adler32Of(h_msg, 0x3C3C3C3CU);
  return std::format("{:016x}", h64);  // 16 hex chars, lowercase
}

CraneErrCode PodInstance::ResolveUserNsMapping_(
    const PasswordEntry& pwd, cri::api::LinuxSandboxSecurityContext* sec_ctx) {
  using cri::CriClient;

  const uint64_t uid = pwd.Uid();
  const uint64_t gid = pwd.Gid();

  const auto& subid_conf = g_config.Container.SubId;
  const uint64_t size = subid_conf.RangeSize;

  uint64_t uid_start{};
  uint64_t gid_start{};

  if (util::MulOverflow(uid, size, uid_start) ||
      util::AddOverflow(subid_conf.BaseOffset, uid_start, uid_start)) {
    CRANE_ERROR("Failed to calculate uid_start for user {}: uid overflowed.",
                pwd.Username());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (util::MulOverflow(gid, size, gid_start) ||
      util::AddOverflow(subid_conf.BaseOffset, gid_start, gid_start)) {
    CRANE_ERROR("Failed to calculate gid_start for user {}: gid overflowed.",
                pwd.Username());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  auto subuid = pwd.SubUidRanges();
  auto subgid = pwd.SubGidRanges();

  bool uid_exists = subuid.Valid() && subuid.Count() > 0;
  bool gid_exists = subgid.Valid() && subgid.Count() > 0;

  auto matches = [](const SubIdRanges& ranges, uint64_t start,
                    uint64_t count) -> bool {
    return ranges.Valid() && ranges.Count() == 1 && ranges[0].start == start &&
           ranges[0].count == count;
  };

  if (!subid_conf.Managed) {
    // Unmanaged mode: require any usable ranges for both UID and GID.
    if (!uid_exists || !gid_exists) {
      CRANE_ERROR(
          "SubId unmanaged mode: missing subuid/subgid ranges for user {} "
          "(subuid: {}, subgid: {})",
          pwd.Username(), uid_exists ? "present" : "missing",
          gid_exists ? "present" : "missing");
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
  } else {
    // Managed mode: validate or ensure mappings
    if (uid_exists || gid_exists) {
      // If any ranges exist, both must match the deterministic mapping
      if (!matches(subuid, uid_start, size) ||
          !matches(subgid, gid_start, size)) {
        CRANE_ERROR(
            "SubId mismatch: expected uid range [{}, {}], gid range [{}, {}] "
            "for user {}, but found uid range [{}, {}], gid range [{}, {}]",
            uid_start, size, gid_start, size, pwd.Username(),
            uid_exists ? subuid[0].start : 0, uid_exists ? subuid[0].count : 0,
            gid_exists ? subgid[0].start : 0, gid_exists ? subgid[0].count : 0);
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
    } else {
      // Both missing: allocate them
      auto ensure_result = PasswordEntry::EnsureSubIdRanges(
          pwd.Username(), uid_start, size, gid_start, size);
      if (!ensure_result) {
        CRANE_ERROR("Failed to ensure SubId ranges for user {}: {}",
                    pwd.Username(), ensure_result.error());
        return CraneErrCode::ERR_SYSTEM_ERR;
      }

      // Re-query to verify
      PasswordEntry pwd_recheck(uid);
      subuid = pwd_recheck.SubUidRanges();
      subgid = pwd_recheck.SubGidRanges();

      if (!matches(subuid, uid_start, size) ||
          !matches(subgid, gid_start, size)) {
        CRANE_ERROR(
            "SubId verification failed after allocation for user {}: expected "
            "uid range [{}, {}], gid range [{}, {}]",
            pwd.Username(), uid_start, size, gid_start, size);
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
    }
  }

  // NOTE: Using 0 as the start of the range no matter run_as_user is 0 or not.
  auto uid_mapping =
      CriClient::MakeIdMapping(subuid[0].start, 0, subuid[0].count);
  auto gid_mapping =
      CriClient::MakeIdMapping(subgid[0].start, 0, subgid[0].count);

  auto* userns_options =
      sec_ctx->mutable_namespace_options()->mutable_userns_options();

  // NOTE: Always use POD mode for userns.
  userns_options->set_mode(cri::api::NamespaceMode::POD);
  userns_options->clear_uids();
  userns_options->clear_gids();

  // K8s/Containerd currently assume only one continuous range is set for
  // userns. Otherwise, netns in userns will fail. See:
  // https://kubernetes.io/docs/concepts/workloads/pods/user-namespaces/
  // https://github.com/containerd/containerd/blob/release/2.1/internal/cri/server/sandbox_run_linux.go#L51
  userns_options->mutable_uids()->Add(std::move(uid_mapping));
  userns_options->mutable_gids()->Add(std::move(gid_mapping));

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::SetPodSandboxConfig_(
    const crane::grpc::PodTaskAdditionalMeta& pod_meta) {
  using cri::kCriLabelJobIdKey;
  using cri::kCriLabelJobNameKey;
  using cri::kCriLabelUidKey;

  // All steps in a job share the same pod.
  job_id_t job_id = g_config.JobId;

  const std::string& job_name = g_config.JobName;
  const std::string& node_name = g_config.CranedIdOfThisNode;

  uid_t uid = m_parent_step_inst_->pwd.Uid();
  gid_t gid = m_parent_step_inst_->pwd.Gid();

  // Generate hash for pod name and uid.
  std::string h16 = MakeHashId_(job_id, job_name, node_name);
  // Hostname: job-<job_id>-<node_name>, might not be used if network is `node`.
  std::string hostname = util::SlugDns1123(
      std::format("job-{}-{}", job_id, node_name), kCriDnsMaxLabelLen);

  // metadata
  auto* metadata = m_pod_config_.mutable_metadata();
  // pod name：same as hostname; pod uid: <h16>
  metadata->set_name(hostname);
  metadata->set_uid(std::move(h16));

  // labels
  m_pod_config_.mutable_labels()->insert(
      {{std::string(kCriLabelUidKey), std::to_string(uid)},
       {std::string(kCriLabelJobIdKey), std::to_string(job_id)},
       {std::string(kCriLabelJobNameKey), job_name}});
  m_pod_config_.mutable_labels()->insert(pod_meta.labels().begin(),
                                         pod_meta.labels().end());

  m_pod_config_.mutable_annotations()->insert(
      {{std::string(kCriLabelUidKey), std::to_string(uid)},
       {std::string(kCriLabelJobIdKey), std::to_string(job_id)},
       {"hostname", hostname}});
  m_pod_config_.mutable_annotations()->insert(pod_meta.annotations().begin(),
                                              pod_meta.annotations().end());

  // log directory
  m_pod_config_.set_log_directory(m_log_dir_);

  // FIXME: This is just a workaround before we have seperated level of step.
  // set cgroup parent
  auto* linux_config = m_pod_config_.mutable_linux();
  linux_config->set_cgroup_parent(
      std::filesystem::path(g_config.CgroupPath).parent_path().string());

  // security context
  auto* sec_ctx = linux_config->mutable_security_context();
  auto* ns_options = sec_ctx->mutable_namespace_options();
  ns_options->set_network(
      static_cast<cri::api::NamespaceMode>(pod_meta.namespace_().network()));
  ns_options->set_pid(
      static_cast<cri::api::NamespaceMode>(pod_meta.namespace_().pid()));
  ns_options->set_ipc(
      static_cast<cri::api::NamespaceMode>(pod_meta.namespace_().ipc()));
  ns_options->set_target_id(pod_meta.namespace_().target_id());

  // If network is not host mode, set hostname and port mappings
  if (ns_options->network() != cri::api::NamespaceMode::NODE) {
    m_pod_config_.set_hostname(std::move(hostname));
    for (const auto& ports : pod_meta.ports()) {
      auto* p = m_pod_config_.add_port_mappings();
      p->set_protocol(static_cast<cri::api::Protocol>(ports.protocol()));
      p->set_host_port(ports.host_port());
      p->set_container_port(ports.container_port());
      p->set_host_ip(ports.host_ip());
    }
  }

  // Setup run_as_user / run_as_group
  if (uid == 0 || pod_meta.userns() ||
      (pod_meta.run_as_user() == uid && pod_meta.run_as_group() == gid)) {
    // If user is root or using userns, run_as_* is always allowed.
    // Non-root user w/o userns cannot use run_as_* other than its own.
    sec_ctx->mutable_run_as_user()->set_value(pod_meta.run_as_user());
    sec_ctx->mutable_run_as_group()->set_value(pod_meta.run_as_group());
  } else {
    CRANE_ERROR(
        "Pod #{} is not allowed to use other identities when not "
        "using userns",
        job_id);
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  // Setup userns
  if (pod_meta.userns()) {
    if (ResolveUserNsMapping_(m_parent_step_inst_->pwd, sec_ctx) !=
        CraneErrCode::SUCCESS) {
      CRANE_ERROR("Failed to setup userns config for pod of job #{}", job_id);
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::PersistPodSandboxInfo_() {
  std::ofstream output(m_config_file_,
                       std::ios::out | std::ios::trunc | std::ios::binary);
  if (!output) {
    CRANE_ERROR("Failed to open config file {}: {}", m_config_file_,
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  if (!m_pod_config_.SerializeToOstream(&output)) {
    CRANE_ERROR("Failed to write config to file {}: {}", m_config_file_,
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  m_lock_file_ = m_log_dir_ / std::format(kPodIdLockFilePattern,
                                          g_config.CranedIdOfThisNode);
  std::ofstream pid_output(m_lock_file_, std::ios::out | std::ios::trunc);
  if (!pid_output) {
    CRANE_WARN("Failed to open pod id file {}: {}", m_lock_file_,
               strerror(errno));
    // Not a critical error; container path can still fall back to label query.
  } else {
    pid_output << m_pod_id_;
  }

  CRANE_DEBUG("Pod config saved to {}, id saved to {}", m_config_file_,
              m_lock_file_);

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::Prepare() {
  const auto& step_to_supv = m_parent_step_inst_->GetStep();
  const auto* pod_meta =
      step_to_supv.has_pod_meta() ? &step_to_supv.pod_meta() : nullptr;

  if (pod_meta == nullptr) {
    CRANE_ERROR("Missing pod meta for container job #{}", g_config.JobId);
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  // Generate log pathes
  m_log_dir_ = std::filesystem::path(step_to_supv.cwd()) /
               std::format(kPodLogDirPattern, g_config.JobId);
  m_config_file_ = m_log_dir_ / std::format(kPodConfigFilePattern,
                                            g_config.CranedIdOfThisNode);

  // Create private directory for container job
  if (!util::os::CreateFoldersForFileEx(m_config_file_,
                                        m_parent_step_inst_->pwd.Uid(),
                                        m_parent_step_inst_->pwd.Gid(), 0700)) {
    CRANE_ERROR("Failed to create log directory {} for container #{}.{}",
                m_log_dir_, g_config.JobId, g_config.StepId);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // If already exists, launch failed.
  if (std::filesystem::exists(m_config_file_)) {
    CRANE_ERROR("Pod config file {} already exists for container #{}.{}",
                m_config_file_, g_config.JobId, g_config.StepId);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Pod is solely associated with job, not step.
  if (auto err = SetPodSandboxConfig_(*pod_meta); err != CraneErrCode::SUCCESS)
    return err;

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::Spawn() {
  auto* cri_client = m_parent_step_inst_->GetCriClient();

  auto pod_id_expt = cri_client->RunPodSandbox(&m_pod_config_);
  if (!pod_id_expt.has_value()) {
    const auto& rich_err = pod_id_expt.error();
    CRANE_ERROR("Failed to run pod sandbox for container job #{}: {}",
                g_config.JobId, rich_err.description());
    return rich_err.code();
  }

  m_pod_id_ = std::move(pod_id_expt.value());
  CRANE_DEBUG("Pod {} created for job #{}", m_pod_id_, g_config.JobId);

  if (auto err = PersistPodSandboxInfo_(); err != CraneErrCode::SUCCESS) {
    CRANE_ERROR("Failed to persist pod sandbox config for job #{}",
                g_config.JobId);
    return err;
  }

  // TODO: Add monitoring for pod status, preferably in Craned, not here.

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::Kill(int /*signum*/) {
  auto* cri_client = m_parent_step_inst_->GetCriClient();

  auto result = cri_client->StopPodSandbox(m_pod_id_);
  if (!result.has_value()) {
    const auto& rich_err = result.error();
    CRANE_ERROR("Failed to stop pod sandbox {} for job #{}: {}", m_pod_id_,
                g_config.JobId, rich_err.description());
    // Try best effort to remove pod sandbox, continue even if stopping fails.
  }

  result = cri_client->RemovePodSandbox(m_pod_id_);
  if (!result.has_value()) {
    const auto& rich_err = result.error();
    CRANE_ERROR("Failed to remove pod sandbox {} for job #{}: {}", m_pod_id_,
                g_config.JobId, rich_err.description());
    return rich_err.code();
  }

  // TODO: Currently pod status monitoring is missing. So we had to call
  // TaskStopAndDoStatusChange() here. See Spawn() for details.
  CRANE_DEBUG("Pod {} stopped for job #{}", m_pod_id_, g_config.JobId);
  HandlePodExited();
  g_task_mgr->TaskStopAndDoStatusChange(0);

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::Cleanup() {
  g_thread_pool->detach_task([cfg = m_config_file_, lock = m_lock_file_]() {
    std::error_code ec;
    bool ok = std::filesystem::remove(cfg, ec);
    if (!ok)
      CRANE_ERROR("Failed to remove pod config file {}: {}", cfg, ec.message());

    ok = std::filesystem::remove(lock, ec);
    if (!ok)
      CRANE_ERROR("Failed to remove pod lock file {}: {}", lock, ec.message());
  });

  return CraneErrCode::SUCCESS;
}

const TaskExitInfo& PodInstance::HandlePodExited() {
  // m_exit_info_.is_terminated_by_signal = status.exit_code() > 128;
  // m_exit_info_.value = m_exit_info_.is_terminated_by_signal
  //                          ? status.exit_code() - 128
  //                          : status.exit_code();
  // TODO: See PodInstance::Spawn() TODO.
  m_exit_info_.is_terminated_by_signal = false;
  m_exit_info_.value = 0;
  return m_exit_info_;
}

void ContainerInstance::InitEnvMap() {
  ITaskInstance::InitEnvMap();
  if (!GetParentStep().has_container_meta()) return;
  const auto& ca_meta = this->GetParentStep().container_meta();
  for (const auto& [name, value] : ca_meta.env()) {
    m_env_.emplace(name, value);
  }
}

CraneErrCode ContainerInstance::Prepare() {
  const auto& step_to_supv = m_parent_step_inst_->GetStep();
  const auto* pod_meta =
      step_to_supv.has_pod_meta() ? &step_to_supv.pod_meta() : nullptr;
  const auto* ca_meta = step_to_supv.has_container_meta()
                            ? &step_to_supv.container_meta()
                            : nullptr;

  auto* cri_client = m_parent_step_inst_->GetCriClient();

  if (ca_meta == nullptr) {
    CRANE_ERROR("Missing container meta for container job #{}", g_config.JobId);
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  // Generate log pathes
  // NOTE：These should be consistent with ccon CLI.
  m_log_dir_ = std::filesystem::path(step_to_supv.cwd()) /
               std::format(PodInstance::kPodLogDirPattern, g_config.JobId);
  m_log_file_ =
      m_log_dir_ / std::format(kContainerLogFilePattern, g_config.StepId,
                               g_config.CranedIdOfThisNode);

  // Create private directory for container job
  if (!util::os::CreateFoldersForFileEx(m_log_file_,
                                        m_parent_step_inst_->pwd.Uid(),
                                        m_parent_step_inst_->pwd.Gid(), 0700)) {
    CRANE_ERROR("Failed to create log directory {} for container #{}.{}",
                m_log_file_, g_config.JobId, g_config.StepId);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Create the log file before container runtime and chmod/chown it,
  // so the log file is always owned by the user, not root.
  std::ofstream(m_log_file_).close();
  if (chmod(m_log_file_.c_str(), 0600) != 0) {
    CRANE_ERROR("Failed to chmod log file {}", m_log_file_);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  if (chown(m_log_file_.c_str(), m_parent_step_inst_->pwd.Uid(),
            m_parent_step_inst_->pwd.Gid()) != 0) {
    CRANE_ERROR("Failed to chown log file {}", m_log_file_);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (auto err = LoadPodSandboxInfo_(pod_meta); err != CraneErrCode::SUCCESS)
    return err;

  // Pull image according to pull policy
  auto image_id_opt = cri_client->PullImage(
      ca_meta->image().image(), ca_meta->image().username(),
      ca_meta->image().password(), ca_meta->image().server_address(),
      ca_meta->image().pull_policy());

  if (!image_id_opt.has_value()) {
    CRANE_ERROR("Failed to pull image {} for container step #{}.{}",
                ca_meta->image().image(), g_config.JobId, g_config.StepId);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  m_image_id_ = std::move(image_id_opt.value());

  // Prepare environment variables, will be used in SetContainerConfig_
  InitEnvMap();

  // Create the container but not start here
  if (auto err = SetContainerConfig_(*ca_meta, pod_meta);
      err != CraneErrCode::SUCCESS)
    return err;
  auto container_id_expt = cri_client->CreateContainer(m_pod_id_, m_pod_config_,
                                                       &m_container_config_);
  if (!container_id_expt.has_value()) {
    const auto& rich_err = container_id_expt.error();
    CRANE_ERROR("Failed to create container for #{}.{}: {}", g_config.JobId,
                g_config.StepId, rich_err.description());
    m_bindfs_mounts_.clear();
    return rich_err.code();
  }

  m_container_id_ = std::move(container_id_expt.value());
  CRANE_DEBUG("Container {} created for #{}.{}", m_container_id_,
              g_config.JobId, g_config.StepId);
  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Spawn() {
  using cri::kCriLabelJobIdKey;
  using cri::kCriLabelStepIdKey;
  using cri::kCriLabelUidKey;

  job_id_t job_id = m_parent_step_inst_->job_id;
  step_id_t step_id = m_parent_step_inst_->step_id;
  auto* cri_client = m_parent_step_inst_->GetCriClient();

  // Subscribe to container event stream
  // NOTE: The callback is called in another thread!
  if (!cri_client->IsEventStreamActive())
    cri_client->StartContainerEventStream(
        [](const cri::api::ContainerEventResponse& ev) {
          // Only care about stopped and deleted events
          if (ev.container_event_type() !=
                  cri::api::ContainerEventType::CONTAINER_STOPPED_EVENT &&
              ev.container_event_type() !=
                  cri::api::ContainerEventType::CONTAINER_DELETED_EVENT)
            return;

          CRANE_TRACE("Received CRI event: {}", ev.ShortDebugString());

          auto changes =
              ev.containers_statuses() |
              std::views::filter([](const auto& status) {
                return cri::CriClient::ParseAndCompareLabel(
                           status, std::string(kCriLabelJobIdKey),
                           g_config.JobId) &&
                       cri::CriClient::ParseAndCompareLabel(
                           status, std::string(kCriLabelStepIdKey),
                           g_config.StepId) &&
                       (status.state() ==
                            cri::api::ContainerState::CONTAINER_EXITED ||
                        status.state() ==
                            cri::api::ContainerState::CONTAINER_UNKNOWN);
              }) |
              std::ranges::to<std::vector<cri::api::ContainerStatus>>();

          if (!changes.empty()) g_task_mgr->HandleCriEvents(std::move(changes));
        });

  auto ret = cri_client->StartContainer(m_container_id_);
  if (!ret.has_value()) {
    const auto& rich_err = ret.error();
    CRANE_ERROR("Failed to start container for #{}.{}: {}", job_id, step_id,
                rich_err.description());
    return rich_err.code();
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Kill(int /*signum*/) {
  // For containers, we ignore the signum parameter and use CRI to stop/cleanup
  // Stop Container -> Remove Container. Pod is cleaned up in PodInstance.
  auto* cri_client = m_parent_step_inst_->GetCriClient();

  // Step 1: Stop Container
  if (!m_container_id_.empty()) {
    auto stop_ret = cri_client->StopContainer(m_container_id_, 10);
    if (!stop_ret.has_value()) {
      const auto& rich_err = stop_ret.error();
      CRANE_ERROR("Failed to stop container {}: {}", m_container_id_,
                  rich_err.description());
      // Try best effort to remove container, continue even if stopping fails.
    }

    // Step 2: Remove Container
    auto remove_ret = cri_client->RemoveContainer(m_container_id_);
    if (!remove_ret.has_value()) {
      const auto& rich_err = remove_ret.error();
      CRANE_ERROR("Failed to remove container {}: {}", m_container_id_,
                  rich_err.description());
      return rich_err.code();
    }
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Cleanup() {
  // For container jobs, Kill() is idempotent.
  // It's ok to call it (at most) twice to remove container.
  CraneErrCode err = Kill(0);
  m_bindfs_mounts_.clear();
  return err;
}

const TaskExitInfo& ContainerInstance::HandleContainerExited(
    const cri::api::ContainerStatus& status) {
  m_exit_info_.is_terminated_by_signal = status.exit_code() > 128;
  m_exit_info_.value = m_exit_info_.is_terminated_by_signal
                           ? status.exit_code() - 128
                           : status.exit_code();
  return m_exit_info_;
}

CraneErrCode ContainerInstance::LoadPodSandboxInfo_(
    const crane::grpc::PodTaskAdditionalMeta* pod_meta) {
  using cri::kCriDefaultLabel;
  using cri::kCriLabelJobIdKey;
  using cri::kCriLabelJobNameKey;
  using cri::kCriLabelUidKey;

  auto* cri_client = m_parent_step_inst_->GetCriClient();
  auto pod_config_file =
      m_log_dir_ / std::format(PodInstance::kPodConfigFilePattern,
                               g_config.CranedIdOfThisNode);
  if (!std::filesystem::exists(pod_config_file)) {
    CRANE_ERROR("Pod config file {} does not exist for container #{}.{}",
                pod_config_file, g_config.JobId, g_config.StepId);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  std::ifstream input(pod_config_file, std::ios::in | std::ios::binary);
  if (!input) {
    CRANE_ERROR("Failed to open pod config file {}: {}", pod_config_file,
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  if (!m_pod_config_.ParseFromIstream(&input)) {
    CRANE_ERROR("Failed to parse pod config from file {}", pod_config_file);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  auto lock_file = m_log_dir_ / std::format(PodInstance::kPodIdLockFilePattern,
                                            g_config.CranedIdOfThisNode);

  std::string pod_id;
  if (std::filesystem::exists(lock_file)) {
    std::ifstream pid_input(lock_file);
    if (!pid_input) {
      CRANE_WARN("Failed to open pod id file {}: {}", lock_file,
                 strerror(errno));
    } else if (!(pid_input >> pod_id)) {
      CRANE_WARN("Pod id file {} is empty for container #{}.{}", lock_file,
                 g_config.JobId, g_config.StepId);
      pod_id.clear();
    } else {
      CRANE_DEBUG("Read pod id {} from lock file {}", pod_id, lock_file);
    }
  }

  if (pod_id.empty()) {
    std::unordered_map<std::string, std::string> label_selector{
        {std::string(kCriDefaultLabel), "true"},
        {std::string(kCriLabelJobIdKey), std::to_string(g_config.JobId)},
        {std::string(kCriLabelUidKey),
         std::to_string(m_parent_step_inst_->pwd.Uid())},
        {std::string(kCriLabelJobNameKey), g_config.StepSpec.name()}};

    if (pod_meta != nullptr) {
      label_selector.insert(pod_meta->labels().begin(),
                            pod_meta->labels().end());
    }

    auto pod_id_expt = cri_client->GetPodSandboxId(label_selector);
    if (!pod_id_expt.has_value()) {
      const auto& rich_err = pod_id_expt.error();
      CRANE_ERROR("Failed to find pod sandbox for #{}.{}: {}", g_config.JobId,
                  g_config.StepId, rich_err.description());
      return rich_err.code();
    }

    pod_id = std::move(pod_id_expt.value());
    CRANE_DEBUG("Resolved pod id {} via label selector for #{}.{}", pod_id,
                g_config.JobId, g_config.StepId);
  }

  auto status_expt = cri_client->GetPodSandboxStatus(pod_id);
  if (!status_expt.has_value()) {
    const auto& rich_err = status_expt.error();
    CRANE_ERROR("Failed to get status for pod {}: {}", pod_id,
                rich_err.description());
    return rich_err.code();
  }

  const auto& status = status_expt.value();
  if (status.state() != cri::api::PodSandboxState::SANDBOX_READY) {
    CRANE_ERROR("Pod {} state is {} (expected READY)", pod_id,
                cri::api::PodSandboxState_Name(status.state()));
    return CraneErrCode::ERR_CRI_GENERIC;
  }

  m_pod_id_ = std::move(pod_id);
  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::SetContainerConfig_(
    const crane::grpc::ContainerTaskAdditionalMeta& ca_meta,
    const crane::grpc::PodTaskAdditionalMeta* pod_meta) {
  using cri::kCriLabelJobIdKey;
  using cri::kCriLabelJobNameKey;
  using cri::kCriLabelStepIdKey;
  using cri::kCriLabelStepNameKey;
  using cri::kCriLabelUidKey;

  // There is only one container in a step. So we use step_id and job_id here.
  job_id_t job_id = g_config.JobId;
  step_id_t step_id = g_config.StepId;

  const std::string& job_name = g_config.JobName;
  const std::string& step_name = GetParentStep().name();

  uid_t uid = m_parent_step_inst_->pwd.Uid();
  gid_t gid = m_parent_step_inst_->pwd.Gid();

  // Using job_id/step_id to generate unique name in pod metadata
  m_container_config_.mutable_metadata()->set_name(
      std::format("job-{}-{}", job_id, step_id));

  // labels
  m_container_config_.mutable_labels()->insert({
      {std::string(kCriLabelUidKey), std::to_string(uid)},
      {std::string(kCriLabelJobIdKey), std::to_string(job_id)},
      {std::string(kCriLabelStepIdKey), std::to_string(step_id)},
      {std::string(kCriLabelJobNameKey), job_name},
      {std::string(kCriLabelStepNameKey), step_name},
  });

  // log_path (a relative path to pod log dir)
  m_container_config_.set_log_path(m_log_file_.filename());

  // image already checked and pulled.
  m_container_config_.mutable_image()->set_image(m_image_id_);

  // interactive options
  m_container_config_.set_stdin(ca_meta.stdin());
  m_container_config_.set_stdin_once(ca_meta.stdin_once());
  m_container_config_.set_tty(ca_meta.tty());

  // security context
  auto* sec_ctx =
      m_container_config_.mutable_linux()->mutable_security_context();

  // Currently we don't support setting namespace mode per container.
  sec_ctx->mutable_namespace_options()->CopyFrom(
      m_pod_config_.mutable_linux()
          ->mutable_security_context()
          ->namespace_options());

  // Setup run_as_user / run_as_group
  if (uid == 0 || pod_meta->userns() ||
      (pod_meta->run_as_user() == uid && pod_meta->run_as_group() == gid)) {
    // If user is root or using userns, run_as_* is always allowed.
    // Non-root user w/o userns cannot use run_as_* other than its own.
    sec_ctx->mutable_run_as_user()->set_value(pod_meta->run_as_user());
    sec_ctx->mutable_run_as_group()->set_value(pod_meta->run_as_group());
  } else {
    CRANE_ERROR(
        "Container #{}.{} is not allowed to use other identities when not "
        "using userns",
        job_id, step_id);
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  bool use_bindfs = pod_meta->userns() && g_config.Container.BindFs.Enabled &&
                    !ca_meta.mounts().empty();
  if (use_bindfs)
    CRANE_TRACE("Using bindfs for container mounts of #{}.{}", job_id, step_id);

  // mounts
  for (const auto& mount : ca_meta.mounts()) {
    auto* m = m_container_config_.add_mounts();

    // If relative path is given, handle it with cwd.
    std::filesystem::path host_path(mount.first);
    if (host_path.is_relative()) host_path = GetParentStep().cwd() / host_path;
    if (!std::filesystem::exists(host_path)) {
      CRANE_ERROR("Mount host path {} does not exist for #{}.{}", host_path,
                  job_id, step_id);
      return CraneErrCode::ERR_INVALID_PARAM;
    }

    // Check file permissions
    if (!util::os::CheckUserHasPermission(uid, gid, host_path)) {
      CRANE_ERROR("User {} does not have permission to access mount path {}",
                  uid, host_path);
      return CraneErrCode::ERR_INVALID_PARAM;
    }

    // Pre check for bindfs
    if (use_bindfs) {
      if (!std::filesystem::is_directory(host_path)) {
        CRANE_ERROR(
            "Bindfs only supports directory mounts. Host path {} is invalid "
            "for #{}.{}",
            host_path, job_id, step_id);
        return CraneErrCode::ERR_INVALID_PARAM;
      }
    }

    m->set_host_path(std::move(host_path));
    m->set_container_path(mount.second);
    m->set_propagation(cri::api::MountPropagation::PROPAGATION_PRIVATE);
  }

  // Setup idmapped mounts if using userns.
  if (pod_meta->userns() &&
      ApplyIdMappedMounts_(m_parent_step_inst_->pwd, &m_container_config_,
                           use_bindfs) != CraneErrCode::SUCCESS) {
    CRANE_ERROR("Failed to apply idmapped mounts for #{}.{}", job_id, step_id);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // workdir
  if (!ca_meta.workdir().empty())
    m_container_config_.set_working_dir(ca_meta.workdir());

  // command and args
  if (!ca_meta.command().empty())
    m_container_config_.mutable_command()->Add()->assign(ca_meta.command());
  m_container_config_.mutable_args()->CopyFrom(ca_meta.args());

  // envs
  for (const auto& [name, value] : m_env_) {
    auto* env = m_container_config_.add_envs();
    env->set_key(name);
    env->set_value(value);
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::ApplyIdMappedMounts_(
    const PasswordEntry& pwd, cri::api::ContainerConfig* config,
    bool use_bindfs) {
  // NOTE: These methods are assuming pwd.Gid() is the same as egid.
  // which could be problematic. But for most HPC scenarios this should be fine.
  if (use_bindfs) {
    // If idmapped mounts not supported by FS/kernel,
    // use bindfs as a workaround.
    return SetupIdMappedBindFs_(pwd, config);
  }
  // Use standard linux idmapped mounts
  return SetupIdMappedMounts_(pwd, config);
}

CraneErrCode ContainerInstance::SetupIdMappedBindFs_(
    const PasswordEntry& pwd, cri::api::ContainerConfig* config) {
  const auto& sec_ctx = config->mutable_linux()->mutable_security_context();
  uid_t run_as_user = sec_ctx->run_as_user().value();
  gid_t run_as_group = sec_ctx->run_as_group().value();

  const auto& uid_mapping =
      sec_ctx->mutable_namespace_options()->mutable_userns_options()->uids(0);
  const auto& gid_mapping =
      sec_ctx->mutable_namespace_options()->mutable_userns_options()->gids(0);

  // For example, leo is 1000 on host, with a subid range
  // 101000-102000.
  // When leo want a userns container and run as 10 inside the container,
  // then:
  //   uid_offset = 101000 - 1000 + 10 = 100010
  // The bindfs will utilize these to create a FUSE mount point.
  // When using the mount point inside the container:
  //   uid 10 in container -> uid 101010 in kernel
  //   101010(kuid) - 100010(offset) = 1000(leo) on host
  uid_t uid_offset = uid_mapping.host_id() - pwd.Uid() + run_as_user;
  gid_t gid_offset = gid_mapping.host_id() - pwd.Gid() + run_as_group;

  std::vector<std::unique_ptr<bindfs::IdMappedBindFs>> bindfs_mounts;
  bindfs_mounts.reserve(config->mounts().size());
  m_bindfs_mounts_.clear();

  try {
    for (auto& m : *config->mutable_mounts()) {
      auto mount = std::make_unique<bindfs::IdMappedBindFs>(
          m.host_path(), m_parent_step_inst_->pwd, pwd.Uid(), pwd.Gid(),
          uid_offset, gid_offset, g_config.Container.BindFs.BindfsBinary,
          g_config.Container.BindFs.FusermountBinary,
          g_config.Container.BindFs.MountBaseDir);

      m.set_host_path(mount->GetMountedPath());
      bindfs_mounts.emplace_back(std::move(mount));
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to setup bindfs: {}", e.what());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  m_bindfs_mounts_ = std::move(bindfs_mounts);
  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::SetupIdMappedMounts_(
    const PasswordEntry& pwd, cri::api::ContainerConfig* config) {
  using cri::CriClient;

  const auto& sec_ctx = config->mutable_linux()->mutable_security_context();
  uid_t run_as_user = sec_ctx->run_as_user().value();
  gid_t run_as_group = sec_ctx->run_as_group().value();

  const auto& uid_mapping =
      sec_ctx->mutable_namespace_options()->mutable_userns_options()->uids(0);
  const auto& gid_mapping =
      sec_ctx->mutable_namespace_options()->mutable_userns_options()->gids(0);

  // Use idmapped mounts for user ns.
  // Code below could be unintuitive. Check vfsuid for details.
  auto kuid = run_as_user + uid_mapping.host_id();
  auto kgid = run_as_group + gid_mapping.host_id();

  auto mount_uid_mapping = CriClient::MakeIdMapping(kuid, pwd.Uid(), 1);
  auto mount_gid_mapping = CriClient::MakeIdMapping(kgid, pwd.Gid(), 1);

  // modify mounts, see comments regarding *mapping members
  for (auto& mount : *config->mutable_mounts()) {
    mount.mutable_uidmappings()->Add()->CopyFrom(mount_uid_mapping);
    mount.mutable_gidmappings()->Add()->CopyFrom(mount_gid_mapping);
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ProcInstance::Prepare() {
  // Write m_meta_
  if (m_parent_step_inst_->IsCrun()) {
    m_meta_ = std::make_unique<CrunInstanceMeta>();
  } else {
    // Prepare file output name for batch tasks.
    /* Perform file name substitutions
     * %j - Job ID
     * %u - Username
     * %x - Job name
     */
    auto meta = std::make_unique<BatchInstanceMeta>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        GetParentStep().batch_meta().output_file_pattern(),
        GetParentStep().cwd());

    // If -e / --error is not defined, leave
    // batch_meta.parsed_error_file_pattern empty;
    if (!GetParentStep().batch_meta().error_file_pattern().empty()) {
      meta->parsed_error_file_pattern = ParseFilePathPattern_(
          GetParentStep().batch_meta().error_file_pattern(),
          GetParentStep().cwd());
    }

    m_meta_ = std::move(meta);
  }

  std::filesystem::path sh_path = m_parent_step_inst_->script_path.value();
  m_meta_->parsed_sh_script_path = sh_path;

  // If interpreter is not set, let system decide.
  m_executable_ = sh_path.string();
  if (!m_parent_step_inst_->IsInteractive() &&
      !GetParentStep().batch_meta().interpreter().empty()) {
    m_executable_ = fmt::format(
        "{} {}", GetParentStep().batch_meta().interpreter(), sh_path.string());
  }
  // TODO: Currently we don't support arguments in scripts.
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

  if (m_parent_step_inst_->IsCrun() && m_parent_step_inst_->x11) {
    auto err = PrepareXauthFiles_();
    if (err != CraneErrCode::SUCCESS) {
      CRANE_WARN("Failed to prepare crun X11 forwarding.");
      return err;
    }
  }

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  pid_t child_pid = -1;
  if (m_parent_step_inst_->IsCrun()) {
    auto pid_expt = ForkCrunAndInitMeta_();
    if (!pid_expt) return pid_expt.error();

    child_pid = pid_expt.value();
  } else {
    child_pid = fork();
  }

  if (child_pid == -1) {
    CRANE_ERROR("fork() failed for task #{}: {}", task_id, strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}", task_id, m_pid_);
    int ctrl_fd = ctrl_sock_pair[0];
    close(ctrl_sock_pair[1]);

    bool ok;
    FileInputStream istream(ctrl_fd);
    FileOutputStream ostream(ctrl_fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    bool crun_init_success{true};
    if (m_parent_step_inst_->IsCrun()) {
      if (m_parent_step_inst_->x11) {
        uint16_t x11_port;
        crun_init_success = SetupCrunFwdAtParent_(&x11_port);
        msg.set_x11_port(x11_port);
      } else {
        crun_init_success = SetupCrunFwdAtParent_(nullptr);
      }

      CRANE_DEBUG("Task #{} has initialized crun forwarding.", task_id);
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                task_id);

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(crun_init_success);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    if (!ok) {
      CRANE_ERROR("Failed to serialize msg to ostream: {}",
                  strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("Failed to send ok=true to subprocess {} for task #{}: {}",
                  child_pid, task_id, strerror(ostream.GetErrno()));
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
        CRANE_ERROR("False from subprocess {} of task #{}", child_pid, task_id);
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

    CraneErrCode err = SetChildProcProperty_();
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

    if (m_parent_step_inst_->IsCrun()) {
      SetupCrunFwdAtChild_();
    } else if (SetChildProcBatchFd_() != CraneErrCode::SUCCESS) {
      std::abort();
    }

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      fmt::print(stderr, "[Subprocess] Error: Failed to flush.\n");
      std::abort();
    }

    close(ctrl_fd);

    // Close stdin for non-interactive(batch) tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    if (!m_parent_step_inst_->IsInteractive()) close(0);
    util::os::CloseFdFrom(3);

    if (m_parent_step_inst_->IsCrun() && m_parent_step_inst_->x11) {
      this->GetCrunMeta_()->x11_port = msg.x11_port();
      SetupChildProcCrunX11_();
    }

    // Apply environment variables
    InitEnvMap();

    if (!g_config.JobLifecycleHook.TaskPrologs.empty()) {
      RunPrologEpilogArgs run_prolog_args{
          .scripts = g_config.JobLifecycleHook.TaskPrologs,
          .envs = m_env_,
          .run_uid = m_parent_step_inst_->uid,
          .run_gid = m_parent_step_inst_->gids[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};
      if (g_config.JobLifecycleHook.PrologTimeout > 0)
        run_prolog_args.timeout_sec = g_config.JobLifecycleHook.PrologTimeout;
      else if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
        run_prolog_args.timeout_sec =
            g_config.JobLifecycleHook.PrologEpilogTimeout;
      auto result = util::os::RunPrologOrEpiLog(run_prolog_args);
      if (!result) {
        auto status = result.error();
        fmt::print(
            stderr,
            "[Subprocess] Error: Failed to run task prolog, status={}:{}\n",
            status.exit_code, status.signal_num);
        std::abort();
      }
      util::os::ApplyPrologOutputToEnvAndStdout(result.value(), &m_env_,
                                                STDOUT_FILENO);
    }

    if (!m_parent_step_inst_->GetStep().task_prolog().empty()) {
      RunPrologEpilogArgs run_prolog_args{
          .scripts =
              std::vector<std::string>{
                  m_parent_step_inst_->GetStep().task_prolog()},
          .envs = m_env_,
          .run_uid = m_parent_step_inst_->uid,
          .run_gid = m_parent_step_inst_->gids[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};
      if (g_config.JobLifecycleHook.PrologTimeout > 0)
        run_prolog_args.timeout_sec = g_config.JobLifecycleHook.PrologTimeout;
      else if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
        run_prolog_args.timeout_sec =
            g_config.JobLifecycleHook.PrologEpilogTimeout;
      auto result = util::os::RunPrologOrEpiLog(run_prolog_args);
      if (!result) {
        auto status = result.error();
        fmt::print(stderr,
                   "[Subprocess] Error: Failed to run step task prolog, "
                   "status={}:{}\n",
                   status.exit_code, status.signal_num);
        std::abort();
      }
      util::os::ApplyPrologOutputToEnvAndStdout(result.value(), &m_env_,
                                                STDOUT_FILENO);
    }

    err = SetChildProcEnv_();
    if (err != CraneErrCode::SUCCESS) {
      fmt::print(stderr, "[Subprocess] Error: Failed to set environment ");
    }

    // Prepare the command line arguments.
    auto argv = GetChildProcExecArgv_();
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
  if (m_pid_ != 0) {
    CRANE_TRACE("Killing pid {} with signal {}", m_pid_, signum);

    // Send the signal to the whole process group.
    int err = kill(-m_pid_, signum);
    if (err == 0) return CraneErrCode::SUCCESS;

    CRANE_TRACE("Killing pid {} failed, error: {}", m_pid_, strerror(errno));
    return CraneErrCode::ERR_GENERIC_FAILURE;
  }

  return CraneErrCode::ERR_NON_EXISTENT;
}

CraneErrCode ProcInstance::Cleanup() {
  // No cleanup for calloc.
  if (!m_parent_step_inst_->IsCalloc()) {
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
                status, task_id);
    return std::nullopt;
  }

  return std::optional<TaskExitInfo>{m_exit_info_};
}

TaskManager::TaskManager()
    : m_supervisor_exit_(false), m_step_(g_config.StepSpec) {
  m_uvw_loop_ = uvw::loop::create();

  m_shutdown_supervisor_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_shutdown_supervisor_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvShutdownSupervisorCb_();
      });

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });
  if (m_sigchld_handle_->start(SIGCHLD) != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle");
  }
  m_sigchld_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_sigchld_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        EvSigchldTimerCb_();
      });
  m_sigchld_timer_handle_->start(std::chrono::seconds(1),
                                 std::chrono::seconds(1));

  m_process_sigchld_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_process_sigchld_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanSigchldQueueCb_();
      });

  m_task_stopped_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_task_stopped_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTaskStopQueueCb_();
      });

  m_cri_event_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_cri_event_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanCriEventQueueCb_();
      });

  m_terminate_task_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_task_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateTaskQueueCb_();
      });
  m_terminate_task_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_terminate_task_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        m_terminate_task_async_handle_->send();
      });
  m_terminate_task_timer_handle_->start(
      std::chrono::milliseconds(kStepRequestCheckIntervalMs * 3),
      std::chrono::milliseconds(kStepRequestCheckIntervalMs));

  m_change_task_time_limit_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_change_task_time_limit_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanChangeTaskTimeLimitQueueCb_();
      });
  m_change_task_time_limit_timer_handle_ =
      m_uvw_loop_->resource<uvw::timer_handle>();
  m_change_task_time_limit_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        m_change_task_time_limit_async_handle_->send();
      });
  m_change_task_time_limit_timer_handle_->start(
      std::chrono::milliseconds(kStepRequestCheckIntervalMs * 3),
      std::chrono::milliseconds(kStepRequestCheckIntervalMs));

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
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
  CRANE_TRACE("TaskManager destroyed.");

  if (!g_config.JobLifecycleHook.Epilogs.empty()) {
    CRANE_TRACE("Running Epilogs...");
    RunPrologEpilogArgs run_epilog_args{
        .scripts = g_config.JobLifecycleHook.Epilogs,
        .envs = g_config.JobEnv,
        .run_uid = 0,
        .run_gid = 0,
        .output_size = g_config.JobLifecycleHook.MaxOutputSize};
    if (g_config.JobLifecycleHook.EpilogTimeout > 0)
      run_epilog_args.timeout_sec = g_config.JobLifecycleHook.EpilogTimeout;
    else if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
      run_epilog_args.timeout_sec =
          g_config.JobLifecycleHook.PrologEpilogTimeout;

    auto result = util::os::RunPrologOrEpiLog(run_epilog_args);
    if (!result) {
      auto status = result.error();
      CRANE_DEBUG("Epilog failed status={}:{}", status.exit_code,
                  status.signal_num);
    } else {
      CRANE_DEBUG("Epilog success");
    }
  }
}

void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::ShutdownSupervisorAsync(crane::grpc::TaskStatus new_status,
                                          uint32_t exit_code,
                                          std::string reason) {
  CRANE_INFO("All tasks finished, exiting...");
  m_shutdown_status_queue_.enqueue(
      std::make_tuple(new_status, exit_code, std::move(reason)));
  m_shutdown_supervisor_handle_->send();
}

void TaskManager::TaskStopAndDoStatusChange(task_id_t task_id) {
  m_task_stopped_queue_.enqueue(task_id);
  m_task_stopped_async_handle_->send();
}

void TaskManager::TaskFinish_(task_id_t task_id,
                              crane::grpc::TaskStatus new_status,
                              uint32_t exit_code,
                              std::optional<std::string> reason) {
  CRANE_TRACE(
      "[Task #{}] Task status changed to {} (exit code: {}, reason: {}).",
      task_id, new_status, exit_code, reason.value_or(""));

  // NOTE: Daemon step's supervisor only sent Running/Failed/Completed status
  // when starting or actively shutting down the step.

  if (!m_step_.IsDaemon() &&
      g_runtime_status.Status.load() != StepStatus::Completing) {
    CRANE_INFO("Step completing now, prev status: {}.",
               g_runtime_status.Status.load());
    g_runtime_status.Status = StepStatus::Completing;
  }

  auto task = m_step_.RemoveTaskInstance(task_id);
  if (task == nullptr) {
    CRANE_DEBUG("[Task #{}] Task not found.", task_id);
    return;
  }

  // One-shot model: nothing to stop, just proceed to cleanup.
  m_step_.oom_baseline_inited = false;

  auto err = task->Cleanup();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_WARN("[Task #{}] Failed to cleanup task: {}", task_id,
               static_cast<int>(err));
  }

  bool orphaned = m_step_.orphaned;
  if (m_step_.AllTaskFinished()) {
    DelTerminationTimer_();
    m_step_.StopCforedClient();
    m_step_.StopCriClient();

    if (!orphaned && !m_step_.IsDaemon()) {
      g_craned_client->StepStatusChangeAsync(new_status, exit_code,
                                             std::move(reason));
    }

    ShutdownSupervisorAsync();
  }
}

std::future<CraneErrCode> TaskManager::ExecuteTaskAsync() {
  CRANE_INFO("[Job #{}.{}] Executing task.", m_step_.job_id, m_step_.step_id);

  std::promise<CraneErrCode> ok_promise;
  std::future ok_future = ok_promise.get_future();

  auto elem = ExecuteTaskElem{.ok_prom = std::move(ok_promise)};

  if (m_step_.IsPod()) {
    // Pod
    elem.instance = std::make_unique<PodInstance>(&m_step_);
  } else if (m_step_.IsContainer()) {
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

CraneErrCode TaskManager::LaunchExecution_(ITaskInstance* task) {
  // Prepare/Spawn should not be called for Calloc steps.
  CRANE_ASSERT_MSG(!m_step_.IsCalloc(), "Calloc step should not launch tasks");

  // Init CriClient before preparation
  if (m_step_.IsPod() || m_step_.IsContainer()) m_step_.InitCriClient();

  // Prepare for execution
  CRANE_INFO("[Task #{}] Preparing task", task->task_id);
  CraneErrCode err = task->Prepare();
  if (err != CraneErrCode::SUCCESS) {
    TaskFinish_(
        task->task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::EC_FILE_NOT_FOUND,
        fmt::format("Failed to prepare task, code: {}", static_cast<int>(err)));
    return err;
  }

  // Init cfored before start the task.
  if (m_step_.IsCrun()) m_step_.InitCforedClient();

  CRANE_INFO("[Task #{}] Spawning process in task", task->task_id);
  err = task->Spawn();
  if (err != CraneErrCode::SUCCESS) {
    TaskFinish_(task->task_id, crane::grpc::TaskStatus::Failed,
                ExitCode::EC_SPAWN_FAILED,
                fmt::format("Cannot spawn child process in task, code: {}",
                            static_cast<int>(err)));
    return err;
  }

  return CraneErrCode::SUCCESS;
}

std::future<CraneExpected<EnvMap>> TaskManager::QueryStepEnvAsync() {
  std::promise<CraneExpected<EnvMap>> env_promise;
  auto env_future = env_promise.get_future();
  m_grpc_query_step_env_queue_.enqueue(std::move(env_promise));
  m_grpc_query_step_env_async_handle_->send();
  return env_future;
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
                                     TerminatedBy terminated_by) {
  m_task_terminate_queue_.enqueue(TaskTerminateQueueElem{
      .termination_reason = terminated_by,
      .mark_as_orphaned = mark_as_orphaned,
  });
  m_terminate_task_async_handle_->send();
}

void TaskManager::EvShutdownSupervisorCb_() {
  std::tuple<crane::grpc::TaskStatus, uint32_t, std::string> final_status;
  bool got_final_status = false;
  do {
    got_final_status = m_shutdown_status_queue_.try_dequeue(final_status);
    if (!got_final_status) continue;

    if (m_step_.IsDaemon() && !m_active_shutdown_.load()) {
      CRANE_WARN("Daemon step not shutting down unless explicitly requested.");
      continue;
    }

    auto& [status, exit_code, reason] = final_status;

    if (m_step_.IsDaemon()) {
      // For daemon steps, always send a status change when shutting down.
      CRANE_DEBUG("Sending a {} status as daemon step.", status);
      g_runtime_status.Status = status;
      if (!m_step_.orphaned)
        g_craned_client->StepStatusChangeAsync(status, exit_code, reason);
    }

    CRANE_INFO("All tasks finished, exiting...");
    m_step_.CleanUp();

    g_craned_client->Shutdown();
    g_server->Shutdown();

    this->Shutdown();

  } while (!got_final_status);
}

void TaskManager::EvSigchldCb_() {
  int status;
  pid_t pid;

  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      CRANE_TRACE("Receiving SIGCHLD for pid {}", pid);
      m_sigchld_queue_.enqueue({pid, status});
      m_process_sigchld_async_handle_->send();
    } else if (pid == 0) {
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void TaskManager::EvSigchldTimerCb_() {
  m_process_sigchld_async_handle_->send();
}

void TaskManager::EvCleanSigchldQueueCb_() {
  std::vector<std::pair<pid_t, int>> not_found_tasks;
  std::pair<pid_t, int> elem;

  while (m_sigchld_queue_.try_dequeue(elem)) {
    auto [pid, status] = elem;
    auto it = m_exec_id_task_id_map_.find(pid);
    if (it == m_exec_id_task_id_map_.end()) {
      not_found_tasks.emplace_back(elem);
      CRANE_TRACE("Cannot find task for pid {}, will check next time.", pid);
      continue;
    }

    auto task_id = it->second;
    CRANE_TRACE("[Task #{}] Receiving SIGCHLD for pid {}", task_id, pid);

    // Only ProcInstance could receive SIGCHLD.
    auto* task = dynamic_cast<ProcInstance*>(m_step_.GetTaskInstance(task_id));
    const auto exit_info = task->HandleSigchld(pid, status);
    if (!exit_info.has_value()) continue;

    CRANE_INFO("Receiving SIGCHLD for pid {}. Signaled: {}, Value: {}", pid,
               exit_info->is_terminated_by_signal, exit_info->value);

    if (m_step_.IsCrun()) {
      // TaskStatusChange of a crun task is triggered in
      // CforedManager.
      auto ok_to_free = m_step_.GetCforedClient()->TaskProcessStop(task_id);
      if (ok_to_free) {
        CRANE_TRACE("It's ok to unregister task #{}", task_id);
        m_step_.GetCforedClient()->TaskEnd(task_id);
      }
    } else /* Batch / Calloc */ {
      // If has no process/container left,
      // send TaskStatusChange for this task.
      TaskStopAndDoStatusChange(task_id);
    }
  }

  // Put not found tasks back to the queue
  for (auto task : not_found_tasks) {
    m_sigchld_queue_.enqueue(task);
  }
}

void TaskManager::EvCleanCriEventQueueCb_() {
  // TODO: Refactor this to Craned.
  cri::api::ContainerStatus status;
  while (m_cri_event_queue_.try_dequeue(status)) {
    // NOTE: a CRI event comes at LEAST once.
    // For container related steps, task_id always equals to 0.
    auto* t = m_step_.GetTaskInstance(0);
    if (t == nullptr) continue;  // Duplicated event

    if (m_step_.IsPod()) {
      auto* task = dynamic_cast<PodInstance*>(t);
      const auto& exit_info = task->HandlePodExited();

      CRANE_INFO("Receiving pod exited event: {}. Signaled: {}, Value: {}",
                 status.id(), exit_info.is_terminated_by_signal,
                 exit_info.value);

    } else if (m_step_.IsContainer()) {
      auto* task = dynamic_cast<ContainerInstance*>(t);
      const auto& exit_info = task->HandleContainerExited(status);

      CRANE_INFO(
          "Receiving container exited event: {}. Signaled: {}, Value: {}",
          status.id(), exit_info.is_terminated_by_signal, exit_info.value);

    } else {
      std::unreachable();
    }

    TaskStopAndDoStatusChange(0);
  }
}

void TaskManager::EvTaskTimerCb_() {
  CRANE_TRACE("task #{} exceeded its time limit. Terminating it...",
              g_config.JobId);

  DelTerminationTimer_();

  if (m_step_.IsCalloc()) {
    // Now calloc and cbatch steps only have one task with id 0.
    TaskFinish_(0, crane::grpc::TaskStatus::ExceedTimeLimit,
                ExitCode::EC_EXCEED_TIME_LIMIT, std::nullopt);
  } else {
    m_task_terminate_queue_.enqueue(TaskTerminateQueueElem{
        .termination_reason = TerminatedBy::TERMINATION_BY_TIMEOUT});
    m_terminate_task_async_handle_->send();
  }
}

void TaskManager::EvCleanTaskStopQueueCb_() {
  task_id_t task_id;
  while (m_task_stopped_queue_.try_dequeue(task_id)) {
    CRANE_INFO("[Task #{}] Stopped and is doing TaskStatusChange...", task_id);
    auto* task = m_step_.GetTaskInstance(task_id);
    if (task->GetExecId().has_value())
      m_exec_id_task_id_map_.erase(*task->GetExecId());

    if (!m_step_.GetStep().task_epilog().empty()) {
      CRANE_TRACE("[Task #{}] Running step task_epilog...", task_id);
      RunPrologEpilogArgs run_epilog_args{
          .scripts = std::vector<std::string>{m_step_.GetStep().task_epilog()},
          .envs = std::unordered_map{task->GetParentStep().env().begin(),
                                     task->GetParentStep().env().end()},
          .run_uid = task->GetParentStep().uid(),
          .run_gid = task->GetParentStep().gid()[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};
      if (g_config.JobLifecycleHook.EpilogTimeout > 0)
        run_epilog_args.timeout_sec = g_config.JobLifecycleHook.EpilogTimeout;
      else if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
        run_epilog_args.timeout_sec =
            g_config.JobLifecycleHook.PrologEpilogTimeout;
      auto result = util::os::RunPrologOrEpiLog(run_epilog_args);
      if (!result) {
        auto status = result.error();
        CRANE_DEBUG("[Task #{}]: step task_epilog failed status={}:{}", task_id,
                    status.exit_code, status.signal_num);
      } else {
        CRANE_DEBUG("[Task #{}]: task_epilog success", task_id);
      }
    }

    if (!g_config.JobLifecycleHook.TaskEpilogs.empty()) {
      CRANE_TRACE("[Task #{}] Running task_epilog...", task_id);
      RunPrologEpilogArgs run_epilog_args{
          .scripts = g_config.JobLifecycleHook.TaskEpilogs,
          .envs = std::unordered_map{task->GetParentStep().env().begin(),
                                     task->GetParentStep().env().end()},
          .run_uid = task->GetParentStep().uid(),
          .run_gid = task->GetParentStep().gid()[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};
      if (g_config.JobLifecycleHook.EpilogTimeout > 0)
        run_epilog_args.timeout_sec = g_config.JobLifecycleHook.EpilogTimeout;
      else if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
        run_epilog_args.timeout_sec =
            g_config.JobLifecycleHook.PrologEpilogTimeout;
      auto result = util::os::RunPrologOrEpiLog(run_epilog_args);
      if (!result) {
        auto status = result.error();
        CRANE_DEBUG("[Task #{}]: task_epilog failed status={}:{}", task_id,
                    status.exit_code, status.signal_num);
      } else {
        CRANE_DEBUG("[Task #{}]: task_epilog success", task_id);
      }
    }

    switch (task->err_before_exec) {
    case CraneErrCode::ERR_PROTOBUF:
      TaskFinish_(task_id, crane::grpc::TaskStatus::Failed,
                  ExitCode::EC_SPAWN_FAILED, std::nullopt);
      return;

    case CraneErrCode::ERR_CGROUP:
      TaskFinish_(task_id, crane::grpc::TaskStatus::Failed,
                  ExitCode::EC_CGROUP_ERR, std::nullopt);
      return;

    default:
      break;
    }

    const auto& exit_info = task->GetExitInfo();
    if (!m_step_.IsCalloc()) {
      bool signalled = exit_info.is_terminated_by_signal;
      if (task->terminated_by == TerminatedBy::NONE) {
        if (m_step_.EvaluateOomOnExit()) {
          task->terminated_by = TerminatedBy::TERMINATION_BY_OOM;
        }
      }

      if (signalled) {
        switch (task->terminated_by) {
        case TerminatedBy::CANCELLED_BY_USER:
          TaskFinish_(task_id, crane::grpc::TaskStatus::Cancelled,
                      exit_info.value + ExitCode::kTerminationSignalBase,
                      std::nullopt);
          break;
        case TerminatedBy::TERMINATION_BY_TIMEOUT:
          TaskFinish_(task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
                      ExitCode::EC_EXCEED_TIME_LIMIT, std::nullopt);
          break;
        case TerminatedBy::TERMINATION_BY_OOM:
          TaskFinish_(task_id, crane::grpc::TaskStatus::OutOfMemory,
                      exit_info.value + ExitCode::kTerminationSignalBase,
                      "Detected by oom_kill counter delta");
          break;
        default:
          TaskFinish_(task_id, crane::grpc::TaskStatus::Failed,
                      exit_info.value + ExitCode::kTerminationSignalBase,
                      std::nullopt);
        }
      } else if (exit_info.value == 0) {
        TaskFinish_(task_id, crane::grpc::TaskStatus::Completed, 0,
                    std::nullopt);
      } else {
        if (task->terminated_by == TerminatedBy::TERMINATION_BY_OOM) {
          TaskFinish_(task_id, crane::grpc::TaskStatus::OutOfMemory,
                      exit_info.value,
                      "Detected by oom_kill counter delta (no signal)");
        } else {
          TaskFinish_(task_id, crane::grpc::TaskStatus::Failed, exit_info.value,
                      std::nullopt);
        }
      }
    } else /* Calloc */ {
      // For a COMPLETING Calloc task with a process running,
      // the end of this process means that this task is done.
      if (exit_info.is_terminated_by_signal)
        TaskFinish_(task_id, crane::grpc::TaskStatus::Completed,
                    exit_info.value + ExitCode::kTerminationSignalBase,
                    std::nullopt);
      else if (exit_info.value == 0)
        TaskFinish_(task_id, crane::grpc::TaskStatus::Completed, 0,
                    std::nullopt);
      else
        TaskFinish_(task_id, crane::grpc::TaskStatus::Failed, exit_info.value,
                    std::nullopt);
    }
  }
}

void TaskManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  std::vector<TaskTerminateQueueElem> not_ready_elems;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE("Receive TerminateRunningTask Request for #{}.{}.",
                g_config.JobId, g_config.StepId);

    if (m_step_.IsDaemon() && !m_active_shutdown_.load()) {
      CRANE_TRACE(
          "TerminateRunningTask request ignored for daemon step unless active "
          "shutdown is in progress.",
          g_config.JobId, g_config.StepId);
      continue;
    }

    if (elem.mark_as_orphaned) m_step_.orphaned = true;
    if (!elem.mark_as_orphaned && !g_runtime_status.CanStepOperate()) {
      not_ready_elems.emplace_back(elem);
      CRANE_DEBUG("Task is not ready to terminate, will check next time.");
      continue;
    }

    if (!elem.mark_as_orphaned && m_step_.AllTaskFinished()) {
      CRANE_DEBUG("Terminating a completing step #{}.{}, ignored.",
                  g_config.JobId, g_config.StepId);
      continue;
    }

    int sig = m_step_.IsInteractive() ? SIGHUP : SIGTERM;

    if (m_exec_id_task_id_map_.empty() && elem.mark_as_orphaned) {
      CRANE_DEBUG("No task is running, shutting down...");
      g_task_mgr->ShutdownSupervisorAsync();
      continue;
    }

    CRANE_TRACE("Terminating all running tasks (orphaned: {}, reason: {})...",
                elem.mark_as_orphaned,
                static_cast<int>(elem.termination_reason));

    for (task_id_t task_id : m_step_.GetTaskIds()) {
      auto* task = m_step_.GetTaskInstance(task_id);
      if (elem.termination_reason == TerminatedBy::TERMINATION_BY_TIMEOUT) {
        task->terminated_by = TerminatedBy::TERMINATION_BY_TIMEOUT;
      } else if (elem.termination_reason == TerminatedBy::CANCELLED_BY_USER) {
        // If termination request is sent by user, send SIGKILL to ensure that
        // even freezing processes will be terminated immediately.
        sig = SIGKILL;
        task->terminated_by = TerminatedBy::CANCELLED_BY_USER;
      }

      if (task->GetExecId().has_value()) {
        // Will kill the following types of tasks:
        // 1. Non-interactive Task
        // 2. Interactive Task with a process running
        task->Kill(sig);  // NOTE: retval discarded
      } else if (m_step_.IsInteractive()) {
        // For an Interactive task with no process running, it ends
        // immediately.
        TaskFinish_(task_id, crane::grpc::TaskStatus::Completed,
                    ExitCode::EC_TERMINATED, std::nullopt);
      } else {
        CRANE_ASSERT_MSG(false, "Terminating a batch step without any task");
      }
    }
  }

  for (auto& not_ready_elem : not_ready_elems) {
    m_task_terminate_queue_.enqueue(not_ready_elem);
  }
}

void TaskManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  absl::Time now = absl::Now();

  ChangeTaskTimeLimitQueueElem elem;
  std::vector<ChangeTaskTimeLimitQueueElem> not_ready_elems;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    if (!g_runtime_status.CanStepOperate()) {
      not_ready_elems.emplace_back(std::move(elem));
      CRANE_DEBUG(
          "Task is not ready to change time limit, will check next time.");
      continue;
    }
    if (m_step_.AllTaskFinished()) {
      CRANE_DEBUG("Change timelimit for a completing task, ignored.");
      continue;
    }
    // Delete the old timer.
    DelTerminationTimer_();

    absl::Time start_time =
        absl::FromUnixSeconds(m_step_.GetStep().start_time().seconds());
    absl::Duration const& new_time_limit = elem.time_limit;

    if (now - start_time >= new_time_limit) {
      // If the task times out, terminate it.
      TaskTerminateQueueElem ev_task_terminate{
          .termination_reason = TerminatedBy::TERMINATION_BY_TIMEOUT};
      m_task_terminate_queue_.enqueue(ev_task_terminate);
      m_terminate_task_async_handle_->send();
    } else {
      // If the task haven't timed out, set up a new timer.
      AddTerminationTimer_(
          ToInt64Seconds((new_time_limit - (absl::Now() - start_time))));
    }

    elem.ok_prom.set_value(CraneErrCode::SUCCESS);
  }
  for (auto& not_ready_elem : not_ready_elems) {
    m_task_time_limit_change_queue_.enqueue(std::move(not_ready_elem));
  }
}

void TaskManager::EvGrpcExecuteTaskCb_() {
  ExecuteTaskElem elem;
  while (m_grpc_execute_task_queue_.try_dequeue(elem)) {
    if (!elem.instance) {
      elem.ok_prom.set_value(CraneErrCode::ERR_GENERIC_FAILURE);
      continue;
    }

    g_runtime_status.Status = StepStatus::Running;
    task_id_t task_id = elem.instance->task_id;
    m_step_.AddTaskInstance(task_id, std::move(elem.instance));

    auto* task = m_step_.GetTaskInstance(task_id);
    if (auto err = m_step_.Prepare(); err != CraneErrCode::SUCCESS) {
      CRANE_ERROR("Failed to prepare step for task execution.");
      elem.ok_prom.set_value(err);
      TaskFinish_(task_id, crane::grpc::TaskStatus::Failed,
                  ExitCode::EC_FILE_NOT_FOUND, "");
      continue;
    }

    if (!m_step_.IsDaemon()) {
      // Add a timer to limit the execution time of a task.
      int64_t sec = m_step_.GetStep().time_limit().seconds();
      AddTerminationTimer_(sec);
      CRANE_INFO("Add a timer of {} seconds", sec);
    }

    m_step_.pwd.Init(m_step_.uid);
    if (!m_step_.pwd.Valid()) {
      CRANE_ERROR("Failed to look up password entry for uid {}", m_step_.uid);
      TaskFinish_(task_id, crane::grpc::TaskStatus::Failed,
                  ExitCode::EC_PERMISSION_DENIED,
                  fmt::format("Failed to look up password entry for uid {}",
                              m_step_.uid));
      elem.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      continue;
    }
    // Calloc tasks have no scripts to run. Just return.
    if (m_step_.IsCalloc()) {
      CRANE_DEBUG("Calloc step, no script to run.");
      m_exec_id_task_id_map_[0] = task_id;  // Placeholder
      m_step_.InitOomBaseline();
      elem.ok_prom.set_value(CraneErrCode::SUCCESS);
      continue;
    }

    // Single threaded here, it is always safe to ask TaskManager to
    // operate (Like terminate due to cfored conn err for crun task) any task.
    auto err = LaunchExecution_(task);
    if (err != CraneErrCode::SUCCESS) {
      CRANE_WARN("[Task #{}] Failed to launch process.", task_id);
    } else {
      auto exec_id = task->GetExecId().value();
      CRANE_INFO("[Task #{}] Launched exection, id: {}.", task_id,
                 std::visit([](auto&& arg) { return std::format("{}", arg); },
                            exec_id));
      m_exec_id_task_id_map_[exec_id] = task_id;
      m_step_.InitOomBaseline();
    }

    elem.ok_prom.set_value(err);
  }
}

void TaskManager::EvGrpcQueryStepEnvCb_() {
  std::promise<CraneExpected<EnvMap>> elem;
  while (m_grpc_query_step_env_queue_.try_dequeue(elem)) {
    elem.set_value(m_step_.GetStepProcessEnv());
  }
}
}  // namespace Craned::Supervisor
