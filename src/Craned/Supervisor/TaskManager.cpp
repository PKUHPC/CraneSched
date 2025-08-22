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
#include <sys/types.h>
#include <sys/wait.h>

#include <utility>

#include "CforedClient.h"
#include "CgroupManager.h"
#include "CranedClient.h"
#include "SupervisorPublicDefs.h"
#include "SupervisorServer.h"
#include "crane/Logger.h"
#include "crane/OS.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"
#include "cri/api.pb.h"

namespace Craned::Supervisor {

using Common::CgroupManager;

bool StepInstance::IsContainer() const noexcept { return !container.empty(); }

bool StepInstance::IsBatch() const noexcept {
  return !interactive_type.has_value();
}

bool StepInstance::IsCrun() const noexcept {
  return interactive_type.has_value() &&
         interactive_type.value() == crane::grpc::Crun;
}

bool StepInstance::IsCalloc() const noexcept {
  return interactive_type.has_value() &&
         interactive_type.value() == crane::grpc::Calloc;
}

EnvMap StepInstance::GetStepProcessEnv() const {
  std::unordered_map<std::string, std::string> env_map;

  // Job env from CraneD
  for (auto& [name, value] : g_config.JobEnv) {
    env_map.emplace(name, value);
  }

  // Crane env will override user task env;
  for (auto& [name, value] : m_step_to_supv_.env()) {
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

  env_map.emplace("CRANE_JOB_NODELIST",
                  absl::StrJoin(m_step_to_supv_.allocated_nodes(), ";"));
  env_map.emplace("CRANE_EXCLUDES",
                  absl::StrJoin(m_step_to_supv_.excludes(), ";"));
  env_map.emplace("CRANE_JOB_NAME", m_step_to_supv_.name());
  env_map.emplace("CRANE_ACCOUNT", m_step_to_supv_.account());
  env_map.emplace("CRANE_PARTITION", m_step_to_supv_.partition());
  env_map.emplace("CRANE_QOS", m_step_to_supv_.qos());

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
  CRANE_ASSERT(m_task_map_.contains(task_id));
  std::unique_ptr task = std::move(m_task_map_.at(task_id));
  m_task_map_.erase(task_id);
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

ITaskInstance::~ITaskInstance() {
  if (m_parent_step_inst_->IsCrun()) {
    auto* crun_meta = GetCrunMeta();

    // For crun non pty job, close out fd in CforedClient
    // For crun pty job, close tty fd in CforedClient
    if (!crun_meta->pty) close(crun_meta->stdin_write);

    if (!crun_meta->x11_auth_path.empty() &&
        !absl::EndsWith(crun_meta->x11_auth_path, "XXXXXX")) {
      std::error_code ec;
      bool ok = std::filesystem::remove(crun_meta->x11_auth_path, ec);
      if (!ok)
        CRANE_ERROR("Failed to remove x11 auth {} for task #{}: {}",
                    crun_meta->x11_auth_path, GetParentStep().task_id(),
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
  for (auto& [name, value] : this->GetParentStep().env()) {
    env_map.emplace(name, value);
  }

  for (auto& [name, value] : this->m_parent_step_inst_->GetStepProcessEnv()) {
    env_map.emplace(name, value);
  }

  // TODO: Move this to step instance.
  if (this->m_parent_step_inst_->IsCrun()) {
    auto const& ia_meta = this->GetParentStep().interactive_meta();
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
        resolved_path / fmt::format("Crane-{}.out", GetParentStep().task_id());

  std::string resolved_path_pattern = std::move(resolved_path.string());
  absl::StrReplaceAll({{"%j", std::to_string(GetParentStep().task_id())},
                       {"%u", m_parent_step_inst_->pwd.Username()},
                       {"%x", GetParentStep().name()}},
                      &resolved_path_pattern);

  return resolved_path_pattern;
}

CraneErrCode ITaskInstance::SetupCrunX11_() {
  auto* inst_crun_meta = this->GetCrunMeta();
  const PasswordEntry& pwd_entry = m_parent_step_inst_->pwd;

  inst_crun_meta->x11_auth_path =
      fmt::sprintf("%s/.crane/xauth/.Xauthority-XXXXXX", pwd_entry.HomeDir());

  bool ok = util::os::CreateFoldersForFileEx(
      inst_crun_meta->x11_auth_path, pwd_entry.Uid(), pwd_entry.Gid(), 0700);
  if (!ok) {
    CRANE_ERROR("Failed to create xauth source file for task #{}",
                this->GetParentStep().task_id());
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

CraneExpected<pid_t> ITaskInstance::ForkCrunAndInitMeta_() {
  if (!m_parent_step_inst_->IsCrun()) return fork();

  auto* meta = GetCrunInstanceMeta();
  meta->pty = GetParentStep().interactive_meta().pty();
  CRANE_DEBUG("Launch crun task #{} pty: {}", GetParentStep().task_id(),
              meta->pty);

  int to_crun_pipe[2], from_crun_pipe[2];
  int crun_pty_fd = -1;

  if (!meta->pty) {
    if (pipe(to_crun_pipe) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  GetParentStep().task_id(), strerror(errno));
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }

    if (pipe(from_crun_pipe) == -1) {
      CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                  GetParentStep().task_id(), strerror(errno));
      close(to_crun_pipe[0]);
      close(to_crun_pipe[1]);
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }
  }

  pid_t pid = -1;
  if (meta->pty) {
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

bool ITaskInstance::SetupCrunFwdAtParent_(uint16_t* x11_port) {
  auto* meta = dynamic_cast<CrunInstanceMeta*>(m_meta_.get());
  pid_t pid = std::get<pid_t>(*GetExecId());

  if (!meta->pty) {
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
      task_id, meta->stdin_write, meta->stdout_read, meta->pty);
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

  return true;
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

  int ngroups = 0;
  auto& pwd = m_parent_step_inst_->pwd;
  // We should not check rc here. It must be -1.
  getgrouplist(pwd.Username().c_str(), GetParentStep().gid(), nullptr,
               &ngroups);

  std::vector<gid_t> gids(ngroups);
  int rc = getgrouplist(pwd.Username().c_str(), GetParentStep().gid(),
                        gids.data(), &ngroups);
  if (rc == -1) {
    fmt::print(stderr, "[Subproc] Error: getgrouplist() for user '{}'\n",
               pwd.Username());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (auto it = std::ranges::find(gids, GetParentStep().gid());
      it != gids.begin()) {
    gids.erase(it);
    gids.insert(gids.begin(), GetParentStep().gid());
  }

  if (!std::ranges::contains(gids, pwd.Gid())) gids.emplace_back(pwd.Gid());

  rc = setgroups(gids.size(), gids.data());
  if (rc == -1) {
    fmt::print(stderr, "[Subprocess] Error: setgroups() failed: {}\n",
               strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  rc = setresgid(GetParentStep().gid(), GetParentStep().gid(),
                 GetParentStep().gid());
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

CraneErrCode ITaskInstance::SetChildProcessBatchFd_() {
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
  if (clearenv() != 0)
    fmt::print(stderr, "[Subprocess] Warning: clearenv() failed.\n");

  for (const auto& [name, value] : m_env_)
    if (setenv(name.c_str(), value.c_str(), 1) != 0)
      fmt::print(stderr, "[Subprocess] Warning: setenv() for {}={} failed.\n",
                 name, value);

  return CraneErrCode::SUCCESS;
}

std::vector<std::string> ITaskInstance::GetChildProcessExecArgv_() const {
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

CraneErrCode ContainerInstance::Prepare() {
  if (this->m_parent_step_inst_->IsCrun()) {
    CRANE_ERROR("crun is not supported in container tasks.");
    return CraneErrCode::ERR_INVALID_PARAM;
  }

  // Generate path and params.
  job_id_t job_id = GetParentStep().task_id();
  m_temp_path_ = g_config.Container.TempDir / fmt::format("{}", job_id);
  m_image_ref_ = GetParentStep().container();

  // Check if image is pulled.
  auto* cri_client = m_parent_step_inst_->GetCriClient();
  auto image_id_opt = cri_client->GetImageId(m_image_ref_);
  if (!image_id_opt.has_value()) {
    // Image is not pulled. Pull the image first.
    image_id_opt = cri_client->PullImage(m_image_ref_);
    if (!image_id_opt.has_value()) {
      CRANE_ERROR("Failed to pull image {} for task #{}",
                  GetParentStep().container(), job_id);
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
  }

  // Write script into the temp folder
  auto sh_path = m_temp_path_ / fmt::format("Crane-{}.sh", job_id);
  if (!util::os::CreateFoldersForFile(sh_path)) {
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}", job_id);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (m_parent_step_inst_->IsBatch()) {
    fputs(GetParentStep().batch_meta().sh_script().c_str(), fptr);
  } else {  // Crun
    fputs(GetParentStep().interactive_meta().sh_script().c_str(), fptr);
  }

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));
  if (chown(sh_path.c_str(), m_parent_step_inst_->pwd.Uid(),
            m_parent_step_inst_->pwd.Gid()) != 0) {
    CRANE_ERROR("Failed to change ownership of script file for task #{}: {}",
                job_id, strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Parse user specified output path and set meta
  if (m_parent_step_inst_->IsBatch()) {
    // For cbatch, log to user defined output directory (error path is
    // ignored.) Note: Only directory is accepted. If file is given, use its
    // parent dir.
    auto meta = std::make_unique<BatchInstanceMeta>();
    meta->parsed_output_file_pattern = ParseFilePathPattern_(
        GetParentStep().batch_meta().output_file_pattern(),
        GetParentStep().cwd());

    m_cri_output_path_ = meta->parsed_output_file_pattern;
    m_meta_ = std::move(meta);
  } else {
    // For crun, just have a placeholder for now.
    m_cri_output_path_ = GetParentStep().cwd();
    m_meta_ = std::make_unique<CrunInstanceMeta>();
  }
  m_meta_->parsed_sh_script_path = sh_path;

  // Set env
  m_env_ = GetChildProcessEnv();

  // Launch the pod after all configurations are set
  if (auto err = SetPodSandboxConfig_(); err != CraneErrCode::SUCCESS)
    return err;
  auto pod_id_expt = cri_client->RunPodSandbox(&m_pod_config_);
  if (!pod_id_expt.has_value()) {
    CRANE_ERROR("Failed to run pod sandbox for task #{}", job_id);
    return pod_id_expt.error();
  }
  m_pod_id_ = std::move(pod_id_expt.value());
  CRANE_DEBUG("Pod {} created for job #{}", m_pod_id_, job_id);

  // Create the container but not start here
  if (auto err = SetContainerConfig_(); err != CraneErrCode::SUCCESS)
    return err;
  auto container_id_expt = cri_client->CreateContainer(m_pod_id_, m_pod_config_,
                                                       &m_container_config_);
  if (!container_id_expt.has_value()) {
    CRANE_ERROR("Failed to create container for task #{}", job_id);
    return container_id_expt.error();
  }
  m_container_id_ = std::move(container_id_expt.value());
  CRANE_DEBUG("Container {} created for task #{}", m_pod_id_, job_id);

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Spawn() {
  auto* cri_client = m_parent_step_inst_->GetCriClient();
  auto ret = cri_client->StartContainer(m_container_id_);
  if (!ret.has_value()) {
    CRANE_ERROR("Failed to start container for task #{}",
                GetParentStep().task_id());
    return ret.error();
  }

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

          CRANE_TRACE("Received CRI event: {}", ev.DebugString());

          std::vector<std::pair<task_id_t, cri::api::ContainerStatus>> changes;
          for (const auto& status : ev.containers_statuses()) {
            const auto& labels = status.labels();
            auto it = labels.find(kCriLabelJobIdKey);
            if (it == labels.end()) continue;

            task_id_t task_id{};
            auto [ptr, ec] =
                std::from_chars(it->second.data(),
                                it->second.data() + it->second.size(), task_id);
            if (ec != std::errc{}) {
              CRANE_ERROR("Failed to parse job_id label: {}", it->second);
              continue;
            }

            if (status.state() == cri::api::ContainerState::CONTAINER_EXITED ||
                status.state() == cri::api::ContainerState::CONTAINER_UNKNOWN)
              changes.emplace_back(task_id, status);
          }

          if (!changes.empty()) g_task_mgr->HandleCriEvents(std::move(changes));
        });

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Kill(int signum) {
  // For containers, we ignore the signum parameter and use CRI to stop/cleanup
  // Stop Container -> Remove Container -> Stop Pod -> Remove Pod
  auto* cri_client = m_parent_step_inst_->GetCriClient();

  // Step 1: Stop Container
  if (!m_container_id_.empty()) {
    auto stop_ret = cri_client->StopContainer(m_container_id_, 10);
    if (!stop_ret.has_value()) {
      CRANE_DEBUG("Failed to stop container {}: {}", m_container_id_,
                  static_cast<int>(stop_ret.error()));
      // Continue with cleanup even if stop fails
    }

    // Step 2: Remove Container
    auto remove_ret = cri_client->RemoveContainer(m_container_id_);
    if (!remove_ret.has_value()) {
      CRANE_DEBUG("Failed to remove container {}: {}", m_container_id_,
                  static_cast<int>(remove_ret.error()));
      // Continue with pod cleanup even if container removal fails
    }
  }

  // Step 3: Stop Pod
  if (!m_pod_id_.empty()) {
    auto stop_pod_ret = cri_client->StopPodSandbox(m_pod_id_);
    if (!stop_pod_ret.has_value()) {
      CRANE_DEBUG("Failed to stop pod {}: {}", m_pod_id_,
                  static_cast<int>(stop_pod_ret.error()));
      // Continue with pod removal even if stop fails
    }

    // Step 4: Remove Pod
    auto remove_pod_ret = cri_client->RemovePodSandbox(m_pod_id_);
    if (!remove_pod_ret.has_value()) {
      CRANE_ERROR("Failed to remove pod {}: {}", m_pod_id_,
                  static_cast<int>(remove_pod_ret.error()));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }

    CRANE_DEBUG("Pod {} removed successfully", m_pod_id_);
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::Cleanup() {
  // For container tasks, Kill() is idempotent.
  // It's ok to call it (at most) twice to remove pod and container.
  CraneErrCode err = Kill(0);
  if (!m_temp_path_.empty()) util::os::DeleteFolders(m_temp_path_);
  return err;
}

const TaskExitInfo& ContainerInstance::HandleContainerExited(
    const cri::api::ContainerStatus& status) {
  m_exit_info_.is_terminated_by_signal = status.exit_code() > 127;
  m_exit_info_.value = m_exit_info_.is_terminated_by_signal
                           ? status.exit_code() - 127
                           : status.exit_code();
  return m_exit_info_;
}

CraneErrCode ContainerInstance::SetPodSandboxConfig_() {
  job_id_t job_id = GetParentStep().task_id();
  const std::string& job_name = GetParentStep().name();
  const std::string& node_name = g_config.CranedIdOfThisNode;

  uid_t uid = m_parent_step_inst_->pwd.Uid();
  gid_t gid = m_parent_step_inst_->pwd.Gid();

  // Generate hash for pod name and uid.
  std::string h16 = MakeHashId_(job_id, job_name, node_name);
  std::string hsuffix = h16.substr(0, kCriPodSuffixLen);

  std::string pod_name_base = util::SlugDns1123(
      job_name.empty() ? std::format("job-{}", job_id) : job_name,
      kCriDnsMaxLabelLen - /*'-'*/ 1 - kCriPodSuffixLen);

  // metadata
  auto* pod_meta = m_pod_config_.mutable_metadata();
  // pod name：<pod_name_base>-<hsuffix>; pod uid: <h16>
  pod_meta->set_name(pod_name_base + "-" + hsuffix);
  pod_meta->set_uid(std::move(h16));

  // hostname
  m_pod_config_.set_hostname(util::SlugDns1123(
      pod_meta->name() + "-" + node_name, kCriDnsMaxLabelLen));

  // labels
  m_pod_config_.mutable_labels()->insert(
      {{std::string(kCriLabelUidKey), std::to_string(uid)},
       {std::string(kCriLabelJobIdKey), std::to_string(job_id)},
       {std::string(kCriLabelJobNameKey), job_name}});

  // log directory
  std::error_code ec;
  std::string log_dir = std::filesystem::is_directory(m_cri_output_path_, ec)
                            ? m_cri_output_path_
                            : m_cri_output_path_.parent_path();
  if (ec && ec != std::errc::no_such_file_or_directory) {
    CRANE_ERROR("Failed to access log directory {}: {}", log_dir, ec.message());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  m_pod_config_.set_log_directory(std::move(log_dir));

  // FIXME: This is just a workaround before we have CgroupManager refactored
  // into a public library.
  auto* linux_config = m_pod_config_.mutable_linux();
  {
    // set cgroup parent
    std::ifstream cgroup_file("/proc/self/cgroup");
    std::string line;
    std::filesystem::path cgroup_path;

    while (std::getline(cgroup_file, line)) {
      // cgroup v1: 10:memory:/user.slice
      // cgroup v2: 0::/user.slice
      auto pos = line.rfind(':');
      if (pos != std::string::npos && pos + 1 < line.size()) {
        cgroup_path = line.substr(pos + 1);
        if (!cgroup_path.empty()) {
          break;
        }
      }
    }

    if (cgroup_path.empty()) {
      CRANE_ERROR("Failed to determine cgroup path from /proc/self/cgroup");
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
    linux_config->set_cgroup_parent(cgroup_path.parent_path().string());
  }

  // Inject fake root config if task is not from root
  if (uid != 0 &&
      InjectFakeRootConfig_(m_parent_step_inst_->pwd, &m_container_config_) !=
          CraneErrCode::SUCCESS) {
    CRANE_ERROR("Failed to inject fake root config for task #{}",
                GetParentStep().task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::SetContainerConfig_() {
  job_id_t job_id = GetParentStep().task_id();
  const std::string& job_name = GetParentStep().name();
  const std::string& node_name = g_config.CranedIdOfThisNode;

  uid_t uid = m_parent_step_inst_->pwd.Uid();
  gid_t gid = m_parent_step_inst_->pwd.Gid();

  // TODO: Using task_id to generate unique name in pod
  // metadata
  m_container_config_.mutable_metadata()->set_name(
      std::format("job-{}", job_id));

  // labels
  m_container_config_.mutable_labels()->insert(
      {{std::string(kCriLabelUidKey), std::to_string(uid)},
       {std::string(kCriLabelJobIdKey), std::to_string(job_id)},
       {std::string(kCriLabelJobNameKey), job_name}});

  // log_path
  // NOTE: This is a relative path to pod log dir.
  std::error_code ec;
  std::string log_path = std::filesystem::is_directory(m_cri_output_path_, ec)
                             ? std::format("{}.log", job_id)
                             : m_cri_output_path_.filename().string();
  if (ec && ec != std::errc::no_such_file_or_directory) {
    CRANE_ERROR("Failed to access log path {}: {}", log_path, ec.message());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  m_container_config_.set_log_path(std::move(log_path));

  // image already checked and pulled.
  m_container_config_.mutable_image()->set_image(m_image_ref_);

  // mount the generated script with uid/gid mapping.
  auto* mount = m_container_config_.add_mounts();
  mount->set_host_path(m_meta_->parsed_sh_script_path);
  mount->set_container_path("/tmp/crane/script.sh");
  mount->set_propagation(cri::api::MountPropagation::PROPAGATION_PRIVATE);

  // Inject fake root config if the task is not from root.
  if (uid != 0 &&
      InjectFakeRootConfig_(m_parent_step_inst_->pwd, &m_container_config_) !=
          CraneErrCode::SUCCESS) {
    CRANE_ERROR("Failed to inject fake root config for task #{}",
                GetParentStep().task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // Force the entrypoint to use the mounted script
  // For container, we use `/bin/sh` as the default interpreter.
  std::string real_exe = "/bin/sh";
  if (m_parent_step_inst_->IsBatch() &&
      !GetParentStep().batch_meta().interpreter().empty())
    real_exe = GetParentStep().batch_meta().interpreter();

  std::vector<std::string> command{real_exe, "/tmp/crane/script.sh"};
  m_container_config_.mutable_command()->Assign(command.begin(), command.end());

  // envs
  for (const auto& [name, value] : m_env_) {
    auto* env = m_container_config_.add_envs();
    env->set_key(name);
    env->set_value(value);
  }

  return CraneErrCode::SUCCESS;
}

cri::api::IDMapping ContainerInstance::MakeIdMapping_(uid_t host_id,
                                                      uid_t container_id,
                                                      size_t size) {
  cri::api::IDMapping mapping{};
  mapping.set_host_id(host_id);
  mapping.set_container_id(container_id);
  mapping.set_length(size);
  return mapping;
}

std::string ContainerInstance::MakeHashId_(job_id_t job_id,
                                           const std::string& job_name,
                                           const std::string& node_name) {
  std::string h_msg =
      std::format("{}\x1f{}\x1f{}", job_id, node_name, job_name);
  uint64_t h64 =
      (static_cast<uint64_t>(util::Crc32Of(h_msg, 0xA5A5A5A5U)) << 32) |
      util::Adler32Of(h_msg, 0x3C3C3C3CU);
  return std::format("{:016x}", h64);  // 16 hex chars, lowercase
}

CraneErrCode ContainerInstance::SetSubIdMappings_(const PasswordEntry& pwd) {
  auto subuid = pwd.SubUidRanges();
  auto subgid = pwd.SubGidRanges();

  if (!subuid.Valid() || !subgid.Valid()) {
    CRANE_ERROR("SubUID/SubGID ranges are not valid for user {}", pwd.Uid());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  m_uid_mapping_ = {MakeIdMapping_(0, pwd.Uid(), 1),
                    MakeIdMapping_(subuid[0].start, 1, subuid[0].count)};

  m_gid_mapping_ = {MakeIdMapping_(0, pwd.Gid(), 1),
                    MakeIdMapping_(subgid[0].start, 1, subgid[0].count)};

  CRANE_TRACE(
      "Adding id mapping for user (uid: {}, gid: {}). Uid map: {}, {}; Gid "
      "map: {}, {}.",
      pwd.Uid(), pwd.Gid(), m_uid_mapping_[0].DebugString(),
      m_uid_mapping_[1].DebugString(), m_gid_mapping_[0].DebugString(),
      m_gid_mapping_[1].DebugString());

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::InjectFakeRootConfig_(
    const PasswordEntry& pwd, cri::api::PodSandboxConfig* config) {
  if (m_uid_mapping_.empty() || m_gid_mapping_.empty() ||
      SetSubIdMappings_(pwd) != CraneErrCode::SUCCESS) {
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // set linux pod options
  auto* linux_sec_context = config->mutable_linux()->mutable_security_context();
  auto* userns_options =
      linux_sec_context->mutable_namespace_options()->mutable_userns_options();

  // Fake root
  userns_options->set_mode(cri::api::NamespaceMode::POD);
  userns_options->mutable_uids()->Assign(m_uid_mapping_.begin(),
                                         m_uid_mapping_.end());
  userns_options->mutable_gids()->Assign(m_gid_mapping_.begin(),
                                         m_gid_mapping_.end());

  linux_sec_context->mutable_run_as_user()->set_value(0);
  linux_sec_context->mutable_run_as_group()->set_value(0);

  return CraneErrCode::SUCCESS;
}

CraneErrCode ContainerInstance::InjectFakeRootConfig_(
    const PasswordEntry& pwd, cri::api::ContainerConfig* config) {
  if (m_uid_mapping_.empty() || m_gid_mapping_.empty() ||
      SetSubIdMappings_(pwd) != CraneErrCode::SUCCESS) {
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // set linux container options
  auto* linux_sec_context = config->mutable_linux()->mutable_security_context();
  auto* userns_options =
      linux_sec_context->mutable_namespace_options()->mutable_userns_options();

  // fake root
  userns_options->set_mode(cri::api::NamespaceMode::POD);
  userns_options->mutable_uids()->Assign(m_uid_mapping_.begin(),
                                         m_uid_mapping_.end());
  userns_options->mutable_gids()->Assign(m_gid_mapping_.begin(),
                                         m_gid_mapping_.end());

  linux_sec_context->mutable_run_as_user()->set_value(0);
  linux_sec_context->mutable_run_as_group()->set_value(0);

  // modify mounts
  for (auto& mount : *config->mutable_mounts()) {
    mount.mutable_uidmappings()->Assign(m_uid_mapping_.begin(),
                                        m_uid_mapping_.end());
    mount.mutable_gidmappings()->Assign(m_gid_mapping_.begin(),
                                        m_gid_mapping_.end());
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode ProcInstance::Prepare() {
  // Write script content into file
  auto sh_path =
      g_config.CraneScriptDir / fmt::format("Crane-{}.sh", g_config.JobId);

  FILE* fptr = fopen(sh_path.c_str(), "w");
  if (fptr == nullptr) {
    CRANE_ERROR("Failed write the script for task #{}",
                GetParentStep().task_id());
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (m_parent_step_inst_->IsBatch())
    fputs(GetParentStep().batch_meta().sh_script().c_str(), fptr);
  else  // Crun
    fputs(GetParentStep().interactive_meta().sh_script().c_str(), fptr);

  fclose(fptr);

  chmod(sh_path.c_str(), strtol("0755", nullptr, 8));
  if (chown(sh_path.c_str(), GetParentStep().uid(), GetParentStep().gid()) !=
      0) {
    CRANE_ERROR("Failed to change ownership of script file for task #{}: {}",
                GetParentStep().task_id(), strerror(errno));
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
  } else {
    m_meta_ = std::make_unique<CrunInstanceMeta>();
  }

  m_meta_->parsed_sh_script_path = sh_path;

  // If interpreter is not set, let system decide.
  m_executable_ = sh_path.string();
  if (m_parent_step_inst_->IsBatch() &&
      !GetParentStep().batch_meta().interpreter().empty()) {
    m_executable_ = fmt::format(
        "{} {}", GetParentStep().batch_meta().interpreter(), sh_path.string());
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
      this->GetParentStep().interactive_meta().x11()) {
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
    CRANE_ERROR("fork() failed for task #{}: {}", GetParentStep().task_id(),
                strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    m_pid_ = child_pid;
    CRANE_DEBUG("Subprocess was created for task #{} pid: {}",
                GetParentStep().task_id(), m_pid_);
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

      CRANE_DEBUG("Task #{} has initialized crun forwarding.",
                  GetParentStep().task_id());
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                GetParentStep().task_id());

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
                  child_pid, GetParentStep().task_id(),
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
                    GetParentStep().task_id());
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
        GetParentStep().interactive_meta().x11()) {
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
                status, GetParentStep().task_id());
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
}

void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::ShutdownSupervisor() {
  CRANE_TRACE("Shutdown requested, TaskManager is exiting...");

  // Explicitly release CriClient
  m_step_.StopCriClient();
  m_step_.StopCforedClient();

  g_craned_client->Shutdown();
  g_server->Shutdown();

  this->Shutdown();
}

void TaskManager::TaskStopAndDoStatusChange(task_id_t task_id) {
  // NOTE: This could be called in mutiple threads. Operate with care.
  m_task_stopped_queue_.enqueue(task_id);
  m_task_stopped_async_handle_->send();
}

void TaskManager::ActivateTaskStatusChange_(task_id_t task_id,
                                            crane::grpc::TaskStatus new_status,
                                            uint32_t exit_code,
                                            std::optional<std::string> reason) {
  // One-shot model: nothing to stop, just proceed to cleanup.
  m_step_.oom_baseline_inited = false;
  auto task = m_step_.RemoveTaskInstance(task_id);
  auto err = task->Cleanup();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_WARN("[Task #{}] Failed to cleanup task: {}", task_id,
               static_cast<int>(err));
  }

  bool orphaned = m_step_.orphaned;
  if (m_step_.AllTaskFinished()) {
    DelTerminationTimer_();
    if (!orphaned)
      g_craned_client->StepStatusChangeAsync(new_status, exit_code,
                                             std::move(reason));
    else {
      ShutdownSupervisor();
    }
  }
}

std::future<CraneErrCode> TaskManager::ExecuteTaskAsync() {
  CRANE_INFO("[Job #{}] Executing task.", m_step_.job_id);

  std::promise<CraneErrCode> ok_promise;
  std::future ok_future = ok_promise.get_future();

  auto elem = ExecuteTaskElem{.ok_prom = std::move(ok_promise)};

  if (m_step_.IsContainer()) {
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
  // Init CriClient before preparation
  if (m_step_.IsContainer()) m_step_.InitCriClient();

  // Prepare for execution
  CraneErrCode err = task->Prepare();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_DEBUG("[Task #{}] Failed to prepare task", task->task_id);
    ActivateTaskStatusChange_(
        task->task_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeFileNotFound,
        fmt::format("Failed to prepare task, code: {}", static_cast<int>(err)));
    return err;
  }

  // Init cfored before start the task.
  if (m_step_.IsCrun()) m_step_.InitCforedClient();

  CRANE_TRACE("[Task #{}] Spawning process in task", task->task_id);
  err = task->Spawn();
  if (err != CraneErrCode::SUCCESS) {
    ActivateTaskStatusChange_(task->task_id, crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeSpawnProcessFail,
                              fmt::format("Failed to spawn the task, code: {}",
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
                                     bool terminated_by_user) {
  TaskTerminateQueueElem elem;
  elem.mark_as_orphaned = mark_as_orphaned;
  elem.termination_reason =
      terminated_by_user ? TerminatedBy::CANCELLED_BY_USER : TerminatedBy::NONE;
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
      CRANE_TRACE("Receiving SIGCHLD for pid {}", m_step_.job_id);
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
    CRANE_TRACE("[Job #{}.{}] Receiving SIGCHLD for pid {}", task_id, 0, pid);

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
  std::pair<task_id_t, cri::api::ContainerStatus> elem;
  while (m_cri_event_queue_.try_dequeue(elem)) {
    // NOTE: a CRI event comes at LEAST once.
    auto [task_id, status] = elem;
    auto* t = m_step_.GetTaskInstance(task_id);
    if (t == nullptr) continue;  // Duplicated event
    auto* task = dynamic_cast<ContainerInstance*>(t);

    const auto& exit_info = task->HandleContainerExited(status);
    CRANE_INFO("Receiving container exited event: {}. Signaled: {}, Value: {}",
               status.id(), exit_info.is_terminated_by_signal, exit_info.value);

    TaskStopAndDoStatusChange(task_id);
  }
}

void TaskManager::EvTaskTimerCb_() {
  CRANE_TRACE("task #{} exceeded its time limit. Terminating it...",
              g_config.JobId);

  DelTerminationTimer_();

  if (m_step_.IsBatch() || m_step_.IsCrun()) {
    m_task_terminate_queue_.enqueue(TaskTerminateQueueElem{
        .termination_reason = TerminatedBy::TERMINATION_BY_TIMEOUT});
    m_terminate_task_async_handle_->send();
  } else {
    ActivateTaskStatusChange_(m_step_.job_id,
                              crane::grpc::TaskStatus::ExceedTimeLimit,
                              ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
  }
}

void TaskManager::EvCleanTaskStopQueueCb_() {
  task_id_t task_id;
  while (m_task_stopped_queue_.try_dequeue(task_id)) {
    CRANE_INFO("[Task #{}] Stopped and is doing TaskStatusChange...", task_id);
    auto* task = m_step_.GetTaskInstance(task_id);
    if (task->GetExecId().has_value())
      m_exec_id_task_id_map_.erase(*task->GetExecId());

    switch (task->err_before_exec) {
    case CraneErrCode::ERR_PROTOBUF:
      ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Failed,
                                ExitCode::kExitCodeSpawnProcessFail,
                                std::nullopt);
      return;

    case CraneErrCode::ERR_CGROUP:
      ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Failed,
                                ExitCode::kExitCodeCgroupError, std::nullopt);
      return;

    default:
      break;
    }

    const auto& exit_info = task->GetExitInfo();
    if (m_step_.IsBatch() || m_step_.IsCrun()) {
      bool considered_signal = exit_info.is_terminated_by_signal;
      if (task->terminated_by == TerminatedBy::NONE) {
        if (m_step_.EvaluateOomOnExit()) {
          task->terminated_by = TerminatedBy::TERMINATION_BY_OOM;
        }
      }
      if (considered_signal) {
        switch (task->terminated_by) {
        case TerminatedBy::CANCELLED_BY_USER:
          ActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::Cancelled,
              exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
          break;
        case TerminatedBy::TERMINATION_BY_TIMEOUT:
          ActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
              ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
          break;
        case TerminatedBy::TERMINATION_BY_OOM:
          ActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::OutOfMemory,
              exit_info.value + ExitCode::kTerminationSignalBase,
              std::optional<std::string>("Detected by oom_kill counter delta"));
          break;
        default:
          ActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::Failed,
              exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
        }
      } else if (exit_info.value == 0) {
        ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Completed,
                                  0, std::nullopt);
      } else {
        if (task->terminated_by == TerminatedBy::TERMINATION_BY_OOM) {
          ActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::OutOfMemory, exit_info.value,
              std::optional<std::string>(
                  "Detected by oom_kill counter delta (no signal)"));
        } else {
          ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Failed,
                                    exit_info.value, std::nullopt);
        }
      }
    } else /* Calloc */ {
      // For a COMPLETING Calloc task with a process running,
      // the end of this process means that this task is done.
      if (exit_info.is_terminated_by_signal)
        ActivateTaskStatusChange_(
            task_id, crane::grpc::TaskStatus::Completed,
            exit_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
      else if (exit_info.value == 0)
        ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Completed,
                                  0, std::nullopt);
      else
        ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Failed,
                                  exit_info.value, std::nullopt);
    }
  }
}

void TaskManager::EvCleanTerminateTaskQueueCb_() {
  TaskTerminateQueueElem elem;
  while (m_task_terminate_queue_.try_dequeue(elem)) {
    // TODO: Replace job id with task id.
    CRANE_TRACE("Receive TerminateRunningTask Request for {}.", g_config.JobId);

    m_step_.orphaned = elem.mark_as_orphaned;
    if (m_step_.AllTaskFinished()) {
      CRANE_DEBUG("Terminating a completing task #{}, ignored.",
                  g_config.JobId);
      continue;
    }

    int sig = m_step_.IsBatch() ? SIGTERM : SIGHUP;

    for (task_id_t task_id : m_step_.GetTaskIds()) {
      auto* task = m_step_.GetTaskInstance(task_id);
      if (elem.termination_reason == TerminatedBy::TERMINATION_BY_TIMEOUT) {
        task->terminated_by = TerminatedBy::TERMINATION_BY_TIMEOUT;
      }
      if (elem.termination_reason == TerminatedBy::CANCELLED_BY_USER) {
        // If termination request is sent by user, send SIGKILL to ensure that
        // even freezing processes will be terminated immediately.
        sig = SIGKILL;
        task->terminated_by = TerminatedBy::CANCELLED_BY_USER;
      }

      if (task->GetExecId().has_value()) {
        // Will kill in 3 cases:
        // 1. Container Task
        // 2. Batch Task
        // 3. Interactive Task having running process
        task->Kill(sig);
      } else if (m_step_.interactive_type.has_value()) {
        // For an Interactive task with no process running, it ends immediately.
        ActivateTaskStatusChange_(task_id, crane::grpc::TaskStatus::Completed,
                                  ExitCode::kExitCodeTerminated, std::nullopt);
      }
    }
  }
}

void TaskManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  absl::Time now = absl::Now();

  ChangeTaskTimeLimitQueueElem elem;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
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
}

void TaskManager::EvGrpcExecuteTaskCb_() {
  struct ExecuteTaskElem elem;
  while (m_grpc_execute_task_queue_.try_dequeue(elem)) {
    // m_step_ is the only owner of tasks.
    // No matter Task is spawned or not, adding to m_step_.
    task_id_t task_id = elem.instance->task_id;
    m_step_.AddTaskInstance(task_id, std::move(elem.instance));
    auto* task = m_step_.GetTaskInstance(task_id);

    // Add a timer to limit the execution time of a task.
    // Note: event_new and event_add in this function is not thread safe,
    //       so we move it outside the multithreading part.
    int64_t sec = m_step_.GetStep().time_limit().seconds();
    AddTerminationTimer_(sec);
    CRANE_TRACE("Add a timer of {} seconds", sec);

    m_step_.pwd.Init(m_step_.uid);
    if (!m_step_.pwd.Valid()) {
      CRANE_DEBUG(
          "[Job #{}] Failed to look up password entry for uid {} of task",
          m_step_.job_id, m_step_.uid);
      ActivateTaskStatusChange_(
          task->task_id, crane::grpc::TaskStatus::Failed,
          ExitCode::kExitCodePermissionDenied,
          fmt::format(
              "[Job #{}] Failed to look up password entry for uid {} of task",
              m_step_.job_id, m_step_.uid));
      elem.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      return;
    }

    // Calloc tasks have no scripts to run. Just return.
    if (m_step_.IsCalloc()) {
      elem.ok_prom.set_value(CraneErrCode::SUCCESS);
      return;
    }

    // TODO: Use thread pool
    auto err = LaunchExecution_(task);
    if (err != CraneErrCode::SUCCESS) {
      CRANE_WARN("[task #{}] Failed to launch process.", m_step_.job_id);
    } else {
      auto exec_id = *task->GetExecId();
      CRANE_INFO("[task #{}] Launched exection, id: {}.", m_step_.job_id,
                 std::visit([](auto&& arg) { return std::format("{}", arg); },
                            exec_id));
      m_exec_id_task_id_map_[exec_id] = task->task_id;
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