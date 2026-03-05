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

#include <iterator>
#include <limits>
#include <variant>

#include "Pmix.h"
#include "CforedClient.h"
#include "CgroupManager.h"
#include "CranedClient.h"
#include "PmixCommon.h"
#include "SupervisorPublicDefs.h"
#include "SupervisorServer.h"
#include "crane/CriClient.h"
#include "crane/Logger.h"
#include "crane/OS.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"

namespace Craned::Supervisor {
using namespace std::chrono_literals;
using Common::CgroupManager;
using Common::kStepRequestCheckIntervalMs;

CraneErrCode StepInstance::Prepare() {
  if (!(IsCalloc() || IsContainer() || IsPod())) {
    auto sh_path =
        g_config.CraneScriptDir /
        fmt::format("Crane-{}.{}.sh", g_config.JobId, g_config.StepId);

    FILE* fptr = fopen(sh_path.c_str(), "w");
    if (fptr == nullptr) {
      CRANE_ERROR("Failed write the script");
      return CraneErrCode::ERR_SYSTEM_ERR;
    }

    fputs(m_step_to_supv_.sh_script().c_str(), fptr);

    fclose(fptr);

    chmod(sh_path.c_str(), strtol("0755", nullptr, 8));
    if (chown(sh_path.c_str(), m_step_to_supv_.uid(), gids[0]) != 0) {
      CRANE_ERROR("Failed to change ownership of script file:{}",
                  strerror(errno));
    }
    script_path = sh_path;
  }

  // Init CriClient before preparation
  if (IsPod() || IsContainer()) InitCriClient();

  // Init cfored
  if (IsCrun()) {
    InitCforedClient();

    auto* cfored_client = GetCforedClient();
    if (x11) {
      X11Meta x11_meta{};
      if (x11_fwd)
        x11_meta.x11_port = cfored_client->InitUvX11FwdHandler();
      else
        x11_meta.x11_port = GetStep().interactive_meta().x11_meta().port();

      CRANE_TRACE("Crun task x11 enabled. Forwarding: {}, X11 Port: {}",
                  x11_fwd, x11_meta.x11_port);

      auto x11_auth_path =
          fmt::sprintf("%s/.crane/xauth/.Xauthority-XXXXXX", pwd.HomeDir());

      bool ok = util::os::CreateFoldersForFileEx(x11_auth_path, pwd.Uid(),
                                                 pwd.Gid(), 0700);
      if (!ok) {
        CRANE_ERROR("Failed to create xauth source file for");
        return CraneErrCode::ERR_SYSTEM_ERR;
      }

      // Default file permission is 0600.
      int xauth_fd = mkstemp(x11_auth_path.data());
      if (xauth_fd == -1) {
        CRANE_ERROR("mkstemp() for xauth file failed: {}\n", strerror(errno));
        return CraneErrCode::ERR_SYSTEM_ERR;
      }

      int ret = fchown(xauth_fd, pwd.Uid(), pwd.Gid());
      if (ret == -1) {
        CRANE_ERROR("fchown() for xauth file failed: {}\n", strerror(errno));
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
      x11_meta.x11_auth_path = x11_auth_path;
      this->x11_meta = std::move(x11_meta);
      close(xauth_fd);
    }
  }
  return CraneErrCode::SUCCESS;
}

void StepInstance::CleanUp() {
  if (script_path.has_value()) util::os::DeleteFile(script_path.value());
  if (x11_meta.has_value()) {
    auto x11_auth_path = x11_meta.value().x11_auth_path;
    if (!x11_auth_path.empty() && !absl::EndsWith(x11_auth_path, "XXXXXX")) {
      std::error_code ec;
      bool ok = std::filesystem::remove(x11_auth_path, ec);
      if (!ok)
        CRANE_ERROR("Failed to remove x11 auth {}: {}", x11_auth_path,
                    ec.message());
    }
  }

  if (step_user_cg) {
    int cnt = 0;

    while (true) {
      if (step_user_cg->Empty()) break;

      if (cnt >= 5) {
        CRANE_ERROR(
            "Couldn't kill the processes in cgroup {} after {} times. "
            "Skipping it.",
            step_user_cg->CgroupName(), cnt);
        break;
      }

      step_user_cg->KillAllProcesses(SIGKILL);
      ++cnt;
      std::this_thread::sleep_for(100ms);
    }
    step_user_cg->Destroy();

    step_user_cg.reset();
  }
  StopCforedClient();
  // Explicitly release CriClient
  StopCriClient();
}

bool StepInstance::IsBatch() const noexcept {
  return this->m_step_to_supv_.type() == crane::grpc::Batch;
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
bool StepInstance::IsPrimary() const noexcept {
  return m_step_to_supv_.step_type() == crane::grpc::StepType::PRIMARY;
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

  // SLURM
  if (g_config.EnableSlurmCompatibleEnv) {
    env_map.emplace("SLURM_CPU_BIND_TYPE", "none");
    env_map.emplace("SLURM_STEP_NUM_TASKS",
                    std::to_string(m_step_to_supv_.task_node_list().size()));
    // The set of task IDs running on the current node
    const auto& task_res_map = m_step_to_supv_.task_res_map();
    std::vector<task_id_t> gtids;
    gtids.reserve(task_res_map.size());
    for (const auto& [task_id, resource] : task_res_map) {
      gtids.emplace_back(task_id);
    }
    env_map.emplace("SLURM_GTIDS", fmt::format("{}", fmt::join(gtids, ",")));
  }

  return env_map;
}

void StepInstance::GotNewStatus(StepStatus new_status) {
  if (IsFinishedStepStatus(new_status)) {
    if (m_status_ == new_status) return;

    if (m_status_ != StepStatus::Running &&
        m_status_ != StepStatus::Completing &&
        m_status_ != StepStatus::Starting &&
        m_status_ != StepStatus::Configuring) {
      CRANE_WARN(
          "[Step {}.{}] Step status is not "
          "Running/Completing/Starting/Configuring when receiving new finished "
          "status {}, current status: {}.",
          job_id, step_id, new_status, m_status_.load());
    }

    m_status_ = new_status;
    return;
  }

  switch (new_status) {
  case StepStatus::Configuring:
  case StepStatus::Pending:
  case StepStatus::Invalid: {
    CRANE_ERROR("[Step #{}.{}] Invalid new status received: {}, ignored.",
                job_id, step_id, new_status);
    return;
  }

  case StepStatus::Running: {
    if (this->IsDaemon()) {
      if (m_status_ != StepStatus::Configuring)
        CRANE_WARN(
            "[Step {}.{}] Daemon step status is not 'Configuring' when "
            "receiving new status 'Running', current status: {}.",
            job_id, step_id, m_status_.load());
    } else {
      if (m_status_ != StepStatus::Starting)
        CRANE_WARN(
            "[Step {}.{}] Step status is not 'Starting' when receiving new "
            "status 'Running', current status: {}.",
            job_id, step_id, m_status_.load());
    }
    break;
  }

  case StepStatus::Starting: {
    CRANE_ASSERT_MSG(!this->IsDaemon(),
                     "Daemon step should never be set to STARTING status");

    if (m_status_ != StepStatus::Configuring)
      CRANE_WARN(
          "[Step {}.{}] Step status is not 'Configuring' when "
          "receiving new status 'Starting', current status: {}.",
          job_id, step_id, m_status_.load());

    break;
  }

  case StepStatus::Completing: {
    // TODO: Daemon pod bootstrap may fail before the step ever reaches
    // Running...
    if (m_status_ != StepStatus::Running)
      CRANE_WARN(
          "[Step {}.{}] Step status is not 'Running' when receiving new "
          "status 'Completing', current status: {}.",
          job_id, step_id, m_status_.load());
    break;
  }

  default: {
    std::unreachable();
  }
  }

  m_status_ = new_status;
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
  // Use the step-level workload cgroup selected by Supervisor.
  if (!step_user_cg) {
    CRANE_ERROR("[OOM] No workload cgroup path selected for OOM monitoring");
    return;
  }

  auto cgroup_path = step_user_cg->CgroupPath();
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
  if (!oom_baseline_inited || !step_user_cg) {
    CRANE_TRACE(
        "[OOM] Skip evaluation: baseline_inited={} or no cgroup "
        "created.",
        oom_baseline_inited);
    return false;
  }

  auto cgroup_path = step_user_cg->CgroupPath();
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
  if (g_config.EnableSlurmCompatibleEnv) {
    // Global task id
    m_env_.emplace("SLURM_PROCID", std::to_string(task_id));
    // Local task id task_node_list()
    task_id_t local_task_id = 0;
    const auto& task_node_list =
        m_parent_step_inst_->GetStep().task_node_list();
    for (uint32_t index = 0; index < task_node_list.size(); index++) {
      const std::string& hostname =
          m_parent_step_inst_->GetStep().nodelist(task_node_list[index]);
      if (hostname == g_config.CranedIdOfThisNode) {
        local_task_id = task_id - index;
        break;
      }
    }
    m_env_.emplace("SLURM_LOCALID", std::to_string(local_task_id));
  }
}

ProcInstance::~ProcInstance() {
  if (m_parent_step_inst_->IsCrun()) {
    auto* crun_meta = GetCrunMeta_();

    // For crun non pty job, close out fd in CforedClient
    // For crun pty job, close tty fd in CforedClient
    if (!m_parent_step_inst_->pty) close(crun_meta->stdin_write);
  }
}

void ProcInstance::InitEnvMap() {
  ITaskInstance::InitEnvMap();
  if (m_parent_step_inst_->IsCrun()) {
    const auto& ia_meta = m_parent_step_inst_->GetStep().interactive_meta();
    if (!ia_meta.term_env().empty()) m_env_.emplace("TERM", ia_meta.term_env());
    if (ia_meta.x11()) {
      auto const& x11_meta = ia_meta.x11_meta();

      std::string target =
          ia_meta.x11_meta().enable_forwarding() ? "" : x11_meta.target();
      m_env_["DISPLAY"] = fmt::format(
          "{}:{}", target, m_parent_step_inst_->x11_meta->x11_port - 6000);
      m_env_["XAUTHORITY"] = m_parent_step_inst_->x11_meta->x11_auth_path;
    }
  }
  m_env_.emplace("CRANE_PROCID", std::to_string(task_id));
  m_env_.emplace("CRANE_PROC_ID", std::to_string(task_id));
}

std::string ProcInstance::ParseFilePathPattern_(const std::string& pattern,
                                                const std::string& cwd,
                                                bool is_batch_stdout,
                                                bool* is_local_file) const {
  if (is_local_file != nullptr) *is_local_file = false;
  if (pattern.empty()) {
    if (is_batch_stdout) {
      std::filesystem::path default_path = cwd;

      // Path ends with a directory, append default stdout/err file name
      // `Crane-<Job ID>.out` to the path.
      if (std::filesystem::is_directory(default_path))
        default_path =
            default_path / fmt::format("Crane-{}.out", g_config.JobId);
      return default_path;
    } else
      return "";
  }

  auto path_to_parse = pattern;
  if (absl::StrContains(path_to_parse, "\\")) {
    absl::StrReplaceAll({{"\\", ""}}, &path_to_parse);
    return path_to_parse;
  }

  auto hostname_expt = util::os::GetHostname();
  if (!hostname_expt) {
    return path_to_parse;
  }
  auto& full_hostname = hostname_expt.value();
  auto short_hostname = full_hostname.substr(0, full_hostname.find('.'));
  auto& grpc_nodelist = m_parent_step_inst_->GetStep().nodelist();
  auto node_it = std::ranges::find(grpc_nodelist, g_config.CranedIdOfThisNode);
  if (node_it == grpc_nodelist.end()) {
    CRANE_ERROR("Failed to find current node {} in nodelist",
                g_config.CranedIdOfThisNode);
    return path_to_parse;
  }
  int node_index = std::distance(grpc_nodelist.begin(), node_it);
  // Pattern with following format specifiers will be created on compute node.
  // For crun, will redirect io to local file instead of forwarding from/to
  // cfored and crun , otherwise will be forwarded from/to cfored and crun.
  const std::unordered_set<char> local_file_replacement{'N', 'n', 't'};
  //clang-format off
  std::unordered_map<char, std::string> replacement_map{
      {'%', "%"},
      // Job array's master job allocation number.
      //  {'A', ""}
      // Job array ID (index) number.
      {'a', "0"},
      // jobid.stepid of the running job (e.g. "128.0")
      {'J', fmt::format("{}.{}", g_config.JobId, g_config.StepId)},
      // job id
      {'j', std::to_string(g_config.JobId)},
      // step id
      {'s', std::to_string(g_config.StepId)},
      // short hostname
      {'N', std::move(short_hostname)},
      // Node identifier relative to current job (e.g. "0" is the first node of
      // the running job)
      {'n', std::to_string(node_index)},
      // task identifier (rank) relative to current step.
      {'t', std::to_string(task_id)},
      // User name
      {'u', m_parent_step_inst_->pwd.Username()},
      // Job name
      {'x', m_parent_step_inst_->GetStep().name()},
  };
  //clang-format on

  static constexpr re2::LazyRE2 kPatternRe = {"%%|%(\\d*)([AajJsNntuUx])"};
  std::string result;
  result.reserve(path_to_parse.size() + 128);

  re2::StringPiece input(path_to_parse);
  re2::StringPiece groups[3];  // 0: full match, 1: padding, 2: specifier
  size_t cursor = 0;

  while (cursor < input.size() &&
         kPatternRe->Match(input, cursor, input.size(), re2::RE2::UNANCHORED,
                           groups, 3)) {
    size_t prefix_len =
        std::distance(input.begin() + cursor, groups[0].begin());
    absl::StrAppend(&result, input.substr(cursor, prefix_len));

    re2::StringPiece full_match = groups[0];  //  "%5j" or "%%"

    // Case 1: "%%"
    if (full_match == "%%") {
      result.push_back('%');
    }  // Case 2: "%5j" or "%j"
    else {
      re2::StringPiece padding_str = groups[1];  // "5"
      char specifier = groups[2][0];             // "j"

      auto it = replacement_map.find(specifier);
      if (it == replacement_map.end()) {
        result.append(full_match.data(), full_match.size());
      } else {
        const std::string& value = it->second;
        if (local_file_replacement.contains(specifier)) {
          if (is_local_file != nullptr) *is_local_file = true;
        }
        int width = 0;
        bool is_numeric_field =
            !(specifier == 'N' || specifier == 'u' || specifier == 'x');

        if (is_numeric_field && !padding_str.empty() &&
            absl::SimpleAtoi(padding_str, &width)) {
          absl::StrAppend(&result, fmt::format("{:0>{}}", value, width));
        } else {
          absl::StrAppend(&result, value);
        }
      }
    }
    cursor += prefix_len + full_match.size();
  }

  if (cursor < input.size()) {
    absl::StrAppend(&result, input.substr(cursor));
  }
  std::filesystem::path path(result);
  if (path.is_relative()) {
    return (cwd / path).string();
  } else {
    return result;
  }
}

std::pair<std::string, bool> ProcInstance::CrunParseFilePattern_(
    const std::string& pattern) const {
  if (pattern == kCrunFwdALL) {
    return {"", true};
  } else if (pattern == kCrunFwdNONE) {
    return {"", false};
  } else {
    task_id_t req_task_id;
    if (absl::SimpleAtoi(pattern, &req_task_id)) {
      if (req_task_id == task_id) {
        return {"", true};
      } else {
        return {"", false};
      }
    }
    bool local_file{false};
    auto path = ParseFilePathPattern_(pattern, GetParentStep().cwd(), false,
                                      &local_file);
    if (!local_file) {
      return {"", true};
    } else {
      return {path, false};
    }
  }
}

CraneExpected<pid_t> ProcInstance::ForkCrunAndInitIOfd_() {
  if (!m_parent_step_inst_->IsCrun()) return fork();
  auto* meta = GetCrunMeta_();
  CRANE_DEBUG("Launch crun task #{} pty: {}", task_id,
              m_parent_step_inst_->pty);

  std::set<int> opened_fd{};
  const auto open_dev_null = [this]() -> int {
    int dev_null_fd = open("/dev/null", O_RDWR);
    if (dev_null_fd == -1) {
      CRANE_ERROR("[Task #{}] Failed to open /dev/null: {}", task_id,
                  std::strerror(errno));
      return -1;
    }
    return dev_null_fd;
  };
  int stdin_local_fd = -1;
  int stdout_local_fd = -1;
  int stderr_local_fd = -1;

  int to_crun_pipe[2];
  int from_crun_stdout_pipe[2];
  int from_crun_stderr_pipe[2];
  int crun_pty_fd = -1;

  int open_mode =
      GetParentStep().io_meta().open_mode_append() ? O_APPEND : O_TRUNC;
  if (!m_parent_step_inst_->pty) {
    if (meta->fwd_stdin) {
      if (pipe(to_crun_pipe) == -1) {
        CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                    task_id, std::strerror(errno));
        return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
      }
      opened_fd.insert(to_crun_pipe[0]);
      opened_fd.insert(to_crun_pipe[1]);
    } else {
      if (meta->parsed_input_file_pattern.empty()) {
        int null_fd = open_dev_null();
        if (null_fd == -1) {
          return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
        }
        stdin_local_fd = null_fd;
      } else {
        int fd = open(meta->parsed_input_file_pattern.c_str(), O_RDONLY);
        if (fd == -1) {
          CRANE_ERROR("[Task #{}] Failed to open input file {}: {}", task_id,
                      meta->parsed_input_file_pattern, std::strerror(errno));
          return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
        }
        stdin_local_fd = fd;
      }
      opened_fd.insert(stdin_local_fd);
    }

    if (meta->fwd_stdout) {
      if (pipe(from_crun_stdout_pipe) == -1) {
        CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                    task_id, strerror(errno));
        for (int fd : opened_fd) {
          close(fd);
        }
        return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
      }
      opened_fd.insert(from_crun_stdout_pipe[0]);
      opened_fd.insert(from_crun_stdout_pipe[1]);
    } else {
      if (meta->parsed_output_file_pattern.empty()) {
        int null_fd = open_dev_null();
        if (null_fd == -1) {
          return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
        }
        stdout_local_fd = null_fd;
      } else {
        int fd = open(meta->parsed_output_file_pattern.c_str(),
                      O_RDWR | O_CREAT | open_mode, 0644);
        if (fd == -1) {
          CRANE_ERROR("[Task #{}] Failed to open output file {}: {}", task_id,
                      meta->parsed_output_file_pattern, std::strerror(errno));
          return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
        }
        stdout_local_fd = fd;
      }
      opened_fd.insert(stdout_local_fd);
    }

    if (meta->fwd_stderr) {
      if (pipe(from_crun_stderr_pipe) == -1) {
        CRANE_ERROR("[Task #{}] Failed to create pipe for task io forward: {}",
                    task_id, strerror(errno));
        for (int fd : opened_fd) {
          close(fd);
        }
        return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
      }
      opened_fd.insert(from_crun_stderr_pipe[0]);
      opened_fd.insert(from_crun_stderr_pipe[1]);
    } else {
      if (meta->parsed_error_file_pattern.empty() &&
          meta->parsed_output_file_pattern.empty()) {
        int null_fd = open_dev_null();
        if (null_fd == -1) {
          return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
        }
        stderr_local_fd = null_fd;
      } else {
        auto* err_path = meta->parsed_error_file_pattern.empty()
                             ? &meta->parsed_output_file_pattern
                             : &meta->parsed_error_file_pattern;
        int fd = open(err_path->c_str(), O_RDWR | O_CREAT | open_mode, 0644);
        if (fd == -1) {
          CRANE_ERROR("[Task #{}] Failed to open err file {}: {}", task_id,
                      *err_path, std::strerror(errno));
          return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
        }
        stderr_local_fd = fd;
      }
      opened_fd.insert(stderr_local_fd);
    }
  }

  pid_t pid = -1;
  if (m_parent_step_inst_->pty && m_parent_step_inst_->task_ids.size() > 1) {
    CRANE_ERROR("Crun pty task with task-per-node >1 not supported.");
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }
  if (m_parent_step_inst_->pty) {
    pid = forkpty(&crun_pty_fd, nullptr, nullptr, nullptr);

    if (pid > 0) {
      meta->stdin_read = -1;
      meta->stdout_read = crun_pty_fd;
      meta->stderr_read = -1;
      meta->stdin_write = crun_pty_fd;
      meta->stdout_write = -1;
      meta->stderr_write = -1;
    }
  } else {
    pid = fork();

    if (meta->fwd_stdin) {
      meta->stdin_write = to_crun_pipe[1];
      meta->stdin_read = to_crun_pipe[0];
    } else {
      meta->stdin_write = -1;
      meta->stdin_read = stdin_local_fd;
    }
    if (meta->fwd_stdout) {
      meta->stdout_write = from_crun_stdout_pipe[1];
      meta->stdout_read = from_crun_stdout_pipe[0];
    } else {
      meta->stdout_write = stdout_local_fd;
      meta->stdout_read = -1;
    }

    if (meta->fwd_stderr) {
      meta->stderr_write = from_crun_stderr_pipe[1];
      meta->stderr_read = from_crun_stderr_pipe[0];
    } else {
      meta->stderr_write = stderr_local_fd;
      meta->stderr_read = -1;
    }

    if (pid == -1) {
      for (int fd : opened_fd) {
        close(fd);
      }
    }
  }

  return pid;
}

bool ProcInstance::SetupCrunFwdAtParent_() {
  auto* meta = GetCrunMeta_();

  if (!m_parent_step_inst_->pty) {
    // For non-pty tasks, pipe is used for stdin/stdout and one end should be
    // closed.
    if (meta->fwd_stdin) close(meta->stdin_read);
    if (meta->fwd_stdout) close(meta->stdout_write);
    if (meta->fwd_stderr) close(meta->stderr_write);
  }
  // For pty tasks, stdin_read and stdout_write are the same fd and should not
  // be closed.

  auto* parent_cfored_client = m_parent_step_inst_->GetCforedClient();

  auto ok = parent_cfored_client->InitFwdMetaAndUvStdoutFwdHandler(
      task_id, meta->stdin_write, meta->stdout_read, meta->stderr_read,
      m_parent_step_inst_->pty);
  if (!ok) return false;
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
  // Prevent any suprocess like x11 trigger uvw signal handler
  signal(SIGCHLD, SIG_DFL);
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
  int stdout_fd{-1}, stderr_fd{-1};

  auto* meta = dynamic_cast<BatchInstanceMeta*>(m_meta_.get());
  const std::string& stdin_file_path = meta->parsed_input_file_pattern;
  const std::string& stdout_file_path = meta->parsed_output_file_pattern;
  const std::string& stderr_file_path = meta->parsed_error_file_pattern;

  int open_mode =
      GetParentStep().io_meta().open_mode_append() ? O_APPEND : O_TRUNC;
  if (stdin_file_path.empty()) {
    // Close stdin for batch tasks.

    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never
    // end.
    if (m_parent_step_inst_->IsBatch()) {
      int dev_null_fd = open("/dev/null", O_RDWR);
      if (dev_null_fd == -1) {
        fmt::println(stderr, "[Task #{}] Subprocess error: open /dev/null. {}",
                     task_id, std::strerror(errno));
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
      if (dup2(dev_null_fd, STDIN_FILENO) == -1) {
        fmt::println(stderr,
                     "[Task #{}] Subprocess error: dup2 /dev/null to stdin. {}",
                     task_id, std::strerror(errno));
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
      fmt::println(stderr, "[Task #{}] Subprocess stdin: dup2 /dev/null",
                   task_id);
    }
    fmt::println(stderr, "[Task #{}] Subprocess stdin: closed", task_id);
  } else {
    int stdin_fd = open(stdin_file_path.c_str(), O_RDONLY);
    if (stdin_fd == -1) {
      fmt::println(stderr, "[Task #{}] Subprocess error: open stdin: {}. {}\n",
                   task_id, stdin_file_path, strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
    fmt::println(stderr, "[Task #{}] Subprocess stdin: {}", task_id,
                 stdin_file_path);
    // STDIN -> input file
    if (dup2(stdin_fd, STDIN_FILENO) == -1) {
      fmt::println(stderr,
                   "[Task #{}] Subprocess error: dup2 stdin file to stdin. {}",
                   task_id, strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
  }

  stdout_fd =
      open(stdout_file_path.c_str(), O_RDWR | O_CREAT | open_mode, 0644);
  if (stdout_fd == -1) {
    fmt::println(stderr, "[Task #{}] Subprocess Error: open {}. {}", task_id,
                 stdout_file_path, strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  if (dup2(stdout_fd, STDOUT_FILENO) == -1) {
    fmt::println(stderr,
                 "[Task #{}] Subprocess error: dup2 stdout file to stdout. {}",
                 task_id, strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }
  fmt::println(stderr, "[Task #{}] Subprocess stdout: {}", task_id,
               stdout_file_path);

  if (stderr_file_path.empty()) {
    fmt::println(stderr, "[Task #{}] Subprocess stderr: {}", task_id,
                 stdout_file_path);
    // if stderr file is not set, redirect stderr to stdout
    if (dup2(stdout_fd, STDERR_FILENO) == -1) {
      fmt::println(stderr,
                   "[Task #{}] Subprocess error: dup2 stderr using stdout. {}",
                   task_id, strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
  } else {
    stderr_fd =
        open(stderr_file_path.c_str(), O_RDWR | O_CREAT | open_mode, 0644);
    if (stderr_fd == -1) {
      fmt::println(stderr, "[Task #{}] Subprocess error: open {}. {}", task_id,
                   stderr_file_path, strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
    fmt::println(stderr, "[Task #{}] Subprocess stderr: {}", task_id,
                 stderr_file_path);
    if (dup2(stderr_fd, STDERR_FILENO) == -1) {
      fmt::println(
          stderr, "[Task #{}] Subprocess error: dup2 stderr file to stderr. {}",
          task_id, strerror(errno));
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
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
    if (dup2(meta->stderr_write, STDERR_FILENO) == -1) std::exit(1);
    close(meta->stdin_read);
    close(meta->stdout_write);
    close(meta->stderr_write);
  }
}

void ProcInstance::SetupChildProcCrunX11_() {
  auto x11_meta = m_parent_step_inst_->x11_meta.value();
  const auto& proto_x11_meta =
      this->GetParentStep().interactive_meta().x11_meta();

  std::string x11_target = proto_x11_meta.enable_forwarding()
                               ? g_config.CranedIdOfThisNode
                               : proto_x11_meta.target();
  std::string x11_disp_fmt =
      proto_x11_meta.enable_forwarding() ? "%s/unix:%u" : "%s:%u";

  std::string display =
      fmt::sprintf(x11_disp_fmt, x11_target,
                   m_parent_step_inst_->x11_meta.value().x11_port - 6000);

  std::vector<const char*> xauth_argv{
      "/usr/bin/xauth",
      "-v",
      "-f",
      x11_meta.x11_auth_path.c_str(),
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

  uint64_t uid_start{};
  uint64_t uid_count{};
  uint64_t gid_start{};
  uint64_t gid_count{};

  auto resolve_managed_range = [&](const auto& mappings, uint64_t id,
                                   std::string_view id_label,
                                   std::string_view field_name, uint64_t& start,
                                   uint64_t& count) -> bool {
    for (const auto& mapping : mappings) {
      uint64_t id_end_exclusive{};
      if (util::AddOverflow(mapping.Id, mapping.IdCount, id_end_exclusive)) {
        CRANE_ERROR(
            "Failed to resolve {} SubId range for user {}: {} mapping "
            "overflowed.",
            id_label, pwd.Username(), field_name);
        return false;
      }

      if (id < mapping.Id || id >= id_end_exclusive) continue;

      const uint64_t id_offset = id - mapping.Id;
      uint64_t start_offset{};
      if (util::MulOverflow(id_offset, mapping.SubIdSize, start_offset) ||
          util::AddOverflow(mapping.SubIdStart, start_offset, start)) {
        CRANE_ERROR(
            "Failed to resolve {} SubId range for user {}: {} mapping "
            "overflowed.",
            id_label, pwd.Username(), field_name);
        return false;
      }

      count = mapping.SubIdSize;
      return true;
    }

    CRANE_ERROR(
        "Failed to resolve {} SubId range for user {}: {} {} is not covered "
        "by Container.SubId.{}.",
        id_label, pwd.Username(), id_label, id, field_name);
    return false;
  };

  if (subid_conf.Managed) {
    if (!resolve_managed_range(subid_conf.UidMappings, uid, "uid",
                               "UidMappings", uid_start, uid_count) ||
        !resolve_managed_range(subid_conf.GidMappings, gid, "gid",
                               "GidMappings", gid_start, gid_count)) {
      return CraneErrCode::ERR_SYSTEM_ERR;
    }
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
  auto fits_runtime_mapping = [](uint64_t start, uint64_t count) -> bool {
    if (count == 0) return false;

    constexpr uint64_t kMaxRuntimeId = std::numeric_limits<uint32_t>::max();
    if (start > kMaxRuntimeId || count > kMaxRuntimeId) return false;

    uint64_t end{};
    if (util::AddOverflow(start, count - 1, end)) return false;
    return end <= kMaxRuntimeId;
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
      // /etc/subuid and /etc/subgid are the higher-priority override.
      // If they exist, both entries must match the configured mapping exactly.
      if (!matches(subuid, uid_start, uid_count) ||
          !matches(subgid, gid_start, gid_count)) {
        CRANE_ERROR(
            "SubId mismatch: expected uid range [{}, {}], gid range [{}, {}] "
            "for user {}, but found uid range [{}, {}], gid range [{}, {}]",
            uid_start, uid_count, gid_start, gid_count, pwd.Username(),
            uid_exists ? subuid[0].start : 0, uid_exists ? subuid[0].count : 0,
            gid_exists ? subgid[0].start : 0, gid_exists ? subgid[0].count : 0);
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
    } else {
      // Both missing: allocate them
      auto ensure_result = PasswordEntry::EnsureSubIdRanges(
          pwd.Username(), uid_start, uid_count, gid_start, gid_count);
      if (!ensure_result) {
        CRANE_ERROR("Failed to ensure SubId ranges for user {}: {}",
                    pwd.Username(), ensure_result.error());
        return CraneErrCode::ERR_SYSTEM_ERR;
      }

      // Re-query to verify
      PasswordEntry pwd_recheck(uid);
      subuid = pwd_recheck.SubUidRanges();
      subgid = pwd_recheck.SubGidRanges();

      if (!matches(subuid, uid_start, uid_count) ||
          !matches(subgid, gid_start, gid_count)) {
        CRANE_ERROR(
            "SubId verification failed after allocation for user {}: expected "
            "uid range [{}, {}], gid range [{}, {}]",
            pwd.Username(), uid_start, uid_count, gid_start, gid_count);
        return CraneErrCode::ERR_SYSTEM_ERR;
      }
    }
  }

  if (!fits_runtime_mapping(subuid[0].start, subuid[0].count) ||
      !fits_runtime_mapping(subgid[0].start, subgid[0].count)) {
    CRANE_ERROR(
        "SubId range exceeds runtime 32-bit id mapping limits for user {}: "
        "uid range [{}, {}], gid range [{}, {}]. Adjust "
        "Container.SubId.UidMappings/GidMappings or the user's uid/gid "
        "assignments.",
        pwd.Username(), subuid[0].start, subuid[0].count, subgid[0].start,
        subgid[0].count);
    return CraneErrCode::ERR_SYSTEM_ERR;
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

void PodInstance::SetPodLabels_(
    const crane::grpc::PodJobAdditionalMeta& pod_meta, uid_t uid,
    job_id_t job_id, const std::string& job_name) {
  m_pod_config_.mutable_labels()->insert(
      {{std::string(cri::kCriUidKey), std::to_string(uid)},
       {std::string(cri::kCriJobIdKey), std::to_string(job_id)},
       {std::string(cri::kCriJobNameKey), job_name}});
  m_pod_config_.mutable_labels()->insert(pod_meta.labels().begin(),
                                         pod_meta.labels().end());
}

void PodInstance::SetPodAnnotations_(
    const crane::grpc::PodJobAdditionalMeta& pod_meta, uid_t uid,
    job_id_t job_id, const std::string& job_name, const std::string& hostname) {
  auto prefix = [](std::string_view key) -> std::string {
    return std::string(cri::kCriAnnotationPrefix) + std::string(key);
  };

  std::string fqdn =
      std::format("{}.{}", hostname, g_config.Container.Dns.ClusterDomain);

  m_pod_config_.mutable_annotations()->insert(
      {{prefix(cri::kCriUidKey), std::to_string(uid)},
       {prefix(cri::kCriJobIdKey), std::to_string(job_id)},
       {prefix(cri::kCriJobNameKey), job_name},
       {prefix(cri::kCriHostnameKey), hostname},
       {prefix(cri::kCriFQDNKey), std::move(fqdn)}});
  m_pod_config_.mutable_annotations()->insert(pod_meta.annotations().begin(),
                                              pod_meta.annotations().end());
}

CraneErrCode PodInstance::SetPodSandboxConfig_(
    const crane::grpc::PodJobAdditionalMeta& pod_meta) {
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
  SetPodLabels_(pod_meta, uid, job_id, job_name);

  // annotations
  SetPodAnnotations_(pod_meta, uid, job_id, job_name, hostname);

  auto* dns_config = m_pod_config_.mutable_dns_config();
  dns_config->clear_servers();
  dns_config->clear_searches();
  dns_config->clear_options();

  // User-specified DNS servers are prepended (higher priority)
  for (const auto& s : pod_meta.dns_servers()) dns_config->add_servers(s);
  // System default DNS servers follow
  for (const auto& s : g_config.Container.Dns.Servers)
    dns_config->add_servers(s);

  // Prepend ClusterDomain as the first search domain
  dns_config->add_searches(g_config.Container.Dns.ClusterDomain);
  for (const auto& s : g_config.Container.Dns.Searches)
    dns_config->add_searches(s);
  for (const auto& s : g_config.Container.Dns.Options)
    dns_config->add_options(s);

  // log directory
  m_pod_config_.set_log_directory(m_log_dir_);

  // FIXME: This is just a workaround. Let's use job's cgroup before resourceV3
  // refactoring. Here set cgroup parent to <job_id>/
  auto* linux_config = m_pod_config_.mutable_linux();
  linux_config->set_cgroup_parent(std::filesystem::path(g_config.SupvCgroupPath)
                                      .parent_path()
                                      .parent_path()
                                      .string());

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

CraneErrCode PodInstance::StopAndRemovePodSandbox_(std::string_view context) {
  auto* cri_client = m_parent_step_inst_->GetCriClient();

  auto stop_ret = cri_client->StopPodSandbox(m_pod_id_);
  if (!stop_ret.has_value()) {
    const auto& rich_err = stop_ret.error();
    CRANE_WARN("Failed to stop pod sandbox {} {} for job #{}: {}", m_pod_id_,
               context, g_config.JobId, rich_err.description());
  }

  auto remove_ret = cri_client->RemovePodSandbox(m_pod_id_);
  if (!remove_ret.has_value()) {
    const auto& rich_err = remove_ret.error();
    CRANE_ERROR("Failed to remove pod sandbox {} {} for job #{}: {}", m_pod_id_,
                context, g_config.JobId, rich_err.description());
    return rich_err.code();
  }

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
    if (StopAndRemovePodSandbox_("after persist error") ==
        CraneErrCode::SUCCESS) {
      m_pod_id_.clear();
    }

    return err;
  }

  // TODO: Add monitoring for pod status, preferably in Craned, not here.

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::Kill(int /*signum*/) {
  if (auto err = StopAndRemovePodSandbox_("during stop request");
      err != CraneErrCode::SUCCESS)
    return err;

  CRANE_DEBUG("Pod {} stopped for job #{}", m_pod_id_, g_config.JobId);
  HandlePodExited();

  return CraneErrCode::SUCCESS;
}

CraneErrCode PodInstance::Cleanup() {
  g_thread_pool->detach_task([cfg = m_config_file_, lock = m_lock_file_]() {
    auto remove_if_exists = [](const std::filesystem::path& path,
                               std::string_view label) {
      if (path.empty()) {
        CRANE_TRACE("Pod {} file was never created; skipping cleanup.", label);
        return;
      }

      std::error_code ec;
      bool removed = std::filesystem::remove(path, ec);
      if (removed) return;

      if (ec) {
        CRANE_ERROR("Failed to remove pod {} file {}: {}", label, path,
                    ec.message());
      } else {
        CRANE_TRACE("Pod {} file {} already absent during cleanup.", label,
                    path);
      }
    };

    remove_if_exists(cfg, "config");
    remove_if_exists(lock, "lock");
  });

  return CraneErrCode::SUCCESS;
}

const TaskExitInfo& PodInstance::HandlePodExited() {
  // m_exit_info_.is_terminated_by_signal = status.exit_code() > 128;
  // m_exit_info_.value = m_exit_info_.is_terminated_by_signal
  //                          ? status.exit_code() - 128
  //                          : status.exit_code();
  // TODO: See PodInstance::Spawn() TODO.
  m_final_info_.raw_exit = TaskExitInfo{
      .is_terminated_by_signal = false,
      .value = 0,
  };

  return m_final_info_.raw_exit.value();
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
  using cri::kCriJobIdKey;
  using cri::kCriStepIdKey;
  using cri::kCriUidKey;

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
                           status, std::string(kCriJobIdKey), g_config.JobId) &&
                       cri::CriClient::ParseAndCompareLabel(
                           status, std::string(kCriStepIdKey),
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
  // It's ok to call it twice to remove container.
  return Kill(0);
}

const TaskExitInfo& ContainerInstance::HandleContainerExited(
    const cri::api::ContainerStatus& status) {
  bool signaled = status.exit_code() > 128;
  m_final_info_.raw_exit = TaskExitInfo{
      .is_terminated_by_signal = signaled,
      .value = signaled ? status.exit_code() - 128 : status.exit_code(),
  };

  return m_final_info_.raw_exit.value();
}

CraneErrCode ContainerInstance::LoadPodSandboxInfo_(
    const crane::grpc::PodJobAdditionalMeta* pod_meta) {
  using cri::kCriDefaultLabel;
  using cri::kCriJobIdKey;
  using cri::kCriJobNameKey;
  using cri::kCriUidKey;

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
        {std::string(kCriJobIdKey), std::to_string(g_config.JobId)},
        {std::string(kCriUidKey),
         std::to_string(m_parent_step_inst_->pwd.Uid())},
        {std::string(kCriJobNameKey), g_config.JobName}};

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

void ContainerInstance::SetContainerLabels_(uid_t uid, job_id_t job_id,
                                            step_id_t step_id,
                                            const std::string& job_name,
                                            const std::string& step_name) {
  m_container_config_.mutable_labels()->insert({
      {std::string(cri::kCriUidKey), std::to_string(uid)},
      {std::string(cri::kCriJobIdKey), std::to_string(job_id)},
      {std::string(cri::kCriStepIdKey), std::to_string(step_id)},
      {std::string(cri::kCriJobNameKey), job_name},
      {std::string(cri::kCriStepNameKey), step_name},
  });
}

CraneErrCode ContainerInstance::SetContainerConfig_(
    const crane::grpc::ContainerJobAdditionalMeta& ca_meta,
    const crane::grpc::PodJobAdditionalMeta* pod_meta) {
  // There is only one container in a step. So we use step_id and job_id here.
  job_id_t job_id = g_config.JobId;
  step_id_t step_id = g_config.StepId;

  const std::string& job_name = g_config.JobName;
  const std::string& step_name = GetParentStep().name();

  uid_t uid = m_parent_step_inst_->pwd.Uid();
  gid_t gid = m_parent_step_inst_->pwd.Gid();

  // Using job_id/step_id to generate unique name in container metadata
  m_container_config_.mutable_metadata()->set_name(
      std::format("job-{}-{}", job_id, step_id));

  // labels
  SetContainerLabels_(uid, job_id, step_id, job_name, step_name);

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

  // CDI devices injection — read from container_meta populated by craned.
  if (ca_meta.cdi_devices_size() > 0) {
    for (const auto& cdi_name : ca_meta.cdi_devices())
      m_container_config_.add_cdi_devices()->set_name(cdi_name);
    CRANE_INFO("Injected {} CDI devices into container config for #{}.{}",
               ca_meta.cdi_devices_size(), job_id, step_id);
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
  // Create cgroup for task.
  auto cg_expt = CgroupManager::AllocateAndGetCgroup(
      CgroupManager::CgroupStrByTaskId(g_config.JobCgStr, g_config.StepId,
                                       task_id),
      m_parent_step_inst_->GetStep().task_res_map().at(task_id), false);
  if (!cg_expt.has_value()) {
    CRANE_WARN("[Step #{}.{}] Failed to allocate cgroup for task #{}: {}",
               g_config.JobId, g_config.StepId, task_id,
               static_cast<int>(cg_expt.error()));
    return cg_expt.error();
  }
  m_task_cg_ = std::move(cg_expt.value());

  // Write m_meta_
  if (m_parent_step_inst_->IsCrun()) {
    m_meta_ = std::make_unique<CrunInstanceMeta>();
  } else {
    // Prepare file output name for batch tasks.
    auto meta = std::make_unique<BatchInstanceMeta>();
    m_meta_ = std::move(meta);
  }
  if (m_parent_step_inst_->IsCrun()) {
    auto* meta = GetCrunMeta_();
    {
      auto [path, fwd] = CrunParseFilePattern_(
          GetParentStep().io_meta().output_file_pattern());
      meta->fwd_stdout = fwd;
      meta->parsed_output_file_pattern = path;
    }
    {
      auto [path, fwd] =
          CrunParseFilePattern_(GetParentStep().io_meta().input_file_pattern());
      meta->fwd_stdin = fwd;
      meta->parsed_output_file_pattern = path;
    }
    {
      auto [path, fwd] =
          CrunParseFilePattern_(GetParentStep().io_meta().error_file_pattern());
      meta->fwd_stderr = fwd;
      meta->parsed_error_file_pattern = path;
    }

  } else {
    m_meta_->parsed_output_file_pattern =
        ParseFilePathPattern_(GetParentStep().io_meta().output_file_pattern(),
                              GetParentStep().cwd(), true, nullptr);

    // If -e / --error is not defined, leave
    // parsed_error_file_pattern empty;
    m_meta_->parsed_error_file_pattern =
        ParseFilePathPattern_(GetParentStep().io_meta().error_file_pattern(),
                              GetParentStep().cwd(), false, nullptr);

    m_meta_->parsed_input_file_pattern =
        ParseFilePathPattern_(GetParentStep().io_meta().input_file_pattern(),
                              GetParentStep().cwd(), false, nullptr);
  }

  // If interpreter is not set, let system decide.
  m_executable_ = m_parent_step_inst_->script_path.value().string();
  if (m_parent_step_inst_->IsBatch() &&
      !GetParentStep().batch_meta().interpreter().empty()) {
    m_executable_ =
        fmt::format("{} {}", GetParentStep().batch_meta().interpreter(),
                    m_parent_step_inst_->script_path.value().string());
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

  // NOLINTNEXTLINE(hicpp-avoid-c-arrays, modernize-avoid-c-arrays)
  int ctrl_sock_pair[2];  // Socket pair for passing control messages.

  std::vector<int> to_crun_pipe(2, -1);
  std::vector<int> from_crun_pipe(2, -1);

  // NOLINTNEXTLINE(hicpp-no-array-decay)
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctrl_sock_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  pid_t child_pid = -1;
  if (m_parent_step_inst_->IsCrun()) {
    auto pid_expt = ForkCrunAndInitIOfd_();
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

    auto handle_comm_err = [this](std::string msg) {
      // Communication failure caused by process crash or grpc error.
      // Since now the parent cannot ask the child process to commit suicide,
      // kill child process here and just return.

      // We must return SUCCESS, as child process will be reaped in SIGCHLD
      // handler and thus only ONE TaskStatusChange will be triggered!

      m_final_info_.cause = TaskFinalizeCause::TASK_SPAWN_FAILED;
      m_final_info_.reason = std::move(msg);
    };

    bool crun_init_success{true};
    if (m_parent_step_inst_->IsCrun()) {
      if (m_parent_step_inst_->x11) {
        crun_init_success = SetupCrunFwdAtParent_();
        msg.set_x11_port(m_parent_step_inst_->x11_meta.value().x11_port);
      } else {
        crun_init_success = SetupCrunFwdAtParent_();
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

      handle_comm_err("Failed to flush message to subprocess");
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

      handle_comm_err("Failed to receive response from subprocess");
      Kill(SIGKILL);
      return CraneErrCode::SUCCESS;
    }

    close(ctrl_fd);
    return CraneErrCode::SUCCESS;

  } else {  // Child proc
    ResetChildProcSigHandler_();

    auto success = m_task_cg_->MigrateProcIn(getpid());
    if (!success) {
      fmt::print(stderr, "[Subprocess] MigrateProcIn returned {}\n", success);
      std::abort();
    }
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
    util::os::CloseFdFrom(3);

    if (m_parent_step_inst_->IsCrun() && m_parent_step_inst_->x11) {
      SetupChildProcCrunX11_();
    }

    // Apply environment variables
    InitEnvMap();

    std::unordered_map<std::string, std::string> pmix_env;
    if (m_parent_step_inst_->IsCrun() &&
        m_parent_step_inst_->GetStep().interactive_meta().mpi() == "pmix") {
      auto result = g_pmix_server->SetupFork(0);
      if (!result) {
        fmt::print(stderr,
                   "[Craned Subprocess] Pmix Server SetupFork() failed.\n");
        std::abort();
      }
      pmix_env = result.value();
    }

    for (const auto& [k, v] : pmix_env) {
      m_env_.emplace(k, v);
    }

    if (!g_config.JobLifecycleHook.TaskPrologs.empty()) {
      RunPrologEpilogArgs run_prolog_args{
          .scripts = g_config.JobLifecycleHook.TaskPrologs,
          .envs = m_env_,
          .timeout_sec = g_config.JobLifecycleHook.PrologTimeout,
          .run_uid = m_parent_step_inst_->uid,
          .run_gid = m_parent_step_inst_->gids[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};

      if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
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

    // TODO: Before exec, bind integer CPU cores from ResourceInNodeV3::CpuSet
    // using sched_setaffinity(). Fractional CPU is handled by cgroup quota
    // only. Integer cores should be pinned to the task process here.

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
    bool success = this->m_task_cg_->KillAllProcesses(signum);
    if (success) return CraneErrCode::SUCCESS;

    CRANE_TRACE("[Task #{}] failed to kill.", task_id);
    return CraneErrCode::ERR_GENERIC_FAILURE;
  }

  return CraneErrCode::ERR_NON_EXISTENT;
}

CraneErrCode ProcInstance::Cleanup() {
  if (m_task_cg_) {
    int cnt = 0;

    while (true) {
      if (m_task_cg_->Empty()) break;

      if (cnt >= 5) {
        CRANE_ERROR(
            "Couldn't kill the processes in cgroup {} after {} times. "
            "Skipping it.",
            m_task_cg_->CgroupName(), cnt);
        break;
      }

      m_task_cg_->KillAllProcesses(SIGKILL);
      ++cnt;
      std::this_thread::sleep_for(100ms);
    }

    m_task_cg_->Destroy();
    m_task_cg_.reset();
  }

  // TODO: Plugin support step cgroup destroy hooks?
  // NOTE: Pod and container have seperated cgroup handling.
  return CraneErrCode::SUCCESS;
}

std::optional<const TaskExitInfo> ProcInstance::HandleSigchld(pid_t pid,
                                                              int status) {
  auto exit_info = TaskExitInfo{};
  exit_info.pid = pid;

  if (WIFEXITED(status)) {
    // Exited with status WEXITSTATUS(status)
    exit_info.is_terminated_by_signal = false;
    exit_info.value = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    // Killed by signal WTERMSIG(status)
    exit_info.is_terminated_by_signal = true;
    exit_info.value = WTERMSIG(status);
  } else {
    CRANE_TRACE("Received SIGCHLD with status {} for task #{} but ignored.",
                status, task_id);
    return std::nullopt;
  }

  m_final_info_.raw_exit = std::move(exit_info);
  return m_final_info_.raw_exit;
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
  m_sigchld_timer_handle_->start(1s, 1s);

  m_process_sigchld_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_process_sigchld_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanSigchldQueueCb_();
      });

  m_task_finalizing_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_task_finalizing_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanFinalizingTaskQueueCb_();
      });

  m_cri_event_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_cri_event_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanCriEventQueueCb_();
      });

  m_execute_daemon_pod_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_execute_daemon_pod_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvExecuteDaemonPodCb_();
      });

  m_terminate_daemon_pod_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_daemon_pod_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateDaemonPodQueueCb_();
      });

  m_terminate_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateStepQueueCb_();
      });

  m_terminate_step_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_terminate_step_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        m_terminate_step_async_handle_->send();
      });
  m_terminate_step_timer_handle_->start(
      std::chrono::milliseconds(kStepRequestCheckIntervalMs * 3),
      std::chrono::milliseconds(kStepRequestCheckIntervalMs));

  m_change_step_time_constraint_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_change_step_time_constraint_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanChangeStepTimeConstraintQueueCb_();
      });
  m_change_step_time_constraint_timer_handle_ =
      m_uvw_loop_->resource<uvw::timer_handle>();
  m_change_step_time_constraint_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        m_change_step_time_constraint_async_handle_->send();
      });
  m_change_step_time_constraint_timer_handle_->start(
      std::chrono::milliseconds(kStepRequestCheckIntervalMs * 3),
      std::chrono::milliseconds(kStepRequestCheckIntervalMs));

  m_grpc_execute_step_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_execute_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvGrpcExecuteStepCb_();
      });

  m_grpc_query_step_env_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_query_step_env_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvGrpcQueryStepEnvCb_();
      });

  m_grpc_check_status_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_check_status_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvGrpcCheckStatusCb_();
      });

  m_grpc_migrate_ssh_proc_to_cgroup_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_migrate_ssh_proc_to_cgroup_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvGrpcMigrateSshProcToCgroupCb_();
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
          std::this_thread::sleep_for(50ms);
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
        .timeout_sec = g_config.JobLifecycleHook.EpilogTimeout,
        .run_uid = 0,
        .run_gid = 0,
        .output_size = g_config.JobLifecycleHook.MaxOutputSize};
    if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
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

void TaskManager::SupervisorFinishInit(StepStatus status) {
  m_step_.GotNewStatus(status);
  g_craned_client->StepStatusChangeAsync(status, 0, std::nullopt);
}

bool TaskManager::InitPmixPreFork() {
  if (m_step_.IsCrun() &&
      m_step_.GetStep().interactive_meta().mpi() == "pmix") {
    g_pmix_server = std::make_unique<pmix::PmixServer>();
    pmix::Config pmix_config{
        .UseTls = g_config.CforedListenConf.TlsConfig.Enabled,
        .TlsCerts = g_config.CforedListenConf.TlsConfig.TlsCerts,
        .CompressedRpc = g_config.CompressedRpc,
        .CraneBaseDir = g_config.CraneBaseDir,
        .CraneScriptDir = g_config.CraneScriptDir,
        .CranedUnixSocketPath = g_config.CranedUnixSocketPath};
    
    if (g_config.JobEnv.contains("CRANE_PMIX_FENCE")) {
      pmix_config.CranePmixFence = g_config.JobEnv.at("CRANE_PMIX_FENCE");
    }

    if (g_config.JobEnv.contains("CRANE_PMIX_TIMEOUT")) {
      pmix_config.CranePmixTimeout = g_config.JobEnv.at("CRANE_PMIX_TIMEOUT");
    }

    if (g_config.JobEnv.contains("CRANE_PMIX_DIRECT_CONN_UCX")) {
      pmix_config.CranePmixDirectConnUcx = g_config.JobEnv.at("CRANE_PMIX_DIRECT_CONN_UCX");
    }

    if (g_config.JobEnv.contains("PMIXP_PMIXLIB_TMPDIR")) {
      pmix_config.PmixpPmixlibTmpDir = g_config.JobEnv.at("PMIXP_PMIXLIB_TMPDIR");
    }

    if (!g_pmix_server->Init(pmix_config, m_step_.GetStep()))
      return false;
  }

  return true;
}

void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::ShutdownSupervisorAsync(crane::grpc::JobStatus new_status,
                                          uint32_t exit_code,
                                          std::string reason) {
  CRANE_INFO("All tasks finished, exiting...");
  m_shutdown_status_queue_.enqueue(
      std::make_tuple(new_status, exit_code, std::move(reason)));
  m_shutdown_supervisor_handle_->send();
}

void TaskManager::FinalizeTaskAsync(task_id_t task_id, TaskFinalizeCause cause,
                                    std::optional<std::string> reason) {
  auto* task = m_step_.GetTaskInstance(task_id);
  if (task == nullptr) {
    CRANE_DEBUG("[Task #{}] Task not found when setting finalize cause {}.",
                task_id, static_cast<int>(cause));
    return;
  }

  auto* final_info = task->GetFinalInfo();
  final_info->cause = cause;
  final_info->reason = std::move(reason);

  m_task_finalizing_queue_.enqueue(task_id);
  m_task_finalizing_async_handle_->send();
}

void TaskManager::FinalizeTaskAsync(task_id_t task_id) {
  m_task_finalizing_queue_.enqueue(task_id);
  m_task_finalizing_async_handle_->send();
}

void TaskManager::ResolveFinishedTask_(task_id_t task_id, StepStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string> reason) {
  CRANE_TRACE(
      "[Task #{}] Task status changed to {} (exit code: {}, reason: {}).",
      task_id, new_status, exit_code, reason.value_or(""));

  auto task = m_step_.RemoveTaskInstance(task_id);
  if (task == nullptr) {
    CRANE_DEBUG("[Task #{}] Task not found.", task_id);
    return;
  }

  // One-shot model: nothing to stop, just proceed to cleanup.
  auto err = task->Cleanup();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_WARN("[Task #{}] Failed to cleanup task: {}", task_id,
               static_cast<int>(err));
  }

  auto& status = m_step_.final_termination_status;
  if (status.max_exit_code < exit_code) {
    // Bigger exit code always mean a FAILED status cased by a none zero exit
    // code, signal or crane error.
    status.max_exit_code = exit_code;
    status.final_status_on_termination = new_status;
    status.final_reason_on_termination = reason.value_or("");
  }

  // Error status has higher priority than success status.
  if (new_status != StepStatus::Completed)
    status.final_status_on_termination = new_status;

  if (m_step_.AllTaskFinished()) {
    m_step_.oom_baseline_inited = false;
    m_step_.GotNewStatus(StepStatus::Completing);
    DelTerminationTimer_();
    DelSignalTimers_();
    m_step_.StopCforedClient();
    if (!m_step_.orphaned) {
      g_craned_client->StepStatusChangeAsync(
          status.final_status_on_termination, status.max_exit_code,
          status.final_reason_on_termination);
    }
    ShutdownSupervisorAsync();
  }
}

std::future<CraneErrCode> TaskManager::ExecuteStepAsync() {
  CRANE_INFO("[Job #{}.{}] Executing step.", m_step_.job_id, m_step_.step_id);

  std::promise<CraneErrCode> ok_promise;
  std::future ok_future = ok_promise.get_future();

  auto elem = ExecuteStepElem{.ok_prom = std::move(ok_promise)};

  m_grpc_execute_step_queue_.enqueue(std::move(elem));
  m_grpc_execute_step_async_handle_->send();
  return ok_future;
}

CraneErrCode TaskManager::LaunchExecution_(ITaskInstance* task) {
  // Prepare/Spawn should not be called for Calloc steps.
  CRANE_ASSERT_MSG(!m_step_.IsCalloc(), "Calloc step should not launch tasks");

  // Prepare for execution
  CRANE_INFO("[Task #{}] Preparing task", task->task_id);
  CraneErrCode err = task->Prepare();
  if (err != CraneErrCode::SUCCESS) {
    FinalizeTaskAsync(
        task->task_id, TaskFinalizeCause::TASK_PREPARE_FAILED,
        std::format("Failed to prepare task, code: {}", static_cast<int>(err)));
    return err;
  }

  if (!InitPmixPreFork()) { 
    CRANE_ERROR("Failed to initialize PMIx server.");
    ActivateTaskStatusChange_(task->task_id, crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeFileNotFound,
                              fmt::format("pmix failed"));
    return ;
  }

  CRANE_INFO("[Task #{}] Spawning in task", task->task_id);
  err = task->Spawn();
  if (err != CraneErrCode::SUCCESS) {
    FinalizeTaskAsync(
        task->task_id, TaskFinalizeCause::TASK_SPAWN_FAILED,
        std::format("Cannot spawn child process in task, code: {}",
                    static_cast<int>(err)));
    return err;
  }

  return CraneErrCode::SUCCESS;
}

void TaskManager::EvExecuteDaemonPodCb_() {
  // This method is only for pod in daemon step.
  CRANE_ASSERT_MSG(m_step_.IsPod(), "PreparePodTask is called in other cases!");

  ExecuteStepElem ev;
  auto err = CraneErrCode::SUCCESS;
  static bool requested = false;

  while (m_grpc_execute_step_queue_.try_dequeue(ev)) {
    // This should never happen.
    if (requested) {
      CRANE_ERROR(
          "Pod step should only execute once, but got another execute "
          "request. Ignoring this request.");
      ev.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      continue;
    }
    requested = true;

    // CRI-managed steps only have one task whose id is fixed to kCriStepTaskId.
    task_id_t task_id = kCriStepTaskId;
    m_step_.AddTaskInstance(task_id,
                            std::make_unique<PodInstance>(&m_step_, task_id));
    m_step_.task_ids.emplace_back(task_id);
    auto* instance = m_step_.GetTaskInstance(task_id);

    m_step_.pwd.Init(m_step_.uid);
    if (!m_step_.pwd.Valid()) {
      CRANE_ERROR("Failed to look up password entry for uid {}", m_step_.uid);
      FinalizeTaskAsync(
          task_id, TaskFinalizeCause::STEP_PWD_LOOKUP_FAILED,
          std::format("Failed to look up password entry for uid {}",
                      m_step_.uid));
      ev.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      continue;
    }

    err = m_step_.Prepare();
    if (err != CraneErrCode::SUCCESS) {
      CRANE_ERROR("Failed to prepare pod: {}", static_cast<int>(err));
      FinalizeTaskAsync(task_id, TaskFinalizeCause::STEP_PREPARE_FAILED,
                        std::format("Failed to prepare pod step, code: {}",
                                    static_cast<int>(err)));
      ev.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      continue;
    }

    // For pod step, we will use job's cgroup directly.
    // NOTE: This assumes the job's cgroup is created before the pod step
    // starts.
    m_step_.step_user_cg = nullptr;
    // m_step_.InitOomBaseline(); No need to init oom for pod step.

    // Launch execution for pod.
    err = LaunchExecution_(instance);
    if (err == CraneErrCode::SUCCESS) {
      auto pod_id = instance->GetExecId().value();
      CRANE_INFO("Pod is running, pod id: {}.", std::get<std::string>(pod_id));
      m_exec_id_task_id_map_[pod_id] = task_id;
    }

    ev.ok_prom.set_value(err);
  }
}

std::future<CraneErrCode> TaskManager::ExecutePodInDaemonStepAsync() {
  std::promise<CraneErrCode> ok_promise;
  std::future ok_future = ok_promise.get_future();

  auto elem = ExecuteStepElem{.ok_prom = std::move(ok_promise)};
  m_grpc_execute_step_queue_.enqueue(std::move(elem));
  m_execute_daemon_pod_async_handle_->send();

  return ok_future;
}

std::future<CraneExpected<EnvMap>> TaskManager::QueryStepEnvAsync() {
  std::promise<CraneExpected<EnvMap>> env_promise;
  auto env_future = env_promise.get_future();
  m_grpc_query_step_env_queue_.enqueue(std::move(env_promise));
  m_grpc_query_step_env_async_handle_->send();
  return env_future;
}

std::future<CraneErrCode> TaskManager::ChangeStepTimeConstraintAsync(
    std::optional<int64_t> time_limit, std::optional<int64_t> deadline_time) {
  std::promise<CraneErrCode> ok_promise;
  auto ok_future = ok_promise.get_future();

  ChangeStepTimeConstraintQueueElem elem;
  elem.ok_prom = std::move(ok_promise);
  if (time_limit) {
    elem.time_limit = time_limit;
    m_step_.GetMutableStep().mutable_time_limit()->set_seconds(
        time_limit.value());
  } else {
    elem.time_limit =
        std::optional<int64_t>(m_step_.GetStep().time_limit().seconds());
  }

  if (deadline_time) {
    elem.deadline_time = deadline_time;
    m_step_.GetMutableStep().mutable_deadline_time()->set_seconds(
        deadline_time.value());
  } else {
    elem.deadline_time =
        std::optional<int64_t>(m_step_.GetStep().deadline_time().seconds());
  }

  m_step_time_constraint_change_queue_.enqueue(std::move(elem));
  m_change_step_time_constraint_async_handle_->send();
  return ok_future;
}

void TaskManager::TerminateStepAsync(bool mark_as_orphaned,
                                     TaskFinalizeCause cause) {
  m_step_terminate_queue_.enqueue(StepTerminateQueueElem{
      .cause = cause,
      .mark_as_orphaned = mark_as_orphaned,
  });
  m_terminate_step_async_handle_->send();
}

void TaskManager::TerminatePodInDaemonStepAsync() {
  m_daemon_pod_terminate_queue_.enqueue(DaemonPodTerminateQueueElem{});
  m_terminate_daemon_pod_async_handle_->send();
}

void TaskManager::CheckStatusAsync(
    crane::grpc::supervisor::CheckStatusReply* response) {
  std::promise<StepStatus> status_promise;
  auto status_future = status_promise.get_future();
  m_grpc_check_status_queue_.enqueue(std::move(status_promise));
  m_grpc_check_status_async_handle_->send();
  response->set_job_id(g_config.JobId);
  response->set_step_id(g_config.StepId);
  response->set_supervisor_pid(getpid());
  response->set_status(status_future.get());
  response->set_ok(true);
}

std::future<CraneErrCode> TaskManager::MigrateSshProcToCgroupAsync(pid_t pid) {
  CRANE_INFO("Migrating ssh process pid {} to cgroup.", pid);
  std::promise<CraneErrCode> ok_promise;
  auto ok_future = ok_promise.get_future();

  std::pair elem{pid, std::move(ok_promise)};

  m_grpc_migrate_ssh_proc_to_cgroup_queue_.enqueue(std::move(elem));
  m_grpc_migrate_ssh_proc_to_cgroup_async_handle_->send();
  return ok_future;
}

void TaskManager::EvShutdownSupervisorCb_() {
  std::tuple<crane::grpc::JobStatus, uint32_t, std::string> final_status;
  bool got_final_status = false;
  do {
    got_final_status = m_shutdown_status_queue_.try_dequeue(final_status);
    if (!got_final_status) continue;
    if (m_step_.IsDaemon() && !m_allow_daemon_shutdown_.load()) {
      CRANE_WARN("Daemon step not shutting down unless explicitly requested.");
      continue;
    }

    auto& [status, exit_code, reason] = final_status;
    if (m_step_.IsDaemon()) {
      m_step_.GotNewStatus(status);
      if (!m_step_.orphaned) {
        CRANE_DEBUG("Sending a {} status as daemon step.", status);
        g_craned_client->StepStatusChangeAsync(status, exit_code, reason);
      }
    }

    // Non-daemon step don't need to report the status change in shutting down.
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
      uint32_t exit_code = exit_info->value;
      if (exit_info->is_terminated_by_signal)
        exit_code += ExitCode::KCrunExitCodeStatusNum;
      auto ok_to_free = m_step_.GetCforedClient()->TaskProcessStop(
          task_id, exit_code, exit_info.value().is_terminated_by_signal);
      if (ok_to_free) {
        CRANE_TRACE("It's ok to unregister task #{}", task_id);
        m_step_.GetCforedClient()->TaskEnd(task_id);
      }
    } else /* Batch / Calloc */ {
      FinalizeTaskAsync(task_id);
    }
  }

  // Put not found tasks back to the queue
  for (auto task : not_found_tasks) {
    m_sigchld_queue_.enqueue(task);
  }
}

void TaskManager::EvCleanCriEventQueueCb_() {
  // TODO: Refactor this to Craned.
  CRANE_ASSERT_MSG(
      m_step_.IsPod() || m_step_.IsContainer(),
      "CRI event queue should only be used by pod/container steps");

  cri::api::ContainerStatus status;
  while (m_cri_event_queue_.try_dequeue(status)) {
    // NOTE: a CRI event comes at LEAST once.
    auto* t = m_step_.GetTaskInstance(kCriStepTaskId);
    if (t == nullptr) continue;  // Duplicated event

    if (m_step_.IsPod()) {
      // TODO: This branch is unused as pod doesn't have exit event in
      // containerd currently.
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

    FinalizeTaskAsync(kCriStepTaskId);
  }
}

void TaskManager::EvStepTimerCb_(bool is_deadline) {
  std::string log_reason =
      is_deadline ? "reached its deadline" : "exceeded its time limit";
  CRANE_TRACE("Step #{}.{} {}. Terminating it...", g_config.JobId,
              g_config.StepId, log_reason);
  DelTerminationTimer_();
  DelSignalTimers_();

  m_step_terminate_queue_.enqueue(StepTerminateQueueElem{
      .cause = is_deadline ? TaskFinalizeCause::DEADLINE
                           : TaskFinalizeCause::TIMEOUT});
  m_terminate_step_async_handle_->send();
}

void TaskManager::EvCleanFinalizingTaskQueueCb_() {
  task_id_t task_id;
  while (m_task_finalizing_queue_.try_dequeue(task_id)) {
    CRANE_INFO("[Task #{}] Stopped and is doing StepStatusChange...", task_id);

    auto* task = m_step_.GetTaskInstance(task_id);
    if (task == nullptr) {
      CRANE_DEBUG("[Task #{}] Task not found in finalizing. Duplicated event?",
                  task_id);
      continue;
    }

    if (task->GetExecId().has_value())
      m_exec_id_task_id_map_.erase(*task->GetExecId());

    if (!m_step_.GetStep().task_epilog().empty()) {
      CRANE_TRACE("[Task #{}] Running step task_epilog...", task_id);
      RunPrologEpilogArgs run_epilog_args{
          .scripts = std::vector<std::string>{m_step_.GetStep().task_epilog()},
          .envs = std::unordered_map{task->GetParentStep().env().begin(),
                                     task->GetParentStep().env().end()},
          .timeout_sec = g_config.JobLifecycleHook.EpilogTimeout,
          .run_uid = task->GetParentStep().uid(),
          .run_gid = task->GetParentStep().gid()[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};

      if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
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
          .timeout_sec = g_config.JobLifecycleHook.EpilogTimeout,
          .run_uid = task->GetParentStep().uid(),
          .run_gid = task->GetParentStep().gid()[0],
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};

      if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
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

    auto* fi = task->GetFinalInfo();
    auto raw_exit_code = [](const TaskExitInfo& raw_exit) -> uint32_t {
      if (raw_exit.is_terminated_by_signal)
        return raw_exit.value + ExitCode::kTerminationSignalBase;
      return raw_exit.value;
    };

    switch (fi->cause) {
    case TaskFinalizeCause::TASK_PREPARE_FAILED:
    case TaskFinalizeCause::STEP_PREPARE_FAILED:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                           ExitCode::EC_FILE_NOT_FOUND, fi->reason);
      continue;
    case TaskFinalizeCause::TASK_SPAWN_FAILED:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                           ExitCode::EC_SPAWN_FAILED, fi->reason);
      continue;
    case TaskFinalizeCause::STEP_PWD_LOOKUP_FAILED:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                           ExitCode::EC_PERMISSION_DENIED, fi->reason);
      continue;
    case TaskFinalizeCause::STEP_CGROUP_FAILED:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                           ExitCode::EC_CGROUP_ERR, fi->reason);
      continue;
    case TaskFinalizeCause::TIMEOUT:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::ExceedTimeLimit,
                           ExitCode::EC_EXCEED_TIME_LIMIT, fi->reason);
      continue;
    case TaskFinalizeCause::CANCELLED_BY_USER: {
      uint32_t exit_code = fi->raw_exit.has_value()
                               ? raw_exit_code(*fi->raw_exit)
                               : ExitCode::EC_TERMINATED;
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Cancelled,
                           exit_code, fi->reason);
      continue;
    }
    case TaskFinalizeCause::CFORED_DISCONNECTED: {
      uint32_t exit_code = fi->raw_exit.has_value()
                               ? raw_exit_code(*fi->raw_exit)
                               : ExitCode::EC_TERMINATED;
      auto reason = fi->reason;
      if (!reason.has_value()) reason = "Cfored connection failure";
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed, exit_code,
                           std::move(reason));
      continue;
    }
    case TaskFinalizeCause::DAEMON_POD_SHUTDOWN_REQUESTED:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Completed, 0,
                           fi->reason);
      continue;
    case TaskFinalizeCause::INTERACTIVE_NO_PROCESS:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Completed,
                           ExitCode::EC_TERMINATED, fi->reason);
      continue;
    case TaskFinalizeCause::DEADLINE:
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Deadline,
                           ExitCode::EC_REACHED_DEADLINE, fi->reason);
      continue;
    case TaskFinalizeCause::NORMAL:
      break;
    }

    if (!fi->raw_exit.has_value()) {
      ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                           ExitCode::EC_TERMINATED,
                           "Missing raw exit info for natural-exit finalizer");
      continue;
    }

    const auto& raw_exit = fi->raw_exit.value();

    if (!m_step_.IsCalloc()) {
      bool oom_detected = m_step_.EvaluateOomOnExit();

      if (raw_exit.is_terminated_by_signal) {
        uint32_t exit_code = raw_exit_code(raw_exit);
        if (oom_detected) {
          ResolveFinishedTask_(task_id, crane::grpc::JobStatus::OutOfMemory,
                               exit_code, "Detected by oom_kill counter delta");
        } else {
          ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                               exit_code, std::nullopt);
        }
      } else if (raw_exit.value == 0) {
        ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Completed, 0,
                             std::nullopt);
      } else if (oom_detected) {
        ResolveFinishedTask_(task_id, crane::grpc::JobStatus::OutOfMemory,
                             raw_exit.value,
                             "Detected by oom_kill counter delta (no signal)");
      } else {
        ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                             raw_exit.value, std::nullopt);
      }
    } else /* Calloc */ {
      // For a COMPLETING Calloc task with a process running,
      // the end of this process means that this task is done.
      if (raw_exit.is_terminated_by_signal)
        ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Completed,
                             raw_exit_code(raw_exit), std::nullopt);
      else if (raw_exit.value == 0)
        ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Completed, 0,
                             std::nullopt);
      else
        ResolveFinishedTask_(task_id, crane::grpc::JobStatus::Failed,
                             raw_exit.value, std::nullopt);
    }
  }
}

void TaskManager::EvCleanTerminateDaemonPodQueueCb_() {
  DaemonPodTerminateQueueElem elem;
  while (m_daemon_pod_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE("Receive shutdown request for daemon pod #{}.{}.",
                g_config.JobId, g_config.StepId);

    if (!m_step_.IsDaemon() || !m_step_.IsPod()) {
      CRANE_ERROR(
          "Daemon pod shutdown requested for a non-daemon-pod step #{}.{}.",
          g_config.JobId, g_config.StepId);
      continue;
    }

    auto status = m_step_.GetStatus();
    if (status != StepStatus::Running) {
      CRANE_DEBUG(
          "Daemon pod step #{}.{} is no longer running (status: {}), "
          "shutting down supervisor directly.",
          g_config.JobId, g_config.StepId, status);

      if (IsFinishedStepStatus(status)) {
        ShutdownSupervisorAsync(status);
      } else if (status == StepStatus::Completing) {
        const auto& final_status = m_step_.final_termination_status;
        ShutdownSupervisorAsync(final_status.final_status_on_termination,
                                final_status.max_exit_code,
                                final_status.final_reason_on_termination);
      } else {
        ShutdownSupervisorAsync();
      }
      continue;
    }

    auto shutdown = [&]() -> CraneExpectedRich<void> {
      auto* task = m_step_.GetTaskInstance(kCriStepTaskId);
      if (task == nullptr) {
        CraneRichError rich_err;
        rich_err.set_code(CraneErrCode::ERR_NON_EXISTENT);
        rich_err.set_description("Daemon pod task missing during shutdown");
        return std::unexpected(std::move(rich_err));
      }

      auto* final_info = task->GetFinalInfo();
      final_info->cause = TaskFinalizeCause::DAEMON_POD_SHUTDOWN_REQUESTED;
      final_info->reason = "Daemon pod shutdown requested";

      auto err = task->Kill(SIGTERM);
      if (err != CraneErrCode::SUCCESS) {
        CraneRichError rich_err;
        rich_err.set_code(err);
        rich_err.set_description(
            std::format("Failed to stop/remove daemon pod: {} ({})",
                        CraneErrStr(err), static_cast<int>(err)));
        return std::unexpected(std::move(rich_err));
      }

      FinalizeTaskAsync(kCriStepTaskId);
      return {};
    };

    auto ret = shutdown();
    if (!ret.has_value()) {
      const auto& rich_err = ret.error();
      CRANE_ERROR("Failed to shutdown daemon pod #{}.{}: {}", g_config.JobId,
                  g_config.StepId, rich_err.description());
      ShutdownSupervisorAsync(StepStatus::Failed, ExitCode::EC_RPC_ERR,
                              rich_err.description());
    }
  }
}

void TaskManager::EvCleanTerminateStepQueueCb_() {
  StepTerminateQueueElem elem;
  std::vector<StepTerminateQueueElem> not_ready_elems;
  while (m_step_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE("Receive TerminateRunningStep Request for #{}.{}.",
                g_config.JobId, g_config.StepId);

    if (m_step_.IsDaemon()) {
      if (elem.mark_as_orphaned) {
        m_step_.orphaned = true;
        CRANE_INFO("Orphaned daemon step is shutting down directly.");
        SetAllowDaemonShutdown();
        ShutdownSupervisorAsync(StepStatus::Cancelled, 0U, "");
      } else {
        CRANE_TRACE(
            "Terminate request for daemon step is ignored. Use "
            "ShutdownSupervisor to actively stop a daemon pod.");
      }
      continue;
    }

    if (elem.mark_as_orphaned) m_step_.orphaned = true;

    auto status = m_step_.GetStatus();
    if (!elem.mark_as_orphaned && status != StepStatus::Running) {
      not_ready_elems.emplace_back(elem);
      CRANE_DEBUG("Step is not ready to terminate, will check next time.");
      continue;
    }

    int sig = m_step_.IsInteractive() ? SIGHUP : SIGTERM;

    if (elem.mark_as_orphaned) {
      CRANE_DEBUG("Step is terminated as orphaned, shutting down...");
      g_task_mgr->ShutdownSupervisorAsync(StepStatus::Cancelled, 0U, "");
      continue;
    }

    CRANE_TRACE("Terminating all running tasks (orphaned: {}, reason: {})...",
                elem.mark_as_orphaned, static_cast<int>(elem.cause));

    for (task_id_t task_id : m_step_.GetTaskIds()) {
      auto* task = m_step_.GetTaskInstance(task_id);
      task->GetFinalInfo()->cause = elem.cause;

      if (task->GetExecId().has_value()) {
        // Will kill the following types of tasks:
        // 1. Non-interactive Task
        // 2. Interactive Task with a process running

        // If termination request is sent by user, send SIGKILL to ensure that
        // even freezing processes will be terminated immediately.
        if (elem.cause == TaskFinalizeCause::CANCELLED_BY_USER) sig = SIGKILL;
        task->Kill(sig);
      } else if (m_step_.IsInteractive()) {
        // Interactive steps such as calloc may have no local process to kill.
        // Preserve external termination causes (timeout/deadline/cancel) and
        // only collapse natural-exit requests into the synthetic completion
        // cause.
        if (elem.cause == TaskFinalizeCause::NORMAL) {
          FinalizeTaskAsync(task_id, TaskFinalizeCause::INTERACTIVE_NO_PROCESS);
        } else {
          FinalizeTaskAsync(task_id, elem.cause);
        }
      } else {
        CRANE_ASSERT_MSG(false, "Terminating a step without any task");
      }
    }
  }

  m_step_terminate_queue_.enqueue_bulk(
      std::make_move_iterator(not_ready_elems.begin()), not_ready_elems.size());
}

void TaskManager::EvCleanChangeStepTimeConstraintQueueCb_() {
  absl::Time now = absl::Now();

  ChangeStepTimeConstraintQueueElem elem;
  std::vector<ChangeStepTimeConstraintQueueElem> not_ready_elems;
  while (m_step_time_constraint_change_queue_.try_dequeue(elem)) {
    auto status = m_step_.GetStatus();
    if (status == StepStatus::Completing || IsFinishedStepStatus(status)) {
      CRANE_DEBUG(
          "Change time constraint for a finished or completing step, ignored.");
      elem.ok_prom.set_value(CraneErrCode::SUCCESS);
      continue;
    }

    if (status != StepStatus::Running) {
      not_ready_elems.emplace_back(std::move(elem));
      CRANE_DEBUG(
          "Step is not ready to change time constraint will check next time.");
      continue;
    }

    // Delete the old timer.
    DelTerminationTimer_();
    DelSignalTimers_();
    CRANE_TRACE("Delete the old timer.");

    absl::Time start_time =
        absl::FromUnixSeconds(m_step_.GetStep().start_time().seconds());
    absl::Duration const& new_time_limit =
        absl::Seconds(elem.time_limit.value());
    absl::Time deadline_time =
        absl::FromUnixSeconds(elem.deadline_time.value());
    absl::Time time_limit_end_time = start_time + new_time_limit;
    bool deadline_reached = now >= deadline_time;
    bool time_limit_reached = now >= time_limit_end_time;

    if (deadline_reached &&
        (!time_limit_reached || deadline_time <= time_limit_end_time)) {
      StepTerminateQueueElem ev_step_terminate{.cause =
                                                   TaskFinalizeCause::DEADLINE};
      m_step_terminate_queue_.enqueue(ev_step_terminate);
      m_terminate_step_async_handle_->send();

    } else if (time_limit_reached) {
      StepTerminateQueueElem ev_step_terminate{.cause =
                                                   TaskFinalizeCause::TIMEOUT};
      m_step_terminate_queue_.enqueue(ev_step_terminate);
      m_terminate_step_async_handle_->send();

    } else {
      // If the step hasn't timed out, set up a new timer.
      int64_t new_sec;
      int64_t deadline_sec =
          elem.deadline_time.value() - absl::ToUnixSeconds(now);
      int64_t new_time_limit_sec =
          ToInt64Seconds((new_time_limit - (absl::Now() - start_time)));

      if (deadline_sec <= new_time_limit_sec) {
        AddTerminationTimer_(deadline_sec, true);
        new_sec = deadline_sec;
      } else {
        AddTerminationTimer_(new_time_limit_sec, false);
        new_sec = new_time_limit_sec;
      }

      for (const auto& signal : m_step_.GetStep().signals()) {
        if (signal.signal_flag() == crane::grpc::Signal_SignalFlag_BATCH_ONLY &&
            !m_step_.IsPrimary())
          continue;
        if (signal.signal_flag() != crane::grpc::Signal_SignalFlag_BATCH_ONLY &&
            m_step_.IsPrimary())
          continue;
        if (signal.signal_time() >= new_sec) {
          CRANE_TRACE(
              "signal {} time of {}s >= time_limit {}s, ignore this signal.",
              signal.signal_number(), signal.signal_time(), new_sec);
          continue;
        }
        AddSignalTimer_(new_sec - signal.signal_time(), signal.signal_number());
      }
    }

    elem.ok_prom.set_value(CraneErrCode::SUCCESS);
  }

  m_step_time_constraint_change_queue_.enqueue_bulk(
      std::make_move_iterator(not_ready_elems.begin()), not_ready_elems.size());
}

void TaskManager::EvGrpcExecuteStepCb_() {
  CRANE_ASSERT_MSG(!m_step_.IsDaemon(),
                   "Daemon step should never call EvGrpcExecuteTaskCb_");

  ExecuteStepElem elem;
  while (m_grpc_execute_step_queue_.try_dequeue(elem)) {
    // ExecuteTaskAsync is only used for executable steps. Reaching here with an
    // empty task list is a fatal error.
    if (m_step_.task_ids.empty()) {
      CRANE_ERROR("No task ids assigned for executable step #{}.{}",
                  m_step_.job_id, m_step_.step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_INVALID_PARAM);
      continue;
    }

    m_step_.GotNewStatus(StepStatus::Running);

    for (auto task_id : m_step_.task_ids) {
      std::unique_ptr<ITaskInstance> instance;
      if (m_step_.IsPod()) {
        // Pod
        CRANE_ERROR(
            "PodInstance should have been created in EvExecutePodCb_, "
            "unexpected to create it here.");
      } else if (m_step_.IsContainer()) {
        // Container
        instance = std::make_unique<ContainerInstance>(&m_step_, task_id);
      } else {
        // Process
        instance = std::make_unique<ProcInstance>(&m_step_, task_id);
      }
      m_step_.AddTaskInstance(task_id, std::move(instance));
    }

    m_step_.pwd.Init(m_step_.uid);
    if (!m_step_.pwd.Valid()) {
      CRANE_ERROR("Failed to look up password entry for uid {}", m_step_.uid);
      for (auto task_id : m_step_.task_ids)
        g_task_mgr->FinalizeTaskAsync(
            task_id, TaskFinalizeCause::STEP_PWD_LOOKUP_FAILED,
            fmt::format("Failed to look up password entry for uid {}",
                        m_step_.uid));
      elem.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
      continue;
    }

    {
      auto err = m_step_.Prepare();
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("[Step #{}.{}] Failed to prepare step: {}", m_step_.job_id,
                    m_step_.step_id, static_cast<int>(err));
        for (auto task_id : m_step_.task_ids) {
          g_task_mgr->FinalizeTaskAsync(
              task_id, TaskFinalizeCause::STEP_PREPARE_FAILED,
              std::format("Failed to prepare step, code: {}",
                          static_cast<int>(err)));
        }
        elem.ok_prom.set_value(CraneErrCode::ERR_SYSTEM_ERR);
        continue;
      }
    }

    // TODO: We dont need to exec task for a calloc job

    if (!m_step_.IsDaemon()) {
      // Add a timer to limit the execution time of a task.

      int64_t time_limit_sec = m_step_.GetStep().time_limit().seconds();
      int64_t deadline_sec = m_step_.GetStep().deadline_time().seconds() -
                             absl::ToUnixSeconds(absl::Now());
      int64_t sec = std::min(deadline_sec, time_limit_sec);

      std::string log_timer =
          deadline_sec <= time_limit_sec ? "deadline" : "time_limit";
      bool is_deadline = deadline_sec <= time_limit_sec ? true : false;

      AddTerminationTimer_(sec, is_deadline);
      CRANE_TRACE("Add a {} timer of {} seconds", log_timer, sec);

      for (const auto& signal : m_step_.GetStep().signals()) {
        if (signal.signal_flag() == crane::grpc::Signal_SignalFlag_BATCH_ONLY &&
            !m_step_.IsPrimary())
          continue;
        if (signal.signal_flag() != crane::grpc::Signal_SignalFlag_BATCH_ONLY &&
            m_step_.IsPrimary())
          continue;
        if (signal.signal_time() >= sec) {
          CRANE_TRACE(
              "signal time of {}s > time_limit {}s, ignore this signal.",
              signal.signal_time(), sec);
          continue;
        }
        AddSignalTimer_(sec - signal.signal_time(), signal.signal_number());
        CRANE_INFO("Add a signal time of {}s for signal {}",
                   signal.signal_time(), signal.signal_number());
      }
    }
    // TODO: We dont need to exec task for a calloc job
    // Calloc tasks have no scripts to run. Just return.
    if (m_step_.IsCalloc()) {
      CRANE_DEBUG("Calloc step, no script to run.");
      // Keep a synthetic running marker so terminate requests for calloc do not
      // get short-circuited by the "no running task" fast path before the
      // interactive no-process finalization logic can run.
      m_exec_id_task_id_map_[0] = 0;
      elem.ok_prom.set_value(CraneErrCode::SUCCESS);
      continue;
    }
    auto cg_expt = CgroupManager::AllocateAndGetCgroup(
        CgroupManager::CgroupStrByStepId(g_config.JobCgStr, m_step_.step_id,
                                         false),
        m_step_.GetStep().res(), false, Common::CgConstant::kCgMinMem);
    if (!cg_expt.has_value()) {
      CRANE_ERROR("[Step #{}.{}] Failed to allocate cgroup", m_step_.job_id,
                  m_step_.step_id);
      for (auto task_id : m_step_.task_ids) {
        g_task_mgr->FinalizeTaskAsync(task_id,
                                      TaskFinalizeCause::STEP_CGROUP_FAILED,
                                      "Failed to allocate cgroup");
      }
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
      continue;
    }
    m_step_.step_user_cg = std::move(cg_expt.value());

    m_step_.InitOomBaseline();
    // Single threaded here, it is always safe to ask TaskManager to
    // operate (Like terminate due to cfored conn err for crun task) any task.
    CraneErrCode err = CraneErrCode::SUCCESS;
    for (auto task_id : m_step_.task_ids) {
      auto* task = m_step_.GetTaskInstance(task_id);
      // TODO: Maybe we can launch tasks in parallel
      auto task_err = LaunchExecution_(task);
      if (task_err != CraneErrCode::SUCCESS) {
        CRANE_WARN("[task #{}] Failed to launch process.", task_id);
        err = task_err;
      } else {
        auto exec_id = task->GetExecId().value();
        CRANE_INFO("[task #{}] Launched exection, id: {}.", task_id,
                   std::visit([](auto&& arg) { return std::format("{}", arg); },
                              exec_id));
        m_exec_id_task_id_map_[exec_id] = task_id;
      }
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

void TaskManager::EvGrpcCheckStatusCb_() {
  std::promise<StepStatus> elem;
  while (m_grpc_check_status_queue_.try_dequeue(elem)) {
    elem.set_value(m_step_.GetStatus());
  }
}

void TaskManager::EvGrpcMigrateSshProcToCgroupCb_() {
  std::pair<pid_t, std::promise<CraneErrCode>> elem;
  while (m_grpc_migrate_ssh_proc_to_cgroup_queue_.try_dequeue(elem)) {
    auto& pid = elem.first;
    auto& prom = elem.second;
    if (!m_step_.IsDaemon()) {
      CRANE_ERROR("Trying to move pid {} to no daemon step", pid);
      prom.set_value(CraneErrCode::ERR_INVALID_PARAM);
      continue;
    }
    if (!m_step_.step_user_cg) {
      auto cg_expt = CgroupManager::AllocateAndGetCgroup(
          CgroupManager::CgroupStrByStepId(g_config.JobCgStr, m_step_.step_id,
                                           false),
          m_step_.GetStep().res(), false, Common::CgConstant::kCgMinMem);
      if (!cg_expt.has_value()) {
        CRANE_ERROR("[Step #{}.{}] Failed to allocate cgroup", m_step_.job_id,
                    m_step_.step_id);
        prom.set_value(CraneErrCode::ERR_CGROUP);
        continue;
      }
      m_step_.step_user_cg = std::move(cg_expt.value());
    }
    if (m_step_.step_user_cg->MigrateProcIn(pid)) {
      prom.set_value(CraneErrCode::SUCCESS);
    } else {
      prom.set_value(CraneErrCode::ERR_CGROUP);
    }
  }
}

}  // namespace Craned::Supervisor
