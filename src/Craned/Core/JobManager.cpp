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

#include "JobManager.h"

#include <fstream>
#include <pty.h>
#include <sys/wait.h>

#include <filesystem>
#include <set>
#include <utility>

#include "CranedPublicDefs.h"
#include "CtldClient.h"
#include "crane/PluginClient.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"
#include "protos/Supervisor.pb.h"

namespace Craned {

using Common::kStepRequestCheckIntervalMs;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t kPendingSigkillMask = 1ULL << (SIGKILL - 1);
constexpr uint64_t kPendingSigchldMask = 1ULL << (SIGCHLD - 1);
constexpr uint64_t kPendingThawMask = kPendingSigkillMask | kPendingSigchldMask;
constexpr int kUnexpectedSupervisorExitGraceRetryCount = 10;

std::filesystem::path GetV1ControllerPath_(const std::string& cg_path,
                                           Common::CgConstant::Controller ctrl) {
  return Common::CgConstant::kSystemCgPathPrefix /
         Common::CgConstant::GetControllerStringView(ctrl) /
         std::filesystem::relative(cg_path,
                                   Common::CgConstant::kSystemCgPathPrefix);
}

bool IsCgroupFrozenByPath_(const std::string& cg_path) {
  std::error_code ec;
  std::filesystem::path state_file;

  if (CgroupManager::IsCgV2()) {
    state_file = std::filesystem::path(cg_path) / "cgroup.freeze";
  } else {
    state_file =
        GetV1ControllerPath_(cg_path,
                             Common::CgConstant::Controller::FREEZE_CONTROLLER) /
        "freezer.state";
  }

  if (!std::filesystem::exists(state_file, ec)) return false;

  std::ifstream ifs(state_file);
  if (!ifs.is_open()) return false;

  std::string state;
  ifs >> state;
  if (CgroupManager::IsCgV2()) return state == "1";
  return state == "FROZEN" || state == "FREEZING";
}

bool ParseHexMask_(const std::string& line, uint64_t* mask) {
  auto pos = line.find(':');
  if (pos == std::string::npos) return false;

  auto value_pos = line.find_first_not_of(" \t", pos + 1);
  if (value_pos == std::string::npos) return false;

  try {
    *mask = std::stoull(line.substr(value_pos), nullptr, 16);
    return true;
  } catch (...) {
    return false;
  }
}

bool ProcHasPendingSigkill_(pid_t pid) {
  std::ifstream ifs(fmt::format("/proc/{}/status", pid));
  if (!ifs.is_open()) return false;

  std::string line;
  uint64_t sig_pnd = 0;
  uint64_t shd_pnd = 0;
  while (std::getline(ifs, line)) {
    if (line.rfind("SigPnd:", 0) == 0) {
      ParseHexMask_(line, &sig_pnd);
    } else if (line.rfind("ShdPnd:", 0) == 0) {
      ParseHexMask_(line, &shd_pnd);
    }
  }

  return ((sig_pnd | shd_pnd) & kPendingThawMask) != 0;
}

bool FindPendingSigkillInCgroupByPath_(const std::string& cg_path,
                                       pid_t* pending_pid) {
  std::filesystem::path root_path = cg_path;
  if (!CgroupManager::IsCgV2()) {
    root_path = GetV1ControllerPath_(
        cg_path, Common::CgConstant::Controller::MEMORY_CONTROLLER);
  }

  std::error_code ec;
  if (!std::filesystem::exists(root_path, ec)) return false;

  std::set<pid_t> pids;
  auto scan_dir = [&](const std::filesystem::path& dir_path) {
    std::ifstream ifs(dir_path / "cgroup.procs");
    if (!ifs.is_open()) return;

    pid_t pid;
    while (ifs >> pid) pids.insert(pid);
  };

  scan_dir(root_path);

  std::filesystem::recursive_directory_iterator dir_it(root_path, ec);
  if (!ec) {
    for (auto& entry : dir_it) {
      if (!entry.is_directory(ec)) continue;
      scan_dir(entry.path());
    }
  }

  for (pid_t pid : pids) {
    if (ProcHasPendingSigkill_(pid)) {
      *pending_pid = pid;
      return true;
    }
  }

  return false;
}

void ThawFrozenJobsWithPendingSigkill_(
    const std::vector<std::pair<job_id_t, std::string>>& jobs) {
  for (const auto& [job_id, cg_path] : jobs) {
    if (!IsCgroupFrozenByPath_(cg_path)) continue;

    pid_t pending_pid = 0;
    if (!FindPendingSigkillInCgroupByPath_(cg_path, &pending_pid)) continue;

    CRANE_WARN(
        "[Job #{}] Detected pending SIGKILL/SIGCHLD on pid {} while frozen. "
        "Thawing job cgroup.",
        job_id, pending_pid);

    bool root_ok = CgroupManager::ThawCgroupByPath(cg_path);
    bool children_ok = CgroupManager::ThawChildCgroupsByPath(cg_path);
    if (!root_ok || !children_ok) {
      CRANE_ERROR(
          "[Job #{}] Failed to thaw job cgroup {} after pending SIGKILL, "
          "root_ok={}, children_ok={}",
          job_id, cg_path, root_ok, children_ok);
    }
  }
}

bool IsFinishedStepStatus_(StepStatus status) {
  switch (status) {
  case StepStatus::ExceedTimeLimit:
  case StepStatus::OutOfMemory:
  case StepStatus::Cancelled:
  case StepStatus::Failed:
  case StepStatus::Completed:
    return true;

  default:
    return false;
  }
}

std::optional<std::pair<uint32_t, std::string>>
BuildUnexpectedSupervisorExitInfo_(pid_t pid, int status) {
  if (WIFSIGNALED(status)) {
    int signal = WTERMSIG(status);
    return std::make_pair(
        static_cast<uint32_t>(ExitCode::kTerminationSignalBase + signal),
        fmt::format("Supervisor pid {} exited unexpectedly due to signal {}.",
                    pid, signal));
  }

  if (WIFEXITED(status)) {
    int exit_code = WEXITSTATUS(status);
    return std::make_pair(
        static_cast<uint32_t>(exit_code),
        fmt::format("Supervisor pid {} exited unexpectedly with code {}.", pid,
                    exit_code));
  }

  return std::nullopt;
}

}  // namespace

EnvMap JobInD::GetJobEnvMap() {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(job_to_d.res());

  auto& daemon_step_to_d = step_map.at(kDaemonStepId)->step_to_d;
  auto nodelist = daemon_step_to_d.nodelist();
  auto node_id_to_str = [nodelist]() -> std::string {
    auto it = std::ranges::find(nodelist, g_config.Hostname);
    if (it == nodelist.end()) {
      return "-1";
    }
    return std::to_string(static_cast<uint32_t>(it - nodelist.begin()));
  };

  auto alloc_node_num = daemon_step_to_d.nodelist().size();
  auto mem_in_node =
      daemon_step_to_d.res().allocatable_res_in_node().memory_limit_bytes() /
      (static_cast<uint64_t>(1024 * 1024));

  auto cpus_on_node =
      daemon_step_to_d.res().allocatable_res_in_node().cpu_core_limit();
  auto mem_per_cpu = (std::abs(cpus_on_node) > 1e-8)
                         ? (static_cast<double>(mem_in_node) / cpus_on_node)
                         : 0.0;

  env_map.emplace("CRANE_JOB_ACCOUNT", job_to_d.account());

  auto time_limit_dur =
      std::chrono::seconds(daemon_step_to_d.time_limit().seconds()) +
      std::chrono::nanoseconds(daemon_step_to_d.time_limit().nanos());
  env_map.emplace(
      "CRANE_JOB_END_TIME",
      std::to_string((std::chrono::system_clock::now() + time_limit_dur)
                         .time_since_epoch()
                         .count()));
  env_map.emplace("CRANE_JOB_ID", std::to_string(job_id));
  env_map.emplace("CRANE_JOB_NAME", daemon_step_to_d.name());
  env_map.emplace("CRANE_JOB_NODELIST",
                  absl::StrJoin(daemon_step_to_d.nodelist(), ";"));
  env_map.emplace("CRANE_JOB_NUM_NODES",
                  std::to_string(daemon_step_to_d.node_num()));
  env_map.emplace("CRANE_JOB_PARTITION", job_to_d.partition());
  env_map.emplace("CRANE_JOB_QOS", job_to_d.qos());
  env_map.emplace("CRANE_SUBMIT_DIR", daemon_step_to_d.submit_dir());
  env_map.emplace("CRANE_CPUS_PER_TASK",
                  std::format("{:.2f}", daemon_step_to_d.cpus_per_task()));
  env_map.insert_or_assign("CRANE_MEM_PER_NODE",
                           std::format("{}M", mem_in_node));
  env_map.emplace("CRANE_NTASKS_PER_NODE",
                  std::to_string(daemon_step_to_d.ntasks_per_node()));
  env_map.emplace("CRANE_GPUS", std::to_string(daemon_step_to_d.total_gpus()));
  env_map.emplace("CRANE_MEM_PER_CPU", std::format("{:.2f}", mem_per_cpu));
  env_map.emplace("CRANE_NTASKS", std::to_string(daemon_step_to_d.ntasks()));
  env_map.emplace("CRANE_CLUSTER_NAME", g_config.CraneClusterName);
  env_map.emplace("CRANE_CPUS_ON_NODE", std::format("{:.2f}", cpus_on_node));
  env_map.emplace("CRANE_NODEID", node_id_to_str());
  env_map.emplace("CRANE_SUBMIT_HOST", daemon_step_to_d.submit_hostname());

  // SLURM
  if (g_config.EnableSlurmCompatibleEnv) {
    env_map.emplace("SLURM_JOBID", std::to_string(job_id));
    env_map.emplace("SLURM_NODEID", node_id_to_str());
    env_map.emplace("SLURMD_NODENAME", g_config.Hostname);
    env_map.emplace("SLURM_WORKING_DIR", daemon_step_to_d.submit_dir());
    env_map.emplace("SLURM_NODELIST",
                    util::HostNameListToStr(daemon_step_to_d.nodelist()));
  }

  return env_map;
}

JobManager::JobManager() {
  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });

  if (int rc = m_sigchld_handle_->start(SIGCHLD); rc != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle: {}", uv_err_name(rc));
  }

  m_sigint_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigint_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigint_handle_->start(SIGINT); rc != 0) {
    CRANE_ERROR("Failed to start the SIGINT handle: {}", uv_err_name(rc));
  }

  m_sigterm_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigterm_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigterm_handle_->start(SIGTERM); rc != 0) {
    CRANE_ERROR("Failed to start the SIGTERM handle: {}", uv_err_name(rc));
  }

  m_check_supervisor_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_check_supervisor_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        EvCheckSupervisorRunning_();
      });
  m_check_supervisor_timer_handle_->start(std::chrono::milliseconds{0ms},
                                          std::chrono::milliseconds{200ms});

  // gRPC Alloc step Event
  m_grpc_alloc_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_alloc_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcAllocStepsQueueCb_();
      });

  // gRPC Execute step Event
  m_grpc_execute_step_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_execute_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcExecuteStepQueueCb_();
      });

  // Step Status Change Event
  m_step_status_change_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_step_status_change_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanStepStatusChangeQueueCb_();
      });

  m_terminate_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateStepQueueCb_();
      });
  m_terminate_step_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_terminate_step_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        m_terminate_step_async_handle_->send();
      });
  m_terminate_step_timer_handle_->start(
      std::chrono::milliseconds{kStepRequestCheckIntervalMs * 3},
      std::chrono::milliseconds{kStepRequestCheckIntervalMs});

  m_uvw_thread_ = std::thread([this]() {
    util::SetCurrentThreadName("JobMgrLoopThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_is_ending_now_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(50ms);
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in JobManager loop.");
    }
    m_uvw_loop_->run();
  });
}

CraneErrCode JobManager::Recover(
    std::unordered_map<job_id_t, JobInD>&& job_map,
    absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                        std::unique_ptr<StepInstance>>&& step_map) {
  CRANE_INFO("Job allocation [{}] recovered.",
             absl::StrJoin(job_map | std::views::keys, ","));

  for (auto&& job : job_map | std::views::values) {
    job_id_t job_id = job.job_id;
    uid_t uid = job.Uid();
    m_job_map_.Emplace(job_id, std::move(job));

    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<job_id_t>({job_id}));
    }
  }

  for (auto&& elem : step_map) {
    job_id_t job_id = elem.first.first;
    step_id_t step_id = elem.first.second;
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    CRANE_ASSERT(job);
    absl::MutexLock lk(job->step_map_mtx.get());
    job->step_map.emplace(step_id, std::move(elem.second));
    CRANE_TRACE("[Step #{}.{}] was recovered.", job_id, step_id);
  }
  std::vector<job_id_t> invalid_jobs;
  for (auto& [job_id, job] : *m_job_map_.GetMapExclusivePtr()) {
    if (job.RawPtr()->step_map.empty()) {
      CRANE_WARN("Job #{} has no step after recovery.", job_id);
      invalid_jobs.push_back(job_id);
    }
  }
  for (auto job_id : invalid_jobs) {
    m_job_map_.Erase(job_id);
  }
  return CraneErrCode::SUCCESS;
}

JobManager::~JobManager() {
  CRANE_DEBUG("JobManager is being destroyed.");
  m_is_ending_now_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

bool JobManager::AllocJobs(std::vector<JobInD>&& jobs) {
  // NOTE: Cgroup is lazily created when the first step is arrived.
  // Only maintain the job info here.
  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();

  for (auto& job : jobs) {
    job_id_t job_id = job.job_id;
    uid_t uid = job.Uid();

    job_map_ptr->emplace(job_id, std::move(job));
    if (uid_map_ptr->contains(uid)) {
      uid_map_ptr->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map_ptr->emplace(uid, absl::flat_hash_set<job_id_t>({job_id}));
    }
  }

  return true;
}

bool JobManager::FreeJobs(std::set<job_id_t>&& job_ids) {
  std::vector<JobInD> jobs_to_free;
  std::vector<StepInstance*> steps_to_free;

  for (job_id_t job_id : job_ids) {
    auto job = FreeJobInfo_(job_id);
    if (!job.has_value()) {
      CRANE_INFO(
          "Try to free non-existent job #{}, sending a status change as "
          "daemon step",
          job_id);
      // Free job implicitly free its daemon step and got a status change from
      // daemon step, send a status change to ctld if not found.
      g_ctld_client->StepStatusChangeAsync(StepStatusChangeQueueElem{
          .job_id = job_id,
          .step_id = kDaemonStepId,
          .new_status = StepStatus::Cancelled,
          .reason = "Job not found on craned during free request",
          .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
      continue;
    }

    for (auto& step : job->step_map | std::views::values) {
      steps_to_free.emplace_back(step.get());
    }

    if (job->step_map.size() != 1)
      CRANE_DEBUG("Job #{} to free has more than one step.", job_id);

    jobs_to_free.emplace_back(std::move(job.value()));
  }

  CRANE_DEBUG(
      "Free jobs [{}], steps [{}].",
      absl::StrJoin(jobs_to_free | std::views::transform([](const auto& job) {
                      return job.job_id;
                    }) | std::views::common,
                    ","),
      absl::StrJoin(steps_to_free | std::views::transform([](const auto* step) {
                      return step->StepIdString();
                    }) | std::views::common,
                    ","));

  CleanUpJobAndStepsAsync(std::move(jobs_to_free), std::move(steps_to_free));

  return true;
}

void JobManager::AllocSteps(std::vector<StepToD>&& steps) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    CRANE_TRACE("JobManager is ending now, ignoring the request.");
    return;
  }

  CRANE_TRACE("Allocating step [{}].",
              absl::StrJoin(steps | std::views::transform(GetStepIdStr), ","));

  for (auto& step : steps) {
    auto job_ptr = m_job_map_.GetValueExclusivePtr(step.job_id());
    if (!job_ptr) {
      CRANE_WARN("Try to allocate step for nonexistent job#{}, ignoring it.",
                 step.job_id());
    } else {
      // Simply wrap the job structure within an Execution structure and
      // pass it to the event loop. The cgroup field of this job is initialized
      // in the corresponding handler.
      auto step_inst = std::make_unique<StepInstance>(step);
      step_inst->step_to_d = std::move(step);
      EvQueueAllocateStepElem elem{.step_inst = std::move(step_inst),
                                   .need_run_prolog = false};
      // GetJobEnvMap must step_map has the daemon step.
      if (elem.step_inst->step_id != kDaemonStepId) {
        elem.need_run_prolog = !job_ptr->is_prolog_run;
        elem.job_env = job_ptr->GetJobEnvMap();
        job_ptr->is_prolog_run = true;
      }
      m_grpc_alloc_step_queue_.enqueue(std::move(elem));
    }
  }
  m_grpc_alloc_step_async_handle_->send();
}

void JobManager::FreeSteps(
    std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps) {
  std::vector<StepInstance*> steps_to_free;
  for (auto& [job_id, step_ids] : steps) {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job) {
      CRANE_WARN("Try to free step [{}] for nonexistent job #{}.",
                 absl::StrJoin(step_ids, ","), job_id);
      continue;
    }
    for (step_id_t step_id : step_ids) {
      absl::MutexLock lk(job->step_map_mtx.get());
      if (!job->step_map.contains(step_id)) {
        CRANE_WARN("[Step #{}.{}] Try to free nonexistent step, ignoring it.",
                   job_id, step_id);
        continue;
      }
      auto& step = job->step_map.at(step_id);
      if (step->IsDaemonStep()) {
        CRANE_ERROR("Not allowed to free daemon step #{}.{}", job_id, step_id);
        continue;
      }
      steps_to_free.emplace_back(job->step_map.at(step_id).release());
      job->step_map.erase(step_id);
    }
  }
  CleanUpJobAndStepsAsync({}, std::move(steps_to_free));
}

void JobManager::RecordUnexpectedSupervisorExit_(pid_t pid, int status) {
  auto exit_info = BuildUnexpectedSupervisorExitInfo_(pid, status);
  if (!exit_info.has_value()) {
    CRANE_TRACE("Ignoring non-terminal child status {} for pid {}", status,
                pid);
    return;
  }

  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  for (auto& [job_id, job] : *job_map_ptr) {
    auto* job_ptr = job.RawPtr();
    absl::MutexLock step_lock(job_ptr->step_map_mtx.get());
    for (const auto& [step_id, step] : job_ptr->step_map) {
      if (step == nullptr || step->supv_pid != pid) continue;

      if (step->err_before_supv_start || step->status == StepStatus::Completing ||
          IsFinishedStepStatus_(step->status)) {
        CRANE_TRACE(
            "[Step #{}.{}] Supervisor pid {} exited after step reached status "
            "{}, ignoring.",
            job_id, step_id, pid, step->status);
        return;
      }

      {
        absl::MutexLock unexpected_lock(&m_unexpected_supervisor_exit_mtx_);
        m_unexpected_supervisor_exit_map_[{job_id, step_id}] = {
            .exit_code = exit_info->first,
            .reason = exit_info->second,
            .retry_count = 0};
      }

      CRANE_WARN(
          "[Step #{}.{}] Supervisor pid {} exited unexpectedly; waiting "
          "briefly for an in-flight final status update before forcing "
          "Failed.",
          job_id, step_id, pid);
      return;
    }
  }

  CRANE_TRACE("Child pid {} exited but is not a tracked supervisor.", pid);
}

void JobManager::HandleUnexpectedSupervisorExits_() {
  using StepKey = std::pair<job_id_t, step_id_t>;

  struct PendingFailure {
    job_id_t job_id;
    step_id_t step_id;
    uint32_t exit_code;
    std::string reason;
  };

  struct PendingSnapshot {
    StepKey key;
    uint32_t exit_code;
    std::string reason;
    int retry_count;
  };

  std::vector<PendingSnapshot> pending;
  {
    absl::MutexLock unexpected_lock(&m_unexpected_supervisor_exit_mtx_);
    pending.reserve(m_unexpected_supervisor_exit_map_.size());
    for (const auto& [key, info] : m_unexpected_supervisor_exit_map_) {
      pending.emplace_back(PendingSnapshot{
          .key = key,
          .exit_code = info.exit_code,
          .reason = info.reason,
          .retry_count = info.retry_count});
    }
  }

  std::vector<PendingFailure> failures;
  std::vector<StepKey> resolved_keys;
  std::vector<StepKey> retry_keys;

  for (const auto& entry : pending) {
    const auto& [job_id, step_id] = entry.key;
    auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job_ptr) {
      resolved_keys.emplace_back(entry.key);
      continue;
    }

    bool resolved = false;
    {
      absl::MutexLock step_lock(job_ptr->step_map_mtx.get());
      auto step_it = job_ptr->step_map.find(step_id);
      if (step_it == job_ptr->step_map.end() || step_it->second == nullptr) {
        resolved = true;
      } else {
        auto* step = step_it->second.get();
        resolved = step->status == StepStatus::Completing ||
                   IsFinishedStepStatus_(step->status);
      }
    }

    if (resolved) {
      resolved_keys.emplace_back(entry.key);
      continue;
    }

    if (entry.retry_count + 1 < kUnexpectedSupervisorExitGraceRetryCount) {
      retry_keys.emplace_back(entry.key);
      continue;
    }

    failures.emplace_back(PendingFailure{
        .job_id = job_id,
        .step_id = step_id,
        .exit_code = entry.exit_code,
        .reason = entry.reason});
    resolved_keys.emplace_back(entry.key);
  }

  {
    absl::MutexLock unexpected_lock(&m_unexpected_supervisor_exit_mtx_);
    for (const auto& key : retry_keys) {
      auto it = m_unexpected_supervisor_exit_map_.find(key);
      if (it != m_unexpected_supervisor_exit_map_.end()) ++it->second.retry_count;
    }
    for (const auto& key : resolved_keys) {
      m_unexpected_supervisor_exit_map_.erase(key);
    }
  }

  for (auto& failure : failures) {
    CRANE_WARN(
        "[Step #{}.{}] Supervisor exited without reporting a final step "
        "status. Marking step Failed.",
        failure.job_id, failure.step_id);
    ActivateStepStatusChangeAsync_(
        failure.job_id, failure.step_id, StepStatus::Failed,
        failure.exit_code, std::move(failure.reason),
        google::protobuf::util::TimeUtil::GetCurrentTime());
  }
}

bool JobManager::EvCheckSupervisorRunning_() {
  std::vector<std::pair<job_id_t, std::string>> jobs_to_check_pending_sigkill;
  std::vector<JobInD> jobs_to_clean;
  // Step is completing, get ownership here.
  std::vector<std::unique_ptr<StepInstance>> steps_to_clean;
  {
    {
      auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
      jobs_to_check_pending_sigkill.reserve(job_map_ptr->size());
      for (auto& [job_id, job] : *job_map_ptr) {
        auto* job_ptr = job.RawPtr();
        if (job_ptr->cgroup) {
          jobs_to_check_pending_sigkill.emplace_back(
              job_id, job_ptr->cgroup->CgroupPath().string());
        }
      }
    }

    ThawFrozenJobsWithPendingSigkill_(jobs_to_check_pending_sigkill);
    HandleUnexpectedSupervisorExits_();

    std::vector<StepInstance*> exit_steps;
    absl::MutexLock lk(&m_free_job_step_mtx_);
    for (auto& [step, retry_count] : m_completing_step_retry_map_) {
      if (step->err_before_supv_start) {
        exit_steps.push_back(step);
        continue;
      }
      job_id_t job_id = step->job_id;
      step_id_t step_id = step->step_id;
      auto exists = kill(step->supv_pid, 0) == 0;

      if (exists) {
        retry_count++;
      }
      if (retry_count >= kMaxSupervisorCheckRetryCount) {
        CRANE_WARN(
            "[Step #{}.{}] Supervisor is still running after {} checks, will "
            "clean up now!",
            job_id, step_id, kMaxSupervisorCheckRetryCount);
        g_ctld_client->StepStatusChangeAsync(StepStatusChangeQueueElem{
            .job_id = job_id,
            .step_id = step_id,
            .new_status = StepStatus::Failed,
            .exit_code = ExitCode::EC_RPC_ERR,
            .reason = "Supervisor not responding during step completion",
            .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
      } else {
        if (exists) continue;
      }
      exit_steps.push_back(step);
    }
    for (auto* step : exit_steps) {
      m_completing_step_retry_map_.erase(step);
      if (!m_completing_job_.contains(step->job_id)) {
        // Only the step is completing
        steps_to_clean.emplace_back(step);
        continue;
      }
      auto& job = m_completing_job_.at(step->job_id);
      if (!job.step_map.contains(step->step_id)) {
        // The step is considered completing before the job considered
        // completing, already removed form step map.
        steps_to_clean.emplace_back(step);
        continue;
      }
      steps_to_clean.emplace_back(job.step_map.at(step->step_id).release());
      job.step_map.erase(step->step_id);
      if (job.step_map.empty()) {
        jobs_to_clean.emplace_back(std::move(job));
        m_completing_job_.erase(step->job_id);
      }
    }
  }

  if (!steps_to_clean.empty()) {
    CRANE_TRACE("Supervisor for Step [{}] found to be exited",
                absl::StrJoin(steps_to_clean |
                                  std::views::transform(
                                      [](std::unique_ptr<StepInstance>& step) {
                                        return step->StepIdString();
                                      }),
                              ","));
    FreeStepAllocation_(std::move(steps_to_clean));
    if (!jobs_to_clean.empty()) FreeJobAllocation_(std::move(jobs_to_clean));
  }

  return m_completing_step_retry_map_.empty();
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
void JobManager::EvSigchldCb_() {
  std::unique_lock<std::mutex> lock(m_fork_reap_mu_);
  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      if (!m_exit_watcher_.TryDeliver(pid, status)) {
        RecordUnexpectedSupervisorExit_(pid, status);
      }
    } else if (pid == 0) {
      // There's no child that needs reaping.
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void JobManager::EvSigintCb_() {
  CRANE_TRACE("Triggering exit event...");
  if (m_sigint_cb_) m_sigint_cb_();
  m_is_ending_now_ = true;
}

void JobManager::EvCleanGrpcAllocStepsQueueCb_() {
  EvQueueAllocateStepElem elem;

  while (m_grpc_alloc_step_queue_.try_dequeue(elem)) {
    std::unique_ptr step_inst = std::move(elem.step_inst);

    if (!m_job_map_.Contains(step_inst->job_id)) {
      CRANE_ERROR("[Step #{}.{}] Failed to find a job allocation for the step",
                  step_inst->job_id, step_inst->step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
      continue;
    }
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);

    g_thread_pool->detach_task([this, execution = step_inst.release(),
                                need_run_prolog = elem.need_run_prolog,
                                job_env = elem.job_env] {
      if (need_run_prolog) {
        if (!RunPrologWhenAllocSteps_(execution->job_id, execution->step_id,
                                      job_env))
          return;
      }

      LaunchStepMt_(std::unique_ptr<StepInstance>(execution));
    });
  }
}

void JobManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void JobManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErrCode JobManager::ExecuteStepAsync(
    std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    return CraneErrCode::ERR_SHUTTING_DOWN;
  }

  for (auto& [job_id, step_ids] : steps) {
    for (auto step_id : step_ids) {
      auto job = m_job_map_[job_id];
      if (!job) {
        CRANE_DEBUG("[Step #{}.{}] without job allocation. Ignoring it.",
                    job_id, step_id);
        return CraneErrCode::ERR_CGROUP;
      }

      EvQueueExecuteStepElem elem{.job_id = job_id,
                                  .step_id = step_id,
                                  .ok_prom = std::promise<CraneErrCode>{}};

      m_grpc_execute_step_queue_.enqueue(std::move(elem));
    }
  }
  m_grpc_execute_step_async_handle_->send();

  return CraneErrCode::SUCCESS;
}

CraneExpected<void> JobManager::ChangeStepTimeConstraint(
    job_id_t job_id, step_id_t step_id,
    std::optional<int64_t> time_limit_seconds,
    std::optional<int64_t> deadline_time) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id, step_id);
    return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
  }
  absl::MutexLock lock(job->step_map_mtx.get());
  auto step_it = job->step_map.find(step_id);
  if (step_it == job->step_map.end()) {
    CRANE_ERROR("[Step #{}.{}] Failed to find step allocation", job_id,
                step_id);
    return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
  }
  auto& stub = step_it->second->supervisor_stub;
  if (!stub) {
    CRANE_ERROR(
        "[Step #{}.{}] Supervisor stub is null when changing time constraint",
        job_id, step_id);
    StepStatusChangeAsync(
        job_id, step_id, StepStatus::Failed, ExitCode::EC_RPC_ERR,
        "Supervisor stub is null when changing time constraint",
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return std::unexpected{CraneErrCode::ERR_RPC_FAILURE};
  }
  auto err =
      stub->ChangeStepTimeConstraint(time_limit_seconds, deadline_time);
  int64_t sec = time_limit_seconds.value_or(deadline_time.value_or(0));
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR(
        "[Step #{}.{}] Failed to change step time constraint to {} seconds via "
        "supervisor RPC, err={}",
        job_id, step_id, sec, static_cast<int>(err));
    return std::unexpected{err};
  }
  return {};
}

CraneExpected<void> JobManager::ChangeAllStepsTimelimit(
    job_id_t job_id, int64_t new_timelimit_sec) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_ERROR("[Job #{}] Failed to find job allocation", job_id);
    return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
  }

  absl::MutexLock lock(job->step_map_mtx.get());

  // For resume operations, supervisor_stub being unavailable should not be
  // a fatal error. After thaw, processes will naturally resume and the time
  // limit will take effect on the next check.
  for (auto& [step_id, step] : job->step_map) {
    if (step->supervisor_stub) {
      auto err =
          step->supervisor_stub->ChangeStepTimeConstraint(new_timelimit_sec, std::nullopt);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_WARN(
            "[Step #{}.{}] Failed to change step timelimit to {} seconds, "
            "but continuing (step may have completed or supervisor unavailable)",
            job_id, step_id, new_timelimit_sec);
      }
    } else {
      CRANE_WARN(
          "[Step #{}.{}] Supervisor stub is null when changing timelimit, "
          "but continuing (step may have completed or be in transition)",
          job_id, step_id);
    }
  }

  // Even if some steps failed, return success as this should not block
  // resume operations
  return {};
}


CraneExpected<EnvMap> JobManager::QuerySshStepEnvVariables(job_id_t job_id,
                                                           step_id_t step_id) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id, step_id);
    return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
  }
  absl::MutexLock lock(job->step_map_mtx.get());
  auto step_it = job->step_map.find(step_id);
  if (step_it == job->step_map.end()) {
    CRANE_ERROR("[Step #{}.{}] Failed to find step allocation", job_id,
                step_id);
    return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
  }
  auto& stub = step_it->second->supervisor_stub;
  if (!stub) {
    CRANE_ERROR("[Step #{}.{}] Supervisor stub is null when query env", job_id,
                step_id);
    StepStatusChangeAsync(job_id, step_id, StepStatus::Failed,
                          ExitCode::EC_RPC_ERR,
                          "Supervisor stub is null when query env",
                          google::protobuf::util::TimeUtil::GetCurrentTime());
    return std::unexpected{CraneErrCode::ERR_RPC_FAILURE};
  }
  return stub->QueryStepEnv();
}

void JobManager::EvCleanGrpcExecuteStepQueueCb_() {
  EvQueueExecuteStepElem elem;

  while (m_grpc_execute_step_queue_.try_dequeue(elem)) {
    // Once ExecuteStep RPC is processed, the Execution goes into m_job_map_.
    auto& [job_id, step_id, ok_prom] = elem;
    auto job = m_job_map_.GetValueExclusivePtr(job_id);

    if (!job) {
      CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id,
                  step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_NON_EXISTENT);
      continue;
    }
    absl::MutexLock lk(job->step_map_mtx.get());
    auto step_it = job->step_map.find(step_id);
    if (step_it == job->step_map.end()) {
      CRANE_ERROR("[Step #{}.{}] Failed to find step allocation", job_id,
                  step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_NON_EXISTENT);
      continue;
    }
    step_it->second->ExecuteStepAsync();
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);
  }
}

std::optional<JobInD> JobManager::FreeJobInfo_(job_id_t job_id) {
  auto map_ptr = m_job_map_.GetMapExclusivePtr();
  auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
  return FreeJobInfoNoLock_(job_id, map_ptr, uid_map);
}

std::optional<JobInD> JobManager::FreeJobInfoNoLock_(
    job_id_t job_id, JobMap::MapExclusivePtr& job_map_ptr,
    UidMap::MapExclusivePtr& uid_map_ptr) {
  std::optional<JobInD> job_opt{std::nullopt};
  if (!job_map_ptr->contains(job_id)) {
    return job_opt;
  }
  job_opt = std::move(*job_map_ptr->at(job_id).RawPtr());
  job_map_ptr->erase(job_id);
  bool erase{false};
  {
    auto& value = uid_map_ptr->at(job_opt.value().Uid());
    value.RawPtr()->erase(job_id);
    erase = value.RawPtr()->empty();
  }
  if (erase) {
    uid_map_ptr->erase(job_opt.value().Uid());
  }

  return job_opt;
}

bool JobManager::FreeJobAllocation_(std::vector<JobInD>&& jobs) {
  CRANE_DEBUG("Freeing job [{}] allocation.",
              absl::StrJoin(jobs | std::views::transform([](auto& job) {
                              return job.job_id;
                            }) | std::views::common,
                            ","));
  std::unordered_map<job_id_t, CgroupInterface*> job_cg_map;
  std::vector<uid_t> uid_vec;

  for (auto& job : jobs) {
    job_cg_map[job.job_id] = job.cgroup.release();
  }

  for (auto [job_id, cgroup] : job_cg_map) {
    if (cgroup == nullptr) continue;

    if (g_config.Plugin.Enabled && g_plugin_client) {
      g_plugin_client->DestroyCgroupHookAsync(job_id, cgroup->CgroupName());
    }

    g_thread_pool->detach_task([cgroup]() {
      int cnt = 0;

      while (true) {
        if (cgroup->Empty()) break;

        if (cnt >= 5) {
          CRANE_ERROR(
              "Couldn't kill the processes in cgroup {} after {} times. "
              "Skipping it.",
              cgroup->CgroupName(), cnt);
          break;
        }

        cgroup->KillAllProcesses(SIGKILL);
        ++cnt;
        std::this_thread::sleep_for(std::chrono::milliseconds{100ms});
      }
      cgroup->Destroy();

      delete cgroup;
    });
  }
  return true;
}

void JobManager::FreeStepAllocation_(
    std::vector<std::unique_ptr<StepInstance>>&& steps) {
  for (auto& step : steps) {
    step->CleanUp();
    step.reset();
  }
  // TODO: delete cgroup
}

bool JobManager::RunPrologWhenAllocSteps_(job_id_t job_id, step_id_t step_id,
                                          const EnvMap& job_env) {
  // force the script to be executed at step allocation
  if (!g_config.JobLifecycleHook.Prologs.empty() &&
      !(g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob)) {
    bool script_lock = false;
    if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::Serial) {
      m_prolog_serial_mutex_.Lock();
      script_lock = true;
    }
    CRANE_TRACE("[Step #{}.{}] Running Prolog In AllocSteps.", job_id, step_id);
    RunPrologEpilogArgs args{
        .scripts = g_config.JobLifecycleHook.Prologs,
        .envs = job_env,
        .timeout_sec = g_config.JobLifecycleHook.PrologTimeout,
        .run_uid = 0,
        .run_gid = 0,
        .output_size = g_config.JobLifecycleHook.MaxOutputSize};

    args.fork_and_watch_fn = [this](std::function<pid_t()> do_fork)
        -> std::optional<std::pair<pid_t, std::future<int>>> {
      std::unique_lock<std::mutex> lock(m_fork_reap_mu_);
      pid_t pid = do_fork();
      if (pid < 0) return std::nullopt;
      if (pid == 0) return std::make_pair(pid, std::future<int>{});
      return std::make_pair(pid, m_exit_watcher_.Watch(pid));
    };

    if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::Contain) {
      args.at_child_setup_cb = [this, job_id](pid_t pid) {
        return MigrateProcToCgroupOfJob(pid, job_id);
      };
    }

    if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
      args.timeout_sec = g_config.JobLifecycleHook.PrologEpilogTimeout;

    auto result = util::os::RunPrologOrEpiLog(args);
    if (script_lock) m_prolog_serial_mutex_.Unlock();
    if (!result) {
      auto status = result.error();
      CRANE_DEBUG("[Step #{}.{}]: Prolog in AllocSteps failed status={}:{}",
                  job_id, step_id, status.exit_code, status.signal_num);
      g_ctld_client->UpdateNodeDrainState(true, "Prolog failed");
      ActivateStepStatusChangeAsync_(
          job_id, step_id, crane::grpc::JobStatus::Failed,
          ExitCode::EC_PROLOG_ERR,
          fmt::format("Failed to run prolog for job#{} ", job_id),
          google::protobuf::util::TimeUtil::GetCurrentTime());
      return false;
    }
    CRANE_DEBUG("[Step #{}.{}]: Prolog in AllocSteps success", job_id, step_id);
  }
  return true;
}

void JobManager::LaunchStepMt_(std::unique_ptr<StepInstance> step) {
  // This function runs in a multi-threading manner. Take care of thread
  // safety. JobInstance will not be free during this function. Take care of
  // data race for job instance.

  job_id_t job_id = step->job_id;
  step_id_t step_id = step->step_id;
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id, step_id);
    ActivateStepStatusChangeAsync_(
        job_id, step_id, crane::grpc::JobStatus::Failed,
        ExitCode::EC_CGROUP_ERR,
        fmt::format("Failed to get the allocation for job#{} ", job_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }
  auto* job = job_ptr.get();

  // Check if the step is acceptable.
  // We keep this container check here in case we support enabling/disabling
  // container functionality on parts of nodes in the future.
  if (step->IsContainer() && !g_config.Container.Enabled) {
    CRANE_ERROR("Container support is disabled but job #{} requires it.",
                job_id);
    ActivateStepStatusChangeAsync_(
        job_id, step_id, crane::grpc::JobStatus::Failed,
        ExitCode::EC_SPAWN_FAILED, "Container is not enabled in this craned.",
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }

  // Create job cgroup first.
  // TODO: Create step cgroup later.
  if (!job->cgroup) {
    auto cg_expt = CgroupManager::AllocateAndGetCgroup(
        CgroupManager::CgroupStrByJobId(job->job_id), job->job_to_d.res(),
        false, Common::CgConstant::kCgMinMem);
    if (cg_expt.has_value()) {
      job->cgroup = std::move(cg_expt.value());
    } else {
      CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
      ActivateStepStatusChangeAsync_(
          job_id, step_id, crane::grpc::JobStatus::Failed,
          ExitCode::EC_CGROUP_ERR,
          fmt::format("Failed to get cgroup for job#{} ", job_id),
          google::protobuf::util::TimeUtil::GetCurrentTime());
      return;
    }
  }

  auto* step_ptr = step.get();
  {
    absl::MutexLock lk(job->step_map_mtx.get());
    job->step_map.emplace(step->step_id, std::move(step));
  }

  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this job, and
  // we should send StepStatusChange manually.
  CraneErrCode err = step_ptr->Prepare();
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR("[Step #{}.{}] Failed to prepare.", job_id, step_id);
    step_ptr->err_before_supv_start = true;
    ActivateStepStatusChangeAsync_(
        job_id, step_id, crane::grpc::JobStatus::Failed,
        ExitCode::EC_CGROUP_ERR,
        fmt::format("Cannot create cgroup for the instance of step {}.{}",
                    job_id, step_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }
  err = step_ptr->SpawnSupervisor(job->GetJobEnvMap());
  if (err != CraneErrCode::SUCCESS) {
    step_ptr->err_before_supv_start = true;
    ActivateStepStatusChangeAsync_(
        job_id, step_id, crane::grpc::JobStatus::Failed,
        ExitCode::EC_SPAWN_FAILED,
        fmt::format("Cannot spawn a new process inside the instance of job #{}",
                    job_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
  } else {
    // kOk means that SpawnSupervisor_ has successfully forked a child
    // process.
    CRANE_TRACE("[Step #{}.{}] Supervisor spawned successfully.", job_id,
                step_id);
  }
}

void JobManager::EvCleanStepStatusChangeQueueCb_() {
  StepStatusChangeQueueElem status_change;
  while (m_step_status_change_queue_.try_dequeue(status_change)) {
    {
      auto job_ptr = m_job_map_.GetValueExclusivePtr(status_change.job_id);
      if (job_ptr) {
        absl::MutexLock lk(job_ptr->step_map_mtx.get());
        if (auto step_it = job_ptr->step_map.find(status_change.step_id);
            step_it != job_ptr->step_map.end()) {
          step_it->second->GotNewStatus(status_change.new_status);
          step_it->second->exit_code = status_change.exit_code;
          step_it->second->end_time = status_change.timestamp;
        }
      }
    }

    CRANE_TRACE("[Step #{}.{}] StepStatusChange status: {}.",
                status_change.job_id, status_change.step_id,
                status_change.new_status);
    g_ctld_client->StepStatusChangeAsync(std::move(status_change));
  }
}

void JobManager::ActivateStepStatusChangeAsync_(
    job_id_t job_id, step_id_t step_id, crane::grpc::JobStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason,
    const google::protobuf::Timestamp& timestamp) {
  StepStatusChangeQueueElem status_change{.job_id = job_id,
                                          .step_id = step_id,
                                          .new_status = new_status,
                                          .exit_code = exit_code,
                                          .timestamp = std::move(timestamp)};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_step_status_change_queue_.enqueue(std::move(status_change));
  m_step_status_change_async_handle_->send();
}

/**
 * @brief Move process on craned to specify job's cgroup. Called from
 * CranedServer
 * @param pid process pid
 * @param job_id
 * @return True if job and cgroup exists
 */
bool JobManager::MigrateProcToCgroupOfJob(pid_t pid, job_id_t job_id) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_DEBUG("Job #{} does not exist when querying its cgroup.", job_id);
    return false;
  }
  absl::MutexLock lk(job->step_map_mtx.get());
  auto daemon_step_it = job->step_map.find(kDaemonStepId);
  if (daemon_step_it == job->step_map.end()) {
    CRANE_DEBUG(
        "[Step #{}.{}] Daemon step not found when migrating pid {} to "
        "cgroup of job#{}.",
        job_id, kDaemonStepId, pid, job_id);
    return false;
  }
  auto& daemon_step = daemon_step_it->second;
  auto stub = daemon_step->supervisor_stub;
  if (!stub) {
    CRANE_ERROR(
        "[Job #{}] Daemon step sSupervisor stub is null when migrating pid {} "
        "to "
        "cgroup of job#{}.",
        job_id, kDaemonStepId, pid, job_id);
    return false;
  }
  auto err = stub->MigrateSshProcToCg(pid);
  if (err == CraneErrCode::SUCCESS) {
    return true;
  }
  CRANE_ERROR("[Job #{}] Failed to migrate pid {} to cgroup of job.", job_id,
              pid);
  return false;
}

std::map<job_id_t, std::map<step_id_t, StepStatus>>
JobManager::GetAllocatedJobSteps() {
  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  std::map<job_id_t, std::map<step_id_t, StepStatus>> job_steps;
  for (auto& [job_id, job] : *job_map_ptr) {
    auto job_ptr = job.GetExclusivePtr();
    absl::MutexLock lk(job_ptr->step_map_mtx.get());
    std::map<step_id_t, StepStatus> step_status_map;
    for (auto& [step_id, step] : job_ptr->step_map) {
      step_status_map[step_id] = step->status;
    }
    job_steps[job_id] = std::move(step_status_map);
  }
  return job_steps;
}

std::vector<step_id_t> JobManager::GetAllocatedJobSteps(job_id_t job_id) {
  std::vector<step_id_t> result;
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) return result;
  absl::MutexLock lk(job_ptr->step_map_mtx.get());
  for (const auto& [step_id, step] : job_ptr->step_map) {
    result.push_back(step_id);
  }
  return result;
}

CraneErrCode JobManager::SuspendJobByCgroup(job_id_t job_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_WARN("[Job #{}] Failed to suspend: job allocation not found.",
               job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  if (!job_ptr->cgroup) {
    CRANE_WARN("[Job #{}] Failed to suspend: job cgroup not found.", job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  // Cancel the supervisor termination timer for the primary step BEFORE
  // freezing the cgroup. The supervisor process lives inside the job's cgroup,
  // so it must be contacted while still running. This prevents the timer from
  // firing while the cgroup is frozen; otherwise pending signals
  // (SIGTERM/SIGKILL) would be delivered when the cgroup is thawed on resume,
  // killing the process before the time constraint can be updated.
  // The resume flow will set a new timer with the updated time limit via
  // ChangeStepTimeConstraint.
  {
    absl::MutexLock lock(job_ptr->step_map_mtx.get());
    auto step_it = job_ptr->step_map.find(kPrimaryStepId);
    if (step_it != job_ptr->step_map.end() &&
        step_it->second->supervisor_stub) {
      auto err = step_it->second->supervisor_stub->ChangeStepTimeConstraint(
          kJobMaxTimeStampSec, std::nullopt);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_WARN(
            "[Step #{}.{}] Failed to cancel timer during suspend, "
            "but continuing (step may have completed)",
            job_id, kPrimaryStepId);
      }
    }
  }

  auto job_cg_abs_path = job_ptr->cgroup->CgroupPath().string();
  bool root_ok = CgroupManager::FreezeCgroupByPath(job_cg_abs_path);
  bool children_ok = CgroupManager::FreezeChildCgroupsByPath(job_cg_abs_path);
  if (!root_ok || !children_ok) {
    CRANE_ERROR(
        "[Job #{}] Failed to freeze job cgroup {}, root_ok={}, children_ok={}",
        job_id, job_cg_abs_path, root_ok, children_ok);
    return CraneErrCode::ERR_CGROUP;
  }

  CRANE_INFO("[Job #{}] Frozen job cgroup: {}", job_id, job_cg_abs_path);

  return CraneErrCode::SUCCESS;
}

CraneErrCode JobManager::ResumeJobByCgroup(job_id_t job_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_WARN("[Job #{}] Failed to resume: job allocation not found.",
               job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  if (!job_ptr->cgroup) {
    CRANE_WARN("[Job #{}] Failed to resume: job cgroup not found.", job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  auto job_cg_abs_path = job_ptr->cgroup->CgroupPath().string();
  bool root_ok = CgroupManager::ThawCgroupByPath(job_cg_abs_path);
  bool children_ok = CgroupManager::ThawChildCgroupsByPath(job_cg_abs_path);
  if (!root_ok || !children_ok) {
    CRANE_ERROR(
        "[Job #{}] Failed to thaw job cgroup {}, root_ok={}, children_ok={}",
        job_id, job_cg_abs_path, root_ok, children_ok);
    return CraneErrCode::ERR_CGROUP;
  }

  CRANE_INFO("[Job #{}] Thawed job cgroup: {}", job_id, job_cg_abs_path);
  return CraneErrCode::SUCCESS;
}

std::shared_ptr<SupervisorStub> JobManager::GetSupervisorStub(
    job_id_t job_id, step_id_t step_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) return nullptr;
  absl::MutexLock lk(job_ptr->step_map_mtx.get());
  auto it = job_ptr->step_map.find(step_id);
  if (it == job_ptr->step_map.end()) return nullptr;
  return it->second->supervisor_stub;
}

uint32_t JobManager::GetStepExitCode(job_id_t job_id, step_id_t step_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_WARN("[Step #{}.{}] Job not found when get exit code", job_id,
               step_id);
    return 0;
  }

  absl::MutexLock lk(job_ptr->step_map_mtx.get());
  auto step_it = job_ptr->step_map.find(step_id);
  if (step_it == job_ptr->step_map.end()) {
    CRANE_WARN("[Step #{}.{}] Step not found when getting exit code", job_id,
               step_id);
    return 0;
  }

  return step_it->second->exit_code;
}

google::protobuf::Timestamp JobManager::GetStepEndTime(job_id_t job_id,
                                                       step_id_t step_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_WARN("[Step #{}.{}] Job not found when getting end time", job_id,
               step_id);
    return {};
  }

  absl::MutexLock lk(job_ptr->step_map_mtx.get());
  auto step_it = job_ptr->step_map.find(step_id);
  if (step_it == job_ptr->step_map.end()) {
    CRANE_WARN("[Step #{}.{}] Step not found when getting end time", job_id,
               step_id);
    return {};
  }

  return step_it->second->end_time;
}

std::optional<JobInfoOfUid> JobManager::QueryJobInfoOfUid(uid_t uid) {
  CRANE_DEBUG("Query job info for uid {}", uid);

  JobInfoOfUid info;
  info.job_cnt = 0;
  info.cgroup_exists = false;

  if (auto job_ids = this->m_uid_to_job_ids_map_[uid]; job_ids) {
    info.cgroup_exists = true;
    info.job_cnt = job_ids->size();
    info.first_job_id = *job_ids->begin();
  } else {
    CRANE_WARN("Uid {} not found in uid_to_job_ids_map", uid);
    return std::nullopt;
  }
  return info;
}

void JobManager::EvCleanTerminateStepQueueCb_() {
  StepTerminateQueueElem elem;
  std::vector<StepTerminateQueueElem> not_ready_elems;

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_step_ids;

  while (m_step_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningJob Request from internal queue. "
        "Step: {}.{}",
        elem.job_id, elem.step_id);

    std::vector<StepInstance*> steps_to_clean;
    std::vector<JobInD> job_to_clean;
    {
      CRANE_INFO(
          "[Step #{}.{}] Terminating step, orphaned:{} terminate_source:{}.",
          elem.job_id, elem.step_id, elem.mark_as_orphaned,
          static_cast<int>(elem.terminate_source));
      bool terminate_job = elem.step_id == kDaemonStepId;
      auto map_ptr = m_job_map_.GetMapExclusivePtr();
      if (!map_ptr->contains(elem.job_id)) {
        CRANE_DEBUG(
            "[Step #{}.{}] Terminating a non-existent job, sending a status "
            "change",
            elem.job_id, elem.step_id);
        g_ctld_client->StepStatusChangeAsync(
            {.job_id = elem.job_id,
             .step_id = elem.step_id,
             .new_status = crane::grpc::JobStatus::Cancelled,
             .exit_code = ExitCode::EC_TERMINATED,
             .reason = "Terminated non-existent job.",
             .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
        continue;
      }
      auto job_instance = map_ptr->at(elem.job_id).RawPtr();

      absl::MutexLock lk(job_instance->step_map_mtx.get());
      if (!job_instance->step_map.contains(elem.step_id)) {
        CRANE_DEBUG(
            "[Step #{}.{}] Terminating a non-existent step, sending a status "
            "change",
            elem.job_id, elem.step_id);

        g_ctld_client->StepStatusChangeAsync(
            {.job_id = elem.job_id,
             .step_id = elem.step_id,
             .new_status = crane::grpc::JobStatus::Cancelled,
             .exit_code = ExitCode::EC_TERMINATED,
             .reason = "Terminated non-existent step.",
             .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
        continue;
      }
      auto& step = job_instance->step_map.at(elem.step_id);
      if (!step->IsRunning()) {
        not_ready_elems.emplace_back(std::move(elem));
        CRANE_DEBUG(
            "[Step #{}.{}] Step not running, current: {}, cannot terminate "
            "now.",
            elem.job_id, elem.step_id, step->status);
        continue;
      }

      std::set<step_id_t> terminate_step_ids;
      if (terminate_job) {
        terminate_step_ids = job_instance->step_map | std::views::keys |
                             std::ranges::to<std::set>();
      } else {
        terminate_step_ids = {elem.step_id};
      }

      // When a job is suspended, its cgroup may be frozen. In cgroup v1,
      // frozen processes cannot respond to SIGKILL. Thaw the job cgroup
      // before terminating steps to ensure cancel can take effect.
      if (job_instance->cgroup) {
        auto cg_path = job_instance->cgroup->CgroupPath().string();
        CgroupManager::ThawCgroupByPath(cg_path);
        CgroupManager::ThawChildCgroupsByPath(cg_path);
        CRANE_DEBUG("[Job #{}] Thawed job cgroup before terminating steps.",
                    elem.job_id);
      }

      for (auto step_id : terminate_step_ids) {
        auto& step_instance = job_instance->step_map.at(step_id);
        auto stub = step_instance->supervisor_stub;
        if (!stub) {
          CRANE_ERROR("[Step #{}.{}] Supervisor stub is null when terminating",
                      elem.job_id, step_id);
          StepStatusChangeAsync(
              elem.job_id, step_id, StepStatus::Failed, ExitCode::EC_RPC_ERR,
              "Supervisor stub is null when terminating",
              google::protobuf::util::TimeUtil::GetCurrentTime());
          continue;
        }
        auto err =
            stub->TerminateStep(elem.mark_as_orphaned, elem.terminate_source);
        if (err != CraneErrCode::SUCCESS) {
          // Supervisor dead for some reason.
          CRANE_ERROR("[Step #{}.{}] Failed to terminate.", elem.job_id,
                      step_id);
          if (!elem.mark_as_orphaned)
            g_ctld_client->StepStatusChangeAsync(
                {.job_id = elem.job_id,
                 .step_id = step_id,
                 .new_status = crane::grpc::JobStatus::Cancelled,
                 .exit_code = ExitCode::EC_TERMINATED,
                 .reason = "Terminated failed."});
        }
        CRANE_TRACE("[Step #{}.{}] Terminated.", elem.job_id, step_id);
      }

      if (elem.mark_as_orphaned) {
        if (terminate_job) {
          auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();
          auto job_opt = FreeJobInfoNoLock_(elem.job_id, map_ptr, uid_map_ptr);
          if (job_opt.has_value()) {
            job_to_clean.emplace_back(std::move(job_opt.value()));
            for (auto& [step_id, step] : job_instance->step_map) {
              CRANE_DEBUG("[Step #{}.{}] Removed orphaned step.", elem.job_id,
                          step_id);
              auto* step_ptr = step.get();
              steps_to_clean.push_back(step_ptr);
            }
          } else {
            CRANE_DEBUG("Trying to terminate a not existed job #{}.",
                        elem.job_id);
          }
        } else {
          for (auto step_id : terminate_step_ids) {
            CRANE_DEBUG("[Step #{}.{}] Removed orphaned step.", elem.job_id,
                        step_id);
            auto it = job_instance->step_map.find(step_id);
            auto* step = it->second.release();
            steps_to_clean.push_back(step);
            job_instance->step_map.erase(it);
          }
        }
      }
    }
    CleanUpJobAndStepsAsync(std::move(job_to_clean), std::move(steps_to_clean));
  }
  m_step_terminate_queue_.enqueue_bulk(
      std::make_move_iterator(not_ready_elems.begin()), not_ready_elems.size());
  not_ready_elems.clear();
}

void JobManager::TerminateStepAsync(
    job_id_t job_id, step_id_t step_id,
    crane::grpc::TerminateSource terminate_source) {
  StepTerminateQueueElem elem{.job_id = job_id,
                              .step_id = step_id,
                              .terminate_source = terminate_source};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

void JobManager::MarkStepAsOrphanedAndTerminateAsync(job_id_t job_id,
                                                     step_id_t step_id) {
  StepTerminateQueueElem elem{
      .job_id = job_id,
      .step_id = step_id,
      .terminate_source = crane::grpc::TERMINATE_SOURCE_NORMAL_COMPLETION,
      .mark_as_orphaned = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

void JobManager::CleanUpJobAndStepsAsync(std::vector<JobInD>&& jobs,
                                         std::vector<StepInstance*>&& steps) {
  std::latch shutdown_step_latch(steps.size());
  for (auto* step : steps) {
    // Only daemon step needs manual shutdown, others will shut down itself when
    // all jobs finished.
    if (!step->IsDaemonStep() || step->err_before_supv_start) {
      shutdown_step_latch.count_down();
      continue;
    }
    step->GotNewStatus(StepStatus::Completed);

    g_thread_pool->detach_task([&shutdown_step_latch, step] {
      auto stub = step->supervisor_stub;
      if (!stub) {
        CRANE_ERROR(
            "[Step #{}.{}] Supervisor stub is null when shutdown supervisor",
            step->job_id, step->step_id);
        g_ctld_client->StepStatusChangeAsync(StepStatusChangeQueueElem{
            .job_id = step->job_id,
            .step_id = step->step_id,
            .new_status = StepStatus::Failed,
            .exit_code = ExitCode::EC_RPC_ERR,
            .reason = "Supervisor stub is null when changing timelimit",
            .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
        shutdown_step_latch.count_down();
        return;
      }
      CRANE_TRACE("[Step #{}.{}] Shutting down daemon supervisor.",
                  step->job_id, step->step_id);
      auto err = stub->ShutdownSupervisor();
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR(
            "[Step #{}.{}] Failed to shutdown supervisor sending a status "
            "change with FAILED status.",
            step->job_id, step->step_id);
        g_ctld_client->StepStatusChangeAsync(StepStatusChangeQueueElem{
            .job_id = step->job_id,
            .step_id = step->step_id,
            .new_status = StepStatus::Failed,
            .exit_code = ExitCode::EC_RPC_ERR,
            .reason = "Supervisor not responding during step completion",
            .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()});
      }

      shutdown_step_latch.count_down();
    });
  }
  shutdown_step_latch.wait();

  absl::MutexLock lk(&m_free_job_step_mtx_);
  for (auto* step : steps) {
    if (m_completing_step_retry_map_.contains(step)) {
      CRANE_DEBUG("[Step #{}.{}] is already completing, ignore clean up.",
                  step->job_id, step->step_id);
      continue;
    }
    m_completing_step_retry_map_[step] = 0;
  }

  for (auto&& job : jobs) {
    job_id_t job_id = job.job_id;
    if (m_completing_job_.contains(job_id)) {
      CRANE_DEBUG("[Job #{}] is already completing, ignore clean up.", job_id);
      continue;
    }

    if (!g_config.JobLifecycleHook.Epilogs.empty() &&
        !(g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob)) {
      g_thread_pool->detach_task([job_id, env_map = job.GetJobEnvMap(),
                                  this]() {
        bool script_lock = false;
        if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::Serial) {
          m_prolog_serial_mutex_.Lock();
          script_lock = true;
        }
        CRANE_TRACE("#{}: Running epilogs...", job_id);
        RunPrologEpilogArgs run_epilog_args{
            .scripts = g_config.JobLifecycleHook.Epilogs,
            .envs = env_map,
            .timeout_sec = g_config.JobLifecycleHook.EpilogTimeout,
            .run_uid = 0,
            .run_gid = 0,
            .output_size = g_config.JobLifecycleHook.MaxOutputSize};

        run_epilog_args.fork_and_watch_fn =
            [this](std::function<pid_t()> do_fork)
            -> std::optional<std::pair<pid_t, std::future<int>>> {
          std::unique_lock<std::mutex> lock(m_fork_reap_mu_);
          pid_t pid = do_fork();
          if (pid < 0) return std::nullopt;
          if (pid == 0) return std::make_pair(pid, std::future<int>{});
          return std::make_pair(pid, m_exit_watcher_.Watch(pid));
        };

        if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0)
          run_epilog_args.timeout_sec =
              g_config.JobLifecycleHook.PrologEpilogTimeout;

        auto result = util::os::RunPrologOrEpiLog(run_epilog_args);
        if (!result) {
          auto status = result.error();
          CRANE_DEBUG("[Job #{}]: Epilog failed status={}:{}", job_id,
                      status.exit_code, status.signal_num);
          g_ctld_client->UpdateNodeDrainState(true, "Epilog failed");
        } else {
          CRANE_DEBUG("[Job #{}]: Epilog success", job_id);
        }
        if (script_lock) m_prolog_serial_mutex_.Unlock();
      });
    }

    m_completing_job_.emplace(job_id, std::move(job));
  }
}

void JobManager::StepStatusChangeAsync(
    job_id_t job_id, step_id_t step_id, crane::grpc::JobStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason,
    const google::protobuf::Timestamp& timestamp) {
  CRANE_INFO("[Step #{}.{}] is doing StepStatusChange, new status: {}", job_id,
             step_id, new_status);
  ActivateStepStatusChangeAsync_(job_id, step_id, new_status, exit_code,
                                 std::move(reason), std::move(timestamp));
}

}  // namespace Craned
