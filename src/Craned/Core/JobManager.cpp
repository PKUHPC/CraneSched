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

#include <pty.h>
#include <sys/wait.h>

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <latch>
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

std::filesystem::path GetV1ControllerPath_(
    const std::string& cg_path, Common::CgConstant::Controller ctrl) {
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
        GetV1ControllerPath_(
            cg_path, Common::CgConstant::Controller::FREEZE_CONTROLLER) /
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

CraneErrCode WaitStepCleanupFutures_(StepCleanupFutureMap&& futures) {
  CraneErrCode result = CraneErrCode::SUCCESS;
  for (auto& [job_id, step_futures] : futures) {
    for (auto& [step_id, future] : step_futures) {
      try {
        auto step_result = future.get();
        if (step_result != CraneErrCode::SUCCESS &&
            result == CraneErrCode::SUCCESS) {
          result = step_result;
        }
      } catch (const std::exception& ex) {
        CRANE_ERROR("[Step #{}.{}] Cleanup future failed: {}", job_id, step_id,
                    ex.what());
        if (result == CraneErrCode::SUCCESS)
          result = CraneErrCode::ERR_SYSTEM_ERR;
      }
    }
  }
  return result;
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
  ResourceInNodeV3 step_res_v3(daemon_step_to_d.res());
  auto mem_in_node =
      step_res_v3.GetMemoryBytes() / (static_cast<uint64_t>(1024 * 1024));

  cpu_t cpus_on_node = step_res_v3.GetCpuSet().cpu_count;
  auto mem_per_cpu = (cpus_on_node > cpu_t{0})
                         ? (static_cast<double>(mem_in_node) /
                            static_cast<double>(cpus_on_node))
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
  env_map.emplace("CRANE_CPUS_ON_NODE",
                  std::format("{:.2f}", static_cast<double>(cpus_on_node)));
  env_map.emplace("CRANE_NODEID", node_id_to_str());
  env_map.emplace("CRANE_SUBMIT_HOST", daemon_step_to_d.submit_hostname());
  if (job_to_d.has_array_task()) {
    const auto& array_task = job_to_d.array_task();
    env_map.emplace("CRANE_ARRAY_JOB_ID",
                    std::to_string(array_task.array_job_id()));
    env_map.emplace("CRANE_ARRAY_TASK_ID",
                    std::to_string(array_task.task_id()));
  }

  // SLURM
  if (g_config.EnableSlurmCompatibleEnv) {
    env_map.emplace("SLURM_JOBID", std::to_string(job_id));
    env_map.emplace("SLURM_JOB_ID", std::to_string(job_id));
    env_map.emplace("SLURM_NODEID", node_id_to_str());
    env_map.emplace("SLURMD_NODENAME", g_config.Hostname);
    env_map.emplace("SLURM_WORKING_DIR", daemon_step_to_d.submit_dir());
    env_map.emplace("SLURM_NODELIST",
                    util::HostNameListToStr(daemon_step_to_d.nodelist()));
    if (job_to_d.has_array_task()) {
      const auto& array_task = job_to_d.array_task();
      env_map.emplace("SLURM_ARRAY_JOB_ID",
                      std::to_string(array_task.array_job_id()));
      env_map.emplace("SLURM_ARRAY_TASK_ID",
                      std::to_string(array_task.task_id()));
    }
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

  m_free_steps_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_free_steps_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanFreeStepsQueueCb_();
      });

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
  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();

  for (const auto& job : jobs) {
    job_id_t job_id = job.job_id;
    if (job_map_ptr->contains(job_id)) {
      auto* old_job = job_map_ptr->at(job_id).RawPtr();
      bool old_job_freeable = false;
      {
        absl::MutexLock lk(old_job->step_map_mtx.get());
        old_job_freeable =
            old_job->step_map.empty() && old_job->cgroup == nullptr;
      }

      if (!old_job_freeable) {
        CRANE_ERROR(
            "[Job #{}] Allocation rejected because the previous job record is "
            "still active or cleaning.",
            job_id);
        return false;
      }
    }
  }

  for (auto& job : jobs) {
    job_id_t job_id = job.job_id;
    uid_t uid = job.Uid();

    if (job_map_ptr->contains(job_id)) {
      CRANE_INFO("[Job #{}] Reclaiming freeable stale job record.", job_id);
      FreeJobInfoNoLock_(job_id, job_map_ptr, uid_map_ptr);
    }

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
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    CRANE_TRACE("JobManager is ending now, ignoring FreeJobs request.");
    return false;
  }

  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();
  for (job_id_t job_id : job_ids) {
    if (!job_map_ptr->contains(job_id)) {
      CRANE_DEBUG("[Job #{}] FreeJobs ignored because job is not found.",
                  job_id);
      continue;
    }

    auto* job = job_map_ptr->at(job_id).RawPtr();
    bool freeable = false;
    size_t step_count = 0;
    bool has_job_cgroup = job->cgroup != nullptr;
    {
      absl::MutexLock lk(job->step_map_mtx.get());
      step_count = job->step_map.size();
      freeable = step_count == 0 && !has_job_cgroup;
    }

    if (!freeable) {
      CRANE_DEBUG(
          "[Job #{}] FreeJobs ignored because job is active or cleaning. "
          "step_count={}, has_job_cgroup={}",
          job_id, step_count, has_job_cgroup);
      continue;
    }

    CRANE_INFO("[Job #{}] Freeing freeable job record.", job_id);
    FreeJobInfoNoLock_(job_id, job_map_ptr, uid_map_ptr);
  }

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

StepCleanupFutureMap JobManager::FreeSteps(
    std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps) {
  StepCleanupFutureMap futures;
  std::vector<std::pair<StepKey, StepCleanupPromise>> waiters;
  for (const auto& [job_id, step_ids] : steps) {
    for (step_id_t step_id : step_ids) {
      auto promise = std::make_shared<std::promise<CraneErrCode>>();
      futures[job_id].emplace(step_id, promise->get_future());
      waiters.emplace_back(StepKey{job_id, step_id}, std::move(promise));
    }
  }

  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    CRANE_TRACE("JobManager is ending now, ignoring FreeSteps request.");
    for (auto& [_, promise] : waiters) {
      promise->set_value(CraneErrCode::ERR_SHUTTING_DOWN);
    }
    return futures;
  }

  {
    absl::MutexLock lk(&m_free_job_step_mtx_);
    for (auto& [key, promise] : waiters) {
      m_step_cleanup_waiters_[key].emplace_back(std::move(promise));
    }
  }

  for (auto& [job_id, step_ids] : steps) {
    for (step_id_t step_id : step_ids) {
      m_free_steps_queue_.enqueue({job_id, step_id});
    }
  }
  if (!steps.empty()) m_free_steps_async_handle_->send();
  return futures;
}

JobCleanupFutureMap JobManager::FreeInvalidJobs(std::set<job_id_t>&& job_ids) {
  JobCleanupFutureMap futures;
  for (job_id_t job_id : job_ids) {
    auto promise = std::make_shared<std::promise<CraneErrCode>>();
    futures.emplace(job_id, promise->get_future());

    if (m_is_ending_now_.load(std::memory_order_acquire)) {
      CRANE_TRACE(
          "JobManager is ending now, ignoring FreeInvalidJobs request.");
      promise->set_value(CraneErrCode::ERR_SHUTTING_DOWN);
      continue;
    }

    std::thread([this, job_id, promise = std::move(promise)]() mutable {
      util::SetCurrentThreadName("InvJobCleanup");

      std::unordered_set<step_id_t> non_daemon_steps;
      std::unordered_set<step_id_t> daemon_steps;
      {
        auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
        if (!job_ptr) {
          CRANE_DEBUG("[Job #{}] Invalid job cleanup skipped; job not found.",
                      job_id);
          promise->set_value(CraneErrCode::SUCCESS);
          return;
        }

        absl::MutexLock lk(job_ptr->step_map_mtx.get());
        for (auto& [step_id, step] : job_ptr->step_map) {
          if (!step) continue;
          step->silent_cleanup = true;
          if (step_id == kDaemonStepId)
            daemon_steps.insert(step_id);
          else
            non_daemon_steps.insert(step_id);
        }
      }

      {
        absl::MutexLock lk(&m_free_job_step_mtx_);
        for (auto& [step_key, state] : m_completing_step_retry_map_) {
          if (step_key.first != job_id || state.step == nullptr) continue;
          state.step->silent_cleanup = true;
          if (step_key.second == kDaemonStepId)
            daemon_steps.insert(step_key.second);
          else
            non_daemon_steps.insert(step_key.second);
        }
      }

      CraneErrCode result = CraneErrCode::SUCCESS;
      if (!non_daemon_steps.empty()) {
        std::unordered_map<job_id_t, std::unordered_set<step_id_t>>
            steps_to_free;
        steps_to_free[job_id] = std::move(non_daemon_steps);
        result = WaitStepCleanupFutures_(FreeSteps(std::move(steps_to_free)));
      }

      if (result == CraneErrCode::SUCCESS && !daemon_steps.empty()) {
        std::unordered_map<job_id_t, std::unordered_set<step_id_t>>
            steps_to_free;
        steps_to_free[job_id] = std::move(daemon_steps);
        result = WaitStepCleanupFutures_(FreeSteps(std::move(steps_to_free)));
      }

      if (result == CraneErrCode::SUCCESS) {
        std::optional<StepInstance::DaemonJobCleanupCtx> fallback_cleanup_ctx;
        bool release_job_record = false;
        {
          auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
          if (job_ptr) {
            size_t remaining_steps = 0;
            {
              absl::MutexLock lk(job_ptr->step_map_mtx.get());
              remaining_steps = job_ptr->step_map.size();
            }

            if (remaining_steps != 0) {
              CRANE_ERROR(
                  "[Job #{}] Invalid job cleanup failed; {} steps remain.",
                  job_id, remaining_steps);
              result = CraneErrCode::ERR_SYSTEM_ERR;
            } else {
              if (job_ptr->cgroup) {
                CRANE_WARN(
                    "[Job #{}] Invalid job has job cgroup but no daemon "
                    "cleanup context; destroying job cgroup directly.",
                    job_id);
                fallback_cleanup_ctx.emplace(StepInstance::DaemonJobCleanupCtx{
                    .resource = job_ptr->job_to_d.res(),
                    .epilog_env = EnvMap{},
                    .job_cgroup = std::move(job_ptr->cgroup)});
              }

              release_job_record = true;
            }
          }
        }

        if (fallback_cleanup_ctx.has_value()) {
          CleanUpJobEnvironment_(job_id, std::move(*fallback_cleanup_ctx),
                                 false);
        }

        if (result == CraneErrCode::SUCCESS && release_job_record &&
            !FreeJobInfo_(job_id).has_value()) {
          CRANE_DEBUG(
              "[Job #{}] Invalid job record disappeared before release.",
              job_id);
        }
      }

      promise->set_value(result);
    }).detach();
  }

  return futures;
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

      if (step->err_before_supv_start ||
          step->status == StepStatus::Completing ||
          IsFinishedStepStatus(step->status)) {
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
      pending.emplace_back(PendingSnapshot{.key = key,
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
                   IsFinishedStepStatus(step->status);
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

    failures.emplace_back(PendingFailure{.job_id = job_id,
                                         .step_id = step_id,
                                         .exit_code = entry.exit_code,
                                         .reason = entry.reason});
    resolved_keys.emplace_back(entry.key);
  }

  {
    absl::MutexLock unexpected_lock(&m_unexpected_supervisor_exit_mtx_);
    for (const auto& key : retry_keys) {
      auto it = m_unexpected_supervisor_exit_map_.find(key);
      if (it != m_unexpected_supervisor_exit_map_.end())
        ++it->second.retry_count;
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
    SendCompletingAndTerminal_(failure.job_id, failure.step_id,
                               StepStatus::Failed, failure.exit_code,
                               std::move(failure.reason));
  }
}

bool JobManager::EvCheckSupervisorRunning_() {
  struct DaemonCleanupRequest {
    job_id_t job_id;
    step_id_t step_id;
    std::optional<StepStatusChangeQueueElem> terminal;
  };

  std::vector<std::pair<job_id_t, std::string>> jobs_to_check_pending_sigkill;
  std::vector<DaemonCleanupRequest> daemon_cleanup_requests;
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

    std::vector<StepKey> exit_steps;
    absl::MutexLock lk(&m_free_job_step_mtx_);
    for (auto& [step_key, state] : m_completing_step_retry_map_) {
      auto* step = state.step;
      if (step == nullptr) {
        exit_steps.push_back(step_key);
        continue;
      }
      if (step->err_before_supv_start) {
        exit_steps.push_back(step_key);
        continue;
      }
      job_id_t job_id = step->job_id;
      step_id_t step_id = step->step_id;
      auto exists = kill(step->supv_pid, 0) == 0;

      if (exists) {
        state.alive_check_count++;
        if (!state.sigkill_sent &&
            state.alive_check_count >= kMaxSupervisorCheckRetryCount) {
          CRANE_WARN(
              "[Step #{}.{}] Supervisor is still running after {} checks, "
              "sending SIGKILL.",
              job_id, step_id, kMaxSupervisorCheckRetryCount);
          kill(step->supv_pid, SIGKILL);
          SendCompletingAndTerminal_(
              job_id, step_id, StepStatus::Failed, ExitCode::EC_RPC_ERR,
              "Supervisor not responding during step completion");
          state.sigkill_sent = true;
        }
        continue;
      }
      // Supervisor exited. Wait for pending_terminal_status to be set
      // (Completing message may still be in the status change queue).
      if (!step->pending_terminal_status.has_value() && !step->silent_cleanup) {
        state.status_wait_count++;
        if (state.status_wait_count >= kMaxStatusWaitRetryCount) {
          CRANE_ERROR(
              "[Step #{}.{}] Timed out waiting for pending_terminal_status "
              "after supervisor exit. Forcing Failed.",
              job_id, step_id);
          step->pending_terminal_status = StepInstance::PendingTerminalStatus{
              .final_status = StepStatus::Failed,
              .exit_code = ExitCode::EC_RPC_ERR,
              .reason =
                  "Timed out waiting for terminal status "
                  "after supervisor exit",
              .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()};
        } else {
          CRANE_TRACE(
              "[Step #{}.{}] Supervisor exited but pending_terminal_status "
              "not set, waiting for next tick.",
              job_id, step_id);
          continue;
        }
      }
      exit_steps.push_back(step_key);
    }
    for (const auto& step_key : exit_steps) {
      auto completing_it = m_completing_step_retry_map_.find(step_key);
      if (completing_it == m_completing_step_retry_map_.end()) continue;
      auto* step = completing_it->second.step;
      m_completing_step_retry_map_.erase(completing_it);
      if (step == nullptr) continue;
      if (step->IsDaemonStep()) {
        std::optional<StepStatusChangeQueueElem> terminal = std::nullopt;
        if (step->pending_terminal_status.has_value() &&
            !step->silent_cleanup) {
          auto& pt = step->pending_terminal_status.value();
          terminal = StepStatusChangeQueueElem{.job_id = step->job_id,
                                               .step_id = step->step_id,
                                               .new_status = pt.final_status,
                                               .exit_code = pt.exit_code,
                                               .reason = pt.reason,
                                               .timestamp = pt.timestamp};
        }
        daemon_cleanup_requests.emplace_back(
            DaemonCleanupRequest{.job_id = step->job_id,
                                 .step_id = step->step_id,
                                 .terminal = std::move(terminal)});
        continue;
      }

      steps_to_clean.emplace_back(step);
    }
  }

  for (auto& request : daemon_cleanup_requests) {
    g_thread_pool->detach_task([this, request = std::move(request)]() mutable {
      StepInstance* step = nullptr;
      std::optional<StepInstance::DaemonJobCleanupCtx> cleanup_ctx;
      CraneErrCode cleanup_result = CraneErrCode::SUCCESS;
      {
        auto job_ptr = m_job_map_.GetValueExclusivePtr(request.job_id);
        if (!job_ptr) {
          CRANE_WARN("[Step #{}.{}] Job not found during daemon cleanup.",
                     request.job_id, request.step_id);
        } else {
          absl::MutexLock lk(job_ptr->step_map_mtx.get());
          auto step_it = job_ptr->step_map.find(request.step_id);
          if (step_it == job_ptr->step_map.end()) {
            CRANE_WARN("[Step #{}.{}] Step not found during daemon cleanup.",
                       request.job_id, request.step_id);
          } else {
            step = step_it->second.get();
            if (step->daemon_job_cleanup.has_value()) {
              cleanup_ctx.emplace(std::move(*step->daemon_job_cleanup));
            } else {
              CRANE_WARN("[Step #{}.{}] Daemon cleanup context not found.",
                         request.job_id, request.step_id);
              cleanup_result = CraneErrCode::ERR_SYSTEM_ERR;
            }
          }
        }
      }

      if (step != nullptr) step->CleanUp(false);
      if (cleanup_ctx.has_value()) {
        CleanUpJobEnvironment_(request.job_id, std::move(*cleanup_ctx));
      }

      {
        auto job_ptr = m_job_map_.GetValueExclusivePtr(request.job_id);
        if (job_ptr) {
          absl::MutexLock lk(job_ptr->step_map_mtx.get());
          job_ptr->step_map.erase(request.step_id);
        }
      }

      if (request.terminal.has_value()) {
        CRANE_INFO(
            "[Step #{}.{}] Daemon cleanup completed, sending terminal "
            "status {}.",
            request.job_id, request.step_id, request.terminal->new_status);
        g_ctld_client->StepStatusChangeAsync(
            std::move(request.terminal.value()));
      }

      ResolveStepCleanupWaiters_(StepKey{request.job_id, request.step_id},
                                 cleanup_result);
    });
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
  }

  {
    absl::MutexLock lk(&m_free_job_step_mtx_);
    return m_completing_step_retry_map_.empty();
  }
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
  bool has_int_job = false;

  while (m_grpc_alloc_step_queue_.try_dequeue(elem)) {
    std::unique_ptr step_inst = std::move(elem.step_inst);

    if (!m_job_map_.Contains(step_inst->job_id)) {
      CRANE_ERROR("[Step #{}.{}] Failed to find a job allocation for the step",
                  step_inst->job_id, step_inst->step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
      continue;
    }

    auto job_ptr = m_job_map_.GetValueExclusivePtr(step_inst->job_id);
    if (job_ptr && !job_ptr->cgroup) {
      ResourceInNodeV3 res_v3(job_ptr->job_to_d.res());
      if (res_v3.GetCpuSet().IsInteger()) {
        CgroupManager::ClaimCoresForIntJob(res_v3.GetCpuSet().core_ids);
        has_int_job = true;
      }
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

  if (has_int_job) CgroupManager::WriteOverflowCpuset();
}

void JobManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void JobManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

// NOTE: V3 changed dedicated_res_in_node() -> gres() in proto.
// This change needs to be applied in StepInstance.cpp which now holds
// the SpawnSupervisor logic (moved there by x11_task branch).
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
  auto is_step_missing_or_finished = [this, job_id, step_id]() {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job) return true;

    absl::MutexLock lock(job->step_map_mtx.get());
    auto step_it = job->step_map.find(step_id);
    if (step_it == job->step_map.end()) return true;

    auto status = step_it->second->status;
    return status == StepStatus::Completing || IsFinishedStepStatus(status);
  };

  std::shared_ptr<SupervisorStub> stub;
  {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job) {
      CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id,
                  step_id);
      return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
    }

    absl::MutexLock lock(job->step_map_mtx.get());
    auto step_it = job->step_map.find(step_id);
    if (step_it == job->step_map.end()) {
      CRANE_ERROR("[Step #{}.{}] Failed to find step allocation", job_id,
                  step_id);
      return std::unexpected{CraneErrCode::ERR_NON_EXISTENT};
    }

    auto status = step_it->second->status;
    if (status == StepStatus::Completing || IsFinishedStepStatus(status))
      return {};

    stub = step_it->second->supervisor_stub;
  }

  if (!stub) {
    if (is_step_missing_or_finished()) return {};

    CRANE_ERROR(
        "[Step #{}.{}] Supervisor stub is null when changing time constraint",
        job_id, step_id);
    SendCompletingAndTerminal_(
        job_id, step_id, StepStatus::Failed, ExitCode::EC_RPC_ERR,
        "Supervisor stub is null when changing time constraint");
    return std::unexpected{CraneErrCode::ERR_RPC_FAILURE};
  }

  auto err = stub->ChangeStepTimeConstraint(time_limit_seconds, deadline_time);
  int64_t sec = time_limit_seconds.value_or(deadline_time.value_or(0));
  if (err != CraneErrCode::SUCCESS) {
    if (is_step_missing_or_finished()) return {};

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
      auto err = step->supervisor_stub->ChangeStepTimeConstraint(
          new_timelimit_sec, std::nullopt);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_WARN(
            "[Step #{}.{}] Failed to change step timelimit to {} seconds, "
            "but continuing (step may have completed or supervisor "
            "unavailable)",
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
    SendCompletingAndTerminal_(job_id, step_id, StepStatus::Failed,
                               ExitCode::EC_RPC_ERR,
                               "Supervisor stub is null when query env");
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

void JobManager::CleanUpJobEnvironment_(job_id_t job_id,
                                        StepInstance::DaemonJobCleanupCtx&& ctx,
                                        bool run_epilog) {
  CRANE_DEBUG("[Job #{}] Cleaning up job environment.", job_id);

  if (run_epilog && !g_config.JobLifecycleHook.Epilogs.empty() &&
      !(g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob)) {
    bool script_lock = false;
    if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::Serial) {
      m_prolog_serial_mutex_.Lock();
      script_lock = true;
    }
    CRANE_TRACE("[Job #{}] Running epilogs...", job_id);
    RunPrologEpilogArgs run_epilog_args{
        .scripts = g_config.JobLifecycleHook.Epilogs,
        .envs = ctx.epilog_env,
        .timeout_sec = g_config.JobLifecycleHook.EpilogTimeout,
        .run_uid = 0,
        .run_gid = 0,
        .output_size = g_config.JobLifecycleHook.MaxOutputSize};

    run_epilog_args.fork_and_watch_fn = [this](std::function<pid_t()> do_fork)
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
  }

  CgroupManager::ReleaseJobCpuPool(job_id, ctx.resource);

  if (ctx.job_cgroup == nullptr) return;

  if (g_config.Plugin.Enabled && g_plugin_client) {
    g_plugin_client->DestroyCgroupHookAsync(job_id,
                                            ctx.job_cgroup->CgroupName());
  }

  CgroupManager::KillAndDestroyCgroup(std::move(ctx.job_cgroup));
}

void JobManager::ResolveStepCleanupWaiters_(const StepKey& key,
                                            CraneErrCode result) {
  std::vector<StepCleanupPromise> waiters;
  {
    absl::MutexLock lk(&m_free_job_step_mtx_);
    auto it = m_step_cleanup_waiters_.find(key);
    if (it == m_step_cleanup_waiters_.end()) return;
    waiters = std::move(it->second);
    m_step_cleanup_waiters_.erase(it);
  }

  for (auto& waiter : waiters) {
    waiter->set_value(result);
  }
}

void JobManager::FreeStepAllocation_(
    std::vector<std::unique_ptr<StepInstance>>&& steps) {
  for (auto& step : steps) {
    job_id_t job_id = step->job_id;
    step_id_t step_id = step->step_id;
    step->CleanUp();
    std::optional<StepInstance::PendingTerminalStatus> terminal_status =
        std::nullopt;
    if (step->pending_terminal_status.has_value() && !step->silent_cleanup) {
      terminal_status = step->pending_terminal_status.value();
    }
    step.reset();
    // Send terminal status after step cleanup is done (skip for silent
    // cleanup).
    if (terminal_status.has_value()) {
      auto& pt = terminal_status.value();
      CRANE_TRACE("[Step #{}.{}] Sending terminal status {} after cleanup.",
                  job_id, step_id, pt.final_status);
      g_ctld_client->StepStatusChangeAsync(
          StepStatusChangeQueueElem{.job_id = job_id,
                                    .step_id = step_id,
                                    .new_status = pt.final_status,
                                    .exit_code = pt.exit_code,
                                    .reason = pt.reason,
                                    .timestamp = pt.timestamp});
    }
    ResolveStepCleanupWaiters_(StepKey{job_id, step_id}, CraneErrCode::SUCCESS);
  }
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
          fmt::format("Failed to run prolog for job#{} ", job_id), std::nullopt,
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
        std::nullopt, google::protobuf::util::TimeUtil::GetCurrentTime());
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
        std::nullopt, google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }

  // Create job cgroup first.
  // TODO: Create step cgroup later.
  if (!job->cgroup) {
    auto cg_expt = CgroupManager::AllocateAndGetCgroupForJob(
        job->job_id, job->job_to_d.res(), false, Common::CgConstant::kCgMinMem);
    if (cg_expt.has_value()) {
      job->cgroup = std::move(cg_expt.value());
      ResourceInNodeV3 res_v3(job->job_to_d.res());
      job->path_info =
          CgroupManager::MakeCgroupPathInfo(job->job_id, res_v3.GetCpuSet());
    } else {
      CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
      ActivateStepStatusChangeAsync_(
          job_id, step_id, crane::grpc::JobStatus::Failed,
          ExitCode::EC_CGROUP_ERR,
          fmt::format("Failed to get cgroup for job#{} ", job_id), std::nullopt,
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
  CraneErrCode err = step_ptr->Prepare(job->path_info);
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR("[Step #{}.{}] Failed to prepare.", job_id, step_id);
    step_ptr->err_before_supv_start = true;
    ActivateStepStatusChangeAsync_(
        job_id, step_id, crane::grpc::JobStatus::Failed,
        ExitCode::EC_CGROUP_ERR,
        fmt::format("Cannot create cgroup for the instance of step {}.{}",
                    job_id, step_id),
        std::nullopt, google::protobuf::util::TimeUtil::GetCurrentTime());
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
        std::nullopt, google::protobuf::util::TimeUtil::GetCurrentTime());
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
    bool should_forward = true;
    bool local_step_found = false;
    const StepKey key{status_change.job_id, status_change.step_id};
    const bool daemon_terminal = status_change.step_id == kDaemonStepId &&
                                 IsFinishedStepStatus(status_change.new_status);
    std::optional<StepInstance::PendingTerminalStatus> pending_terminal;
    if (status_change.new_status == StepStatus::Completing &&
        status_change.final_status.has_value()) {
      pending_terminal = StepInstance::PendingTerminalStatus{
          .final_status = status_change.final_status.value(),
          .exit_code = status_change.exit_code,
          .reason = status_change.reason.value_or(""),
          .timestamp = status_change.timestamp};
    } else if (daemon_terminal) {
      pending_terminal = StepInstance::PendingTerminalStatus{
          .final_status = status_change.new_status,
          .exit_code = status_change.exit_code,
          .reason = status_change.reason.value_or(""),
          .timestamp = status_change.timestamp};
    }

    // Find and update the local step under its owning lock. StepInstance
    // pointers must not escape these guards.
    {
      absl::MutexLock lk(&m_free_job_step_mtx_);
      if (auto completing_it = m_completing_step_retry_map_.find(key);
          completing_it != m_completing_step_retry_map_.end()) {
        auto* step = completing_it->second.step;
        local_step_found = true;
        if (step == nullptr) {
          should_forward = false;
        } else {
          step->GotNewStatus(status_change.new_status);
          if (pending_terminal)
            step->pending_terminal_status = *pending_terminal;
          should_forward = !step->silent_cleanup;
        }
      }
    }

    if (!local_step_found) {
      auto job_ptr = m_job_map_.GetValueExclusivePtr(status_change.job_id);
      if (job_ptr) {
        absl::MutexLock lk(job_ptr->step_map_mtx.get());
        if (auto it = job_ptr->step_map.find(status_change.step_id);
            it != job_ptr->step_map.end()) {
          auto* step = it->second.get();
          local_step_found = true;
          step->GotNewStatus(status_change.new_status);
          if (pending_terminal)
            step->pending_terminal_status = *pending_terminal;
          should_forward = !step->silent_cleanup;
        }
      }
    }

    if (!local_step_found && daemon_terminal) {
      CRANE_DEBUG(
          "[Step #{}.{}] Daemon terminal status arrived after local cleanup, "
          "suppressing direct forward.",
          status_change.job_id, status_change.step_id);
    }
    should_forward &= !daemon_terminal;

    // Forward only statuses that are not consumed by local cleanup.
    CRANE_TRACE("[Step #{}.{}] StepStatusChange status: {}.",
                status_change.job_id, status_change.step_id,
                status_change.new_status);

    if (should_forward) {
      status_change.final_status = std::nullopt;
      g_ctld_client->StepStatusChangeAsync(std::move(status_change));
    } else {
      CRANE_DEBUG(
          "[Step #{}.{}] Status change is handled locally, not forwarding to "
          "CraneCtld.",
          status_change.job_id, status_change.step_id);
    }
  }
}

void JobManager::ActivateStepStatusChangeAsync_(
    job_id_t job_id, step_id_t step_id, crane::grpc::JobStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason,
    std::optional<crane::grpc::JobStatus> final_status,
    const google::protobuf::Timestamp& timestamp) {
  StepStatusChangeQueueElem status_change{.job_id = job_id,
                                          .step_id = step_id,
                                          .new_status = new_status,
                                          .exit_code = exit_code,
                                          .final_status = final_status,
                                          .timestamp = std::move(timestamp)};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_step_status_change_queue_.enqueue(std::move(status_change));
  m_step_status_change_async_handle_->send();
}

void JobManager::SendCompletingAndTerminal_(
    job_id_t job_id, step_id_t step_id, crane::grpc::JobStatus terminal_status,
    uint32_t exit_code, std::string reason) {
  auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
  // Completing (drives AllNodesCompleting → FreeSteps)
  ActivateStepStatusChangeAsync_(job_id, step_id, StepStatus::Completing,
                                 exit_code, reason, terminal_status, now);
  // Terminal (drives AllNodesFinished → release/FreeJobs)
  ActivateStepStatusChangeAsync_(job_id, step_id, terminal_status, exit_code,
                                 std::move(reason), std::nullopt, now);
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
    CRANE_WARN("[Job #{}] Failed to resume: job allocation not found.", job_id);
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
  if (step_it->second->pending_terminal_status.has_value())
    return step_it->second->pending_terminal_status->exit_code;
  else {
    CRANE_WARN(
        "[Step #{}.{}] Pending terminal status not found when getting exit "
        "code, returning 0",
        job_id, step_id);
    return 0;
  }
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

  if (step_it->second->pending_terminal_status.has_value())
    return step_it->second->pending_terminal_status->timestamp;
  else {
    CRANE_WARN(
        "[Step #{}.{}] Pending terminal status not found when getting end time",
        job_id, step_id);
    return {};
  }
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
      CRANE_INFO("[Step #{}.{}] Terminating step, terminate_source:{}.",
                 elem.job_id, elem.step_id,
                 static_cast<int>(elem.terminate_source));
      bool terminate_job = elem.step_id == kDaemonStepId;
      auto map_ptr = m_job_map_.GetMapExclusivePtr();
      if (!map_ptr->contains(elem.job_id)) {
        CRANE_DEBUG(
            "[Step #{}.{}] Terminating a non-existent job, sending a status "
            "change",
            elem.job_id, elem.step_id);
        SendCompletingAndTerminal_(
            elem.job_id, elem.step_id, crane::grpc::JobStatus::Cancelled,
            ExitCode::EC_TERMINATED, "Terminated non-existent job.");
        continue;
      }
      auto job_instance = map_ptr->at(elem.job_id).RawPtr();

      absl::MutexLock lk(job_instance->step_map_mtx.get());
      if (!job_instance->step_map.contains(elem.step_id)) {
        CRANE_DEBUG(
            "[Step #{}.{}] Terminating a non-existent step, sending a status "
            "change",
            elem.job_id, elem.step_id);

        SendCompletingAndTerminal_(
            elem.job_id, elem.step_id, crane::grpc::JobStatus::Cancelled,
            ExitCode::EC_TERMINATED, "Terminated non-existent step.");
        continue;
      }
      auto& step = job_instance->step_map.at(elem.step_id);
      if (step->status != StepStatus::Running &&
          step->status != StepStatus::Starting) {
        if (step->status == StepStatus::Completing ||
            IsFinishedStepStatus(step->status)) {
          CRANE_DEBUG(
              "[Step #{}.{}] Step is already {}, terminate request is "
              "idempotently complete.",
              elem.job_id, elem.step_id, step->status);
          continue;
        }
        not_ready_elems.emplace_back(std::move(elem));
        CRANE_DEBUG(
            "[Step #{}.{}] Step not running/starting, current: {}, cannot "
            "terminate now.",
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
          SendCompletingAndTerminal_(
              elem.job_id, step_id, StepStatus::Failed, ExitCode::EC_RPC_ERR,
              "Supervisor stub is null when terminating");
          continue;
        }
        auto err = stub->TerminateStep(elem.terminate_source);
        if (err != CraneErrCode::SUCCESS) {
          // Supervisor dead for some reason.
          CRANE_ERROR("[Step #{}.{}] Failed to terminate.", elem.job_id,
                      step_id);
          SendCompletingAndTerminal_(
              elem.job_id, step_id, crane::grpc::JobStatus::Cancelled,
              ExitCode::EC_TERMINATED, "Terminated failed.");
        }
        CRANE_TRACE("[Step #{}.{}] Terminated.", elem.job_id, step_id);
      }
    }
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

void JobManager::MarkStepSilentCleanup(job_id_t job_id, step_id_t step_id) {
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (job_ptr) {
    absl::MutexLock lk(job_ptr->step_map_mtx.get());
    if (auto it = job_ptr->step_map.find(step_id);
        it != job_ptr->step_map.end()) {
      it->second->silent_cleanup = true;
    }
  }

  {
    absl::MutexLock lk(&m_free_job_step_mtx_);
    if (auto it = m_completing_step_retry_map_.find(StepKey{job_id, step_id});
        it != m_completing_step_retry_map_.end() &&
        it->second.step != nullptr) {
      it->second.step->silent_cleanup = true;
    }
  }
}

void JobManager::EvCleanFreeStepsQueueCb_() {
  FreeStepElem elem;
  std::vector<StepInstance*> steps_to_shutdown;
  {
    std::vector<StepInstance*> steps_to_free;
    absl::flat_hash_set<StepKey> steps_to_free_keys;
    while (m_free_steps_queue_.try_dequeue(elem)) {
      const StepKey key{elem.job_id, elem.step_id};
      auto job = m_job_map_.GetValueExclusivePtr(elem.job_id);
      if (!job) {
        CRANE_WARN("Try to free step [{}] for nonexistent job #{}.",
                   elem.step_id, elem.job_id);
        ResolveStepCleanupWaiters_(key, CraneErrCode::SUCCESS);
        continue;
      }
      absl::MutexLock lk(job->step_map_mtx.get());
      if (!job->step_map.contains(elem.step_id)) {
        bool step_is_cleaning = false;
        {
          absl::MutexLock retry_lk(&m_free_job_step_mtx_);
          step_is_cleaning = m_completing_step_retry_map_.contains(key);
        }
        if (step_is_cleaning || steps_to_free_keys.contains(key)) {
          CRANE_DEBUG("[Step #{}.{}] is already cleaning, wait for cleanup.",
                      elem.job_id, elem.step_id);
          continue;
        }

        CRANE_WARN(
            "[Step #{}.{}] Try to free nonexistent step, treating as "
            "idempotent success.",
            elem.job_id, elem.step_id);
        ResolveStepCleanupWaiters_(key, CraneErrCode::SUCCESS);
        continue;
      }

      auto& step = job->step_map.at(elem.step_id);
      if (step->IsDaemonStep()) {
        if (step->daemon_job_cleanup.has_value()) {
          CRANE_DEBUG("[Step #{}.{}] Daemon step is already cleaning.",
                      elem.job_id, elem.step_id);
          ResolveStepCleanupWaiters_(key, CraneErrCode::SUCCESS);
          continue;
        }

        StepInstance::DaemonJobCleanupCtx cleanup_ctx{
            .resource = job->job_to_d.res(),
            .epilog_env = job->GetJobEnvMap(),
            .job_cgroup = std::move(job->cgroup)};
        step->daemon_job_cleanup = std::move(cleanup_ctx);
        steps_to_free.emplace_back(step.get());
        steps_to_free_keys.insert(key);
        continue;
      }

      steps_to_free.emplace_back(step.release());
      steps_to_free_keys.insert(key);
      job->step_map.erase(elem.step_id);
    }

    if (steps_to_free.empty()) return;

    absl::MutexLock lk(&m_free_job_step_mtx_);
    for (auto* step : steps_to_free) {
      const StepKey key{step->job_id, step->step_id};
      if (m_completing_step_retry_map_.contains(key)) {
        CRANE_DEBUG("[Step #{}.{}] is already cleaning, ignore clean up.",
                    step->job_id, step->step_id);
        continue;
      }
      m_completing_step_retry_map_[key] = CompletingStepState{.step = step};
      if (step->err_before_supv_start) continue;
      if (!step->silent_cleanup && !step->IsDaemonStep()) continue;
      steps_to_shutdown.emplace_back(step);
    }
  }

  if (steps_to_shutdown.empty()) return;
  std::latch shutdown_latch(steps_to_shutdown.size());

  for (auto* step : steps_to_shutdown) {
    auto stub = step->supervisor_stub;
    if (!stub) {
      CRANE_ERROR("[Step #{}.{}] Supervisor stub is null when terminating",
                  step->job_id, step->step_id);
      shutdown_latch.count_down();
      continue;
    }

    g_thread_pool->detach_task([step, stub = std::move(stub), &shutdown_latch] {
      CraneErrCode err;
      if (step->IsDaemonStep()) {
        CRANE_TRACE("[Step #{}.{}] Shutting down daemon supervisor.",
                    step->job_id, step->step_id);
        err = stub->ShutdownSupervisor();
      } else {
        CRANE_TRACE(
            "[Step #{}.{}] Terminating non-daemon supervisor for silent "
            "cleanup.",
            step->job_id, step->step_id);
        err = stub->TerminateStep(
            crane::grpc::TerminateSource::TERMINATE_SOURCE_NORMAL_COMPLETION);
      }
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("[Step #{}.{}] Failed to terminate supervisor.",
                    step->job_id, step->step_id);
      }
      shutdown_latch.count_down();
    });
  }
  shutdown_latch.wait();
}

void JobManager::StepStatusChangeAsync(
    job_id_t job_id, step_id_t step_id, crane::grpc::JobStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason,
    std::optional<crane::grpc::JobStatus> final_status,
    const google::protobuf::Timestamp& timestamp) {
  CRANE_INFO("[Step #{}.{}] is doing StepStatusChange, new status: {}", job_id,
             step_id, new_status);
  ActivateStepStatusChangeAsync_(job_id, step_id, new_status, exit_code,
                                 std::move(reason), std::move(final_status),
                                 std::move(timestamp));
}

bool JobManager::ReceivePmixPort(
    const std::vector<std::pair<CranedId, std::string>>& pmix_ports,
    job_id_t job_id, step_id_t step_id) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_ERROR(
        "[Step #{}.{}] Failed to find job allocation when receiving PMIx ports",
        job_id, step_id);
    return false;
  }

  absl::MutexLock lk(job->step_map_mtx.get());
  auto step_it = job->step_map.find(step_id);
  if (step_it == job->step_map.end()) {
    CRANE_ERROR(
        "[Step #{}.{}] Failed to find step allocation when receiving PMIx "
        "ports",
        job_id, step_id);
    return false;
  }

  auto& stub = step_it->second->supervisor_stub;
  if (!stub) {
    CRANE_ERROR(
        "[Step #{}.{}] Supervisor stub is null when receiving PMIx ports",
        job_id, step_id);
    return false;
  }

  auto result = stub->ReceivePmixPort(pmix_ports);
  if (result != CraneErrCode::SUCCESS) {
    CRANE_ERROR("[Step #{}.{}] Failed to deliver PMIx ports to supervisor: {}",
                job_id, step_id, CraneErrStr(result));
    return false;
  }

  CRANE_TRACE(
      "[Step #{}.{}] Successfully delivered {} PMIx ports to supervisor",
      job_id, step_id, pmix_ports.size());

  return true;
}

}  // namespace Craned
