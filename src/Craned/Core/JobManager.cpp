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

#include "CranedPublicDefs.h"
#include "CtldClient.h"
#include "crane/PluginClient.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"
#include "protos/Supervisor.pb.h"

namespace Craned {

using Common::kStepRequestCheckIntervalMs;
using namespace std::chrono_literals;

EnvMap JobInD::GetJobEnvMap() {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(job_to_d.res());

  auto& daemon_step_to_d = step_map.at(kDaemonStepId)->step_to_d;
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
                                          std::chrono::milliseconds{500ms});

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

  // Task Status Change Event
  m_task_status_change_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_task_status_change_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTaskStatusChangeQueueCb_();
      });

  m_terminate_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateTaskQueueCb_();
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
    std::unordered_map<task_id_t, JobInD>&& job_map,
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
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
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
    task_id_t job_id = job.job_id;
    uid_t uid = job.Uid();

    job_map_ptr->emplace(job_id, std::move(job));
    if (uid_map_ptr->contains(uid)) {
      uid_map_ptr->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map_ptr->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }

  return true;
}

bool JobManager::FreeJobs(std::set<task_id_t>&& job_ids) {
  std::vector<JobInD> jobs_to_free;
  std::vector<StepInstance*> steps_to_free;

  for (job_id_t job_id : job_ids) {
    auto job = FreeJobInfo_(job_id);
    if (!job.has_value()) {
      CRANE_INFO("Try to free non-existent job #{}.", job_id);
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

  for (const auto& step : steps) {
    auto job_ptr = m_job_map_.GetValueExclusivePtr(step.job_id());
    if (!job_ptr) {
      CRANE_WARN("Try to allocate step for nonexistent job#{}, ignoring it.",
                 step.job_id());
    } else {
      // Simply wrap the Task structure within an Execution structure and
      // pass it to the event loop. The cgroup field of this task is initialized
      // in the corresponding handler (EvGrpcExecuteTaskCb_).
      auto step_inst = std::make_unique<StepInstance>(step);
      step_inst->step_to_d = std::move(step);

      EvQueueAllocateStepElem elem{.step_inst = std::move(step_inst)};
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

bool JobManager::EvCheckSupervisorRunning_() {
  std::vector<JobInD> jobs_to_clean;
  // Step is completing, get ownership here.
  std::vector<std::unique_ptr<StepInstance>> steps_to_clean;
  {
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
        // TODO: Send status change for dead step
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
  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      CRANE_TRACE("Child pid {} exit", pid);
      // We do nothing now
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

    g_thread_pool->detach_task([this, execution = step_inst.release()] {
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

CraneExpected<void> JobManager::ChangeStepTimelimit(job_id_t job_id,
                                                    step_id_t step_id,
                                                    int64_t new_timelimit_sec) {
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
  auto err = stub->ChangeTaskTimeLimit(absl::Seconds(new_timelimit_sec));
  if (err != CraneErrCode::SUCCESS) {
    CRANE_ERROR(
        "[Step #{}.{}] Failed to change step timelimit to {} seconds via "
        "supervisor RPC",
        job_id, step_id, new_timelimit_sec);
    return std::unexpected{err};
  }
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
  return stub->QueryStepEnv();
}

void JobManager::EvCleanGrpcExecuteStepQueueCb_() {
  EvQueueExecuteStepElem elem;

  while (m_grpc_execute_step_queue_.try_dequeue(elem)) {
    // Once ExecuteTask RPC is processed, the Execution goes into m_job_map_.
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
  std::unordered_map<task_id_t, CgroupInterface*> job_cg_map;
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

void JobManager::LaunchStepMt_(std::unique_ptr<StepInstance> step) {
  // This function runs in a multi-threading manner. Take care of thread
  // safety. JobInstance will not be free during this function. Take care of
  // data race for job instance.

  job_id_t job_id = step->job_id;
  step_id_t step_id = step->step_id;
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id, step_id);
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::EC_CGROUP_ERR,
        fmt::format("Failed to get the allocation for job#{} ", job_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }
  auto* job = job_ptr.get();

  // Check if the step is acceptable.
  // TODO: Is this check necessary?
  if (step->IsContainer() && !g_config.Container.Enabled) {
    CRANE_ERROR("Container support is disabled but job #{} requires it.",
                job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
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
      ActivateTaskStatusChangeAsync_(
          job_id, step_id, crane::grpc::TaskStatus::Failed,
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
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErrCode err = step_ptr->CreateCg();
  if (err != CraneErrCode::SUCCESS) {
    step_ptr->err_before_supv_start = true;
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::EC_CGROUP_ERR,
        fmt::format("Cannot create cgroup for the instance of step {}.{}",
                    job_id, step_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }
  err = step_ptr->SpawnSupervisor(job->GetJobEnvMap());
  if (err != CraneErrCode::SUCCESS) {
    step_ptr->err_before_supv_start = true;
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::EC_SPAWN_FAILED,
        fmt::format("Cannot spawn a new process inside the instance of job #{}",
                    job_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
  } else {
    // kOk means that SpawnSupervisor_ has successfully forked a child
    // process. Now we put the child pid into index maps. SIGCHLD sent just
    // after fork() and before putting pid into maps will repeatedly be sent
    // by timer and eventually be handled once the SIGCHLD processing callback
    // sees the pid in index maps. Now we do not support launch multiple tasks
    // in a job.
    CRANE_TRACE("[Step #{}.{}] Supervisor spawned successfully.", job_id,
                step_id);
  }
}

void JobManager::EvCleanTaskStatusChangeQueueCb_() {
  StepStatusChangeQueueElem status_change;
  while (m_task_status_change_queue_.try_dequeue(status_change)) {
    {
      auto job_ptr = m_job_map_.GetValueExclusivePtr(status_change.job_id);
      if (job_ptr) {
        absl::MutexLock lk(job_ptr->step_map_mtx.get());
        if (auto step_it = job_ptr->step_map.find(status_change.step_id);
            step_it != job_ptr->step_map.end()) {
          step_it->second->GotNewStatus(status_change.new_status);
        }
      }
    }

    CRANE_TRACE("[Step #{}.{}] StepStatusChange status: {}.",
                status_change.job_id, status_change.step_id,
                status_change.new_status);
    g_ctld_client->StepStatusChangeAsync(std::move(status_change));
  }
}

void JobManager::ActivateTaskStatusChangeAsync_(
    job_id_t job_id, step_id_t step_id, crane::grpc::TaskStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason,
    google::protobuf::Timestamp timestamp) {
  StepStatusChangeQueueElem status_change{.job_id = job_id,
                                          .step_id = step_id,
                                          .new_status = new_status,
                                          .exit_code = exit_code,
                                          .timestamp = timestamp};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  m_task_status_change_async_handle_->send();
}

/**
 * @brief Move process on craned to specify job's cgroup. Called from
 * CranedServer
 * @param pid process pid
 * @param job_id
 * @return True if job and cgroup exists
 */
bool JobManager::MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id) {
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
  }
  auto& daemon_step = daemon_step_it->second;
  auto stub = daemon_step->supervisor_stub;
  if (!stub) {
    CRANE_ERROR(
        "[Job #{}] Daemon step sSupervisor stub is null when migrating pid {} "
        "to "
        "cgroup of job#{}.",
        job_id, kDaemonStepId, pid, job_id);
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
    ;
    for (auto& [step_id, step] : job_ptr->step_map) {
      step_status_map[step_id] = step->status;
    }
    job_steps[job_id] = std::move(step_status_map);
  }
  return job_steps;
}

std::optional<TaskInfoOfUid> JobManager::QueryTaskInfoOfUid(uid_t uid) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  TaskInfoOfUid info;
  info.job_cnt = 0;
  info.cgroup_exists = false;

  if (auto task_ids = this->m_uid_to_job_ids_map_[uid]; task_ids) {
    info.cgroup_exists = true;
    info.job_cnt = task_ids->size();
    info.first_task_id = *task_ids->begin();
  } else {
    CRANE_WARN("Uid {} not found in uid_to_task_ids_map", uid);
    return std::nullopt;
  }
  return info;
}

void JobManager::EvCleanTerminateTaskQueueCb_() {
  StepTerminateQueueElem elem;
  std::vector<StepTerminateQueueElem> not_ready_elems;

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_step_ids;

  while (m_step_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Step: {}.{}",
        elem.job_id, elem.step_id);

    std::vector<StepInstance*> steps_to_clean;
    std::vector<JobInD> job_to_clean;
    {
      CRANE_INFO(
          "[Step #{}.{}] Terminating step, orphaned:{} terminated_by_user:{}.",
          elem.job_id, elem.step_id, elem.mark_as_orphaned,
          elem.terminated_by_user);
      bool terminate_job = elem.step_id == kDaemonStepId;
      auto map_ptr = m_job_map_.GetMapExclusivePtr();
      if (!map_ptr->contains(elem.job_id)) {
        CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent job.",
                    elem.job_id, elem.step_id);

        continue;
      }
      auto job_instance = map_ptr->at(elem.job_id).RawPtr();

      absl::MutexLock lk(job_instance->step_map_mtx.get());
      if (!job_instance->step_map.contains(elem.step_id)) {
        CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent step.",
                    elem.job_id, elem.step_id);

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

      for (auto step_id : terminate_step_ids) {
        auto& step_instance = job_instance->step_map.at(step_id);
        auto stub = step_instance->supervisor_stub;
        auto err =
            stub->TerminateTask(elem.mark_as_orphaned, elem.terminated_by_user);
        if (err != CraneErrCode::SUCCESS) {
          // Supervisor dead for some reason.
          CRANE_ERROR("[Step #{}.{}] Failed to terminate.", elem.job_id,
                      step_id);
          if (!elem.mark_as_orphaned)
            g_ctld_client->StepStatusChangeAsync(
                {.job_id = elem.job_id,
                 .step_id = step_id,
                 .new_status = crane::grpc::TaskStatus::Cancelled,
                 .exit_code = 0U,
                 .reason = "Termination failed."});
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

void JobManager::TerminateStepAsync(job_id_t job_id, step_id_t step_id) {
  StepTerminateQueueElem elem{.job_id = job_id,
                              .step_id = step_id,
                              .terminated_by_user = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

void JobManager::MarkStepAsOrphanedAndTerminateAsync(job_id_t job_id,
                                                     step_id_t step_id) {
  StepTerminateQueueElem elem{.job_id = job_id,
                              .step_id = step_id,
                              .mark_as_orphaned = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

void JobManager::CleanUpJobAndStepsAsync(std::vector<JobInD>&& jobs,
                                         std::vector<StepInstance*>&& steps) {
  std::latch shutdown_step_latch(steps.size());
  for (auto* step : steps) {
    // Only daemon step needs manual shutdown, others will shut down itself when
    // all task finished.
    if (!step->IsDaemonStep() || step->err_before_supv_start) {
      shutdown_step_latch.count_down();
      continue;
    }
    step->GotNewStatus(StepStatus::Completed);

    g_thread_pool->detach_task([&shutdown_step_latch, step] {
      auto stub = step->supervisor_stub;
      CRANE_TRACE("[Step #{}.{}] Shutting down supervisor.", step->job_id,
                  step->step_id);
      auto err = stub->ShutdownSupervisor();
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("[Step #{}.{}] Failed to shutdown supervisor.",
                    step->job_id, step->step_id);
        g_job_mgr->StepStatusChangeAsync(
            step->job_id, step->step_id, crane::grpc::TaskStatus::Failed,
            ExitCode::EC_RPC_ERR, "Failed to shutdown supervisor.",
            google::protobuf::util::TimeUtil::GetCurrentTime());
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
    m_completing_job_.emplace(job_id, std::move(job));
  }
}

void JobManager::StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                                       crane::grpc::TaskStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string> reason,
                                       google::protobuf::Timestamp timestamp) {
  CRANE_INFO("[Step #{}.{}] is doing StepStatusChange, new status: {}", job_id,
             step_id, new_status);
  ActivateTaskStatusChangeAsync_(job_id, step_id, new_status, exit_code,
                                 std::move(reason), timestamp);
}

}  // namespace Craned
