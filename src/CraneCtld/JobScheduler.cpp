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

#include "JobScheduler.h"

#include <absl/time/internal/cctz/src/time_zone_if.h>
#include <google/protobuf/util/time_util.h>

#include "AccountManager.h"
#include "AccountMetaContainer.h"
#include "CranedMetaContainer.h"
#include "CtldPublicDefs.h"
#include "EmbeddedDbClient.h"
#include "LicensesManager.h"
#include "Lua/LuaJobHandler.h"
#include "RpcService/CranedKeeper.h"
#include "crane/PluginClient.h"
#include "protos/Crane.pb.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {
using namespace std::chrono_literals;

namespace {

bool PersistJobRuntimeStateNoLock_(JobInCtld* job, task_id_t job_id,
                                   crane::grpc::JobStatus status,
                                   absl::Time end_time, absl::Time suspend_time,
                                   const char* failure_reason) {
  auto prev_status = job->Status();
  auto prev_end_time = job->EndTime();
  auto prev_suspend_time = job->SuspendTime();

  job->SetStatus(status);
  job->SetEndTime(end_time);
  job->SetSuspendTime(suspend_time);

  if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
          0, job->JobDbId(), job->RuntimeAttr())) {
    job->SetStatus(prev_status);
    job->SetEndTime(prev_end_time);
    job->SetSuspendTime(prev_suspend_time);
    CRANE_ERROR("Failed to persist runtime state for job #{} during {}", job_id,
                failure_reason);
    return false;
  }

  return true;
}

bool SuspendJobOnNodes_(const std::vector<CranedId>& craned_ids,
                        const std::vector<job_id_t>& broadcast_job_ids,
                        task_id_t job_id, const char* failure_reason) {
  if (craned_ids.empty()) return true;

  bool suspend_failed = false;
  absl::Mutex suspend_result_mtx;
  absl::BlockingCounter suspend_counter(craned_ids.size());

  for (const auto& craned_id : craned_ids) {
    g_thread_pool->detach_task([&, craned_id]() {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      if (!stub || stub->Invalid()) {
        CRANE_ERROR(
            "{} for job #{} on craned {}: stub unavailable. "
            "Node may remain running.",
            failure_reason, job_id, craned_id);
        {
          absl::MutexLock lock(&suspend_result_mtx);
          suspend_failed = true;
        }
        suspend_counter.DecrementCount();
        return;
      }

      CraneErrCode err = stub->SuspendJobs(broadcast_job_ids);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("{} for job #{} on craned {}: {}. Node may remain running.",
                    failure_reason, job_id, craned_id, CraneErrStr(err));
        {
          absl::MutexLock lock(&suspend_result_mtx);
          suspend_failed = true;
        }
      }

      suspend_counter.DecrementCount();
    });
  }

  suspend_counter.Wait();
  return !suspend_failed;
}

}  // namespace

JobScheduler::JobScheduler() {
  if (g_config.PriorityConfig.Type == Config::Priority::Basic) {
    CRANE_INFO("basic priority sorter is selected.");
    m_priority_sorter_ = std::make_unique<BasicPriority>();
  } else if (g_config.PriorityConfig.Type == Config::Priority::MultiFactor) {
    CRANE_INFO("multifactorial priority sorter is selected.");
    m_priority_sorter_ = std::make_unique<MultiFactorPriority>();
  }

  m_node_selection_algo_ =
      std::make_unique<SchedulerAlgo>(m_priority_sorter_.get());
  m_rpc_worker_pool_ = std::make_unique<BS::thread_pool>(
      std::max(g_config.Nodes.size() / kNodePerRpcWorker, kMinRpcWorkerNum),
      [] { util::SetCurrentThreadName("SchedRpcWorker"); });
}

JobScheduler::~JobScheduler() {
  m_thread_stop_ = true;
  if (m_schedule_thread_.joinable()) m_schedule_thread_.join();
  if (m_step_schedule_thread_.joinable()) m_step_schedule_thread_.join();
  if (m_job_release_thread_.joinable()) m_job_release_thread_.join();
  if (m_job_cancel_thread_.joinable()) m_job_cancel_thread_.join();
  if (m_job_submit_thread_.joinable()) m_job_submit_thread_.join();
  if (m_step_submit_thread_.joinable()) m_step_submit_thread_.join();
  if (m_job_status_change_thread_.joinable())
    m_job_status_change_thread_.join();
  if (m_resv_clean_thread_.joinable()) m_resv_clean_thread_.join();
  if (m_job_deadline_timer_thread_.joinable())
    m_job_deadline_timer_thread_.join();
  m_rpc_worker_pool_->wait();
  m_rpc_worker_pool_.reset();
}

bool JobScheduler::Init() {
  using crane::grpc::JobInEmbeddedDb;

  bool ok;
  CraneErrCode err;

  uvw_deadline_loop = uvw::loop::create();
  using DeadlineTimerQueueElem = std::pair<job_id_t, int64_t>;
  std::vector<DeadlineTimerQueueElem> deadline_timer_vec;

  EmbeddedDbClient::DbSnapshot snapshot;
  ok = g_embedded_db_client->RetrieveLastSnapshot(&snapshot);
  if (!ok) {
    CRANE_ERROR("Failed to retrieve embedded DB snapshot!");
    return false;
  }

  auto& running_queue = snapshot.running_queue;
  std::unordered_map<job_id_t, std::unique_ptr<JobInCtld>>
      recovered_running_jobs;
  if (!running_queue.empty()) {
    CRANE_INFO("{} running job(s) recovered.", running_queue.size());

    for (auto&& [job_db_id, job_in_embedded_db] : running_queue) {
      auto job = std::make_unique<JobInCtld>();
      job->SetFieldsByJobToCtld(job_in_embedded_db.job_to_ctld());
      // Must be called after SetFieldsByJobToCtld!
      job->SetFieldsByRuntimeAttrOfJob(job_in_embedded_db.runtime_attr());
      job_id_t job_id = job->JobId();

      CRANE_TRACE("Restore job #{} from embedded running queue.", job->JobId());
      auto result = AcquireJobAttributes(job.get());
      if (!result || job->type == crane::grpc::Interactive) {
        job->SetStatus(crane::grpc::Failed);
        job->SetEndTime(absl::Now());
        ok = g_embedded_db_client->UpdateRuntimeAttrOfJob(0, job_db_id,
                                                          job->RuntimeAttr());
        if (!ok) {
          CRANE_ERROR(
              "UpdateRuntimeAttrOfJob failed for job #{} when "
              "mark the job as FAILED.",
              job_id);
        }
        if (!result)
          CRANE_INFO(
              "Failed to acquire job attributes for restored running job "
              "#{}. "
              "Error Code: {}. "
              "Mark it as FAILED and move it to the ended queue.",
              job_id, CraneErrStr(result.error()));
        else {
          CRANE_INFO("Mark running interactive job {} as FAILED.", job_id);

          ok = g_db_client->InsertJob(job.get());
          if (!ok) {
            CRANE_ERROR(
                "InsertJob failed for job #{} "
                "when recovering running queue.",
                job->JobId());
          }

          ok =
              g_embedded_db_client->PurgeEndedJobs({{job->JobId(), job_db_id}});
          if (!ok) {
            CRANE_ERROR(
                "PurgeEndedJobs failed for job #{} when recovering "
                "running queue.",
                job->JobId());
          }
        }

        // Move this problematic job into ended queue and
        // process next job.
        continue;
      }
      recovered_running_jobs.emplace(job_id, std::move(job));
    }
  }
  //  Process the pending jobs in the embedded pending queue.
  auto& pending_queue = snapshot.pending_queue;
  absl::Time infiniteFuture = absl::FromUnixSeconds(kJobMaxTimeStampSec);
  if (!pending_queue.empty()) {
    CRANE_INFO("{} pending job(s) recovered.", pending_queue.size());

    absl::Time recovery_time = absl::Now();
    for (auto&& [job_db_id, job_in_embedded_db] : pending_queue) {
      auto job = std::make_unique<JobInCtld>();
      job->SetFieldsByJobToCtld(job_in_embedded_db.job_to_ctld());
      // Must be called after SetFieldsByJobToCtld!
      job->SetFieldsByRuntimeAttrOfJob(job_in_embedded_db.runtime_attr());

      job_id_t job_id = job->JobId();

      CRANE_TRACE("Restore job #{} from embedded pending queue.", job->JobId());

      bool mark_job_as_failed = false;

      if (job->type == crane::grpc::Interactive) {
        CRANE_INFO("Mark interactive job #{} as FAILED", job_id);
        mark_job_as_failed = true;
      }

      if (!mark_job_as_failed && !(AcquireJobAttributes(job.get()))) {
        CRANE_ERROR("AcquireJobAttributes failed for job #{}", job_id);
        mark_job_as_failed = true;
      }

      if (!mark_job_as_failed && !CheckJobValidity(job.get())) {
        CRANE_ERROR("CheckJobValidity failed for job #{}", job_id);
        mark_job_as_failed = true;
      }

      if (!mark_job_as_failed) {
        const auto& user_ptr =
            g_account_manager->GetExistedUserInfo(job->Username());
        if (!user_ptr) {
          CRANE_ERROR(
              "The current user {} is not in the user list when submitting the "
              "job",
              job->Username());
          mark_job_as_failed = true;
        } else
          g_account_meta_container->UserAddJob(job->Username());
      }

      if (!mark_job_as_failed && job->deadline_time != infiniteFuture) {
        deadline_timer_vec.emplace_back(
            job_id, absl::ToUnixSeconds(job->deadline_time));
      }

      if (!mark_job_as_failed) {
        RequeueRecoveredJobIntoPendingQueueLock_(std::move(job));
      } else {
        // If a batch job failed to requeue the job into pending queue due to
        // insufficient resource or other reasons or the job is an interactive
        // job , Mark it as FAILED and move it to the ended queue.
        CRANE_INFO(
            "Failed to requeue job #{}. Mark it as FAILED and "
            "move it to the ended queue.",
            job_id);
        job->SetStatus(crane::grpc::Failed);
        job->SetStartTime(recovery_time);
        job->SetEndTime(recovery_time);
        ok = g_embedded_db_client->UpdateRuntimeAttrOfJob(0, job_db_id,
                                                          job->RuntimeAttr());
        if (!ok) {
          CRANE_ERROR(
              "UpdateRuntimeAttrOfJob failed for job #{} when "
              "mark the job as FAILED.",
              job_id);
        }

        ok = g_db_client->InsertJob(job.get());
        if (!ok) {
          CRANE_ERROR(
              "InsertJob failed for job #{} when recovering pending "
              "queue.",
              job->JobId());
        }

        ok = g_embedded_db_client->PurgeEndedJobs(
            {{job->JobId(), job->JobDbId()}});
        if (!ok) {
          CRANE_ERROR(
              "PurgeEndedJobs failed for job #{} when recovering "
              "pending queue.",
              job->JobId());
        }
      }
    }
  }

  if (!snapshot.final_queue.empty()) {
    CRANE_INFO("{} final job(s) might not have been put to mongodb.",
               snapshot.final_queue.size());

    std::unordered_map<job_id_t, job_db_id_t> db_ids;
    for (auto& [db_id, job_in_embedded_db] : snapshot.final_queue) {
      job_id_t job_id = job_in_embedded_db.runtime_attr().job_id();
      ok = g_db_client->CheckJobDbIdExisted(db_id);
      if (!ok) {
        if (!g_db_client->InsertRecoveredJob(job_in_embedded_db)) {
          CRANE_ERROR(
              "Failed to call g_db_client->InsertRecoveredJob() "
              "for job #{}",
              job_id);
        }
      }

      db_ids[job_id] = db_id;
    }

    ok = g_embedded_db_client->PurgeEndedJobs(db_ids);
    if (!ok) {
      CRANE_ERROR("Failed to call g_embedded_db_client->PurgeEndedJobs()");
    }
  }

  EmbeddedDbClient::StepDbSnapshot step_snapshot;
  ok = g_embedded_db_client->RetrieveStepInfo(&step_snapshot);
  if (!ok) {
    CRANE_ERROR("Failed to retrieve embedded DB step snapshot!");
    return false;
  }
  CRANE_INFO("{} step(s) fetched from DB.",
             std::ranges::fold_left(
                 step_snapshot.steps | std::views::values |
                     std::views::transform(
                         [](const auto& step_vec) { return step_vec.size(); }),
                 0, std::plus{}));
  std::unordered_map<job_id_t, std::vector<crane::grpc::StepInEmbeddedDb>>
      invalid_steps;
  std::vector<crane::grpc::StepInEmbeddedDb> completed_steps;
  const std::unordered_set completed_step_status{
      crane::grpc::JobStatus::Completed,
      crane::grpc::JobStatus::Failed,
      crane::grpc::JobStatus::ExceedTimeLimit,
      crane::grpc::JobStatus::Cancelled,
      crane::grpc::JobStatus::OutOfMemory,
  };
  auto mark_job_invalid = [&recovered_running_jobs](JobInCtld* job) {
    job_id_t job_id = job->JobId();
    CRANE_ERROR("[Job #{}] Running job without step, mark the job as FAILED!",
                job_id);
    job->SetStatus(crane::grpc::Failed);
    auto ok = g_embedded_db_client->UpdateRuntimeAttrOfJob(0, job->JobDbId(),
                                                           job->RuntimeAttr());
    if (!ok) {
      CRANE_ERROR(
          "[Job #{}] UpdateRuntimeAttrOfJob failed when "
          "mark the job as FAILED.",
          job_id);
    }

    ok = g_db_client->InsertJob(job);
    if (!ok) {
      CRANE_ERROR(
          "InsertJob failed for job #{} when recovering pending "
          "queue.",
          job_id);
    }

    ok = g_embedded_db_client->PurgeEndedJobs({{job->JobId(), job->JobDbId()}});
    if (!ok) {
      CRANE_ERROR(
          "PurgeEndedJobs failed for job #{} when recovering "
          "pending queue.",
          job_id);
    }
    auto it = recovered_running_jobs.find(job_id);
    return recovered_running_jobs.erase(it);
  };
  // When store, write/purge step info first, then job info.
  // When read, iterate by job.
  for (auto job_it = recovered_running_jobs.begin();
       job_it != recovered_running_jobs.end();) {
    auto& [job_id, job] = *job_it;
    auto it = step_snapshot.steps.find(job_id);
    if (it == step_snapshot.steps.end()) {
      job_it = mark_job_invalid(job.get());
      continue;
    }
    for (auto&& step_info : it->second) {
      step_id_t step_id = step_info.runtime_attr().step_id();
      auto step_type = step_info.runtime_attr().step_type();
      if (step_type == crane::grpc::StepType::INVALID) {
        CRANE_ERROR("[Step #{}{}] Invalid step type, dropped!", job_id,
                    step_id);
        invalid_steps[job_id].emplace_back(std::move(step_info));
        continue;
      }

      auto step_status = step_info.runtime_attr().status();
      if (completed_step_status.contains(step_status)) {
        CRANE_INFO("[Step #{}{}] Step is completed, put to mongodb!", job_id,
                   step_id);
        completed_steps.emplace_back(std::move(step_info));
      }
      std::unique_ptr<StepInCtld> step;
      if (step_type == crane::grpc::StepType::DAEMON) {
        step = std::make_unique<DaemonStepInCtld>();
      } else {
        step = std::make_unique<CommonStepInCtld>();
      }

      step->RecoverFromDb(*job, step_info);
      if (auto err_expt = AcquireStepAttributes(step.get());
          !err_expt.has_value()) {
        CRANE_ERROR(
            "[Step #{}.{}] AcquireStepAttributes failed: {}, step dropped!",
            job_id, step_id, CraneErrStr(err_expt.error()));
        step.reset();
        invalid_steps[job_id].emplace_back(std::move(step_info));

        continue;
      }

      if (auto err_expt = CheckStepValidity(step.get());
          !err_expt.has_value()) {
        CRANE_ERROR("[Step #{}.{}] CheckStepValidity failed: {}, step dropped!",
                    job_id, step_id, CraneErrStr(err_expt.error()));
        step.reset();
        invalid_steps[job_id].emplace_back(std::move(step_info));
        continue;
      }

      if (step_status == crane::grpc::JobStatus::Pending) {
        // Not support to recover pending step now. All pending steps are crun
        // which can not recover now.
        step.reset();
        invalid_steps[job_id].emplace_back(std::move(step_info));
        continue;
      }

      if (step_type == crane::grpc::StepType::DAEMON) {
        job->SetDaemonStep(std::unique_ptr<DaemonStepInCtld>(
            static_cast<DaemonStepInCtld*>(step.release())));

        CRANE_INFO("Daemon step recovered for job #{}", job->JobId());

      } else if (step_type == crane::grpc::StepType::PRIMARY) {
        CRANE_INFO("Primary step recovered for job #{}", job->JobId());

        job->SetPrimaryStep(std::unique_ptr<CommonStepInCtld>(
            static_cast<CommonStepInCtld*>(step.release())));
      } else {
        CRANE_INFO("Common step {} recovered for job #{}", step->StepId(),
                   job->JobId());
        job->AddStep(std::unique_ptr<CommonStepInCtld>(
            static_cast<CommonStepInCtld*>(step.release())));
      }
    }

    if (!job->PrimaryStep() && !job->DaemonStep() && job->Steps().empty()) {
      job_it = mark_job_invalid(job.get());
    } else {
      ++job_it;
    }
    step_snapshot.steps.erase(it);
  }

  for (auto& [job_id, step] : step_snapshot.steps) {
    for (auto& step_info : step) {
      step_id_t step_id = step_info.runtime_attr().step_id();
      CRANE_ERROR("[Step #{}.{}] without a valid job, dropped.", job_id,
                  step_id);
      invalid_steps[job_id].emplace_back(std::move(step_info));
    }
  }
  for (auto& [job_id, job] : recovered_running_jobs)
    PutRecoveredJobIntoRunningQueueLock_(std::move(job));

  {
    std::unordered_map<
        job_id_t, std::vector<std::pair<job_id_t, crane::grpc::DependencyType>>>
        dependee_to_dependents;

    for (const auto& [job_id, job] : m_pending_job_map_) {
      for (const auto& [dep_id, dep_info] : job->Dependencies().deps) {
        const auto& [dep_type, delay_seconds] = dep_info;
        dependee_to_dependents[dep_id].emplace_back(job_id, dep_type);
      }
    }

    std::unordered_set<job_id_t> missing_dependee_ids;

    for (const auto& [dependee_id, dependents] : dependee_to_dependents) {
      if (m_pending_job_map_.contains(dependee_id)) {
        for (const auto& dep_info : dependents) {
          m_pending_job_map_.at(dependee_id)
              ->AddDependent(dep_info.second, dep_info.first);
        }
      } else if (m_running_job_map_.contains(dependee_id)) {
        for (const auto& dep_info : dependents) {
          m_running_job_map_.at(dependee_id)
              ->AddDependent(dep_info.second, dep_info.first);
        }
        continue;
      } else {
        missing_dependee_ids.insert(dependee_id);
      }
    }

    if (!missing_dependee_ids.empty()) {
      CRANE_INFO("Querying MongoDB for {} missing dependee job(s)",
                 missing_dependee_ids.size());

      auto dependee_status_map =
          g_db_client->FetchJobStatus(missing_dependee_ids);

      for (auto& [job_id, job] : m_pending_job_map_) {
        for (const auto& [dep_id, dep_info] : job->Dependencies().deps) {
          if (!missing_dependee_ids.contains(dep_id)) continue;

          const auto& [dep_type, delay_seconds] = dep_info;
          absl::Time event_time = absl::InfiniteFuture();
          auto it = dependee_status_map.find(dep_id);
          if (it != dependee_status_map.end()) {
            const auto& [status, exit_code, time_end, time_start] = it->second;

            switch (dep_type) {
            case crane::grpc::DependencyType::AFTER:
              if (time_start != 0) {
                event_time = absl::FromUnixSeconds(time_start);
              } else if (status == crane::grpc::Cancelled) {
                event_time = absl::FromUnixSeconds(time_end);
              } else {
                CRANE_ERROR(
                    "Dependee Job #{} ended without start or cancel time.",
                    dep_id);
              }
              break;
            case crane::grpc::DependencyType::AFTER_ANY:
              if (time_end != 0) {
                event_time = absl::FromUnixSeconds(time_end);
              } else {
                event_time = absl::Now();
                CRANE_ERROR("Dependee Job #{} ended without end time.", dep_id);
              }
              break;
            case crane::grpc::DependencyType::AFTER_OK:
              if (status == crane::grpc::Completed && exit_code == 0) {
                event_time = absl::FromUnixSeconds(time_end);
              }
              break;
            case crane::grpc::DependencyType::AFTER_NOT_OK:
              if (exit_code != 0) {
                event_time = absl::FromUnixSeconds(time_end);
              }
              break;
            default:
              std::unreachable();
            }
          } else {
            CRANE_WARN("Dependee Job #{} not found in database.", dep_id);
          }

          AddDependencyEvent(job_id, dep_id, event_time);
        }
      }
    }
  }

  std::vector<step_db_id_t> purged_step_db_ids;
  for (auto& steps : invalid_steps | std::views::values) {
    for (auto& step_info : steps)
      purged_step_db_ids.push_back(step_info.runtime_attr().step_db_id());
  }
  for (auto& step_info : completed_steps) {
    purged_step_db_ids.push_back(step_info.runtime_attr().step_db_id());
    if (!g_db_client->CheckStepExisted(step_info.step_to_ctld().job_id(),
                                       step_info.runtime_attr().step_id())) {
      g_db_client->InsertRecoveredStep(step_info);
    }
  }
  g_embedded_db_client->PurgeEndedSteps(purged_step_db_ids);

  std::shared_ptr<uvw::loop> uvw_release_loop = uvw::loop::create();
  m_job_timer_handle_ = uvw_release_loop->resource<uvw::timer_handle>();
  m_job_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        CleanJobTimerCb_();
      });
  m_job_timer_handle_->start(
      std::chrono::milliseconds(kJobHoldTimerTimeoutMs * 3),
      std::chrono::milliseconds(kJobHoldTimerTimeoutMs));

  m_job_timeout_async_handle_ = uvw_release_loop->resource<uvw::async_handle>();
  m_job_timeout_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        JobTimerAsyncCb_();
      });

  m_clean_job_timer_queue_handle_ =
      uvw_release_loop->resource<uvw::async_handle>();
  m_clean_job_timer_queue_handle_->on<uvw::async_event>(
      [this, loop = uvw_release_loop](const uvw::async_event&,
                                      uvw::async_handle&) {
        CleanJobTimerQueueCb_(loop);
      });

  m_job_release_thread_ = std::thread(
      [this, loop = uvw_release_loop]() { ReleaseJobThread_(loop); });

  std::shared_ptr<uvw::loop> uvw_cancel_loop = uvw::loop::create();
  m_cancel_job_timer_handle_ = uvw_cancel_loop->resource<uvw::timer_handle>();
  m_cancel_job_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        CancelJobTimerCb_();
      });
  m_cancel_job_timer_handle_->start(
      std::chrono::milliseconds(kCancelJobTimeoutMs * 3),
      std::chrono::milliseconds(kCancelJobTimeoutMs));

  m_cancel_job_async_handle_ = uvw_cancel_loop->resource<uvw::async_handle>();
  m_cancel_job_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CancelJobAsyncCb_();
      });

  m_clean_cancel_job_queue_handle_ =
      uvw_cancel_loop->resource<uvw::async_handle>();
  m_clean_cancel_job_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanCancelJobQueueCb_();
      });

  m_job_cancel_thread_ = std::thread(
      [this, loop = std::move(uvw_cancel_loop)]() { CancelJobThread_(loop); });

  std::shared_ptr<uvw::loop> uvw_submit_loop = uvw::loop::create();
  m_submit_job_timer_handle_ = uvw_submit_loop->resource<uvw::timer_handle>();
  m_submit_job_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        SubmitJobTimerCb_();
      });
  m_submit_job_timer_handle_->start(
      std::chrono::milliseconds(kSubmitJobTimeoutMs * 3),
      std::chrono::milliseconds(kSubmitJobTimeoutMs));

  m_submit_job_async_handle_ = uvw_submit_loop->resource<uvw::async_handle>();
  m_submit_job_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        SubmitJobAsyncCb_();
      });

  m_clean_submit_job_queue_handle_ =
      uvw_submit_loop->resource<uvw::async_handle>();
  m_clean_submit_job_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanSubmitJobQueueCb_();
      });

  m_job_submit_thread_ = std::thread(
      [this, loop = std::move(uvw_submit_loop)]() { SubmitJobThread_(loop); });

  std::shared_ptr<uvw::loop> uvw_submit_step_loop = uvw::loop::create();
  m_submit_step_timer_handle_ =
      uvw_submit_step_loop->resource<uvw::timer_handle>();
  m_submit_step_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        SubmitStepTimerCb_();
      });
  m_submit_step_timer_handle_->start(
      std::chrono::milliseconds(kSubmitJobTimeoutMs * 3),
      std::chrono::milliseconds(kSubmitJobTimeoutMs));

  m_submit_step_async_handle_ =
      uvw_submit_step_loop->resource<uvw::async_handle>();
  m_submit_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        SubmitStepAsyncCb_();
      });

  m_clean_step_submit_queue_handle_ =
      uvw_submit_step_loop->resource<uvw::async_handle>();
  m_clean_step_submit_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanStepSubmitQueueCb_();
      });

  m_step_submit_thread_ =
      std::thread([this, loop = std::move(uvw_submit_step_loop)]() {
        StepSubmitThread_(loop);
      });

  std::shared_ptr<uvw::loop> uvw_job_status_change_loop = uvw::loop::create();
  m_job_status_change_timer_handle_ =
      uvw_job_status_change_loop->resource<uvw::timer_handle>();
  m_job_status_change_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        JobStatusChangeTimerCb_();
      });
  m_job_status_change_timer_handle_->start(
      std::chrono::milliseconds(kJobStatusChangeTimeoutMS * 3),
      std::chrono::milliseconds(kJobStatusChangeTimeoutMS));

  m_job_status_change_async_handle_ =
      uvw_job_status_change_loop->resource<uvw::async_handle>();
  m_job_status_change_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        JobStatusChangeAsyncCb_();
      });

  m_clean_job_status_change_handle_ =
      uvw_job_status_change_loop->resource<uvw::async_handle>();
  m_clean_job_status_change_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanJobStatusChangeQueueCb_();
      });

  m_job_status_change_thread_ =
      std::thread([this, loop = std::move(uvw_job_status_change_loop)]() {
        JobStatusChangeThread_(loop);
      });

  std::shared_ptr<uvw::loop> uvw_reservation_loop = uvw::loop::create();

  m_clean_resv_timer_queue_handle_ =
      uvw_reservation_loop->resource<uvw::async_handle>();
  m_clean_resv_timer_queue_handle_->on<uvw::async_event>(
      [this, loop = uvw_reservation_loop](const uvw::async_event&,
                                          uvw::async_handle&) {
        CleanResvTimerQueueCb_(loop);
      });

  m_resv_clean_thread_ = std::thread(
      [this, loop = uvw_reservation_loop]() { CleanResvThread_(loop); });

  // TODO: Move this to Reservation Mini-Scheduler.
  // Reservation should be recovered after creating m_resv_clean_thread_ thread.
  std::unordered_map<ResvId, crane::grpc::CreateReservationRequest>
      resv_req_map;
  ok = g_embedded_db_client->RetrieveReservationInfo(&resv_req_map);
  if (!ok) {
    CRANE_ERROR("Failed to retrieve reservation info from embedded DB.");
    return false;
  }

  if (!resv_req_map.empty()) {
    CRANE_INFO("{} reservation(s) recovered.", resv_req_map.size());
    for (auto&& [resv_id, reservation_req] : resv_req_map) {
      auto res = CreateResv_(reservation_req);
      if (res) continue;

      CRANE_ERROR("Failed to add reservation {}: {}", resv_id, res.error());

      txn_id_t txn_id{0};
      auto ok = g_embedded_db_client->BeginReservationDbTransaction(&txn_id);
      if (!ok)
        CRANE_ERROR("Failed to begin transaction for reservation {}.", resv_id);

      ok = g_embedded_db_client->DeleteReservationInfo(txn_id, resv_id);
      if (!ok)
        CRANE_ERROR("Failed to delete reservation {} from resv DB.", resv_id);

      ok = g_embedded_db_client->CommitReservationDbTransaction(txn_id);
      if (!ok) CRANE_ERROR("Failed to commit txn for reservation {}.", resv_id);
    }
  }

  m_job_deadline_timer_async_handle_ =
      uvw_deadline_loop->resource<uvw::async_handle>();
  m_job_deadline_timer_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CancelDeadlineJobCb_();
      });
  m_job_deadline_timer_thread_ =
      std::thread([this]() { DeadlineTimerThread_(); });

  m_job_deadline_timer_create_async_handle_ =
      uvw_deadline_loop->resource<uvw::async_handle>();
  m_job_deadline_timer_create_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CreateDeadlineTimerCb_();
      });

  if (!deadline_timer_vec.empty()) {
    m_job_deadline_timer_create_queue_.enqueue_bulk(deadline_timer_vec.data(),
                                                    deadline_timer_vec.size());
    m_job_deadline_timer_create_async_handle_->send();
  }

  // Start schedule thread first.
  m_schedule_thread_ = std::thread([this] { ScheduleThread_(); });
  m_step_schedule_thread_ = std::thread([this] { StepScheduleThread_(); });
  return true;
}

void JobScheduler::RequeueRecoveredJobIntoPendingQueueLock_(
    std::unique_ptr<JobInCtld> job) {
  // The newly modified QoS resource limits do not apply to jobs that have
  // already been evaluated, which is the same as before the restart.
  g_account_meta_container->MallocQosSubmitResource(*job);

  // The order of LockGuards matters.
  LockGuard pending_guard(&m_pending_job_map_mtx_);
  m_pending_job_map_.emplace(job->JobId(), std::move(job));
}

void JobScheduler::PutRecoveredJobIntoRunningQueueLock_(
    std::unique_ptr<JobInCtld> job) {
  // The newly modified QoS resource limits do not apply to jobs that have
  // already been evaluated, which is the same as before the restart.
  g_account_meta_container->MallocQosResourceToRecoveredRunningJob(*job);

  for (const CranedId& craned_id : job->CranedIds())
    g_meta_container->MallocResourceFromNode(craned_id, job->JobId(),
                                             job->AllocatedRes());
  if (!job->reservation.empty()) {
    g_meta_container->MallocResourceFromResv(job->reservation, job->JobId(),
                                             job->AllocatedRes());
  }
  if (!job->licenses_count.empty())
    g_licenses_manager->MallocLicenseWhenRecoverRunning(job->licenses_count);

  // The order of LockGuards matters.
  LockGuard running_guard(&m_running_job_map_mtx_);
  LockGuard indexes_guard(&m_job_indexes_mtx_);

  for (const CranedId& craned_id : job->CranedIds())
    m_node_to_jobs_map_[craned_id].emplace(job->JobId());

  m_running_job_map_.emplace(job->JobId(), std::move(job));
}

void JobScheduler::ReleaseJobThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("ReleaseJobThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(50ms);
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in cancel loop.");
  }

  uvw_loop->run();
}

void JobScheduler::CancelJobThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("CancelJobThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(50ms);
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in cancel loop.");
  }

  uvw_loop->run();
}

void JobScheduler::SubmitJobThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("SubmitJobThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(50ms);
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in submit loop.");
  }

  uvw_loop->run();
}

void JobScheduler::StepSubmitThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("SubmitStepThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(50ms);
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in submit step loop.");
  }

  uvw_loop->run();
}

void JobScheduler::JobStatusChangeThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("JobStatChThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(50ms);
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR(
        "Failed to start the idle event in JobStatusChangeWithReasonAsync "
        "loop.");
  }

  uvw_loop->run();
}

void JobScheduler::DeadlineTimerThread_() {
  util::SetCurrentThreadName("JobDeadlineThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_deadline_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in JobDeadlineTimer loop.");
  }

  uvw_deadline_loop->run();
}

void JobScheduler::CleanResvThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("CleanResvThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();

  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(50ms);
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in reservation loop.");
  }

  uvw_loop->run();
}

void JobScheduler::ScheduleThread_() {
  util::SetCurrentThreadName("ScheduleThread");

  std::chrono::steady_clock::time_point schedule_begin;
  std::chrono::steady_clock::time_point schedule_end;
  size_t num_jobs_single_schedule;
  size_t num_jobs_single_execution;

  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  while (!m_thread_stop_) {
    // Note: In other parts of code, we must avoid the happening of the
    // situation where m_running_job_map_mtx is acquired and then
    // m_pending_job_map_mtx_ needs to be acquired. Deadlock may happen under
    // such a situation.
    m_pending_job_map_mtx_.Lock();
    if (!m_pending_job_map_.empty()) {  // all_part_metas is locked here.
      // Running map must be locked before g_meta_container's lock.
      // Otherwise, DEADLOCK may happen because StepStatusChange() locks running
      // map first and then locks g_meta_container.

      // Truncated by 1s.
      // We use the time now as the base time across the whole algorithm.
      absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

      std::vector<DependencyEvent> dep_events;
      size_t approx_size = m_dependency_event_queue_.size_approx();
      if (approx_size > 0) {
        dep_events.resize(approx_size);
        size_t actual_size = m_dependency_event_queue_.try_dequeue_bulk(
            dep_events.begin(), approx_size);
        dep_events.resize(actual_size);

        for (const auto& event : dep_events) {
          auto it = m_pending_job_map_.find(event.dependent_job_id);
          if (it != m_pending_job_map_.end()) {
            it->second->UpdateDependency(event.dependee_job_id,
                                         event.event_time);
          }
        }
      }

      std::vector<std::unique_ptr<PdJobInScheduler>> pending_jobs;
      pending_jobs.reserve(m_pending_job_map_.size());
      for (auto& it : m_pending_job_map_) {
        const auto& job = it.second;
        if (job->Held()) {
          job->pending_reason = "Held";
          continue;
        }
        if (job->begin_time > now) {
          job->pending_reason = "BeginTime";
          continue;
        }
        if (!job->Dependencies().is_met(now)) {
          if (job->Dependencies().is_failed()) {
            job->pending_reason = "DependencyNeverSatisfied";
          } else {
            job->pending_reason = "Dependency";
          }
          continue;
        }

        pending_jobs.emplace_back(
            std::make_unique<PdJobInScheduler>(job.get()));
      }

      // ScheduleThread_ is the only thread move jobs from pending to
      // running, so it's safe to release m_pending_job_map_mtx_ before
      // locking m_running_job_map_mtx_.
      m_pending_job_map_mtx_.Unlock();

      m_running_job_map_mtx_.Lock();

      std::vector<std::unique_ptr<RnJobInScheduler>> running_jobs;
      running_jobs.reserve(m_running_job_map_.size());
      for (auto& it : m_running_job_map_) {
        running_jobs.emplace_back(
            std::make_unique<RnJobInScheduler>(it.second.get()));
      }

      // ScheduleThread_ is the only thread start jobs, so it's safe to release
      // m_running_job_map_mtx_ before monitoring resources on nodes.
      m_running_job_map_mtx_.Unlock();

      schedule_begin = std::chrono::steady_clock::now();
      num_jobs_single_schedule = std::min((size_t)g_config.ScheduledBatchSize,
                                          m_pending_job_map_.size());

      g_meta_container->StartLogging();

      begin = std::chrono::steady_clock::now();

      m_node_selection_algo_->NodeSelect(now, running_jobs, pending_jobs);

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "NodeSelect costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      // All events reduce resources during scheduling should be handled here.
      // Resource increase(such as job ended) events are not considered here,
      // because they won't lead to schedule failure.
      // NOTICE: Features added in the future which reduce resources on node or
      // reservation should be handled here.
      // Check:
      // 1. for non-reservation jobs, if the allocated nodes are still
      //    available.
      // 2. for reservation jobs:
      // -  If the reservation still exists.
      // -  If the allocated nodes are still in the reservation and the
      //    reservation still has enough resource.

      // Eearliest end time of new reservations on craned created during
      // scheduling.
      absl::flat_hash_map<CranedId, absl::Time> craned_id_change_time_map;
      absl::flat_hash_set<ResvId> affected_resv_set;
      const auto& res_reduce_events =
          g_meta_container->LockAndGetResReduceEvents();
      for (const auto& event : res_reduce_events) {
        if (std::holds_alternative<ResvId>(event.affected_resources)) {
          affected_resv_set.insert(std::get<ResvId>(event.affected_resources));
        } else {
          auto& affected_nodes =
              std::get<std::pair<absl::Time, std::vector<CranedId>>>(
                  event.affected_resources);
          absl::Time end_time = affected_nodes.first;
          for (const CranedId& craned_id : affected_nodes.second) {
            auto it = craned_id_change_time_map.find(craned_id);
            if (it == craned_id_change_time_map.end() ||
                it->second > end_time) {
              craned_id_change_time_map[craned_id] = end_time;
            }
          }
        }
      }

      std::vector<std::unique_ptr<JobInCtld>> jobs_to_run;
      LockGuard pending_guard(&m_pending_job_map_mtx_);
      LockGuard running_guard(&m_running_job_map_mtx_);

      for (auto& job_in_scheduler : pending_jobs) {
        auto it = m_pending_job_map_.find(job_in_scheduler->job_id);
        if (it != m_pending_job_map_.end()) {
          auto& job = it->second;
          job->SetCachedPriority(job_in_scheduler->priority);
          job->SetStartTime(job_in_scheduler->start_time);
          if (!job_in_scheduler->reason.empty()) {
            job->pending_reason = job_in_scheduler->reason;
            continue;
          }
          absl::Time end_time =
              job_in_scheduler->start_time + job_in_scheduler->time_limit;
          if (job_in_scheduler->reservation.empty()) {
            // Non-reservation job.
            for (const CranedId& craned_id : job_in_scheduler->craned_ids) {
              auto it = craned_id_change_time_map.find(craned_id);
              if (it != craned_id_change_time_map.end() &&
                  it->second < end_time) {
                job_in_scheduler->reason = "Resource changed";
              }
            }
          } else if (affected_resv_set.contains(
                         job_in_scheduler->reservation)) {
            // Reservation job, till now, reservation always reserves whole
            // nodes, so we only need to check if the reservation ends after
            // job ends and if the allocated nodes are still in the
            // reservation.
            const auto& resv_meta =
                g_meta_container->GetResvMetaPtr(job_in_scheduler->reservation);
            if (resv_meta.get() == nullptr) {
              job_in_scheduler->reason = "Reservation deleted";
            } else if (resv_meta->end_time < end_time) {
              job_in_scheduler->reason = "Resource";
            } else {
              for (const CranedId& craned_id : job_in_scheduler->craned_ids) {
                if (!resv_meta->craned_ids.contains(craned_id)) {
                  job_in_scheduler->reason = "Reservation changed";
                  break;
                }
              }
            }
          }
          if (!job_in_scheduler->reason.empty()) {
            job->pending_reason = job_in_scheduler->reason;
            continue;
          }

          if (!job_in_scheduler->actual_licenses.empty()) {
            if (!g_licenses_manager->MallocLicense(
                    job_in_scheduler->actual_licenses)) {
              job->pending_reason = "Licenses";
              continue;
            }
          }

          if (auto result = g_account_meta_container->CheckAndMallocQosResource(
                  *job_in_scheduler);
              !result) {
            // free licenses
            if (!job_in_scheduler->actual_licenses.empty()) {
              g_licenses_manager->FreeLicense(
                  job_in_scheduler->actual_licenses);
            }
            job->pending_reason = result.error();
            continue;
          }

          PartitionId const& partition_id = job->partition_id;

          job->SetEndTime(end_time);
          job->SetCranedIds(std::move(job_in_scheduler->craned_ids));
          job->SetStepResAvail(job_in_scheduler->allocated_res);
          job->SetAllocatedRes(std::move(job_in_scheduler->allocated_res));
          job->SetActualLicenses(std::move(job_in_scheduler->actual_licenses));
          job->allocated_res_view.SetToZero();
          job->allocated_res_view += job->AllocatedRes();
          job->nodes_alloc = job->CranedIds().size();

          job->SetStatus(crane::grpc::JobStatus::Configuring);

          job->allocated_craneds_regex =
              util::HostNameListToStr(job->CranedIds());

          for (CranedId const& craned_id : job->CranedIds())
            g_meta_container->MallocResourceFromNode(craned_id, job->JobId(),
                                                     job->AllocatedRes());
          if (job->reservation != "") {
            g_meta_container->MallocResourceFromResv(
                job->reservation, job->JobId(), job->AllocatedRes());
          }

          if (job->ShouldLaunchOnAllNodes()) {
            for (auto const& craned_id : job->CranedIds())
              job->executing_craned_ids.emplace_back(craned_id);
          } else {
            job->executing_craned_ids.emplace_back(job->CranedIds().front());
          }

          jobs_to_run.push_back(std::move(job));
          m_pending_job_map_.erase(it);
        } else {
          CRANE_TRACE(
              "Pending job #{} not found in pending map, may has been "
              "canceled",
              job_in_scheduler->job_id);
        }
      }

      // Resource has been subtracted, other resource reduce events are
      // allowed now.
      g_meta_container->StopLoggingAndUnlock();

      num_jobs_single_execution = jobs_to_run.size();

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Set job fields costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      // Now we have the ownerships of to-run jobs in jobs_to_run. Add job
      // ids to node maps immediately before CreateCgroupForJobs to ensure
      // that if a CraneD crash, the callback of CranedKeeper can call
      // TerminateJobsOnCraned in which m_node_to_jobs_map_ will be searched
      // and send TerminateJobsOnCraned to appropriate CraneD to release the
      // cgroups.

      // NOTE: If unlock pending_map here, jobs may be unable to be find
      // before transferring to running_map or DB.

      m_job_indexes_mtx_.Lock();
      for (auto& job : jobs_to_run) {
        for (CranedId const& craned_id : job->CranedIds())
          m_node_to_jobs_map_[craned_id].emplace(job->JobId());
      }
      m_job_indexes_mtx_.Unlock();

      Mutex thread_pool_mtx;
      HashSet<job_id_t> failed_job_id_set;

      // RPC is time-consuming. Clustering rpc to one craned for performance.

      // Map for AllocJobs rpc.
      HashMap<CranedId, std::vector<crane::grpc::JobToD>> craned_alloc_job_map;
      // Map for AllocSteps rpc.
      std::unordered_map<CranedId, std::vector<crane::grpc::StepToD>>
          craned_alloc_steps;

      std::vector<StepInCtld*> step_in_ctld_vec;

      std::vector<std::unique_ptr<JobInCtld>> jobs_failed;

      // Move jobs into running queue.
      txn_id_t txn_id{0};
      bool ok = g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
      if (!ok) {
        CRANE_ERROR(
            "JobScheduler failed to start transaction when scheduling.");
        jobs_failed = std::move(jobs_to_run);
      }

      for (auto& job : jobs_to_run) {
        // IMPORTANT: job must be put into running_job_map before any
        // time-consuming operation, otherwise StepStatusChange RPC will come
        // earlier before job is put into running_job_map.
        g_embedded_db_client->UpdateRuntimeAttrOfJob(txn_id, job->JobDbId(),
                                                     job->RuntimeAttr());
      }

      ok = g_embedded_db_client->CommitVariableDbTransaction(txn_id);
      if (!ok) {
        CRANE_ERROR("Embedded database failed to commit manual transaction.");
        jobs_failed = std::move(jobs_to_run);
      }

      for (auto& job : jobs_to_run) {
        job->SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
        std::unique_ptr daemon_step = std::make_unique<DaemonStepInCtld>();
        daemon_step->InitFromJob(*job);
        step_in_ctld_vec.push_back(daemon_step.get());
        job->SetDaemonStep(std::move(daemon_step));
      }

      begin = std::chrono::steady_clock::now();
      if (!g_embedded_db_client->AppendSteps(step_in_ctld_vec)) {
        jobs_failed.insert(jobs_failed.end(),
                           std::make_move_iterator(jobs_to_run.begin()),
                           std::make_move_iterator(jobs_to_run.end()));
        CRANE_ERROR("Failed to append steps to embedded database.");
      } else {
        for (auto& job : jobs_to_run) {
          auto* daemon_step = job->DaemonStep();
          for (CranedId const& craned_id : job->CranedIds()) {
            craned_alloc_job_map[craned_id].push_back(
                daemon_step->GetJobToD(craned_id));
          }
          for (const auto& craned_id : daemon_step->CranedIds())
            craned_alloc_steps[craned_id].emplace_back(
                daemon_step->GetStepToD(craned_id));
        }
      }
      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Append steps to embedded DB costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();
      // FIXME: Put jobs to running map before sending RPC to craned, or
      // StatusChange will unable to lookup the jobs.
      std::latch alloc_job_latch(craned_alloc_job_map.size());
      for (auto&& iter : craned_alloc_job_map) {
        CranedId const& craned_id = iter.first;
        std::vector<crane::grpc::JobToD>& jobs = iter.second;

        m_rpc_worker_pool_->detach_task([&] {
          auto stub = g_craned_keeper->GetCranedStub(craned_id);
          CRANE_TRACE("Send AllocJobs for {} jobs to {}", jobs.size(),
                      craned_id);
          if (stub == nullptr || stub->Invalid()) {
            CRANE_TRACE(
                "AllocJobs for jobs [{}] to {} failed: Craned down.",
                absl::StrJoin(
                    jobs | std::views::transform(
                               [](const crane::grpc::JobToD& job_to_d) {
                                 return std::to_string(job_to_d.job_id());
                               }),
                    ","),
                craned_id);
            absl::MutexLock lk(&thread_pool_mtx);
            for (const auto& job_to_d : jobs)
              failed_job_id_set.emplace(job_to_d.job_id());
            alloc_job_latch.count_down();
            return;
          }
          auto err = stub->AllocJobs(jobs);
          if (err == CraneErrCode::SUCCESS) {
            alloc_job_latch.count_down();
            return;
          }
          CRANE_TRACE("AllocJobs for jobs [{}] to {} failed: Rpc failure.",
                      absl::StrJoin(
                          jobs | std::views::transform(
                                     [](const crane::grpc::JobToD& job_to_d) {
                                       return std::to_string(job_to_d.job_id());
                                     }),
                          ","),
                      craned_id);

          thread_pool_mtx.Lock();
          for (const auto& job_to_d : jobs)
            failed_job_id_set.emplace(job_to_d.job_id());
          thread_pool_mtx.Unlock();

          // If jobs in task_uid_pairs failed to start, they will be moved to
          // the completed jobs and do the following steps:
          // 1. call g_meta_container->FreeResources() for the failed jobs.
          // 2. Release all cgroups related to these failed jobs.
          // 3. Move these jobs to the completed queue.
          CRANE_ERROR("Craned #{} failed when AllocJobs.", craned_id);

          alloc_job_latch.count_down();
        });
      }
      alloc_job_latch.wait();
      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Alloc job costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();
      std::latch alloc_step_latch(craned_alloc_steps.size());
      for (const auto& craned_id : craned_alloc_steps | std::views::keys) {
        m_rpc_worker_pool_->detach_task([&, craned_id] {
          auto stub = g_craned_keeper->GetCranedStub(craned_id);
          auto& steps = craned_alloc_steps[craned_id];
          CRANE_TRACE("Send AllocSteps for [{}] steps to {}",
                      util::StepToDRangeIdString(steps), craned_id);

          if (stub == nullptr || stub->Invalid()) {
            thread_pool_mtx.Lock();
            for (const auto& step_to_d : steps)
              failed_job_id_set.emplace(step_to_d.job_id());
            thread_pool_mtx.Unlock();

            CRANE_DEBUG("AllocSteps for steps [{}] to {} failed: Craned down.",
                        util::StepToDRangeIdString(steps), craned_id);
            alloc_step_latch.count_down();
            return;
          }

          auto err = stub->AllocSteps(steps);
          if (err == CraneErrCode::SUCCESS) {
            alloc_step_latch.count_down();
            return;
          }
          CRANE_DEBUG("AllocSteps for steps [{}] to {} failed: {}.",
                      util::StepToDRangeIdString(steps), craned_id,
                      CraneErrStr(err));

          thread_pool_mtx.Lock();
          for (const auto& step_to_d : steps)
            failed_job_id_set.emplace(step_to_d.job_id());
          thread_pool_mtx.Unlock();

          // If jobs in job_uid_pairs failed to start,
          // they will be moved to the completed jobs and do the following
          // steps:
          // 1. call g_meta_container->FreeResources() for the failed jobs.
          // 2. Release all cgroups related to these failed jobs.
          // 3. Move these jobs to the completed queue.
          CRANE_ERROR("Craned #{} failed when AllocSteps.", craned_id);

          alloc_step_latch.count_down();
        });
      }
      alloc_step_latch.wait();
      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Alloc daemon steps costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      std::vector<std::unique_ptr<JobInCtld>> jobs_created;
      for (auto& job : jobs_to_run) {
        if (failed_job_id_set.contains(job->JobId())) {
          jobs_failed.emplace_back(std::move(job));
        } else {
          jobs_created.emplace_back(std::move(job));
        }
      }

      begin = std::chrono::steady_clock::now();

      // Now we have the ownerships of succeeded jobs in `jobs_created` and
      // the ownerships of failed jobs in `jobs_failed`.
      // For successfully created jobs, add them to m_node_to_jobs_map_.
      // For failed jobs, free all the resource and move them to the completed
      // queue.

      // Set succeed jobs status and do callbacks.
      for (auto& job : jobs_created) {
        job->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER,
                                     job->StartTime());
        // Start CraneCtld prolog before transferring ownership.
        // Sets m_ctld_prolog_pending_ on DaemonStep if prolog is configured.
        StartCraneCtldPrologThread(job.get());
        // The ownership of JobInCtld is transferred to the running queue.
        m_running_job_map_.emplace(job->JobId(), std::move(job));
      }

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Move jobs into running queue costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      // TODO: Refactor here! Add filter chain for post-scheduling stage.
      absl::Time post_sched_time_point = absl::Now();
      for (auto const& craned_id : craned_alloc_job_map | std::views::keys) {
        g_meta_container->GetCranedMetaPtr(craned_id)->last_busy_time =
            post_sched_time_point;
      }

      schedule_end = end;
      CRANE_TRACE(
          "Scheduling {} pending jobs. {} get scheduled. Time elapsed: {}ms",
          num_jobs_single_schedule, num_jobs_single_execution,
          std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end -
                                                                schedule_begin)
              .count());

      // Note: If unlock pending_map here, jobs may be unable to be find
      // before transferring to DB.
      if (!jobs_failed.empty()) {
        // Then handle failed jobs in `jobs_failed_to_create_cg` if there's
        // any.
        begin = std::chrono::steady_clock::now();

        for (auto& job : jobs_failed) {
          for (CranedId const& craned_id : job->CranedIds())
            g_meta_container->FreeResourceFromNode(craned_id, job->JobId());
          if (job->reservation != "")
            g_meta_container->FreeResourceFromResv(job->reservation,
                                                   job->JobId());
          g_account_meta_container->FreeQosResource(*job);
          if (!job->licenses_count.empty())
            g_licenses_manager->FreeLicense(job->licenses_count);
          LockGuard indexes_guard(&m_job_indexes_mtx_);
          for (const CranedId& craned_id : job->CranedIds()) {
            m_node_to_jobs_map_[craned_id].erase(job->JobId());
            if (m_node_to_jobs_map_[craned_id].empty()) {
              m_node_to_jobs_map_.erase(craned_id);
            }
          }
        }

        // Move failed jobs to the completed queue.
        std::unordered_set<JobInCtld*> failed_job_raw_ptrs;
        for (auto& job : jobs_failed) {
          failed_job_raw_ptrs.emplace(job.get());

          job->SetStatus(crane::grpc::Failed);
          job->SetExitCode(ExitCode::EC_CGROUP_ERR);
          job->SetEndTime(absl::Now());
        }
        ProcessFinalJobs_(failed_job_raw_ptrs);

        // Failed jobs have been handled properly. Free them explicitly.
        jobs_failed.clear();

        end = std::chrono::steady_clock::now();
        CRANE_TRACE(
            "Handling failed jobs costed {} ms",
            std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                .count());
      }
    } else {
      m_pending_map_cached_size_.store(m_pending_job_map_.size(),
                                       std::memory_order::release);
      m_pending_job_map_mtx_.Unlock();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kJobScheduleIntervalMs));
  }
}

void JobScheduler::StepScheduleThread_() {
  util::SetCurrentThreadName("StepSchedThread");
  while (!m_thread_stop_) {
    {
      absl::MutexLock running_lk(&m_running_job_map_mtx_);
      absl::MutexLock step_lk(&m_step_num_mutex_);
      if (!m_job_pending_step_num_map_.empty()) {
        std::vector<job_id_t> jobs_to_remove;
        std::vector<CommonStepInCtld*> scheduled_steps;
        for (auto& [job_id, step_num] : m_job_pending_step_num_map_) {
          // TODO: schedule here
          auto rn_iter = m_running_job_map_.find(job_id);
          if (rn_iter != m_running_job_map_.end()) {
            auto& job = rn_iter->second;
            auto popped_cnt = job->SchedulePendingSteps(&scheduled_steps);
            if ((m_job_pending_step_num_map_[job_id] -= popped_cnt) == 0) {
              jobs_to_remove.push_back(job_id);
            }
          } else {
            jobs_to_remove.push_back(job_id);
            CRANE_ERROR("Job #{} not in Rn queue for step scheduling", job_id);
          }
        }

        for (auto job_id : jobs_to_remove) {
          m_job_pending_step_num_map_.erase(job_id);
        }
        CRANE_TRACE("StepScheduleThread_ scheduled {} steps",
                    scheduled_steps.size());

        auto now = google::protobuf::util::TimeUtil::GetCurrentTime();

        for (const auto& step : scheduled_steps) {
          if (!g_embedded_db_client->UpdateRuntimeAttrOfStepIfExists(
                  0, step->StepDbId(), step->RuntimeAttr())) {
            CRANE_ERROR("Failed to update steps to embedded database.");
            StepStatusChangeAsync(step->job_id, step->StepId(), "",
                                  crane::grpc::JobStatus::Failed, 0,
                                  "DbUpdateError", now);
          }
        }

        absl::flat_hash_map<CranedId, std::vector<crane::grpc::StepToD>>
            craned_alloc_steps;
        for (auto* step : scheduled_steps) {
          for (const auto& craned_id : step->CranedIds()) {
            craned_alloc_steps[craned_id].emplace_back(
                step->GetStepToD(craned_id));
          }
        }

        Mutex thread_pool_mtx;
        std::latch alloc_step_latch(craned_alloc_steps.size());
        for (const auto& craned_id : craned_alloc_steps | std::views::keys) {
          m_rpc_worker_pool_->detach_task([&, craned_id] {
            auto stub = g_craned_keeper->GetCranedStub(craned_id);
            auto& steps = craned_alloc_steps[craned_id];
            CRANE_TRACE("Send AllocSteps for [{}] steps to {}",
                        util::StepToDRangeIdString(steps), craned_id);

            if (stub == nullptr || stub->Invalid()) {
              thread_pool_mtx.Lock();
              for (const auto& step : steps)
                StepStatusChangeAsync(step.job_id(), step.step_id(), craned_id,
                                      crane::grpc::JobStatus::Failed, 0,
                                      "CranedDown", now);

              thread_pool_mtx.Unlock();

              CRANE_DEBUG(
                  "AllocSteps for steps [{}] to {} failed: Craned down.",
                  util::StepToDRangeIdString(steps), craned_id);
              alloc_step_latch.count_down();
              return;
            }

            auto err = stub->AllocSteps(steps);
            if (err == CraneErrCode::SUCCESS) {
              alloc_step_latch.count_down();
              return;
            }
            CRANE_DEBUG("AllocSteps for steps [{}] to {} failed: {}.",
                        util::StepToDRangeIdString(steps), craned_id,
                        CraneErrStr(err));

            thread_pool_mtx.Lock();
            for (const auto& step : steps)
              StepStatusChangeAsync(step.job_id(), step.step_id(), craned_id,
                                    crane::grpc::JobStatus::Failed, 0,
                                    "AllocRpcError", now);
            thread_pool_mtx.Unlock();
            CRANE_ERROR("Craned #{} failed when AllocSteps.", craned_id);

            alloc_step_latch.count_down();
          });
        }
        alloc_step_latch.wait();
      }
    }
    std::this_thread::sleep_for(100ms);
  }
}

std::future<CraneExpected<job_id_t>> JobScheduler::SubmitJobAsync(
    std::unique_ptr<JobInCtld> job) {
  std::promise<CraneExpected<job_id_t>> promise;
  std::future<CraneExpected<job_id_t>> future = promise.get_future();

  m_submit_job_queue_.enqueue({std::move(job), std::move(promise)});
  m_submit_job_async_handle_->send();

  return std::move(future);
}

std::future<CraneExpected<step_id_t>> JobScheduler::SubmitStepAsync(
    std::unique_ptr<CommonStepInCtld> step) {
  std::promise<CraneExpected<step_id_t>> promise;
  std::future<CraneExpected<step_id_t>> future = promise.get_future();

  m_submit_step_queue_.enqueue({std::move(step), std::move(promise)});
  m_submit_step_async_handle_->send();

  return std::move(future);
}

void JobScheduler::CreateDeadlineTimerCb_() {
  absl::Time now = absl::Now();
  using DeadlineTimerQueueElem = std::pair<job_id_t, int64_t>;
  DeadlineTimerQueueElem elem;
  while (m_job_deadline_timer_create_queue_.try_dequeue(elem)) {
    job_id_t job_id = elem.first;
    int64_t deadline_time = elem.second;

    auto it = m_deadline_timer_map_.find(job_id);
    if (it != m_deadline_timer_map_.end()) {
      it->second->close();
      it->second.reset();
      m_deadline_timer_map_.erase(it);
    }

    if (deadline_time <= absl::ToUnixSeconds(now)) {
      CRANE_ERROR("Job #{}'s deadline is earlier than now", job_id);
      {
        LockGuard pending_guard(&m_pending_job_map_mtx_);
        auto pd_it = m_pending_job_map_.find(job_id);
        if (pd_it != m_pending_job_map_.end()) {
          auto& job = pd_it->second;
          m_cancel_job_queue_.enqueue(CancelPendingJobQueueElem{
              .job = std::move(job),
              .finish_status = crane::grpc::JobStatus::Deadline});
          m_pending_job_map_.erase(pd_it);
        }
      }
      return;
    }

    auto deadline_timer = uvw_deadline_loop->resource<uvw::timer_handle>();
    deadline_timer->on<uvw::timer_event>(
        [this, job_id](const uvw::timer_event&, uvw::timer_handle& h) {
          CRANE_TRACE("Pending job #{} reaches its deadline", job_id);
          DelDeadlineTimer_(job_id);
          m_job_deadline_timer_queue_.enqueue(job_id);
          m_job_deadline_timer_async_handle_->send();
        });

    deadline_timer->start(
        std::chrono::seconds(deadline_time - absl::ToUnixSeconds(now)),
        std::chrono::seconds(0));

    m_deadline_timer_map_.emplace(job_id, deadline_timer);
  }
}

void JobScheduler::DelDeadlineTimer_(job_id_t job_id) {
  auto it = m_deadline_timer_map_.find(job_id);
  if (it != m_deadline_timer_map_.end()) {
    it->second->close();
    it->second.reset();
    m_deadline_timer_map_.erase(it);
  }
}

void JobScheduler::CancelDeadlineJobCb_() {
  job_id_t job_id;
  m_pending_job_map_mtx_.Lock();
  while (m_job_deadline_timer_queue_.try_dequeue(job_id)) {
    auto pd_it = m_pending_job_map_.find(job_id);
    if (pd_it != m_pending_job_map_.end()) {
      auto& job = pd_it->second;
      m_cancel_job_queue_.enqueue(CancelPendingJobQueueElem{
          .job = std::move(job),
          .finish_status = crane::grpc::JobStatus::Deadline});
      m_pending_job_map_.erase(pd_it);
    }
  }
  m_pending_job_map_mtx_.Unlock();
  m_cancel_job_async_handle_->send();
}

std::future<CraneErrCode> JobScheduler::HoldReleaseJobAsync(job_id_t job_id,
                                                            int64_t secs) {
  std::promise<CraneErrCode> promise;
  std::future<CraneErrCode> future = promise.get_future();

  m_job_timer_queue_.enqueue(
      {std::make_pair(job_id, secs), std::move(promise)});
  m_job_timeout_async_handle_->send();

  return std::move(future);
}

CraneErrCode JobScheduler::ChangeJobTimeConstraint(
    job_id_t job_id, std::optional<int64_t> time_limit_seconds,
    std::optional<int64_t> deadline_time) {
  absl::Time now = absl::Now();
  bool is_pending = false;
  std::optional<absl::Time> absl_deadline_time =
      deadline_time.transform(absl::FromUnixSeconds);

  if (absl_deadline_time && absl_deadline_time <= now) {
    return CraneErrCode::ERR_INVALID_PARAM;
  }
  if (time_limit_seconds &&
      !CheckIfTimeLimitSecIsValid(time_limit_seconds.value()))
    return CraneErrCode::ERR_INVALID_PARAM;

  std::vector<CranedId> craned_ids;

  {
    LockGuard pending_guard(&m_pending_job_map_mtx_);
    LockGuard running_guard(&m_running_job_map_mtx_);

    JobInCtld* job;
    bool found = false;

    auto pd_iter = m_pending_job_map_.find(job_id);
    if (pd_iter != m_pending_job_map_.end()) {
      found = true, is_pending = true, job = pd_iter->second.get();

      if (job->reservation != "") {
        auto resv_end_time =
            g_meta_container->GetResvMetaPtr(job->reservation)->end_time;
        if (time_limit_seconds &&
            resv_end_time <=
                absl::Now() + absl::Seconds(time_limit_seconds.value())) {
          CRANE_DEBUG("Job #{}'s time limit exceeds reservation end time",
                      job_id);
          return CraneErrCode::ERR_INVALID_PARAM;
        }
        if (absl_deadline_time && resv_end_time <= absl_deadline_time.value()) {
          CRANE_DEBUG("Job #{}'s deadline time exceeds reservation end time",
                      job_id);
          return CraneErrCode::ERR_INVALID_PARAM;
        }
      }
    }
    if (!found) {
      auto rn_iter = m_running_job_map_.find(job_id);
      if (rn_iter != m_running_job_map_.end()) {
        found = true, job = rn_iter->second.get();
        craned_ids = job->executing_craned_ids;

        if (job->reservation != "") {
          const auto& reservation_meta =
              g_meta_container->GetResvMetaPtr(job->reservation);
          auto resv_end_time = reservation_meta->end_time;

          if (time_limit_seconds &&
              resv_end_time <= job->StartTime() +
                                   absl::Seconds(time_limit_seconds.value())) {
            CRANE_DEBUG("Job #{}'s time limit exceeds reservation end time",
                        job_id);
            return CraneErrCode::ERR_INVALID_PARAM;
          }

          if (absl_deadline_time &&
              resv_end_time <= absl_deadline_time.value()) {
            CRANE_DEBUG("Job #{}'s deadline time exceeds reservation end time",
                        job_id);
            return CraneErrCode::ERR_INVALID_PARAM;
          }

          if (time_limit_seconds) {
            g_meta_container->GetResvMetaPtr(job->reservation)->end_time =
                std::min(job->StartTime() +
                             absl::Seconds(time_limit_seconds.value()),
                         job->deadline_time);
          }

          if (absl_deadline_time) {
            g_meta_container->GetResvMetaPtr(job->reservation)->end_time =
                std::min(job->StartTime() + job->time_limit,
                         absl_deadline_time.value());
          }
        }
      }
    }

    if (!found) {
      CRANE_DEBUG("Job #{} not in Pd/Rn queue for time limit change!", job_id);
      return CraneErrCode::ERR_NON_EXISTENT;
    }

    if (time_limit_seconds) {
      job->time_limit = absl::Seconds(time_limit_seconds.value());
      job->MutableJobToCtld()->mutable_time_limit()->set_seconds(
          time_limit_seconds.value());
    }

    if (absl_deadline_time) {
      job->deadline_time = absl_deadline_time.value();
      job->MutableJobToCtld()->mutable_deadline_time()->set_seconds(
          absl::ToUnixSeconds(absl_deadline_time.value()));
      if (is_pending &&
          job->deadline_time != absl::FromUnixSeconds(kJobMaxTimeStampSec)) {
        m_job_deadline_timer_create_queue_.enqueue(
            {job_id, deadline_time.value()});
        m_job_deadline_timer_create_async_handle_->send();
      }
    }

    g_embedded_db_client->UpdateJobToCtldIfExists(0, job->JobDbId(),
                                                  job->JobToCtld());
  }

  // Only send request to the executing node
  if (!is_pending) {
    for (const CranedId& craned_id : craned_ids) {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      if (stub && !stub->Invalid()) {
        CraneErrCode err = stub->ChangeJobTimeConstraint(
            job_id, time_limit_seconds, deadline_time);
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR(
              "Failed to change time limit or deadline of job #{} on Node {}",
              job_id, craned_id);
          return err;
        }
      }
    }
  }

  return CraneErrCode::SUCCESS;
}

std::vector<CraneErrCode> JobScheduler::SuspendRunningJobs(
    const std::vector<task_id_t>& job_ids) {
  std::vector<CraneErrCode> results;
  results.reserve(job_ids.size());
  for (task_id_t job_id : job_ids) {
    std::vector<CranedId> executing_nodes;

    auto persist_status = [&](crane::grpc::JobStatus status,
                              const char* failure_reason) {
      LockGuard running_guard(&m_running_job_map_mtx_);
      auto iter = m_running_job_map_.find(job_id);
      if (iter == m_running_job_map_.end()) {
        CRANE_WARN("Job #{} disappeared while persisting status {} during {}",
                   job_id, static_cast<int>(status), failure_reason);
        return false;
      }

      JobInCtld* job = iter->second.get();
      auto prev_status = job->Status();
      if (prev_status == status) return true;

      job->SetStatus(status);

      // Manage suspend_time based on status transitions:
      // - When transitioning to Suspended: record current time
      // - When transitioning away from Suspended: clear suspend_time
      absl::Time prev_suspend_time = job->SuspendTime();
      if (status == crane::grpc::Suspended) {
        job->SetSuspendTime(absl::Now());
      } else if (prev_status == crane::grpc::Suspended) {
        // Transitioning away from Suspended, clear suspend_time
        job->SetSuspendTime(absl::InfinitePast());
      }

      if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
              0, job->JobDbId(), job->RuntimeAttr())) {
        job->SetStatus(prev_status);
        job->SetSuspendTime(prev_suspend_time);
        CRANE_ERROR("Failed to persist status {} for job #{} during {}",
                    static_cast<int>(status), job_id, failure_reason);
        return false;
      }

      return true;
    };

    auto persist_current_suspended_state = [&](absl::Time suspend_time,
                                               const char* failure_reason) {
      LockGuard running_guard(&m_running_job_map_mtx_);
      auto iter = m_running_job_map_.find(job_id);
      if (iter == m_running_job_map_.end()) {
        CRANE_WARN(
            "Job #{} disappeared while converging to Suspended during {}",
            job_id, failure_reason);
        return false;
      }

      JobInCtld* job = iter->second.get();
      return PersistJobRuntimeStateNoLock_(job, job_id, crane::grpc::Suspended,
                                           job->EndTime(), suspend_time,
                                           failure_reason);
    };

    // Check if job is pending (cannot be suspended).
    // Lock ordering: pending_guard before running_guard.
    {
      LockGuard pending_guard(&m_pending_job_map_mtx_);
      if (m_pending_job_map_.contains(job_id)) {
        CRANE_TRACE("Job #{} is pending, cannot suspend", job_id);
        results.emplace_back(CraneErrCode::ERR_INVALID_PARAM);
        continue;
      }
    }

    {
      LockGuard running_guard(&m_running_job_map_mtx_);
      auto iter = m_running_job_map_.find(job_id);
      if (iter == m_running_job_map_.end()) {
        CRANE_TRACE("Job #{} not in Rn queue for suspend", job_id);
        results.emplace_back(CraneErrCode::ERR_NON_EXISTENT);
        continue;
      }

      JobInCtld* job = iter->second.get();
      if (job->Status() == crane::grpc::Suspended) {
        CRANE_TRACE("Job #{} already suspended", job_id);
        results.emplace_back(CraneErrCode::ERR_INVALID_PARAM);
        continue;
      }
      if (job->Status() != crane::grpc::Running) {
        CRANE_TRACE("Job #{} is not running (status {}) for suspend", job_id,
                    static_cast<int>(job->Status()));
        results.emplace_back(CraneErrCode::ERR_INVALID_PARAM);
        continue;
      }

      executing_nodes = job->executing_craned_ids;
    }

    if (executing_nodes.empty()) {
      CRANE_WARN("Job #{} has no executing craned when suspending", job_id);
      results.emplace_back(CraneErrCode::ERR_INVALID_PARAM);
      continue;
    }

    std::vector<CranedId> offline_nodes;
    offline_nodes.reserve(executing_nodes.size());
    for (const auto& craned_id : executing_nodes) {
      if (!g_meta_container->CheckCranedOnline(craned_id)) {
        offline_nodes.push_back(craned_id);
      }
    }

    if (!offline_nodes.empty()) {
      // Ensure suspension only proceeds when every craned involved is online.
      CRANE_WARN("Job #{} suspend skipped because craned(s) {} are offline.",
                 job_id, absl::StrJoin(offline_nodes, ","));
      results.emplace_back(CraneErrCode::ERR_RPC_FAILURE);
      continue;
    }

    std::vector<job_id_t> broadcast_job_ids{job_id};
    CraneErrCode failure_code = CraneErrCode::SUCCESS;
    bool has_failure = false;
    Mutex result_mtx;
    std::vector<CranedId> suspended_nodes;
    suspended_nodes.reserve(executing_nodes.size());

    // Set status to Suspended BEFORE freezing cgroups to close the race window.
    // This ensures the scheduling thread will skip this job immediately.
    if (!persist_status(crane::grpc::Suspended, "pre-suspend status change")) {
      CRANE_ERROR(
          "Failed to persist Suspended status for job #{} before freezing",
          job_id);
      results.emplace_back(CraneErrCode::ERR_GENERIC_FAILURE);
      continue;
    }

    // Broadcast SuspendJobs via the thread pool and track successes/errors.
    absl::BlockingCounter blocking_counter(executing_nodes.size());
    for (const auto& craned_id : executing_nodes) {
      g_thread_pool->detach_task([&, craned_id]() {
        auto stub = g_craned_keeper->GetCranedStub(craned_id);
        if (!stub || stub->Invalid()) {
          CRANE_WARN("SuspendJobs stub for {} unavailable", craned_id);
          {
            LockGuard lock(&result_mtx);
            if (failure_code == CraneErrCode::SUCCESS)
              failure_code = CraneErrCode::ERR_RPC_FAILURE;
            has_failure = true;
          }
          blocking_counter.DecrementCount();
          return;
        }

        CraneErrCode err = stub->SuspendJobs(broadcast_job_ids);
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("Failed to suspend job #{} on craned {}: {}", job_id,
                      craned_id, CraneErrStr(err));
          {
            LockGuard lock(&result_mtx);
            if (failure_code == CraneErrCode::SUCCESS) failure_code = err;
            has_failure = true;
          }
        } else {
          LockGuard lock(&result_mtx);
          suspended_nodes.emplace_back(craned_id);
        }

        blocking_counter.DecrementCount();
      });
    }

    blocking_counter.Wait();

    if (has_failure) {
      CRANE_ERROR(
          "Job #{} suspend partially failed on node(s); rolling back by "
          "resuming successfully suspended nodes and reverting status.",
          job_id);

      bool rollback_resume_failed = false;
      if (!suspended_nodes.empty()) {
        absl::BlockingCounter rollback_counter(suspended_nodes.size());
        Mutex rollback_result_mtx;
        for (const auto& craned_id : suspended_nodes) {
          g_thread_pool->detach_task([&, craned_id]() {
            auto stub = g_craned_keeper->GetCranedStub(craned_id);
            if (!stub || stub->Invalid()) {
              CRANE_ERROR(
                  "Rollback resume failed for job #{} on craned {}: "
                  "stub unavailable. Node may remain frozen.",
                  job_id, craned_id);
              {
                LockGuard lock(&rollback_result_mtx);
                rollback_resume_failed = true;
              }
              rollback_counter.DecrementCount();
              return;
            }

            CraneErrCode rb_err = stub->ResumeJobs(broadcast_job_ids);
            if (rb_err != CraneErrCode::SUCCESS) {
              CRANE_ERROR(
                  "Rollback resume failed for job #{} on craned {}: {}. "
                  "Node may remain frozen.",
                  job_id, craned_id, CraneErrStr(rb_err));
              {
                LockGuard lock(&rollback_result_mtx);
                rollback_resume_failed = true;
              }
            }

            rollback_counter.DecrementCount();
          });
        }
        rollback_counter.Wait();
      }

      // Rollback status to Running only if all nodes were successfully resumed.
      // If rollback resume failed, keep status as Suspended to reflect reality.
      if (!rollback_resume_failed) {
        if (!persist_status(crane::grpc::Running,
                            "rollback after suspend failure")) {
          CRANE_ERROR(
              "Failed to rollback status to Running for job #{} after suspend "
              "failure. Re-suspending all nodes to converge with ctld state.",
              job_id);

          if (SuspendJobOnNodes_(executing_nodes, broadcast_job_ids, job_id,
                                 "Converge suspend failed")) {
            const absl::Time converge_suspend_time = absl::Now();
            if (!persist_current_suspended_state(
                    converge_suspend_time,
                    "converging after suspend rollback status persist "
                    "failure")) {
              CRANE_ERROR(
                  "Job #{} was re-suspended to match ctld state, but failed "
                  "to persist the refreshed suspend_time.",
                  job_id);
            }
          } else {
            CRANE_ERROR(
                "Job #{} converge suspend incomplete after suspend rollback "
                "status persist failure. Manual intervention may be required.",
                job_id);
          }
        }
      } else {
        CRANE_ERROR(
            "Job #{} rollback incomplete: some nodes may remain frozen while "
            "status is Suspended. Manual intervention may be required.",
            job_id);
      }

      results.emplace_back(failure_code == CraneErrCode::SUCCESS
                               ? CraneErrCode::ERR_GENERIC_FAILURE
                               : failure_code);
      continue;
    }

    results.emplace_back(CraneErrCode::SUCCESS);
  }
  return results;
}

std::vector<CraneErrCode> JobScheduler::ResumeSuspendedJobs(
    const std::vector<task_id_t>& job_ids) {
  std::vector<CraneErrCode> results;
  results.reserve(job_ids.size());
  for (task_id_t job_id : job_ids) {
    std::vector<CranedId> executing_nodes;
    int64_t new_time_limit_secs = 0;
    std::optional<int64_t> new_deadline_secs;
    absl::Time prev_end_time = absl::InfinitePast();
    absl::Time prev_suspend_time = absl::InfinitePast();
    bool resume_prep_applied = false;

    auto rollback_resume_preparation = [&] {
      if (!resume_prep_applied) return;

      LockGuard running_guard(&m_running_job_map_mtx_);
      auto iter = m_running_job_map_.find(job_id);
      if (iter == m_running_job_map_.end()) {
        CRANE_WARN("Job #{} disappeared while rolling back resume preparation.",
                   job_id);
        return;
      }

      JobInCtld* job = iter->second.get();
      job->SetEndTime(prev_end_time);
      job->SetSuspendTime(prev_suspend_time);

      if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
              0, job->JobDbId(), job->RuntimeAttr())) {
        CRANE_WARN(
            "Failed to persist rollback of resume preparation for job #{}",
            job_id);
      }
    };

    auto persist_resuspended_state_after_failed_resume =
        [&](absl::Time rollback_suspend_time, const char* failure_reason) {
          LockGuard running_guard(&m_running_job_map_mtx_);
          auto iter = m_running_job_map_.find(job_id);
          if (iter == m_running_job_map_.end()) {
            CRANE_WARN(
                "Job #{} disappeared while persisting resuspended state during "
                "{}.",
                job_id, failure_reason);
            return false;
          }

          JobInCtld* job = iter->second.get();
          // Keep the already-extended end_time because the suspend interval
          // before this resume attempt is real. Only time after the rollback
          // re-freeze should count as newly suspended.
          return PersistJobRuntimeStateNoLock_(
              job, job_id, crane::grpc::Suspended, job->EndTime(),
              rollback_suspend_time, failure_reason);
        };

    auto persist_running_after_incomplete_resume_rollback =
        [&](const char* failure_reason) {
          LockGuard running_guard(&m_running_job_map_mtx_);
          auto iter = m_running_job_map_.find(job_id);
          if (iter == m_running_job_map_.end()) {
            CRANE_WARN(
                "Job #{} disappeared while persisting Running state during {}.",
                job_id, failure_reason);
            return false;
          }

          JobInCtld* job = iter->second.get();
          return PersistJobRuntimeStateNoLock_(
              job, job_id, crane::grpc::Running, job->EndTime(),
              absl::InfinitePast(), failure_reason);
        };

    {
      LockGuard running_guard(&m_running_job_map_mtx_);
      auto iter = m_running_job_map_.find(job_id);
      if (iter == m_running_job_map_.end()) {
        CRANE_TRACE("Job #{} not in Rn queue for resume", job_id);
        results.emplace_back(CraneErrCode::ERR_NON_EXISTENT);
        continue;
      }

      JobInCtld* job = iter->second.get();
      if (job->Status() != crane::grpc::Suspended) {
        CRANE_TRACE("Job #{} is not suspended (status {}) for resume", job_id,
                    static_cast<int>(job->Status()));
        results.emplace_back(CraneErrCode::ERR_INVALID_PARAM);
        continue;
      }

      executing_nodes = job->executing_craned_ids;
      if (executing_nodes.empty()) {
        CRANE_WARN("Job #{} has no executing craned when resuming", job_id);
        results.emplace_back(CraneErrCode::ERR_INVALID_PARAM);
        continue;
      }

      prev_end_time = job->EndTime();
      prev_suspend_time = job->SuspendTime();

      // Extend end_time BEFORE sending resume RPC to avoid race condition
      // with StepStatusChangeAsyncCb_. If the job completes immediately
      // after SIGCONT (e.g. sleep timer expired during suspension),
      // the completion handler will already see the extended end_time
      // since both code paths contend on m_running_job_map_mtx_.
      //
      // IMPORTANT: When deadline is set, the effective end_time should be
      // min(start_time + time_limit + suspended_time, deadline).
      // This ensures the job respects both time_limit and deadline constraints.
      if (job->SuspendTime() != absl::InfinitePast()) {
        absl::Duration suspended_duration = absl::Now() - job->SuspendTime();
        if (suspended_duration > absl::ZeroDuration()) {
          absl::Time new_end_time = job->EndTime() + suspended_duration;

          // If deadline is set, take the minimum of extended end_time and
          // deadline
          if (job->deadline_time !=
              absl::FromUnixSeconds(kJobMaxTimeStampSec)) {
            new_end_time = std::min(new_end_time, job->deadline_time);
            CRANE_INFO(
                "Job #{} pre-resume: extended end_time by {:.1f}s "
                "(suspended duration), capped by deadline. New end_time: {}",
                job_id, absl::ToDoubleSeconds(suspended_duration),
                absl::FormatTime(new_end_time));
          } else {
            CRANE_INFO(
                "Job #{} pre-resume: extended end_time by {:.1f}s "
                "(suspended duration). New end_time: {}",
                job_id, absl::ToDoubleSeconds(suspended_duration),
                absl::FormatTime(new_end_time));
          }

          job->SetEndTime(new_end_time);
        }
        job->SetSuspendTime(absl::InfinitePast());
      }

      // Persist the updated end_time and reset suspend_time together.
      if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
              0, job->JobDbId(), job->RuntimeAttr())) {
        job->SetEndTime(prev_end_time);
        job->SetSuspendTime(prev_suspend_time);
        CRANE_ERROR("Failed to persist pre-resume runtime attrs for job #{}",
                    job_id);
        results.emplace_back(CraneErrCode::ERR_GENERIC_FAILURE);
        continue;
      }

      resume_prep_applied = true;

      // Effective time limit for craned = end_time - start_time
      // (includes original time_limit + total suspended time, capped by
      // deadline if set).
      new_time_limit_secs =
          absl::ToInt64Seconds(job->EndTime() - job->StartTime());

      // Also send deadline to craned if it's set
      if (job->deadline_time != absl::FromUnixSeconds(kJobMaxTimeStampSec)) {
        new_deadline_secs = absl::ToUnixSeconds(job->deadline_time);
      }
    }

    std::vector<CranedId> offline_nodes;
    offline_nodes.reserve(executing_nodes.size());
    for (const auto& craned_id : executing_nodes) {
      if (!g_meta_container->CheckCranedOnline(craned_id)) {
        offline_nodes.push_back(craned_id);
      }
    }

    if (!offline_nodes.empty()) {
      CRANE_WARN("Job #{} resume skipped because craned(s) {} are offline.",
                 job_id, absl::StrJoin(offline_nodes, ","));
      rollback_resume_preparation();
      results.emplace_back(CraneErrCode::ERR_RPC_FAILURE);
      continue;
    }

    std::vector<job_id_t> broadcast_job_ids{job_id};
    CraneErrCode failure_code = CraneErrCode::SUCCESS;
    bool has_failure = false;
    Mutex result_mtx;
    std::vector<CranedId> resumed_nodes;
    resumed_nodes.reserve(executing_nodes.size());

    // Broadcast ResumeJobs via the thread pool and track successes/errors.
    absl::BlockingCounter blocking_counter(executing_nodes.size());
    for (const auto& craned_id : executing_nodes) {
      g_thread_pool->detach_task([&, craned_id]() {
        auto stub = g_craned_keeper->GetCranedStub(craned_id);
        if (!stub || stub->Invalid()) {
          CRANE_WARN("ResumeJobs stub for {} unavailable", craned_id);
          {
            LockGuard lock(&result_mtx);
            if (failure_code == CraneErrCode::SUCCESS)
              failure_code = CraneErrCode::ERR_RPC_FAILURE;
            has_failure = true;
          }
          blocking_counter.DecrementCount();
          return;
        }

        CraneErrCode err = stub->ResumeJobs(broadcast_job_ids);
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("Failed to resume job #{} on craned {}: {}", job_id,
                      craned_id, CraneErrStr(err));
          {
            LockGuard lock(&result_mtx);
            if (failure_code == CraneErrCode::SUCCESS) failure_code = err;
            has_failure = true;
          }
        } else {
          LockGuard lock(&result_mtx);
          resumed_nodes.emplace_back(craned_id);
        }

        blocking_counter.DecrementCount();
      });
    }

    blocking_counter.Wait();

    if (has_failure) {
      CRANE_ERROR(
          "Job #{} resume partially failed on node(s); rolling back by "
          "re-suspending successfully resumed nodes.",
          job_id);

      const bool rollback_suspend_succeeded = SuspendJobOnNodes_(
          resumed_nodes, broadcast_job_ids, job_id, "Rollback suspend failed");

      if (resumed_nodes.empty()) {
        rollback_resume_preparation();
      } else if (rollback_suspend_succeeded) {
        if (!persist_resuspended_state_after_failed_resume(
                absl::Now(), "partial resume rollback")) {
          CRANE_WARN(
              "Failed to persist resuspended state for job #{} after partial "
              "resume rollback",
              job_id);
        }
      } else {
        if (!persist_running_after_incomplete_resume_rollback(
                "partial resume rollback incomplete")) {
          CRANE_ERROR(
              "Job #{} rollback incomplete after partial resume failure and "
              "failed to persist fallback Running state.",
              job_id);
        }
        CRANE_ERROR(
            "Job #{} rollback incomplete after partial resume failure: some "
            "nodes may remain running. Manual intervention may be required.",
            job_id);
      }

      results.emplace_back(failure_code == CraneErrCode::SUCCESS
                               ? CraneErrCode::ERR_GENERIC_FAILURE
                               : failure_code);
      continue;
    }

    {
      LockGuard running_guard(&m_running_job_map_mtx_);
      auto iter = m_running_job_map_.find(job_id);
      if (iter == m_running_job_map_.end()) {
        // Job completed concurrently after resume RPC returned.
        // end_time was already extended before the RPC (under the same
        // lock), so the completion handler used the correct value.
        CRANE_INFO(
            "Job #{} completed concurrently after resume RPC; "
            "end_time was pre-extended.",
            job_id);
        results.emplace_back(CraneErrCode::SUCCESS);
        continue;
      }

      JobInCtld* job = iter->second.get();

      if (job->Status() != crane::grpc::Running) {
        auto prev_status = job->Status();
        job->SetStatus(crane::grpc::Running);
        if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
                0, job->JobDbId(), job->RuntimeAttr())) {
          job->SetStatus(prev_status);
          CRANE_ERROR("Failed to persist resumed status for job #{}", job_id);

          if (SuspendJobOnNodes_(
                  executing_nodes, broadcast_job_ids, job_id,
                  "Rollback suspend failed after status persist failure")) {
            const absl::Time rollback_suspend_time = absl::Now();

            // Keep the extended end_time because the prior suspended interval
            // has already been accounted for before sending ResumeJobs.
            if (!PersistJobRuntimeStateNoLock_(
                    job, job_id, crane::grpc::Suspended, job->EndTime(),
                    rollback_suspend_time,
                    "resume status persist failure rollback")) {
              CRANE_WARN(
                  "Failed to persist rollback after resume status persist "
                  "failure for job #{}",
                  job_id);
            }
          } else {
            if (!PersistJobRuntimeStateNoLock_(
                    job, job_id, crane::grpc::Running, job->EndTime(),
                    absl::InfinitePast(),
                    "resume status persist failure incomplete rollback")) {
              CRANE_ERROR(
                  "Job #{} rollback incomplete after resume status persist "
                  "failure and failed to persist fallback Running state.",
                  job_id);
            }
            CRANE_ERROR(
                "Job #{} rollback incomplete after resume status persist "
                "failure: some nodes may remain running. Manual intervention "
                "may be required.",
                job_id);
          }
          results.emplace_back(CraneErrCode::ERR_GENERIC_FAILURE);
          continue;
        }
      }
    }

    // Notify all executing craned nodes about the effective time limit and
    // deadline so they can reset their termination timers correctly. The timer
    // on craned side should be min(end_time, deadline) - now. If any node
    // fails, rollback the entire resume operation. Check again if job still
    // exists before updating time limits.
    bool job_still_exists = false;
    {
      LockGuard running_guard(&m_running_job_map_mtx_);
      job_still_exists = m_running_job_map_.contains(job_id);
    }

    if (!job_still_exists) {
      // Job completed between status update and time limit update.
      CRANE_INFO(
          "Job #{} completed before time limit update; skipping time limit "
          "update.",
          job_id);
      results.emplace_back(CraneErrCode::SUCCESS);
      continue;
    }

    bool timelimit_update_failed = false;
    CraneErrCode timelimit_failure_code = CraneErrCode::SUCCESS;
    for (const CranedId& craned_id : executing_nodes) {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      if (stub && !stub->Invalid()) {
        CraneErrCode err = stub->ChangeJobTimeConstraint(
            job_id, new_time_limit_secs, new_deadline_secs);
        if (err != CraneErrCode::SUCCESS) {
          // Check if job completed concurrently before treating as failure
          bool job_completed = false;
          {
            LockGuard running_guard(&m_running_job_map_mtx_);
            job_completed = !m_running_job_map_.contains(job_id);
          }

          if (job_completed) {
            CRANE_INFO(
                "Job #{} completed concurrently during time limit update on "
                "craned {}. Treating as success.",
                job_id, craned_id);
            // Job completed, no need to continue updating other nodes
            break;
          }

          CRANE_ERROR(
              "Failed to update time constraint of job #{} on craned {} "
              "after resume: {}. Rolling back resume operation.",
              job_id, craned_id, CraneErrStr(err));
          timelimit_update_failed = true;
          timelimit_failure_code = err;
          break;
        }
      } else {
        // Check if job completed concurrently before treating stub
        // unavailability as failure
        bool job_completed = false;
        {
          LockGuard running_guard(&m_running_job_map_mtx_);
          job_completed = !m_running_job_map_.contains(job_id);
        }

        if (job_completed) {
          CRANE_INFO(
              "Job #{} completed concurrently; stub unavailable for craned {} "
              "is acceptable. Treating as success.",
              job_id, craned_id);
          // Job completed, no need to continue updating other nodes
          break;
        }

        CRANE_ERROR(
            "Failed to get valid stub for craned {} when updating time limit "
            "of job #{}. Rolling back resume operation.",
            craned_id, job_id);
        timelimit_update_failed = true;
        timelimit_failure_code = CraneErrCode::ERR_RPC_FAILURE;
        break;
      }
    }

    if (timelimit_update_failed) {
      // Rollback: re-suspend the job on all nodes
      const bool rollback_suspend_succeeded = SuspendJobOnNodes_(
          executing_nodes, broadcast_job_ids, job_id,
          "Rollback suspend failed after timelimit update failure");

      // Rollback status and time attributes only if all nodes were successfully
      // suspended
      {
        LockGuard running_guard(&m_running_job_map_mtx_);
        auto iter = m_running_job_map_.find(job_id);
        if (iter != m_running_job_map_.end()) {
          JobInCtld* job = iter->second.get();

          if (rollback_suspend_succeeded) {
            // Keep the extended end_time so the already-finished suspend
            // interval before this resume attempt remains accounted for.
            if (!PersistJobRuntimeStateNoLock_(
                    job, job_id, crane::grpc::Suspended, job->EndTime(),
                    absl::Now(), "timelimit update failure rollback")) {
              CRANE_WARN(
                  "Failed to persist rollback status for job #{} after "
                  "timelimit update failure",
                  job_id);
            }
          } else {
            // Some nodes failed to suspend, keep status as Running and keep
            // extended time to reflect reality (job is still running with
            // extended time limit)
            CRANE_ERROR(
                "Job #{} rollback incomplete: some nodes may remain running "
                "while "
                "others are suspended. Status kept as Running with extended "
                "time. "
                "Manual intervention may be required.",
                job_id);
            // Do NOT rollback end_time/suspend_time because the job is still
            // running and needs the extended time limit
          }
        }
      }

      results.emplace_back(timelimit_failure_code);
      continue;
    }

    results.emplace_back(CraneErrCode::SUCCESS);
  }
  return results;
}

CraneErrCode JobScheduler::ChangeJobPriority(job_id_t job_id, double priority) {
  m_pending_job_map_mtx_.Lock();

  auto pd_iter = m_pending_job_map_.find(job_id);
  if (pd_iter == m_pending_job_map_.end()) {
    m_pending_job_map_mtx_.Unlock();
    CRANE_TRACE("Job #{} not in Pd queue for priority change", job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  pd_iter->second->mandated_priority = priority;
  m_pending_job_map_mtx_.Unlock();
  return CraneErrCode::SUCCESS;
}

CraneErrCode JobScheduler::ChangeJobExtraAttrs(
    job_id_t job_id, const std::string& new_extra_attr) {
  LockGuard pending_guard(&m_pending_job_map_mtx_);
  LockGuard running_guard(&m_running_job_map_mtx_);

  JobInCtld* job;
  bool found = false;

  auto pd_iter = m_pending_job_map_.find(job_id);
  if (pd_iter != m_pending_job_map_.end()) {
    found = true, job = pd_iter->second.get();
  }
  if (!found) {
    auto rn_iter = m_running_job_map_.find(job_id);
    if (rn_iter != m_running_job_map_.end()) {
      found = true, job = rn_iter->second.get();
    }
  }

  if (!found) {
    CRANE_DEBUG("Job #{} not in Pd/Rn queue for extra attribute change!",
                job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  job->extra_attr = new_extra_attr;
  job->MutableJobToCtld()->set_extra_attr(new_extra_attr);
  g_embedded_db_client->UpdateJobToCtldIfExists(0, job->JobDbId(),
                                                job->JobToCtld());
  return CraneErrCode::SUCCESS;
}

std::optional<std::future<CraneRichError>> JobScheduler::JobSubmitLuaCheck(
    JobInCtld* job) {
#ifdef HAVE_LUA
  if (g_config.JobSubmitLuaScript.empty()) return std::nullopt;
  return g_lua_pool->ExecuteLuaScript([job]() {
    return LuaJobHandler::JobSubmit(g_config.JobSubmitLuaScript, job);
  });
#else
  return std::nullopt;
#endif
}

void JobScheduler::JobModifyLuaCheck(
    const crane::grpc::ModifyJobRequest& request,
    crane::grpc::ModifyJobReply* response, std::list<job_id_t>* job_ids) {
#ifdef HAVE_LUA
  LockGuard pending_guard(&m_pending_job_map_mtx_);
  LockGuard running_guard(&m_running_job_map_mtx_);

  std::vector<std::pair<job_id_t, std::future<CraneRichError>>> futures;
  futures.reserve(request.job_ids().size());
  for (const auto job_id : request.job_ids()) {
    auto pd_iter = m_pending_job_map_.find(job_id);
    if (pd_iter != m_pending_job_map_.end()) {
      auto fut = g_lua_pool->ExecuteLuaScript([pd_iter]() {
        return LuaJobHandler::JobModify(g_config.JobSubmitLuaScript,
                                        pd_iter->second.get());
      });
      futures.emplace_back(job_id, std::move(fut));
      continue;
    }

    auto rn_iter = m_running_job_map_.find(job_id);
    if (rn_iter != m_running_job_map_.end()) {
      auto fut = g_lua_pool->ExecuteLuaScript([rn_iter]() {
        return LuaJobHandler::JobModify(g_config.JobSubmitLuaScript,
                                        rn_iter->second.get());
      });
      futures.emplace_back(job_id, std::move(fut));
    }
  }

  for (auto& [job_id, fut] : futures) {
    auto rich_err = fut.get();
    if (rich_err.code() != CraneErrCode::SUCCESS) {
      response->add_not_modified_jobs(job_id);
      if (rich_err.description().empty())
        response->add_not_modified_reasons(CraneErrStr(rich_err.code()));
      else
        response->add_not_modified_reasons(rich_err.description());
    } else {
      job_ids->emplace_back(job_id);
    }
  }
#endif
}

CraneExpected<std::future<CraneExpected<job_id_t>>>
JobScheduler::SubmitJobToScheduler(std::unique_ptr<JobInCtld> job) {
  if (!job->password_entry->Valid()) {
    CRANE_DEBUG("Uid {} not found on the controller node", job->uid);
    return std::unexpected(CraneErrCode::ERR_INVALID_UID);
  }
  job->SetUsername(job->password_entry->Username());

  {  // Limit the lifecycle of user_scoped_ptr
    auto user_scoped_ptr =
        g_account_manager->GetExistedUserInfo(job->Username());
    if (!user_scoped_ptr) {
      CRANE_DEBUG("User '{}' not found in the account database",
                  job->Username());
      return std::unexpected(CraneErrCode::ERR_INVALID_USER);
    }

    if (job->account.empty()) {
      job->account = user_scoped_ptr->default_account;
      job->MutableJobToCtld()->set_account(user_scoped_ptr->default_account);
    } else {
      if (!user_scoped_ptr->account_to_attrs_map.contains(job->account)) {
        CRANE_DEBUG(
            "Account '{}' is not in the user account list when submitting the "
            "job",
            job->account);
        return std::unexpected(CraneErrCode::ERR_USER_ACCOUNT_MISMATCH);
      }
    }
  }

  if (!g_account_manager->CheckUserPermissionToPartition(
          job->Username(), job->account, job->partition_id)) {
    CRANE_DEBUG(
        "User '{}' doesn't have permission to use partition '{}' when using "
        "account '{}'",
        job->Username(), job->partition_id, job->account);
    return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);
  }

  auto enable_res = g_account_manager->CheckIfUserOfAccountIsEnabled(
      job->Username(), job->account);
  if (!enable_res) {
    return std::unexpected(enable_res.error());
  }

  auto result = g_meta_container->CheckIfAccountIsAllowedInPartition(
      job->partition_id, job->account);
  if (!result) return std::unexpected(result.error());

  job->SetSubmitTime(absl::Now());

  result = JobScheduler::HandleUnsetOptionalInJobToCtld(job.get());
  if (result) result = JobScheduler::AcquireJobAttributes(job.get());
  if (result) result = JobScheduler::CheckJobValidity(job.get());
  if (result) {
    {
      const auto& user_ptr =
          g_account_manager->GetExistedUserInfo(job->Username());
      if (!user_ptr) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

      auto res = g_account_meta_container->TryMallocQosSubmitResource(*job);
      if (res != CraneErrCode::SUCCESS) {
        CRANE_DEBUG("The requested QoS resources have reached the limit.");
        return std::unexpected(res);
      }
      g_account_meta_container->UserAddJob(user_ptr->name);
    }
    std::future<CraneExpected<job_id_t>> future =
        g_job_scheduler->SubmitJobAsync(std::move(job));
    return {std::move(future)};
  }

  return std::unexpected(result.error());
}

CraneErrCode JobScheduler::SetHoldForJobInRamAndDb_(job_id_t job_id,
                                                    bool hold) {
  m_pending_job_map_mtx_.Lock();

  auto pd_iter = m_pending_job_map_.find(job_id);
  if (pd_iter == m_pending_job_map_.end()) {
    m_pending_job_map_mtx_.Unlock();
    CRANE_TRACE("Job #{} not in Pd queue for hold/release", job_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  JobInCtld* job = pd_iter->second.get();
  job->SetHeld(hold);

  // Copy persisted data to prevent inconsistency.
  job_db_id_t db_id = job->JobDbId();
  auto runtime_attr = job->RuntimeAttr();

  m_pending_job_map_mtx_.Unlock();

  if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(0, db_id,
                                                            runtime_attr))
    CRANE_ERROR("Failed to update runtime attr of job #{} to DB", job_id);

  return CraneErrCode::SUCCESS;
}

CraneErrCode JobScheduler::TerminateRunningStepNoLock_(
    CommonStepInCtld* step, crane::grpc::TerminateSource terminate_source) {
  auto* common_step = static_cast<CommonStepInCtld*>(step);
  bool need_to_be_terminated = false;
  if (step->type == crane::grpc::Interactive) {
    auto& meta = common_step->ia_meta.value();
    if (!meta.has_been_terminated_on_craned) {
      meta.has_been_terminated_on_craned = true;
      need_to_be_terminated = true;
    }
  } else {
    need_to_be_terminated = true;
  }

  if (need_to_be_terminated) {
    for (CranedId const& craned_id : step->ExecutionNodes()) {
      m_cancel_job_queue_.enqueue(
          CancelRunningJobQueueElem{.job_id = step->job_id,
                                    .step_id = step->StepId(),
                                    .craned_id = craned_id,
                                    .terminate_source = terminate_source});
      m_cancel_job_async_handle_->send();
    }
  }

  return CraneErrCode::SUCCESS;
}

crane::grpc::CancelJobReply JobScheduler::CancelPendingOrRunningJob(
    const crane::grpc::CancelJobRequest& request) {
  crane::grpc::CancelJobReply reply;

  uint32_t operator_uid = request.operator_uid();

  // When an ordinary user tries to cancel jobs, they are automatically filtered
  // to their own jobs.
  std::string filter_uname = request.filter_username();
  if (filter_uname.empty() &&
      !g_account_manager->CheckUidIsAdmin(operator_uid)) {
    PasswordEntry entry(operator_uid);
    filter_uname = entry.Username();
  }

  auto rng_filter_state = [&](JobInCtld* job) {
    return request.filter_state() == crane::grpc::Invalid ||
           job->Status() == request.filter_state();
  };

  auto rng_filter_partition = [&](JobInCtld* job) {
    return request.filter_partition().empty() ||
           job->partition_id == request.filter_partition();
  };

  auto rng_filter_account = [&](JobInCtld* job) {
    return request.filter_account().empty() ||
           job->account == request.filter_account();
  };

  auto rng_filter_job_name = [&](JobInCtld* job) {
    return request.filter_job_name().empty() ||
           job->name == request.filter_job_name();
  };

  auto rng_filter_user_name = [&](JobInCtld* job) {
    return filter_uname.empty() || job->Username() == filter_uname;
  };

  ranges::any_view<JobInCtld*, ranges::category::forward> pd_input_rng;
  ranges::any_view<JobInCtld*, ranges::category::forward> rn_input_rng;
  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> filter_ids;
  for (auto& [job_id, step_ids] : request.filter_ids()) {
    filter_ids[job_id].insert(step_ids.steps().begin(), step_ids.steps().end());
  }
  auto not_found_jobs =
      filter_ids | std::views::keys | std::ranges::to<std::unordered_set>();
  if (filter_ids.empty()) {
    auto get_raw_ptr = [](auto& ptr) { return ptr.get(); };
    pd_input_rng = m_pending_job_map_ | ranges::views::values |
                   ranges::views::transform(get_raw_ptr);
    rn_input_rng = m_running_job_map_ | ranges::views::values |
                   ranges::views::transform(get_raw_ptr);
  } else {
    auto filter_nullptr = [](auto task_ptr) { return task_ptr != nullptr; };
    pd_input_rng = filter_ids |
                   ranges::views::transform(
                       [this, &not_found_jobs](auto& it) -> JobInCtld* {
                         job_id_t job_id = it.first;
                         auto pd_it = m_pending_job_map_.find(job_id);
                         if (pd_it != m_pending_job_map_.end()) {
                           // Pending jobs have no steps, we just consider job
                           // existence here
                           not_found_jobs.erase(job_id);
                           return pd_it->second.get();
                         }
                         return nullptr;
                       }) |
                   ranges::views::filter(filter_nullptr);

    rn_input_rng = filter_ids |
                   ranges::views::transform(
                       [this, &not_found_jobs](auto& it) -> JobInCtld* {
                         job_id_t job_id = it.first;
                         auto rn_it = m_running_job_map_.find(job_id);
                         if (rn_it != m_running_job_map_.end()) {
                           not_found_jobs.erase(job_id);
                           return rn_it->second.get();
                         }
                         return nullptr;
                       }) |
                   ranges::views::filter(filter_nullptr);
  }

  std::unordered_set<std::string> filter_nodes_set(
      std::begin(request.filter_nodes()), std::end(request.filter_nodes()));
  auto rng_filter_nodes = [&](JobInCtld* job) {
    if (request.filter_nodes().empty()) return true;

    for (const auto& node : job->CranedIds())
      if (filter_nodes_set.contains(node)) return true;

    return false;
  };

  auto rng_get_job_id = [&](JobInCtld* job) { return job->JobId(); };

  auto fn_cancel_pending_job = [&](job_id_t job_id) {
    CRANE_TRACE("Cancelling pending job #{}", job_id);

    auto it = m_pending_job_map_.find(job_id);
    CRANE_ASSERT(it != m_pending_job_map_.end());

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        operator_uid, it->second->Username(), false);
    if (!result) {
      auto& not_cancelled_job =
          (*reply.mutable_not_cancelled_job_steps())[job_id];
      not_cancelled_job.set_reason("Permission Denied");
    } else {
      auto& cancelled_job_steps = *reply.mutable_cancelled_steps();
      cancelled_job_steps[job_id] = crane::grpc::JobStepIds{};

      m_cancel_job_queue_.enqueue(CancelPendingJobQueueElem{
          .job = std::move(it->second),
          .finish_status = crane::grpc::JobStatus::Cancelled});
      m_cancel_job_async_handle_->send();

      m_pending_job_map_.erase(it);
    }
  };

  auto fn_cancel_running_job = [&](JobInCtld* job) {
    job_id_t job_id = job->JobId();

    CRANE_TRACE("Cancelling running job #{}", job_id);

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        operator_uid, job->Username(), false);
    if (!result) {
      auto& not_cancelled_job =
          (*reply.mutable_not_cancelled_job_steps())[job_id];
      not_cancelled_job.set_reason("Permission Denied");
    } else {
      // User specified job and step ids to cancel
      if (filter_ids.contains(job_id) && !filter_ids[job_id].empty()) {
        std::vector<CommonStepInCtld*> cancel_steps;
        // cancel step
        for (step_id_t step_id : filter_ids[job_id]) {
          StepInCtld* step = job->GetStep(step_id);
          if (!step) {
            auto& not_found_job_steps =
                *reply.mutable_not_cancelled_job_steps();
            not_found_job_steps[job_id].mutable_step_ids()->Add(step_id);
            not_found_job_steps[job_id].mutable_step_reasons()->Add(
                "Step not found");
          } else {
            cancel_steps.push_back(static_cast<CommonStepInCtld*>(step));
          }
        }

        for (CommonStepInCtld* step : cancel_steps) {
          if (step->type == crane::grpc::Interactive) {
            auto& meta = step->ia_meta.value();
            if (!meta.has_been_cancelled_on_front_end) {
              meta.has_been_cancelled_on_front_end = true;
              meta.cb_step_cancel({step->job_id, step->StepId()});
            }
          } else {
            TerminateRunningStepNoLock_(
                step, crane::grpc::TERMINATE_SOURCE_USER_CANCEL);
          }
          auto& cancelled_job_steps = *reply.mutable_cancelled_steps();
          auto& job_steps = cancelled_job_steps[job_id];
          job_steps.add_steps(step->StepId());
        }
      } else {
        if (job->Status() == crane::grpc::JobStatus::Configuring) {
          // Daemon step is still being configured on Craned nodes.
          // Record the cancel intent; DaemonStepInCtld::StepStatusChange will
          // act on it once all nodes have reported their configuring status,
          // and the existing Configuring-failed path will handle frontend
          // notification for interactive jobs at that point.
          job->SetCancelRequested(true);

          auto& cancelled_job_steps = *reply.mutable_cancelled_steps();
          cancelled_job_steps[job_id] = crane::grpc::JobStepIds{};

          CRANE_INFO(
              "[Job #{}] Cancel requested during Configuring state. "
              "Deferring until all nodes finish configuring.",
              job_id);
          return;
        }
        auto& cancelled_job_steps = *reply.mutable_cancelled_steps();
        cancelled_job_steps[job_id] = crane::grpc::JobStepIds{};
        // User cancel jobs with node/name... filter or cancel a job.
        auto primary_step = job->PrimaryStep();
        if (!primary_step) {
          CRANE_DEBUG(
              "[Job #{}] Primary step not found when cancelling running job, "
              "maybe completing.",
              job_id);
          return;
        }
        if (primary_step->type == crane::grpc::Interactive &&
            primary_step->ia_meta.has_value()) {
          auto& meta = primary_step->ia_meta.value();
          if (!meta.has_been_cancelled_on_front_end) {
            meta.has_been_cancelled_on_front_end = true;
            meta.cb_step_cancel({primary_step->job_id, primary_step->StepId()});
            return;
          }
        }
        TerminateRunningStepNoLock_(primary_step,
                                    crane::grpc::TERMINATE_SOURCE_USER_CANCEL);
      }
    }
  };
  auto joined_filters = ranges::views::filter(rng_filter_state) |
                        ranges::views::filter(rng_filter_partition) |
                        ranges::views::filter(rng_filter_account) |
                        ranges::views::filter(rng_filter_user_name) |
                        ranges::views::filter(rng_filter_job_name) |
                        ranges::views::filter(rng_filter_nodes);

  auto pending_job_id_rng =
      pd_input_rng | joined_filters | ranges::views::transform(rng_get_job_id);

  LockGuard pending_guard(&m_pending_job_map_mtx_);
  LockGuard running_guard(&m_running_job_map_mtx_);

  // Evaluate immediately. fn_cancel_pending_job will change the contents
  // of m_pending_job_map_ and invalidate the end() of
  // pending_task_rng.
  ranges::for_each(pending_job_id_rng | ranges::to<std::vector<job_id_t>>(),
                   fn_cancel_pending_job);

  auto running_job_rng = rn_input_rng | joined_filters;
  ranges::for_each(running_job_rng, fn_cancel_running_job);
  for (auto job_id : not_found_jobs) {
    auto& not_found_job_steps = *reply.mutable_not_cancelled_job_steps();
    not_found_job_steps[job_id].set_reason("Job not found");
  }
  return reply;
}

crane::grpc::AttachContainerStepReply JobScheduler::AttachContainerStep(
    const crane::grpc::AttachContainerStepRequest& request) {
  crane::grpc::AttachContainerStepReply response;

  CranedId target_craned_id;
  {
    LockGuard pending_guard(&m_pending_job_map_mtx_);
    LockGuard running_guard(&m_running_job_map_mtx_);

    job_id_t job_id = request.job_id();
    auto pd_it = m_pending_job_map_.find(job_id);
    if (pd_it != m_pending_job_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
      err->set_description("Job is still pending. Try again later.");
      response.set_ok(false);
      return response;
    }

    auto rn_it = m_running_job_map_.find(job_id);
    if (rn_it == m_running_job_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested job is not running.");
      response.set_ok(false);
      return response;
    }

    JobInCtld* job = rn_it->second.get();
    if (!job->IsContainer()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested job is not a container job.");
      response.set_ok(false);
      return response;
    }

    auto* step = job->GetStep(request.step_id());
    if (step == nullptr) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested step not running, already done?");
      response.set_ok(false);
      return response;
    }

    if (step->type != crane::grpc::JobType::Container ||
        !step->StepToCtld().has_container_meta() ||
        !step->container_meta.has_value()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested step is not a container step.");
      response.set_ok(false);
      return response;
    }

    const auto& container_meta = step->container_meta.value();

    if (step->Status() != crane::grpc::JobStatus::Running &&
        step->Status() != crane::grpc::JobStatus::Starting) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
      err->set_description("Step is not running.");
      response.set_ok(false);
      return response;
    }

    // If tty is requested, tty must be enabled when creating the container
    if (request.tty() && !container_meta.tty) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description(
          "TTY not enabled when creating this container step.");
      response.set_ok(false);
      return response;
    }

    // If stdin is requested, stdin must be enabled when creating the container
    if (request.stdin() && !container_meta.stdin) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description(
          "STDIN not opened when creating this container step.");
      response.set_ok(false);
      return response;
    }

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        request.uid(), job->Username(), false);
    if (!result) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_PERMISSION_USER);
      err->set_description("Insufficient permission to attach.");
      response.set_ok(false);
      return response;
    }

    auto exec_nodes = step->ExecutionNodes();
    if (request.node_name().empty()) {
      if (exec_nodes.size() > 1) {
        auto* err = response.mutable_status();
        err->set_code(CraneErrCode::ERR_CRI_MULTIPLE_NODES);
        err->set_description(
            "Cannot attach to container running on multiple nodes without "
            "target node name.");
        response.set_ok(false);
        return response;
      }
      // Set default node name to the only running node
      target_craned_id = *exec_nodes.begin();
    } else if (exec_nodes.contains(request.node_name())) {
      target_craned_id = request.node_name();
    } else {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested node is not executing this step.");
      response.set_ok(false);
      return response;
    }
  }

  auto stub = g_craned_keeper->GetCranedStub(target_craned_id);
  if (stub == nullptr || stub->Invalid()) {
    auto* err = response.mutable_status();
    err->set_code(CraneErrCode::ERR_RPC_FAILURE);
    err->set_description("Craned is not available.");
    response.set_ok(false);
    return response;
  }

  return stub->AttachContainerStep(request);
}

crane::grpc::ExecInContainerStepReply JobScheduler::ExecInContainerStep(
    const crane::grpc::ExecInContainerStepRequest& request) {
  crane::grpc::ExecInContainerStepReply response;

  CranedId target_craned_id;
  {
    LockGuard pending_guard(&m_pending_job_map_mtx_);
    LockGuard running_guard(&m_running_job_map_mtx_);

    job_id_t job_id = request.job_id();
    auto pd_it = m_pending_job_map_.find(job_id);
    if (pd_it != m_pending_job_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
      err->set_description("Job is still pending. Try again later.");
      response.set_ok(false);
      return response;
    }

    auto rn_it = m_running_job_map_.find(job_id);
    if (rn_it == m_running_job_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested job is not running.");
      response.set_ok(false);
      return response;
    }

    JobInCtld* job = rn_it->second.get();
    if (!job->IsContainer()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested job is not a container job.");
      response.set_ok(false);
      return response;
    }

    auto* step = job->GetStep(request.step_id());
    if (step == nullptr) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested step not running, already done?");
      response.set_ok(false);
      return response;
    }

    if (step->type != crane::grpc::JobType::Container ||
        !step->StepToCtld().has_container_meta() ||
        !step->container_meta.has_value()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested step is not a container step.");
      response.set_ok(false);
      return response;
    }

    // Validate command
    if (request.command_size() == 0) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Command cannot be empty.");
      response.set_ok(false);
      return response;
    }

    const auto& container_meta = step->container_meta.value();

    if (step->Status() != crane::grpc::JobStatus::Running &&
        step->Status() != crane::grpc::JobStatus::Starting) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
      err->set_description("Step is not running.");
      response.set_ok(false);
      return response;
    }

    // If tty is requested, tty must be enabled when creating the container
    if (request.tty() && !container_meta.tty) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description(
          "TTY not enabled when creating this container step.");
      response.set_ok(false);
      return response;
    }

    // If stdin is requested, stdin must be enabled when creating the container
    if (request.stdin() && !container_meta.stdin) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description(
          "STDIN not opened when creating this container step.");
      response.set_ok(false);
      return response;
    }

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        request.uid(), job->Username(), false);
    if (!result) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_PERMISSION_USER);
      err->set_description("Insufficient permission to exec.");
      response.set_ok(false);
      return response;
    }

    auto exec_nodes = step->ExecutionNodes();
    if (request.node_name().empty()) {
      if (exec_nodes.size() > 1) {
        auto* err = response.mutable_status();
        err->set_code(CraneErrCode::ERR_CRI_MULTIPLE_NODES);
        err->set_description(
            "Cannot attach to container running on multiple nodes without "
            "target node name.");
        response.set_ok(false);
        return response;
      }
      // Set default node name to the only running node
      target_craned_id = *exec_nodes.begin();
    } else if (exec_nodes.contains(request.node_name())) {
      target_craned_id = request.node_name();
    } else {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested node is not executing this step.");
      response.set_ok(false);
      return response;
    }
  }

  auto stub = g_craned_keeper->GetCranedStub(target_craned_id);
  if (stub == nullptr || stub->Invalid()) {
    auto* err = response.mutable_status();
    err->set_code(CraneErrCode::ERR_RPC_FAILURE);
    err->set_description("Craned is not available.");
    response.set_ok(false);
    return response;
  }

  return stub->ExecInContainerStep(request);
}

crane::grpc::CreateReservationReply JobScheduler::CreateResv(
    const crane::grpc::CreateReservationRequest& request) {
  crane::grpc::CreateReservationReply reply;

  auto res = CreateResv_(request);
  if (!res.has_value()) {
    reply.set_ok(false);
    reply.set_reason(res.error());
  } else {
    reply.set_ok(true);
  }

  return reply;
}

std::expected<void, std::string> JobScheduler::CreateResv_(
    const crane::grpc::CreateReservationRequest& request) {
  std::vector<std::string> validation_errors;

  std::unordered_set<std::string> allowed_accounts;
  for (const auto& account : request.allowed_accounts()) {
    if (!g_account_manager->GetExistedAccountInfo(account)) {
      validation_errors.push_back(account);
    }
    allowed_accounts.insert(account);
  }
  std::unordered_set<std::string> denied_accounts;
  for (const auto& account : request.denied_accounts()) {
    if (!g_account_manager->GetExistedAccountInfo(account)) {
      validation_errors.push_back(account);
    }
    denied_accounts.insert(account);
  }
  if (!validation_errors.empty()) {
    return std::unexpected(fmt::format("Invalid Accounts: {}",
                                       absl::StrJoin(validation_errors, ", ")));
  }

  std::unordered_set<std::string> allowed_users;
  for (const auto& user : request.allowed_users()) {
    if (!g_account_manager->GetExistedUserInfo(user)) {
      validation_errors.push_back(user);
    }
    allowed_users.insert(user);
  }
  std::unordered_set<std::string> denied_users;
  for (const auto& user : request.denied_users()) {
    if (!g_account_manager->GetExistedUserInfo(user)) {
      validation_errors.push_back(user);
    }
    denied_users.insert(user);
  }
  if (!validation_errors.empty()) {
    return std::unexpected(fmt::format("Invalid Users: {}",
                                       absl::StrJoin(validation_errors, ", ")));
  }

  std::list<CranedId> craned_ids;
  if (!request.craned_regex().empty() &&
      !util::ParseHostList(request.craned_regex(), &craned_ids)) {
    return std::unexpected("Invalid craned_regex");
  }
  uint32_t node_num = craned_ids.size();

  absl::Time start_time =
      absl::FromUnixSeconds(request.start_time_unix_seconds());
  absl::Duration duration = absl::Seconds(request.duration_seconds());
  absl::Time end_time = start_time + duration;

  absl::Time now = absl::Now();
  if (end_time <= now)
    return std::unexpected("Reservation end time is in the past");

  if (start_time < now) CRANE_WARN("Reservation start time is in the past");

  const PartitionId& partition = request.partition();
  if (partition.empty()) {
    if (node_num == 0) {
      return std::unexpected("Nodes must be specified if partition is empty");
    }
  } else {
    auto all_partitions_meta_map =
        g_meta_container->GetAllPartitionsMetaMapConstPtr();
    if (!all_partitions_meta_map->contains(partition)) {
      return std::unexpected(fmt::format("Partition {} not found", partition));
    }
    const auto part_meta_ptr =
        all_partitions_meta_map->at(partition).GetExclusivePtr();

    if (node_num == 0) {
      // Take all nodes in the partition
      for (CranedId const& craned_id : part_meta_ptr->craned_ids) {
        craned_ids.emplace_back(craned_id);
      }
      if (request.node_num() != 0) {
        // NodeCnt is valid only when craned_regex is empty
        node_num = request.node_num();
        if (craned_ids.size() < node_num) {
          return std::unexpected(
              fmt::format("Not enough nodes in partition {}. "
                          "Requested: {}, Available: {}",
                          partition, node_num, craned_ids.size()));
        }
      } else {
        // All nodes need to be reserved
        node_num = craned_ids.size();
      }
    } else {
      // Check if specified nodes are in the partition
      for (CranedId const& craned_id : craned_ids) {
        if (!part_meta_ptr->craned_ids.contains(craned_id)) {
          validation_errors.push_back(craned_id);
        }
      }
      if (!validation_errors.empty()) {
        return std::unexpected(
            fmt::format("Nodes not in partition {}: {}", partition,
                        util::HostNameListToStr(validation_errors)));
      }
    }
  }

  g_meta_container->LockResReduceEvents();
  std::vector<CranedMetaContainer::CranedMetaPtr> craned_meta_vec;
  ResourceV3 allocated_res;
  {
    LockGuard running_guard(&m_running_job_map_mtx_);
    std::vector<CranedId> nodes_not_found;
    std::vector<CranedId> nodes_conflicted;

    for (CranedId const& craned_id : craned_ids) {
      auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
      if (!craned_meta) {
        nodes_not_found.emplace_back(craned_id);
        continue;
      }

      bool failed = false;
      for (job_id_t job_id : craned_meta->rn_job_res_map | std::views::keys) {
        const auto& job = m_running_job_map_.at(job_id);
        absl::Time job_end_time = job->StartTime() + job->time_limit;

        if (job_end_time > start_time) {
          nodes_conflicted.emplace_back(craned_id);
          failed = true;
          break;
        }
      }
      if (failed) continue;

      for (const auto& [st, ed] :
           craned_meta->resv_in_node_map | std::views::values) {
        if (st < end_time && ed > start_time) {
          nodes_conflicted.emplace_back(craned_id);
          failed = true;
          break;
        }
      }
      if (failed) continue;

      // use static_meta in case of craned dead
      allocated_res.AddResourceInNode(craned_id, craned_meta->static_meta.res);
      craned_meta_vec.emplace_back(std::move(craned_meta));
      if (craned_meta_vec.size() >= node_num) {
        break;
      }
    }

    if (craned_meta_vec.size() < node_num) {
      std::string failed_msg = fmt::format(
          "Not enough nodes available for reservation. "
          "Requested: {}, Available: {}",
          node_num, craned_meta_vec.size());
      if (!nodes_not_found.empty()) {
        failed_msg +=
            ". " + fmt::format("Nodes not found: {}",
                               util::HostNameListToStr(nodes_not_found));
      }
      if (!nodes_conflicted.empty()) {
        failed_msg +=
            ". " +
            fmt::format(
                "Nodes conflicted by running jobs or other reservation: {}",
                util::HostNameListToStr(nodes_conflicted));
      }
      g_meta_container->UnlockResReduceEvents();
      return std::unexpected(failed_msg);
    }
  }

  const ResvId& resv_name = request.reservation_name();
  {
    auto resv_meta_map = g_meta_container->GetResvMetaMapExclusivePtr();
    if (resv_meta_map->contains(resv_name)) {
      g_meta_container->UnlockResReduceEvents();
      return std::unexpected("Reservation name already exists");
    }

    std::vector<CranedId> affected_nodes_vec;
    for (auto& craned_meta : craned_meta_vec) {
      const auto& [it, ok] = craned_meta->resv_in_node_map.emplace(
          resv_name, std::make_pair(start_time, end_time));
      if (!ok) {
        CRANE_ERROR("Failed to insert reservation resource to {}",
                    craned_meta->static_meta.hostname);
      } else {
        affected_nodes_vec.emplace_back(craned_meta->static_meta.hostname);
      }
    }
    craned_meta_vec.clear();

    g_meta_container->AddResReduceEventsAndUnlock(
        {std::make_pair(start_time, std::move(affected_nodes_vec))});

    ResvMeta resv{.name = resv_name,
                  .part_id = partition,
                  .start_time = start_time,
                  .end_time = end_time,
                  .accounts_black_list = allowed_accounts.empty(),
                  .users_black_list = allowed_users.empty(),
                  .accounts = allowed_accounts.empty()
                                  ? std::move(denied_accounts)
                                  : std::move(allowed_accounts),
                  .users = allowed_users.empty() ? std::move(denied_users)
                                                 : std::move(allowed_users),
                  .craned_ids = {craned_ids.begin(), craned_ids.end()},
                  .res_total = allocated_res,
                  .res_avail = allocated_res,
                  .rn_job_res_map = {}};

    const auto& [it, ok] = resv_meta_map->emplace(resv_name, std::move(resv));
    if (!ok) {
      // unreachable
      CRANE_ERROR(
          "Failed to insert reservation meta : Reservation name {} "
          "already exists",
          resv_name);
      return std::unexpected(
          fmt::format("Failed to insert reservation meta : Reservation name {} "
                      "already exists",
                      resv_name));
    }
  }

  {
    txn_id_t txn_id{0};
    bool ok = g_embedded_db_client->BeginReservationDbTransaction(&txn_id);
    if (!ok) CRANE_ERROR("Failed to begin txn for reservation {}.", resv_name);

    ok =
        g_embedded_db_client->UpdateReservationInfo(txn_id, resv_name, request);
    if (!ok) {
      CRANE_ERROR("Failed to insert reservation {} to resv DB", resv_name);
    }

    ok = g_embedded_db_client->CommitReservationDbTransaction(txn_id);
    if (!ok) CRANE_ERROR("Failed to commit txn for reservation {}.", resv_name);
  }

  m_resv_timer_queue_.enqueue(std::make_pair(resv_name, end_time));
  m_clean_resv_timer_queue_handle_->send();

  return {};
}

crane::grpc::DeleteReservationReply JobScheduler::DeleteResv(
    const crane::grpc::DeleteReservationRequest& request) {
  crane::grpc::DeleteReservationReply reply;

  const ResvId& resv_name = request.reservation_name();

  auto res = DeleteResvMeta_(resv_name);
  if (res.has_value()) {
    reply.set_ok(true);
  } else {
    reply.set_ok(false);
    reply.set_reason(res.error());
  }

  return reply;
}

std::expected<void, std::string> JobScheduler::DeleteResvMeta_(
    const ResvId& resv_id) {
  g_meta_container->LockResReduceEvents();
  absl::flat_hash_set<CranedId> craned_ids;
  {
    auto resv_meta_map = g_meta_container->GetResvMetaMapExclusivePtr();
    CRANE_TRACE("Deleting reservation {}", resv_id);
    if (!resv_meta_map->contains(resv_id)) {
      g_meta_container->UnlockResReduceEvents();
      return std::unexpected(fmt::format("Reservation {} not found", resv_id));
    }

    const auto& resv_meta = resv_meta_map->at(resv_id).GetExclusivePtr();

    if (!resv_meta->rn_job_res_map.empty()) {
      g_meta_container->UnlockResReduceEvents();
      return std::unexpected(fmt::format(
          "Not allowed to delete reservation {} with running jobs", resv_id));
    }

    craned_ids = std::move(resv_meta->craned_ids);
    resv_meta_map->erase(resv_id);
  }
  g_meta_container->AddResReduceEventsAndUnlock({resv_id});

  for (const auto& craned_id : craned_ids) {
    auto craned_meta_ptr = g_meta_container->GetCranedMetaPtr(craned_id);
    if (!craned_meta_ptr) {
      CRANE_ERROR("Node {} not found when deleting reservation {}", craned_id,
                  resv_id);
      continue;
    }

    auto& reservation_resource_map = craned_meta_ptr->resv_in_node_map;
    auto it = reservation_resource_map.find(resv_id);
    if (it == reservation_resource_map.end()) {
      CRANE_ERROR(
          "Reservation not found on node {} when deleting reservation {}",
          craned_id, resv_id);
      continue;
    }
    reservation_resource_map.erase(it);
  }

  // TODO: Implement Rollback?
  {
    txn_id_t txn_id{0};
    auto ok = g_embedded_db_client->BeginReservationDbTransaction(&txn_id);
    if (!ok)
      CRANE_ERROR("Failed to begin transaction for reservation {}.", resv_id);

    ok = g_embedded_db_client->DeleteReservationInfo(txn_id, resv_id);
    if (!ok)
      CRANE_ERROR("Failed to delete reservation {} from resv DB", resv_id);

    ok = g_embedded_db_client->CommitReservationDbTransaction(txn_id);
    if (!ok)
      CRANE_ERROR("Failed to commit transaction for reservation {}.", resv_id);
  }
  return {};
}

crane::grpc::DeleteReservationReply JobScheduler::PurgeAllReservations() {
  crane::grpc::DeleteReservationReply reply;

  // Collect all reservation names first
  std::vector<ResvId> resv_names;
  {
    auto resv_meta_map = g_meta_container->GetResvMetaMapConstPtr();
    for (const auto& [name, _] : *resv_meta_map) {
      resv_names.push_back(name);
    }
  }

  std::vector<std::string> failed_reasons;
  for (const auto& name : resv_names) {
    auto res = DeleteResvMeta_(name);
    if (!res) {
      failed_reasons.push_back(res.error());
    }
  }

  if (failed_reasons.empty()) {
    reply.set_ok(true);
  } else {
    reply.set_ok(false);
    reply.set_reason(fmt::format("{}", fmt::join(failed_reasons, "; ")));
  }

  return reply;
}

void JobScheduler::CleanJobTimerCb_() {
  m_clean_job_timer_queue_handle_->send();
}

void JobScheduler::JobTimerAsyncCb_() {
  if (m_job_timer_queue_.size_approx() >= kJobHoldTimerBatchNum) {
    m_clean_job_timer_queue_handle_->send();
  }
}

void JobScheduler::CleanJobTimerQueueCb_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  // It's ok to use an approximate size.
  size_t approximate_size = m_job_timer_queue_.size_approx();

  std::vector<JobTimerQueueElem> timer_to_create;
  timer_to_create.resize(approximate_size);

  size_t actual_size = m_job_timer_queue_.try_dequeue_bulk(
      timer_to_create.begin(), approximate_size);

  timer_to_create.resize(actual_size);

  for (auto& [req, promise] : timer_to_create) {
    const auto& [job_id, secs] = req;

    // If any timer for the job exists, remove it.
    auto timer_it = m_job_timer_handles_.find(job_id);
    if (timer_it != m_job_timer_handles_.end()) {
      timer_it->second->close();
      m_job_timer_handles_.erase(timer_it);
    }

    CraneErrCode err;
    if (secs == 0) {  // Remove timer
      CRANE_TRACE("Remove hold constraint timer for job #{}.", job_id);
      err = SetHoldForJobInRamAndDb_(job_id, false);
    } else if (secs == std::numeric_limits<int64_t>::max()) {
      CRANE_TRACE("Add a hold constraint for job #{} without timer.", job_id);
      err = SetHoldForJobInRamAndDb_(job_id, true);
    } else {  // Set timer
      CRANE_TRACE("Add a hold constraint for job #{} with {}s timer.", job_id,
                  secs);

      auto on_timer_cb = [this, job_id](const uvw::timer_event&,
                                        uvw::timer_handle& handle) {
        CraneErrCode err = SetHoldForJobInRamAndDb_(job_id, false);
        if (err != CraneErrCode::SUCCESS)
          CRANE_ERROR("Failed to release job #{} after hold.", job_id);

        handle.close();
        m_job_timer_handles_.erase(job_id);
      };

      err = SetHoldForJobInRamAndDb_(job_id, true);

      auto job_timer_handle = uvw_loop->resource<uvw::timer_handle>();
      job_timer_handle->on<uvw::timer_event>(std::move(on_timer_cb));
      job_timer_handle->start(std::chrono::seconds(secs), 0s);
      m_job_timer_handles_[job_id] = std::move(job_timer_handle);
    }

    promise.set_value(err);
  }
}

void JobScheduler::CleanResvTimerQueueCb_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  // It's ok to use an approximate size.
  size_t approximate_size = m_resv_timer_queue_.size_approx();

  std::vector<ResvTimerQueueElem> timer_to_create;
  timer_to_create.resize(approximate_size);

  size_t actual_size = m_resv_timer_queue_.try_dequeue_bulk(
      timer_to_create.begin(), approximate_size);

  timer_to_create.resize(actual_size);

  absl::Time now = absl::Now();
  for (const auto& [reservation_id, end_time] : timer_to_create) {
    // If any timer for the reservation exists, remove it.
    auto timer_it = m_resv_timer_handles_.find(reservation_id);
    if (timer_it != m_resv_timer_handles_.end()) {
      timer_it->second->close();
      m_resv_timer_handles_.erase(timer_it);
    }

    int64_t secs = absl::ToInt64Seconds(end_time - now);
    auto on_timer_cb = [this, reservation_id](const uvw::timer_event&,
                                              uvw::timer_handle& handle) {
      auto err = DeleteResvMeta_(reservation_id);

      if (err.has_value()) {
        handle.close();
        m_resv_timer_handles_.erase(reservation_id);
      } else {
        CRANE_WARN("Failed to clean up reservation {}: {}", reservation_id,
                   err.error());
      }
    };
    auto resv_timer_handle_ = uvw_loop->resource<uvw::timer_handle>();
    resv_timer_handle_->on<uvw::timer_event>(std::move(on_timer_cb));
    resv_timer_handle_->start(std::chrono::seconds(secs),
                              std::chrono::seconds(kEraseResvIntervalSec));
    m_resv_timer_handles_[reservation_id] = std::move(resv_timer_handle_);
  }
}

void JobScheduler::CancelJobTimerCb_() {
  m_clean_cancel_job_queue_handle_->send();
}

void JobScheduler::CancelJobAsyncCb_() {
  if (m_cancel_job_queue_.size_approx() >= kCancelJobBatchNum) {
    m_clean_cancel_job_queue_handle_->send();
  }
}

void JobScheduler::CleanCancelJobQueueCb_() {
  // It's ok to use an approximate size.
  size_t approximate_size = m_cancel_job_queue_.size_approx();
  std::vector<CancelJobQueueElem> jobs_to_cancel;
  jobs_to_cancel.resize(approximate_size);

  // Carry the ownership of JobInCtld for automatic destruction.
  std::vector<std::unique_ptr<JobInCtld>> pending_job_ptr_vec;
  std::vector<std::unique_ptr<CommonStepInCtld>> pending_step_ptr_vec;
  HashMap<CranedId,
          std::unordered_map<crane::grpc::TerminateSource,
                             std::unordered_map<job_id_t, std::set<step_id_t>>,
                             absl::Hash<crane::grpc::TerminateSource>>>
      running_job_craned_id_map;

  size_t actual_size = m_cancel_job_queue_.try_dequeue_bulk(
      jobs_to_cancel.begin(), approximate_size);

  // To cancel a job, there is two cases:
  // For pending job, we just need to set the job status to Cancelled.
  // For running job, we need to send a TerminateJobs RPC to the craned.
  for (auto& elem : jobs_to_cancel) {
    std::visit(  //
        VariantVisitor{
            [&](CancelPendingJobQueueElem& pd_elem) {
              pd_elem.job->SetStatus(pd_elem.finish_status);
              pending_job_ptr_vec.emplace_back(std::move(pd_elem.job));
            },
            [&](CancelRunningJobQueueElem& rn_elem) {
              running_job_craned_id_map[rn_elem.craned_id]
                                       [rn_elem.terminate_source]
                                       [rn_elem.job_id]
                                           .insert(rn_elem.step_id);
            },
            [&](CancelPendingStepQueueElem& pd_step_elem) {
              pending_step_ptr_vec.emplace_back(std::move(pd_step_elem.step));
            },
        },
        elem);
  }

  auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
  absl::Time current_time =
      absl::FromUnixSeconds(now.seconds()) + absl::Nanoseconds(now.nanos());

  // Process offline craned nodes with timeout check
  for (auto&& [craned_id, terminate_batches] : running_job_craned_id_map) {
    if (!g_meta_container->CheckCranedOnline(craned_id)) {
      for (auto&& [terminate_source, steps] : terminate_batches) {
        (void)terminate_source;
        for (auto [job_id, step_ids] : steps) {
          // Check if the job has exceeded time limit
          google::protobuf::Timestamp end_timestamp = now;

          {
            LockGuard running_guard_for_cancel(&m_running_job_map_mtx_);
            auto job_it = m_running_job_map_.find(job_id);
            if (job_it != m_running_job_map_.end()) {
              JobInCtld* job = job_it->second.get();
              absl::Time timeout_time = job->StartTime() + job->time_limit;

              // If job exceeded time limit, use timeout time as end_time
              if (current_time > timeout_time) {
                end_timestamp.set_seconds(ToUnixSeconds(timeout_time));
                end_timestamp.set_nanos(0);
                CRANE_DEBUG(
                    "[Job #{}] Job exceeded time limit during craned {} "
                    "offline. "
                    "Using timeout time {} as end_time instead of cancel time "
                    "{}.",
                    job_id, craned_id, absl::FormatTime(timeout_time),
                    absl::FormatTime(current_time));
              }
            }
          }
          for (auto step_id : step_ids)
            StepStatusChangeAsync(job_id, step_id, craned_id,
                                  crane::grpc::JobStatus::Cancelled,
                                  ExitCode::EC_TERMINATED, "", end_timestamp);
        }
      }
      continue;
    }
    for (auto&& [terminate_source, steps] : terminate_batches) {
      g_thread_pool->detach_task(
          [id = craned_id, steps_to_cancel = steps, terminate_source]() {
            CRANE_TRACE("Craned {} is going to terminate [{}] with source {}.",
                        id, util::JobStepsToString(steps_to_cancel),
                        static_cast<int>(terminate_source));
            auto stub = g_craned_keeper->GetCranedStub(id);

            if (stub && !stub->Invalid()) {
              stub->TerminateSteps(steps_to_cancel, terminate_source);
            }
          });
    }
  }

  absl::Time cancel_time = absl::Now();

  if (!pending_job_ptr_vec.empty()) {
    for (auto& job : pending_job_ptr_vec) {
      job->SetStartTime(cancel_time);
      job->SetEndTime(cancel_time);
      g_account_meta_container->FreeQosSubmitResource(*job);

      job->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER,
                                   cancel_time);
      job->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER_ANY,
                                   cancel_time);
      job->TriggerDependencyEvents(crane::grpc::AFTER_OK,
                                   absl::InfiniteFuture());
      job->TriggerDependencyEvents(crane::grpc::AFTER_NOT_OK,
                                   absl::InfiniteFuture());

      if (job->type == crane::grpc::Interactive) {
        auto& meta = std::get<InteractiveMeta>(job->meta);
        // Cancel request may not come from crun/calloc but from ccancel,
        // ask them to exit
        if (!meta.has_been_cancelled_on_front_end) {
          meta.has_been_cancelled_on_front_end = true;
          g_thread_pool->detach_task(
              [cb = meta.cb_step_cancel, job_id = job->JobId(),
               step_id = kPrimaryStepId] { cb({job_id, step_id}); });
        } else {
          // Cancel request from crun/calloc, reply CompletionAck
          g_thread_pool->detach_task(
              [cb = meta.cb_step_completed, cfored = meta.cfored_name,
               job_id = job->JobId(), step_id = kPrimaryStepId] {
                cb({job_id, step_id, true, cfored});
              });
        }
      }
    }

    std::unordered_set<JobInCtld*> pd_job_raw_ptrs;

    for (auto& job : pending_job_ptr_vec) pd_job_raw_ptrs.emplace(job.get());
    ProcessFinalJobs_(pd_job_raw_ptrs);
  }

  if (pending_step_ptr_vec.empty()) return;
  for (auto& step : pending_step_ptr_vec) {
    step->SetStatus(crane::grpc::Cancelled);
    step->SetStartTime(cancel_time);
    step->SetEndTime(cancel_time);

    if (step->type == crane::grpc::Interactive) {
      auto& meta = step->ia_meta.value();
      // Cancel request may not come from crun/calloc but from ccancel,
      // ask them to exit
      if (!meta.has_been_cancelled_on_front_end) {
        meta.has_been_cancelled_on_front_end = true;
        g_thread_pool->detach_task(
            [cb = meta.cb_step_cancel, job_id = step->job_id,
             step_id = step->StepId()] { cb({job_id, step_id}); });
      } else {
        // Cancel request from crun/calloc, reply CompletionAck
        g_thread_pool->detach_task(
            [cb = meta.cb_step_completed, cfored = meta.cfored_name,
             job_id = step->job_id, step_id = step->StepId()] {
              cb({job_id, step_id, true, cfored});
            });
      }
    } else {
      CRANE_ERROR(
          "[Step #{}.{}] Only interactive steps support pending step "
          "cancellation.",
          step->job_id, step->StepId());
    }
  }

  std::unordered_set<StepInCtld*> pd_step_raw_ptrs;
  for (auto& step : pending_step_ptr_vec) pd_step_raw_ptrs.emplace(step.get());

  ProcessFinalSteps_(pd_step_raw_ptrs);
}

void JobScheduler::SubmitJobTimerCb_() {
  m_clean_submit_job_queue_handle_->send();
}

void JobScheduler::SubmitJobAsyncCb_() {
  if (m_submit_job_queue_.size_approx() >= kSubmitJobBatchNum)
    m_clean_submit_job_queue_handle_->send();
}

void JobScheduler::CleanSubmitJobQueueCb_() {
  using SubmitQueueElem = std::pair<std::unique_ptr<JobInCtld>,
                                    std::promise<CraneExpected<job_id_t>>>;
  using DeadlineTimerQueueElem = std::pair<job_id_t, int64_t>;
  // It's ok to use an approximate size.
  size_t approximate_size = m_submit_job_queue_.size_approx();

  std::vector<SubmitQueueElem> accepted_jobs;
  std::vector<JobInCtld*> accepted_job_ptrs;
  std::vector<SubmitQueueElem> rejected_jobs;
  std::vector<DeadlineTimerQueueElem> deadline_timer_vec;
  size_t map_size = m_pending_map_cached_size_.load(std::memory_order_acquire);
  size_t accepted_size;
  size_t rejected_size;

  if (g_config.RejectJobsBeyondCapacity) {
    accepted_size =
        std::min(approximate_size, g_config.PendingQueueMaxSize - map_size);
    rejected_size = approximate_size - accepted_size;
  } else {
    accepted_size = approximate_size;
    rejected_size = 0;
  }

  size_t accepted_actual_size;
  size_t rejected_actual_size;
  absl::Time infiniteFuture = absl::FromUnixSeconds(kJobMaxTimeStampSec);
  // Accept jobs within queue capacity.
  do {
    if (accepted_size == 0) break;
    accepted_jobs.resize(accepted_size);

    accepted_actual_size = m_submit_job_queue_.try_dequeue_bulk(
        accepted_jobs.begin(), accepted_size);
    if (accepted_actual_size == 0) break;

    accepted_job_ptrs.reserve(accepted_actual_size);

    // The order of element inside the bulk is reverse.
    for (uint32_t i = 0; i < accepted_jobs.size(); i++) {
      uint32_t pos = accepted_jobs.size() - 1 - i;
      auto* job = accepted_jobs[pos].first.get();
      // Add the job to the pending job queue.
      job->SetStatus(crane::grpc::Pending);
      accepted_job_ptrs.emplace_back(job);
    }

    if (!g_embedded_db_client->AppendJobsToPendingAndAdvanceJobIds(
            accepted_job_ptrs)) {
      CRANE_ERROR("Failed to append a batch of jobs to embedded db queue.");
      for (auto& pair : accepted_jobs) {
        g_account_meta_container->FreeQosSubmitResource(*pair.first);
        pair.second /*promise*/.set_value(
            std::unexpected(CraneErrCode::ERR_DB_INSERT_FAILED));
      }
      break;
    }

    m_pending_job_map_mtx_.Lock();

    std::unordered_map<job_id_t, job_db_id_t> jobs_to_purge;

    for (uint32_t i = 0; i < accepted_jobs.size(); i++) {
      uint32_t pos = accepted_jobs.size() - 1 - i;
      job_id_t id = accepted_jobs[pos].first->JobId();
      auto& job_id_promise = accepted_jobs[pos].second;
      auto* job = accepted_jobs[pos].first.get();
      std::vector<std::pair<crane::grpc::DependencyType, job_id_t>>
          running_deps;
      for (const auto& [dep_job_id, dep_info] : job->Dependencies().deps) {
        const auto& [dep_type, delay_seconds] = dep_info;
        auto dep_job_it = m_pending_job_map_.find(dep_job_id);
        if (dep_job_it != m_pending_job_map_.end()) {
          dep_job_it->second->AddDependent(dep_type, id);
          continue;
        }
        running_deps.emplace_back(dep_type, dep_job_id);
      }
      if (!running_deps.empty()) {
        bool missing_deps = false;
        {
          LockGuard running_guard(&m_running_job_map_mtx_);
          for (const auto& [dep_type, dep_job_id] : running_deps) {
            auto dep_job_it = m_running_job_map_.find(dep_job_id);
            if (dep_job_it != m_running_job_map_.end()) {
              dep_job_it->second->AddDependent(dep_type, id);
            } else {
              missing_deps = true;
              break;
            }
          }
        }
        if (missing_deps) {
          CRANE_WARN("Job #{} rejected: missing dependencies.", id);
          g_account_meta_container->FreeQosSubmitResource(*job);
          job_id_promise.set_value(
              std::unexpected(CraneErrCode::ERR_MISSING_DEPENDENCY));
          jobs_to_purge[id] = job->JobDbId();
          continue;
        }
      }
      absl::Time deadline_time = accepted_jobs[pos].first->deadline_time;
      if (deadline_time != infiniteFuture) {
        deadline_timer_vec.emplace_back(id, absl::ToUnixSeconds(deadline_time));
      }

      m_pending_job_map_.emplace(id, std::move(accepted_jobs[pos].first));
      job_id_promise.set_value(id);
    }

    m_pending_map_cached_size_.store(m_pending_job_map_.size(),
                                     std::memory_order_release);
    m_pending_job_map_mtx_.Unlock();

    if (!jobs_to_purge.empty()) {
      if (!g_embedded_db_client->PurgeEndedJobs(jobs_to_purge)) {
        CRANE_ERROR(
            "Failed to purge {} job(s) with missing dependencies from "
            "embedded db.",
            jobs_to_purge.size());
      }
    }

    if (!deadline_timer_vec.empty()) {
      m_job_deadline_timer_create_queue_.enqueue_bulk(
          deadline_timer_vec.data(), deadline_timer_vec.size());
      m_job_deadline_timer_create_async_handle_->send();
    }
  } while (false);
  // Reject jobs beyond queue capacity
  do {
    if (rejected_size == 0) break;
    rejected_jobs.resize(rejected_size);

    rejected_actual_size = m_submit_job_queue_.try_dequeue_bulk(
        rejected_jobs.begin(), rejected_size);
    if (rejected_actual_size == 0) break;

    CRANE_TRACE("Rejecting {} jobs...", rejected_actual_size);
    for (size_t i = 0; i < rejected_actual_size; i++) {
      g_account_meta_container->FreeQosSubmitResource(*rejected_jobs[i].first);
      rejected_jobs[i].second.set_value(
          std::unexpected(CraneErrCode::ERR_BEYOND_JOB_ID));
    }
  } while (false);
}

void JobScheduler::SubmitStepTimerCb_() {
  m_clean_step_submit_queue_handle_->send();
}

void JobScheduler::SubmitStepAsyncCb_() {
  if (m_submit_step_queue_.size_approx() >= kSubmitJobBatchNum)
    m_clean_step_submit_queue_handle_->send();
}

void JobScheduler::CleanStepSubmitQueueCb_() {
  using SubmitQueueElem = std::pair<std::unique_ptr<CommonStepInCtld>,
                                    std::promise<CraneExpected<step_id_t>>>;

  // It's ok to use an approximate size.
  size_t approximate_size = m_submit_step_queue_.size_approx();

  std::vector<SubmitQueueElem> elems;

  if (approximate_size == 0) return;
  elems.resize(approximate_size);

  auto actual_size =
      m_submit_step_queue_.try_dequeue_bulk(elems.begin(), approximate_size);
  if (actual_size == 0) return;
  elems.resize(actual_size);

  // The order of element inside the bulk is reverse.
  for (uint32_t i = 0; i < elems.size(); i++) {
    uint32_t pos = elems.size() - 1 - i;
    elems[pos].first->SetStatus(crane::grpc::Pending);
  }

  std::vector<std::pair<std::unique_ptr<StepInCtld>,
                        std::promise<CraneExpected<step_id_t>>>>
      valid_steps;
  absl::MutexLock lk(&m_running_job_map_mtx_);
  auto now = absl::Now();
  for (uint32_t i = 0; i < elems.size(); i++) {
    uint32_t pos = elems.size() - 1 - i;
    auto& step = elems[pos].first;
    auto it = m_running_job_map_.find(step->job_id);
    if (it != m_running_job_map_.end()) {
      step->job = it->second.get();
      step->SetSubmitTime(now);
      auto ok = HandleUnsetOptionalInStepToCtld(step.get());
      if (ok) ok = AcquireStepAttributes(step.get());
      if (ok) ok = CheckStepValidity(step.get());
      if (ok) {
        valid_steps.emplace_back(step.release(), std::move(elems[pos].second));
      } else {
        elems[pos].second.set_value(std::unexpected{ok.error()});
        step.reset();
      }
    } else {
      elems[pos].second.set_value(
          std::unexpected(CraneErrCode::ERR_INVALID_JOB_ID));
      step.reset();
    }
  }
  std::vector<StepInCtld*> valid_step_ptrs =
      valid_steps | std::views::keys |
      std::views::transform([](const auto& step) { return step.get(); }) |
      std::ranges::to<std::vector<StepInCtld*>>();
  absl::MutexLock step_lk(&m_step_num_mutex_);
  // TODO: Potential performance issue
  if (g_embedded_db_client->AppendSteps(valid_step_ptrs)) {
    for (auto& [step, promise] : valid_steps) {
      promise.set_value(step->StepId());
      m_job_pending_step_num_map_[step->job_id]++;
      step->job->AddStep(std::unique_ptr<CommonStepInCtld>(
          static_cast<CommonStepInCtld*>(step.release())));
    }
  } else {
    for (auto& [step, promise] : valid_steps) {
      promise.set_value(std::unexpected(CraneErrCode::ERR_SYSTEM_ERR));
      step.reset();
    }
    CRANE_ERROR("Failed to append a batch of steps to embedded db queue.");
  }
}

void JobScheduler::StartCraneCtldPrologThread(JobInCtld* job) {
  // CraneCtldProlog runs during the DaemonStep Configuring phase, in parallel
  // with craned node configuration. The DaemonStep's Configuring->Running
  // transition is gated on both AllNodesConfigured() and PrologComplete().
  // Prolog result is reported to the DaemonStep via status change queue using
  // kCtldPrologInternalNodeIndex as craned_id.
  if (!g_config.JobLifecycleHook.CranectldPrologs.empty()) {
    // TODO: cbatch job must be requeue
    job->DaemonStep()->SetCtldPrologPending(true);
    g_thread_pool->detach_task([this, job_id = job->JobId(), env = job->env]() {
      // run prolog ctld script
      RunPrologEpilogArgs run_prolog_args{
          .scripts = g_config.JobLifecycleHook.CranectldPrologs,
          .envs = env,
          .timeout_sec = g_config.JobLifecycleHook.PrologTimeout,
          .run_uid = 0,
          .run_gid = 0,
          .output_size = g_config.JobLifecycleHook.MaxOutputSize};
      run_prolog_args.timeout_sec = g_config.JobLifecycleHook.PrologTimeout;
      if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0) {
        run_prolog_args.timeout_sec =
            g_config.JobLifecycleHook.PrologEpilogTimeout;
      }
      CRANE_TRACE(
          "[Job #{}]: Running CraneCtldProlog as UID {} with timeout {}s",
          job_id, run_prolog_args.run_uid, run_prolog_args.timeout_sec);
      auto run_prolog_result = util::os::RunPrologOrEpiLog(run_prolog_args);

      auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
      if (!run_prolog_result) {
        auto status = run_prolog_result.error();
        CRANE_DEBUG("[Job #{}]: CraneCtldProlog failed status={}:{}", job_id,
                    status.exit_code, status.signal_num);
        this->StepStatusChangeAsync(
            job_id, kDaemonStepId, kCtldPrologInternalNodeIndex,
            crane::grpc::JobStatus::Cancelled, ExitCode::EC_PROLOG_ERR,
            "CraneCtldPrologError", now);
      } else {
        CRANE_DEBUG("[Job #{}]: CraneCtldProlog success", job_id);
        this->StepStatusChangeAsync(
            job_id, kDaemonStepId, kCtldPrologInternalNodeIndex,
            crane::grpc::JobStatus::Running, 0, "", now);
      }
    });
  }
}

void JobScheduler::StepStatusChangeAsync(
    job_id_t job_id, step_id_t step_id, const CranedId& craned_index,
    crane::grpc::JobStatus new_status, uint32_t exit_code, std::string reason,
    google::protobuf::Timestamp timestamp) {
  m_job_status_change_queue_.enqueue({.job_id = job_id,
                                      .step_id = step_id,
                                      .exit_code = exit_code,
                                      .new_status = new_status,
                                      .craned_index = craned_index,
                                      .reason = std::move(reason),
                                      .timestamp = std::move(timestamp)});
  m_job_status_change_async_handle_->send();
}

void JobScheduler::JobStatusChangeTimerCb_() {
  m_clean_job_status_change_handle_->send();
}

void JobScheduler::JobStatusChangeAsyncCb_() {
  if (m_job_status_change_queue_.size_approx() >= kJobStatusChangeBatchNum)
    m_clean_job_status_change_handle_->send();
}

void JobScheduler::CleanJobStatusChangeQueueCb_() {
  size_t approximate_size = m_job_status_change_queue_.size_approx();

  std::vector<JobStatusChangeArg> args;
  args.resize(approximate_size);

  size_t actual_size = m_job_status_change_queue_.try_dequeue_bulk(
      args.begin(), approximate_size);
  if (actual_size == 0) return;
  args.resize(actual_size);
  auto begin_time = std::chrono::steady_clock::now();

  StepStatusChangeContext context{};
  context.rn_step_raw_ptrs.reserve(actual_size);
  context.step_ptrs.reserve(actual_size);
  context.step_raw_ptrs.reserve(actual_size);

  context.job_ptrs.reserve(actual_size);
  context.job_raw_ptrs.reserve(actual_size);
  context.rn_job_raw_ptrs.reserve(actual_size);

  LockGuard running_guard(&m_running_job_map_mtx_);
  LockGuard indexes_guard(&m_job_indexes_mtx_);

  for (const auto& [job_id, step_id, exit_code, new_status, craned_index,
                    reason, timestamp] : args) {
    auto iter = m_running_job_map_.find(job_id);
    if (iter == m_running_job_map_.end()) {
      CRANE_WARN(
          "[Job #{}] Ignoring unknown job in CleanJobStatusChangeQueueCb_ "
          "(status: {}).",
          job_id, util::StepStatusToString(new_status));
      continue;
    }

    // Free job allocation
    std::optional<std::pair<crane::grpc::JobStatus, uint32_t /*exit code*/>>
        job_finished_status{std::nullopt};

    std::unique_ptr<JobInCtld>& job = iter->second;
    if (job->DaemonStep() != nullptr &&
        step_id == job->DaemonStep()->StepId()) {
      CRANE_TRACE(
          "[Step #{}.{}] Daemon step status change received, status: {}.",
          job->JobId(), step_id, new_status);
      auto* step = job->DaemonStep();
      job_finished_status = step->StepStatusChange(
          new_status, exit_code, reason, craned_index, timestamp, &context);
    } else {
      CommonStepInCtld* step = job->GetStep(step_id);
      if (step == nullptr) {
        CRANE_WARN("[Step #{}.{}] Ignoring unknown step in StepStatusChange.",
                   job_id, step_id);
        continue;
      }
      CRANE_TRACE("[Step #{}.{}] Step status change received, status: {}.",
                  job_id, step_id, new_status);
      job_finished_status = step->StepStatusChange(
          new_status, exit_code, reason, craned_index, timestamp, &context);
    }

    if (job_finished_status.has_value()) {
      CRANE_TRACE("[Job #{}] Completed with status {}.", job_id,
                  job_finished_status.value());
      job->SetStatus(job_finished_status.value().first);
      job->SetExitCode(job_finished_status.value().second);

      // Validate and adjust end_time to prevent it from exceeding time_limit
      // by too much. Allow 5 seconds of floating tolerance.
      absl::Time end_time = absl::FromUnixSeconds(timestamp.seconds()) +
                            absl::Nanoseconds(timestamp.nanos());
      absl::Time expected_end_time = job->StartTime() + job->time_limit;

      if (end_time > expected_end_time + absl::Seconds(kEndTimeToleranceSec)) {
        CRANE_WARN(
            "[Job #{}] Reported end_time {} exceeds expected end_time {} by "
            "more than {}s. Adjusting to expected end_time.",
            job->JobId(), absl::FormatTime(end_time),
            absl::FormatTime(expected_end_time), kEndTimeToleranceSec);
        end_time = expected_end_time;
      }
      job->SetEndTime(end_time);

      uint32_t job_exit_code = job_finished_status.value().second;
      job->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER_ANY,
                                   end_time);
      job->TriggerDependencyEvents(
          crane::grpc::DependencyType::AFTER_OK,
          job_exit_code == 0 ? end_time : absl::InfiniteFuture());
      job->TriggerDependencyEvents(
          crane::grpc::DependencyType::AFTER_NOT_OK,
          job_exit_code != 0 ? end_time : absl::InfiniteFuture());

      for (CranedId const& craned_id : job->CranedIds()) {
        auto node_to_job_map_it = m_node_to_jobs_map_.find(craned_id);
        if (node_to_job_map_it == m_node_to_jobs_map_.end()) [[unlikely]] {
          CRANE_ERROR("Failed to find craned_id {} in m_node_to_jobs_map_",
                      craned_id);
        } else {
          node_to_job_map_it->second.erase(job_id);
          if (node_to_job_map_it->second.empty()) {
            m_node_to_jobs_map_.erase(node_to_job_map_it);
          }
        }
      }

      for (CranedId const& craned_id : job->CranedIds()) {
        g_meta_container->FreeResourceFromNode(craned_id, job_id);
      }
      if (job->reservation != "")
        g_meta_container->FreeResourceFromResv(job->reservation, job->JobId());
      g_account_meta_container->FreeQosResource(*job);
      if (!job->licenses_count.empty())
        g_licenses_manager->FreeLicense(job->licenses_count);

      if (!g_config.JobLifecycleHook.CranectldEpilogs.empty()) {
        g_thread_pool->detach_task(
            [job_id = job->JobId(), env_copy = job->env]() {
              RunPrologEpilogArgs run_epilog_ctld_args{
                  .scripts = g_config.JobLifecycleHook.CranectldEpilogs,
                  .envs = env_copy,
                  .timeout_sec = g_config.JobLifecycleHook.EpilogTimeout,
                  .run_uid = 0,
                  .run_gid = 0,
                  .output_size = g_config.JobLifecycleHook.MaxOutputSize};
              if (g_config.JobLifecycleHook.PrologEpilogTimeout > 0) {
                run_epilog_ctld_args.timeout_sec =
                    g_config.JobLifecycleHook.PrologEpilogTimeout;
              }
              CRANE_TRACE("Running CraneCtldEpilog as UID {} with timeout {}s",
                          run_epilog_ctld_args.run_uid,
                          run_epilog_ctld_args.timeout_sec);
              auto result = util::os::RunPrologOrEpiLog(run_epilog_ctld_args);
              if (!result) {
                auto status = result.error();
                CRANE_DEBUG("Job #[{}]: CraneCtldEpilog failed status={}:{}",
                            job_id, status.exit_code, status.signal_num);
              } else {
                CRANE_DEBUG("Job #[{}]: CraneCtldEpilog success", job_id);
              }
            });
      }

      context.job_raw_ptrs.insert(job.get());
      context.job_ptrs.emplace(std::move(job));

      // As for now, job status change includes only
      // Pending / Running -> Completed / Failed / Cancelled.
      // It means all job status changes will put the job into mongodb,
      // so we don't have any branch code here and just put it into mongodb.
      m_running_job_map_.erase(iter);
    }
  }

  std::latch alloc_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_step_alloc_map.size())};

  for (const auto& craned_id : context.craned_step_alloc_map | std::views::keys)
    m_rpc_worker_pool_->detach_task([this, &alloc_step_latch, craned_id,
                                     &context] {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      // If the craned is down, just ignore it.
      if (stub && !stub->Invalid()) {
        auto err =
            stub->AllocSteps(context.craned_step_alloc_map.at(craned_id));
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR(
              "Failed to AllocSteps for [{}] jobs on Node {}: Rpc failure",
              absl::StrJoin(context.craned_step_alloc_map.at(craned_id) |
                                std::views::transform(
                                    [](const crane::grpc::StepToD& step) {
                                      return fmt::format("{}.{}", step.job_id(),
                                                         step.step_id());
                                    }),
                            ","),
              craned_id);
        }
      } else {
        CRANE_ERROR(
            "Failed to AllocSteps for [{}] jobs on Node {}: Craned down",
            absl::StrJoin(
                context.craned_step_alloc_map.at(craned_id) |
                    std::views::transform([](const crane::grpc::StepToD& step) {
                      return fmt::format("{}.{}", step.job_id(),
                                         step.step_id());
                    }),
                ","),
            craned_id);
        auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
        for (const auto& steps : context.craned_step_alloc_map.at(craned_id)) {
          StepStatusChangeWithReasonAsync(
              steps.job_id(), steps.step_id(), craned_id,
              crane::grpc::JobStatus::Failed, ExitCode::EC_CRANED_DOWN,
              "CranedDown", now);
        }
      }
      alloc_step_latch.count_down();
    });

  std::latch free_step_latch(
      static_cast<std::ptrdiff_t>(context.craned_step_free_map.size()));
  for (const auto& craned_id :
       context.craned_step_free_map | std::views::keys) {
    m_rpc_worker_pool_->detach_task([&free_step_latch, craned_id, &context] {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      if (stub && !stub->Invalid()) {
        auto err = stub->FreeSteps(context.craned_step_free_map.at(craned_id));
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR(
              "Failed to FreeSteps for [{}] steps on Node {}. Rpc failure",
              util::JobStepsToString(
                  context.craned_step_free_map.at(craned_id)),
              craned_id);
        }
      } else {
        CRANE_ERROR(
            "Failed to FreeSteps for [{}] steps on Node {}, stub invalid",
            util::JobStepsToString(context.craned_step_free_map.at(craned_id)),
            craned_id);
      }
      free_step_latch.count_down();
    });
  }

  std::latch exec_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_step_exec_map.size())};
  for (const auto& craned_id :
       context.craned_step_exec_map | std::views::keys) {
    m_rpc_worker_pool_->detach_task([this, &exec_step_latch, craned_id,
                                     &context]() {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
      // If the craned is down, just ignore it.
      if (stub && !stub->Invalid()) {
        CraneExpected failed_steps =
            stub->ExecuteSteps(context.craned_step_exec_map.at(craned_id));
        if (failed_steps.has_value() && !failed_steps.value().empty()) {
          CRANE_ERROR("Failed to ExecuteStep for [{}] steps on Node {}",
                      util::JobStepsToString(failed_steps.value()), craned_id);
          for (const auto& [job_id, step_ids] : failed_steps.value()) {
            for (const auto& step_id : step_ids)
              StepStatusChangeWithReasonAsync(
                  job_id, step_id, craned_id, crane::grpc::JobStatus::Failed,
                  ExitCode::EC_RPC_ERR, "ExecRpcError", now);
          }
        }
      } else {
        CRANE_ERROR(
            "Failed to ExecuteStep for [{}] steps on Node {}, craned down.",
            util::JobStepsToString(context.craned_step_exec_map.at(craned_id)),
            craned_id);
        for (const auto& [job_id, step_ids] :
             context.craned_step_exec_map.at(craned_id)) {
          for (const auto& step_id : step_ids)
            StepStatusChangeWithReasonAsync(
                job_id, step_id, craned_id, crane::grpc::JobStatus::Failed,
                ExitCode::EC_CRANED_DOWN, "CranedDown", now);
        }
      }
      exec_step_latch.count_down();
    });
  }

  std::latch orphaned_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_orphaned_steps.size())};
  for (const auto& craned_id :
       context.craned_orphaned_steps | std::views::keys) {
    m_rpc_worker_pool_->detach_task(
        [&orphaned_step_latch, craned_id, &context]() {
          auto stub = g_craned_keeper->GetCranedStub(craned_id);

          // If the craned is down, just ignore it.
          if (stub && !stub->Invalid()) {
            const auto& steps = context.craned_orphaned_steps.at(craned_id);
            auto err = stub->TerminateOrphanedSteps(steps);
            if (err != CraneErrCode::SUCCESS) {
              CRANE_ERROR(
                  "Failed to TerminateOrphanedSteps for [{}] jobs on Node {}",
                  util::JobStepsToString(steps), craned_id);
            }
          }
          orphaned_step_latch.count_down();
        });
  }

  std::latch cancel_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_cancel_steps.size())};
  for (const auto& craned_id : context.craned_cancel_steps | std::views::keys) {
    m_rpc_worker_pool_->detach_task(
        [&cancel_step_latch, craned_id, &context]() {
          auto stub = g_craned_keeper->GetCranedStub(craned_id);

          // If the craned is down, just ignore it.
          if (stub && !stub->Invalid()) {
            const auto& steps = context.craned_cancel_steps.at(craned_id);
            auto err = stub->TerminateSteps(steps);
            if (err != CraneErrCode::SUCCESS) {
              CRANE_ERROR("Failed to TerminateSteps for [{}] jobs on Node {}",
                          util::JobStepsToString(steps), craned_id);
            }
          }
          cancel_step_latch.count_down();
        });
  }

  // Jobs to free
  std::latch free_job_latch{
      static_cast<std::ptrdiff_t>(context.craned_jobs_to_free.size())};
  for (const auto& craned_id : context.craned_jobs_to_free | std::views::keys) {
    m_rpc_worker_pool_->detach_task([this, &free_job_latch, craned_id,
                                     &context] {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      bool success{false};
      // If the craned is down, just ignore it.
      if (stub && !stub->Invalid()) {
        const auto& jobs = context.craned_jobs_to_free.at(craned_id);
        auto err = stub->FreeJobs(jobs);
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("Failed to FreeJobs for [{}] on Node {}",
                      absl::StrJoin(jobs, ","), craned_id);
        } else
          success = true;
      }
      if (!success) {
        auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
        for (const auto& job_id : context.craned_jobs_to_free.at(craned_id)) {
          StepStatusChangeWithReasonAsync(
              job_id, kDaemonStepId, craned_id, crane::grpc::JobStatus::Failed,
              ExitCode::EC_RPC_ERR, "Rpc failure when free job", now);
        }
      }
      free_job_latch.count_down();
    });
  }

  alloc_step_latch.wait();
  free_step_latch.wait();
  exec_step_latch.wait();
  orphaned_step_latch.wait();
  cancel_step_latch.wait();
  free_job_latch.wait();

  txn_id_t txn_id;

  auto now = std::chrono::steady_clock::now();
  // When store, write step info first, then job info.
  auto ok = g_embedded_db_client->BeginStepVarDbTransaction(&txn_id);
  if (!ok) {
    CRANE_ERROR(
        "JobScheduler failed to start step transaction when clean step status "
        "change");
  }
  // Steps will update in embedded db
  for (auto* step : context.rn_step_raw_ptrs) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfStep(txn_id, step->StepDbId(),
                                                       step->RuntimeAttr()))
      CRANE_ERROR("[Job #{}.{}] Failed to call UpdateRuntimeAttrOfStep()",
                  step->job_id, step->StepId());
  }
  ok = g_embedded_db_client->CommitStepVarDbTransaction(txn_id);
  if (!ok) {
    CRANE_ERROR(
        "JobScheduler failed to commit step transaction when clean step "
        "status change.");
  }
  auto duration = std::chrono::steady_clock::now() - now;
  CRANE_TRACE(
      "Persist step status changes to embedded db cost {} ms",
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
  now = std::chrono::steady_clock::now();

  ProcessFinalSteps_(context.step_raw_ptrs);
  duration = std::chrono::steady_clock::now() - now;
  CRANE_TRACE(
      "ProcessFinalSteps_ took {} ms",
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());

  now = std::chrono::steady_clock::now();
  ok = g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  if (!ok) {
    CRANE_ERROR(
        "JobScheduler failed to start job transaction when clean step status "
        "change.");
  }
  // Jobs will update in embedded db
  for (auto* job : context.rn_job_raw_ptrs) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJob(txn_id, job->JobDbId(),
                                                      job->RuntimeAttr()))
      CRANE_ERROR("[Job #{}] Failed to call UpdateRuntimeAttrOfJob()",
                  job->JobId());
  }

  ok = g_embedded_db_client->CommitVariableDbTransaction(txn_id);
  if (!ok) {
    CRANE_ERROR(
        "JobScheduler failed to commit job transaction when clean  step "
        "status change.");
  }
  duration = std::chrono::steady_clock::now() - now;
  CRANE_TRACE(
      "Persist job status changes to embedded db cost {} ms",
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
  now = std::chrono::steady_clock::now();
  ProcessFinalJobs_(context.job_raw_ptrs);
  duration = std::chrono::steady_clock::now() - now;
  CRANE_TRACE(
      "ProcessFinalJobs_ took {} ms",
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());

  auto end_time = std::chrono::steady_clock::now();
  CRANE_TRACE("Cleaning {} StepStatusChanges cost {} ms.", actual_size,
              std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                    begin_time)
                  .count());
}

void JobScheduler::QueryJobsInRam(
    const crane::grpc::QueryJobsInfoRequest* request,
    std::unordered_map<job_id_t, crane::grpc::JobInfo>* job_info_map) {
  auto now = absl::Now();

  auto job_rng_filter_time = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    bool has_submit_time_interval = request->has_filter_submit_time_interval();
    bool has_start_time_interval = request->has_filter_start_time_interval();
    bool has_end_time_interval = request->has_filter_end_time_interval();

    bool valid = true;
    if (has_submit_time_interval) {
      const auto& interval = request->filter_submit_time_interval();
      valid &= !interval.has_lower_bound() ||
               job.RuntimeAttr().submit_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               job.RuntimeAttr().submit_time() <= interval.upper_bound();
    }

    if (has_start_time_interval) {
      const auto& interval = request->filter_start_time_interval();
      valid &= !interval.has_lower_bound() ||
               job.RuntimeAttr().start_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               job.RuntimeAttr().start_time() <= interval.upper_bound();
    }

    if (has_end_time_interval) {
      const auto& interval = request->filter_end_time_interval();
      valid &= !interval.has_lower_bound() ||
               job.RuntimeAttr().end_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               job.RuntimeAttr().end_time() <= interval.upper_bound();
    }

    return valid;
  };

  bool no_accounts_constraint = request->filter_accounts().empty();
  std::unordered_set<std::string> req_accounts(
      request->filter_accounts().begin(), request->filter_accounts().end());
  auto job_rng_filter_account = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    return no_accounts_constraint || req_accounts.contains(job.account);
  };

  bool no_username_constraint = request->filter_users().empty();
  std::unordered_set<std::string> req_users(request->filter_users().begin(),
                                            request->filter_users().end());
  auto job_rng_filter_username = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    return no_username_constraint || req_users.contains(job.Username());
  };

  bool no_qos_constraint = request->filter_qos().empty();
  std::unordered_set<std::string> req_qos(request->filter_qos().begin(),
                                          request->filter_qos().end());
  auto job_rng_filter_qos = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    return no_qos_constraint || req_qos.contains(job.qos);
  };

  bool no_job_names_constraint = request->filter_job_names().empty();
  std::unordered_set<std::string> req_job_names(
      request->filter_job_names().begin(), request->filter_job_names().end());
  auto job_rng_filter_name = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    return no_job_names_constraint ||
           req_job_names.contains(job.JobToCtld().name());
  };

  bool no_partitions_constraint = request->filter_partitions().empty();
  std::unordered_set<std::string> req_partitions(
      request->filter_partitions().begin(), request->filter_partitions().end());
  auto job_rng_filter_partition = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    return no_partitions_constraint ||
           req_partitions.contains(job.partition_id);
  };

  bool no_ids_constraint = request->filter_ids().empty();
  bool no_licenses_constraint = request->filter_licenses().empty();
  std::unordered_set<std::string> req_licenses(
      request->filter_licenses().begin(), request->filter_licenses().end());
  auto job_rng_filter_licenses = [&](auto* job_ptr) {
    if (no_licenses_constraint) {
      return true;
    }
    for (auto& license : job_ptr->licenses_count) {
      if (req_licenses.contains(license.first)) {
        return true;
      }
    }

    return no_licenses_constraint;
  };

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> req_steps;
  for (auto& [job_id, step_ids] : request->filter_ids()) {
    req_steps[job_id] = std::unordered_set<step_id_t>(step_ids.steps().begin(),
                                                      step_ids.steps().end());
  }

  bool no_job_states_constraint = request->filter_states().empty();
  std::unordered_set<int> req_job_states(request->filter_states().begin(),
                                         request->filter_states().end());
  auto job_rng_filter_state = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    return no_job_states_constraint ||
           req_job_states.contains(job.RuntimeAttr().status());
  };

  bool no_job_types_constraint = request->filter_job_types().empty();
  std::unordered_set<int> req_job_types(request->filter_job_types().begin(),
                                        request->filter_job_types().end());
  auto job_rng_filter_job_type = [&](auto* job_ptr) {
    return no_job_types_constraint || req_job_types.contains(job_ptr->type);
  };

  bool no_nodename_list_constraint = request->filter_nodename_list().empty();
  std::unordered_set<std::string> req_nodename_list(
      request->filter_nodename_list().begin(),
      request->filter_nodename_list().end());
  auto job_rng_filter_nodename_list = [&](auto* job_ptr) {
    if (no_nodename_list_constraint) return true;
    for (const auto& nodename : job_ptr->RuntimeAttr().craned_ids()) {
      if (req_nodename_list.contains(nodename)) return true;
    }
    return false;
  };

  size_t num_limit = request->num_limit() == 0 ? kDefaultQueryJobNumLimit
                                               : request->num_limit();

  auto append_step_fn = [&](auto* step_info_list, StepInCtld* step) {
    if (!step) return;
    if (no_ids_constraint || req_steps[step->job_id].empty() ||
        req_steps[step->job_id].contains(step->StepId())) {
      auto* step_info = step_info_list->Add();
      step->SetFieldsOfStepInfo(step_info);
      step_info->mutable_elapsed_time()->set_seconds(
          ToInt64Seconds(now - step->StartTime()));
    }
  };

  auto append_fn = [&](auto* job_ptr) {
    JobInCtld& job = *job_ptr;
    crane::grpc::JobInfo job_info;
    job.SetFieldsOfJobInfo(&job_info);

    job_info.set_status(job.Status());
    job_info.mutable_elapsed_time()->set_seconds(
        ToInt64Seconds(now - job.StartTime()));

    // Check if job has exceeded time limit
    if (job.Status() == crane::grpc::JobStatus::Running ||
        job.Status() == crane::grpc::JobStatus::Configuring) {
      if (job.StartTime() + job.time_limit < now) {
        job_info.set_status(crane::grpc::JobStatus::Completing);
        job_info.mutable_end_time()->set_seconds(
            ToUnixSeconds(job.StartTime() + job.time_limit));
        job_info.mutable_elapsed_time()->set_seconds(
            ToInt64Seconds(job.time_limit));
      }
    }

    auto* proto_steps = job_info.mutable_step_info_list();
    append_step_fn(proto_steps, job.DaemonStep());
    append_step_fn(proto_steps, job.PrimaryStep());

    for (const auto& step : job.Steps() | std::views::values) {
      append_step_fn(proto_steps, step.get());
    }
    job_info_map->emplace(job.JobId(), std::move(job_info));
  };

  auto joined_filters = ranges::views::filter(job_rng_filter_account) |
                        ranges::views::filter(job_rng_filter_name) |
                        ranges::views::filter(job_rng_filter_partition) |
                        ranges::views::filter(job_rng_filter_state) |
                        ranges::views::filter(job_rng_filter_username) |
                        ranges::views::filter(job_rng_filter_time) |
                        ranges::views::filter(job_rng_filter_qos) |
                        ranges::views::filter(job_rng_filter_job_type) |
                        ranges::views::filter(job_rng_filter_licenses) |
                        ranges::views::filter(job_rng_filter_nodename_list) |
                        ranges::views::take(num_limit);

  auto get_job_ptr_by_id = [&](auto& job_id) -> JobInCtld* {
    {
      auto job_it = m_pending_job_map_.find(job_id);
      if (job_it != m_pending_job_map_.end()) {
        return job_it->second.get();
      }
    }

    {
      auto job_it = m_running_job_map_.find(job_id);
      if (job_it != m_running_job_map_.end()) {
        return job_it->second.get();
      }
    }
    return nullptr;
  };

  auto pending_rng = m_pending_job_map_ | ranges::views::all;
  auto running_rng = m_running_job_map_ | ranges::views::all;
  auto pd_r_rng = ranges::views::concat(pending_rng, running_rng);

  ranges::any_view<JobInCtld*, ranges::category::forward> filtered_job_rng =
      req_steps | ranges::views::keys | ranges::views::common |
      ranges::views::transform(get_job_ptr_by_id) |
      ranges::views::filter([](auto* job_ptr) { return job_ptr != nullptr; });

  ranges::any_view<JobInCtld*, ranges::category::forward> all_job_rng =
      pd_r_rng |
      ranges::views::transform([](auto& it) { return it.second.get(); }) |
      joined_filters;

  ranges::any_view<JobInCtld*, ranges::category::forward> id_filtered_job_rng =
      no_ids_constraint ? all_job_rng : filtered_job_rng;
  LockGuard pending_guard(&m_pending_job_map_mtx_);
  LockGuard running_guard(&m_running_job_map_mtx_);

  ranges::for_each(id_filtered_job_rng, append_fn);
}

bool JobScheduler::QueryStepAndNodeRegex(job_id_t job_id, step_id_t step_id,
                                         crane::grpc::StepToCtld* step) {
  LockGuard running_guard(&m_running_job_map_mtx_);
  auto iter = m_running_job_map_.find(job_id);
  if (iter == m_running_job_map_.end()) return false;

  if (step_id == 0) return false;

  auto* job = iter->second.get();

  if (step_id == kPrimaryStepId) {
    *step = job->PrimaryStep()->StepToCtld();
    step->set_nodelist(iter->second->allocated_craneds_regex);
    return true;
  }

  auto step_iter = job->Steps().find(step_id);
  if (step_iter == job->Steps().end()) return false;
  *step = step_iter->second->StepToCtld();
  step->set_nodelist(iter->second->allocated_craneds_regex);
  return true;
}

void JobScheduler::QueryRnJobOnCtldForNodeConfig(
    const CranedId& craned_id, crane::grpc::ConfigureCranedRequest* req) {
  LockGuard running_job_guard(&m_running_job_map_mtx_);
  LockGuard indexes_guard(&m_job_indexes_mtx_);

  auto it = m_node_to_jobs_map_.find(craned_id);
  if (it == m_node_to_jobs_map_.end()) return;

  auto& job_steps = *req->mutable_job_steps();
  absl::Time now = absl::Now();

  for (const auto& job_id : it->second) {
    auto job_it = m_running_job_map_.find(job_id);
    if (job_it == m_running_job_map_.end()) continue;

    JobInCtld* job = job_it->second.get();
    auto* daemon_step = job->DaemonStep();

    // Check if job has exceeded time limit during craned offline
    if (job->Status() == crane::grpc::JobStatus::Running &&
        job->StartTime() + job->time_limit < now) {
      CRANE_INFO(
          "[Job #{}] Job exceeded time limit during craned {} offline. "
          "StartTime: {}, TimeLimit: {}s, Current: {}. "
          "State will be updated when craned reconnects.",
          job_id, craned_id, absl::FormatTime(job->StartTime()),
          absl::ToInt64Seconds(job->time_limit), absl::FormatTime(now));
    }

    *job_steps[job_id].mutable_job() = daemon_step->GetJobToD(craned_id);
    auto& steps = *job_steps[job_id].mutable_steps();
    auto& step_status = *job_steps[job_id].mutable_step_status();
    steps[daemon_step->StepId()] = daemon_step->GetStepToD(craned_id);

    // Send the current (possibly updated) status to craned
    step_status[daemon_step->StepId()] = daemon_step->Status();

    if (job->PrimaryStep() &&
        std::ranges::contains(job->PrimaryStep()->ExecutionNodes(),
                              craned_id)) {
      const auto* step = job->PrimaryStep();
      steps[step->StepId()] = step->GetStepToD(craned_id);
      step_status[step->StepId()] = step->Status();
    }
    for (const auto& step : job->Steps() | std::views::values)
      if (std::ranges::contains(step->ExecutionNodes(), craned_id)) {
        steps[step->StepId()] = step->GetStepToD(craned_id);
        step_status[step->StepId()] = step->Status();
      }
  }
}

void JobScheduler::TerminateOrphanedSteps(
    const std::unordered_map<job_id_t, std::set<step_id_t>>& steps,
    const CranedId& excluded_node) {
  CRANE_INFO("Terminate orphaned steps: [{}] synced by {}.",
             util::JobStepsToString(steps), excluded_node);

  // Now we just terminate all jobs.
  std::unordered_map<CranedId,
                     std::unordered_map<job_id_t, std::set<step_id_t>>>
      craned_steps_map;
  {
    LockGuard running_job_guard(&m_running_job_map_mtx_);
    LockGuard indexes_guard(&m_job_indexes_mtx_);
    for (const auto& [job_id, step_ids] : steps) {
      auto job_it = m_running_job_map_.find(job_id);
      if (job_it == m_running_job_map_.end()) {
        CRANE_WARN("Job {} not found in running job map.", job_id);
        continue;
      }

      auto& job = job_it->second;
      for (const auto& step_id : step_ids) {
        StepInCtld* step{nullptr};
        if (auto* daemon_step = job->DaemonStep();
            daemon_step != nullptr && daemon_step->StepId() == step_id) {
          step = daemon_step;
        } else if (auto* primary_step = job->PrimaryStep();
                   primary_step != nullptr &&
                   step_id == primary_step->StepId()) {
          step = primary_step;
        } else {
          step = job->GetStep(step_id);
        }

        if (step == nullptr) {
          CRANE_WARN("[Step #{}.{}] Step not found in job.", job_id, step_id);
          continue;
        }

        for (const auto& craned_id : step->ExecutionNodes()) {
          craned_steps_map[craned_id][job_id].insert(step_id);
        }
      }
    }
  }

  for (auto& [craned_id, craned_steps] : craned_steps_map) {
    if (craned_id == excluded_node) continue;
    g_thread_pool->detach_task(
        [craned_id, node_job_steps = std::move(craned_steps)] {
          auto stub = g_craned_keeper->GetCranedStub(craned_id);
          if (stub && !stub->Invalid()) {
            if (auto err = stub->TerminateOrphanedSteps(node_job_steps);
                err != CraneErrCode::SUCCESS) {
              CRANE_ERROR("Failed to terminate orphaned steps [{}] on node {}.",
                          util::JobStepsToString(node_job_steps), craned_id);
            }
          }
        });
  }
}

bool SchedulerAlgo::LocalScheduler::CalculateRunningNodesAndStartTime_(
    const absl::Time& now, PdJobInScheduler* job) {
  std::vector<NodeState*> nodes_to_sched;

  absl::Time earliest_end_time = now + job->time_limit;

  const ResourceView min_res_view =
      job->req_node_res_view +
      job->req_task_res_view * job->ntasks_per_node_min;
  ResourceV3 job_alloc_res;
  struct node_info {
    int ntasks_on_node;
    ResourceInNodeV3 res;
    NodeState* node_state;
    bool operator<(const node_info& other) const {
      return ntasks_on_node > other.ntasks_on_node;
    }
  };
  std::priority_queue<node_info> topk_nodes_total;
  int topk_ntasks_sum_total = 0;

  std::priority_queue<node_info> topk_nodes_avail;
  int topk_ntasks_sum_avail = 0;

  auto get_max_tasks = [&](const ResourceInNodeV3& res_on_node) {
    ResourceInNodeV3 feasible_res;
    if (!min_res_view.GetFeasibleResourceInNode(res_on_node, &feasible_res)) {
      return 0;
    }
    ResourceInNodeV3 res_avail = res_on_node;
    res_avail -= feasible_res;
    int ntasks_on_node = job->ntasks_per_node_min;
    while (ntasks_on_node < static_cast<int>(job->ntasks_per_node_max) &&
           job->req_task_res_view.GetFeasibleResourceInNode(res_avail,
                                                            &feasible_res)) {
      ++ntasks_on_node;
      res_avail -= feasible_res;
    }
    return ntasks_on_node;
  };

  for (const auto& node_state :
       m_node_selector_->GetOrderedNodesSet() | std::views::values) {
    const auto& craned_id = node_state->craned_id;
    auto& time_avail_res_map = node_state->time_avail_res_map;
    // Number of jobs is not less than map size.
    // When condition is true, the craned has too many jobs.
    if (time_avail_res_map.size() >= kAlgoMaxJobNumPerNode) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE("Craned {} has too many jobs. Skipping this craned.",
                    craned_id);
      }
      continue;
    }

    if (!job->included_nodes.empty() &&
        !job->included_nodes.contains(craned_id)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Craned {} is not in the nodelist of job #{}. "
            "Skipping this craned.",
            craned_id, job->job_id);
      }
      continue;
    }

    if (!job->excluded_nodes.empty() &&
        job->excluded_nodes.contains(craned_id)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE("Job #{} excludes craned {}. Skipping this craned.",
                    job->job_id, craned_id);
      }
      continue;
    }

    int ntasks_on_node_total = get_max_tasks(node_state->res_total);
    if (ntasks_on_node_total == 0) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Job #{} needs more resource than that of craned {}. "
            "Skipping this craned.",
            job->job_id, craned_id);
      }
      continue;
    }

    if (topk_nodes_total.size() < job->node_num ||
        topk_ntasks_sum_total < job->ntasks) {
      topk_ntasks_sum_total += ntasks_on_node_total;
      topk_nodes_total.push(
          node_info{ntasks_on_node_total, node_state->res_total, node_state});
      if (topk_nodes_total.size() > job->node_num) {
        topk_ntasks_sum_total -= topk_nodes_total.top().ntasks_on_node;
        topk_nodes_total.pop();
      }
    }

    // Given N as the required number of nodes,
    // all the nodes that is able to run the job at some time-point will be
    // iterated and the first N nodes will be in the craned_ids_.

    // Find all possible nodes that can run the job now.
    if (job->exclusive) {
      bool satisfied = true;
      for (const auto& [time, res] : time_avail_res_map) {
        if (time >= earliest_end_time) break;
        if (!(node_state->res_total <= res)) {
          satisfied = false;
          break;
        }
      }
      if (!satisfied) {
        continue;
      }
      topk_ntasks_sum_avail += ntasks_on_node_total;
      topk_nodes_avail.push(
          node_info{ntasks_on_node_total, node_state->res_total, node_state});
      if (topk_nodes_avail.size() > job->node_num) {
        topk_ntasks_sum_avail -= topk_nodes_avail.top().ntasks_on_node;
        topk_nodes_avail.pop();
      }
      if (topk_nodes_avail.size() == job->node_num &&
          topk_ntasks_sum_avail >= job->ntasks) {
        break;
      }
    } else {
      ResourceInNodeV3 feasible_res;
      if (!min_res_view.GetFeasibleResourceInNode(node_state->res_avail,
                                                  &feasible_res)) {
        continue;
      }
      ResourceInNodeV3 min_res_on_node = node_state->res_avail;

      for (const auto& [time, res] : time_avail_res_map) {
        if (time >= earliest_end_time) break;
        min_res_on_node.Ckmin(res);
      }

      int ntasks_on_node_avail = get_max_tasks(min_res_on_node);
      if (ntasks_on_node_avail) {
        topk_ntasks_sum_avail += ntasks_on_node_avail;
        topk_nodes_avail.push(
            node_info{ntasks_on_node_avail, min_res_on_node, node_state});
        if (topk_nodes_avail.size() > job->node_num) {
          topk_ntasks_sum_avail -= topk_nodes_avail.top().ntasks_on_node;
          topk_nodes_avail.pop();
        }
        if (topk_nodes_avail.size() == job->node_num &&
            topk_ntasks_sum_avail >= job->ntasks) {
          break;
        }
      }
    }
  }

  if (topk_nodes_avail.size() == job->node_num &&
      topk_ntasks_sum_avail >= job->ntasks) {
    int rest_ntasks = job->ntasks - job->node_num;
    while (!topk_nodes_avail.empty()) {
      const auto& info = topk_nodes_avail.top();
      const auto& res = info.res;
      int ntasks_on_node = std::min(rest_ntasks, info.ntasks_on_node - 1) + 1;
      if (job->exclusive) {
        job->allocated_res.AddResourceInNode(info.node_state->craned_id, res);
      } else {
        ResourceInNodeV3 feasible_res;
        bool ok =
            (job->req_node_res_view + job->req_task_res_view * ntasks_on_node)
                .GetFeasibleResourceInNode(res, &feasible_res);
        CRANE_ASSERT_MSG(
            ok, fmt("Failed to get feasible resource on craned {} for job #{}",
                    info.node_state->craned_id, job->job_id));
        job->allocated_res.AddResourceInNode(info.node_state->craned_id,
                                             feasible_res);
      }
      job->craned_id_to_task_num[info.node_state->craned_id] = ntasks_on_node;
      rest_ntasks -= ntasks_on_node - 1;
      topk_nodes_avail.pop();
    }
    job->start_time = now;
    job->craned_ids.clear();
    job->craned_ids.reserve(job->craned_id_to_task_num.size());
    for (const auto& [craned_id, _] : job->craned_id_to_task_num) {
      job->craned_ids.emplace_back(craned_id);
    }
    return true;
  }

  if (topk_nodes_total.size() < job->node_num ||
      topk_ntasks_sum_total < job->ntasks) {
    CRANE_TRACE(
        "Only {} nodes with {} jobs are available for job #{} but {} nodes "
        "with {} jobs are required.",
        topk_nodes_total.size(), topk_ntasks_sum_total, job->job_id,
        job->node_num, job->ntasks);
    return false;
  } else {
    int rest_ntasks = job->ntasks - job->node_num;
    while (!topk_nodes_total.empty()) {
      const auto& info = topk_nodes_total.top();
      const auto& res = info.res;
      int ntasks_on_node = std::min(rest_ntasks, info.ntasks_on_node - 1) + 1;
      if (job->exclusive) {
        job->allocated_res.AddResourceInNode(info.node_state->craned_id, res);
      } else {
        ResourceInNodeV3 feasible_res;
        bool ok =
            (job->req_node_res_view + job->req_task_res_view * ntasks_on_node)
                .GetFeasibleResourceInNode(res, &feasible_res);
        CRANE_ASSERT_MSG(
            ok, fmt("Failed to get feasible resource on craned {} for job #{}",
                    info.node_state->craned_id, job->job_id));
        job->allocated_res.AddResourceInNode(info.node_state->craned_id,
                                             feasible_res);
      }
      job->craned_id_to_task_num[info.node_state->craned_id] = ntasks_on_node;
      nodes_to_sched.push_back(info.node_state);
      rest_ntasks -= ntasks_on_node - 1;
      topk_nodes_total.pop();
    }
  }

  EarliestStartSubsetSelector scheduler(job, nodes_to_sched);
  return scheduler.CalcEarliestStartTime(now, job);
}

void SchedulerAlgo::NodeSelect(
    const absl::Time& now,
    const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
    const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs) {
  // Split pending jobs by partition and reservation
  absl::flat_hash_map<PartitionId, std::vector<PdJobInScheduler*>>
      part_pd_job_ptr_map;
  absl::flat_hash_map<ResvId, std::vector<PdJobInScheduler*>>
      resv_pd_job_ptr_map;

  for (const auto& job : pending_jobs) {
    auto& pd_job_ptr_vec = job->reservation.empty()
                               ? part_pd_job_ptr_map[job->partition_id]
                               : resv_pd_job_ptr_map[job->reservation];
    pd_job_ptr_vec.emplace_back(job.get());
  }

  // For nodes in partition, reservations and running jobs on node are
  // collected
  absl::flat_hash_map<CranedId, NodeState> node_state_map;

  absl::flat_hash_map<PartitionId, std::vector<CranedId>> part_node_ids_map;
  absl::flat_hash_map<PartitionId, std::vector<NodeState*>>
      part_node_state_ptrs_map;

  {
    auto all_partitions_meta_map =
        g_meta_container->GetAllPartitionsMetaMapConstPtr();

    for (auto& [partition_id, partition_metas] : *all_partitions_meta_map) {
      if (!part_pd_job_ptr_map.contains(partition_id)) {
        continue;  // no pending jobs, skip
      }
      auto part_meta_ptr = partition_metas.GetExclusivePtr();
      part_node_ids_map.emplace(
          partition_id, std::vector<CranedId>{part_meta_ptr->craned_ids.begin(),
                                              part_meta_ptr->craned_ids.end()});
    }
  }

  {
    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
    for (const auto& [partition_id, craned_ids] : part_node_ids_map) {
      for (const auto& craned_id : craned_ids) {
        auto it = node_state_map.find(craned_id);
        if (it == node_state_map.end()) {
          auto craned_meta = craned_meta_map->at(craned_id).GetExclusivePtr();
          if (!craned_meta) {
            CRANE_ERROR("Craned {} not found", craned_id);
            continue;
          }
          if (!craned_meta->alive || craned_meta->drain) {
            CRANE_TRACE("Craned {} is not alive or in drain mode, skip it",
                        craned_id);
            continue;
          }
          it = node_state_map
                   .emplace(craned_id,
                            NodeState(craned_id, craned_meta->res_total))
                   .first;
        }
      }
    }
  }

  for (const auto& [partition_id, craned_ids] : part_node_ids_map) {
    auto& node_info_vec = part_node_state_ptrs_map[partition_id];
    node_info_vec.reserve(craned_ids.size());
    for (const auto& craned_id : craned_ids) {
      auto it = node_state_map.find(craned_id);
      if (it == node_state_map.end()) continue;
      node_info_vec.emplace_back(&it->second);
    }
  }

  // For nodes in reservation, res_avail on node and running jobs are
  // collected
  absl::flat_hash_map<
      ResvId, std::pair<absl::Time, absl::flat_hash_map<CranedId, NodeState>>>
      resv_node_state_map;
  absl::flat_hash_map<ResvId, std::vector<NodeState*>> resv_node_state_ptrs_map;
  absl::flat_hash_map<CranedId, absl::Time> craned_id_first_resv_map;

  {
    auto reservation_meta_map = g_meta_container->GetResvMetaMapPtr();
    for (auto& [reservation_id, reservation_meta] : *reservation_meta_map) {
      auto resv_meta = reservation_meta.GetExclusivePtr();
      if (now >= resv_meta->end_time) {
        CRANE_WARN("Reservation {} expired but not cleaned up", reservation_id);
        continue;
      }
      for (const CranedId& craned_id : resv_meta->craned_ids) {
        auto it = craned_id_first_resv_map.find(craned_id);
        if (it == craned_id_first_resv_map.end()) {
          craned_id_first_resv_map[craned_id] = resv_meta->start_time;
        } else if (resv_meta->start_time < it->second) {
          it->second = resv_meta->start_time;
        }
      }
      if (now >= resv_meta->start_time) {
        for (const auto& [craned_id, res] :
             resv_meta->res_total.EachNodeResMap()) {
          auto node_state_it = node_state_map.find(craned_id);
          if (node_state_it != node_state_map.end()) {
            node_state_it->second.allocated_res.emplace_back(
                resv_meta->end_time, res);
          }
        }
        auto it = resv_pd_job_ptr_map.find(reservation_id);
        if (it == resv_pd_job_ptr_map.end()) {
          continue;  // no pending jobs, skip
        }
        auto& [resv_end_time, resv_craned_meta] =
            resv_node_state_map[reservation_id];
        resv_end_time = resv_meta->end_time;
        for (const auto& [craned_id, res] :
             resv_meta->res_total.EachNodeResMap()) {
          auto it =
              resv_craned_meta.emplace(craned_id, NodeState(craned_id, res));
        }
        auto& resv_node_info = resv_node_state_ptrs_map[reservation_id];
        for (auto& [craned_id, node_state] : resv_craned_meta) {
          resv_node_info.emplace_back(&node_state);
        }
      } else {
        for (const auto& [craned_id, res] :
             resv_meta->res_total.EachNodeResMap()) {
          auto node_state_it = node_state_map.find(craned_id);
          if (node_state_it != node_state_map.end()) {
            node_state_it->second.reserved_res.emplace_back(
                resv_meta->start_time, resv_meta->end_time, res);
          }
        }
      }
    }
  }

  {
    for (const auto& job : running_jobs) {
      absl::Time end_time = std::max(
          job->end_time,
          now + absl::Seconds(1));  // In case StepStatusChange is delayed
      if (job->reservation.empty()) {
        for (auto& [craned_id, res] : job->allocated_res.EachNodeResMap()) {
          auto it = node_state_map.find(craned_id);
          if (it != node_state_map.end()) {
            it->second.allocated_res.emplace_back(end_time, res);
          }
        }
      } else {
        auto it = resv_node_state_map.find(job->reservation);
        if (it == resv_node_state_map.end()) {
          CRANE_ERROR(
              "Reservation {} of running job #{} not found in "
              "resv_node_state_"
              "map",
              job->reservation, job->job_id);
          continue;
        }
        auto& craned_id_node_map = it->second.second;
        for (auto& [craned_id, res] : job->allocated_res.EachNodeResMap()) {
          craned_id_node_map.at(craned_id).allocated_res.emplace_back(end_time,
                                                                      res);
        }
      }
    }
  }

  // Build TimeAvailResMap
  for (auto& [craned_id, node_state] : node_state_map) {
    node_state.InitTimeAvailResMap(now);
  }
  for (auto& [resv_id, resv_info] : resv_node_state_map) {
    for (auto& [craned_id, node_state] : resv_info.second) {
      node_state.InitTimeAvailResMap(now, resv_info.first);
    }
  }

  // Build LocalSchedulers
  // TODO: do it in parallel
  absl::flat_hash_map<PartitionId, LocalScheduler> part_scheduler_map;
  absl::flat_hash_map<ResvId, LocalScheduler> resv_scheduler_map;
  for (const auto& [part_id, node_state_ptrs_vec] : part_node_state_ptrs_map) {
    part_scheduler_map[part_id].InitializeNodeSelector<MinCpuTimeRatioFirst>(
        now, node_state_ptrs_vec);
  }
  for (const auto& [resv_id, node_state_ptrs_vec] : resv_node_state_ptrs_map) {
    resv_scheduler_map[resv_id].InitializeNodeSelector<MinCpuTimeRatioFirst>(
        now, node_state_ptrs_vec);
  }

  std::vector<PdJobInScheduler*> job_ptr_vec;
  m_priority_sorter_->GetOrderedJobPtrVec(now, pending_jobs, running_jobs,
                                          g_config.ScheduledBatchSize,
                                          job_ptr_vec);

  g_licenses_manager->CheckLicenseCountSufficient(&job_ptr_vec);

  // Schedule pending jobs
  // TODO: do it in parallel
  for (const auto& job : job_ptr_vec) {
    if (!job->reason.empty()) continue;

    LocalScheduler* scheduler;
    if (job->reservation.empty()) {
      auto it = part_scheduler_map.find(job->partition_id);
      if (it == part_scheduler_map.end()) {
        job->reason = "Partition Not Found";
        continue;
      }
      scheduler = &part_scheduler_map[job->partition_id];
    } else {
      auto it = resv_scheduler_map.find(job->reservation);
      if (it == resv_scheduler_map.end()) {
        job->reason = "Reservation Not Found";
        continue;
      }
      scheduler = &resv_scheduler_map[job->reservation];
    }

    bool ok = scheduler->CalculateRunningNodesAndStartTime_(now, job);

    if (!ok) {
      // Leave start_time unset
      job->reason = "Resource";
    } else {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "\t job #{} ExpectedStartTime=now+{}s, EndTime=now+{}s",
            job->job_id, absl::ToInt64Seconds(job->start_time - now),
            absl::ToInt64Seconds(job->start_time + job->time_limit - now));
      }
      scheduler->UpdateNodeSelector(job);

      if (job->start_time != now) {
        if (job->reservation.empty()) {
          for (const CranedId& craned_id : job->craned_ids) {
            auto it = craned_id_first_resv_map.find(craned_id);
            if (it != craned_id_first_resv_map.end() &&
                it->second < now + job->time_limit) {
              job->reason = "Resource Reserved";
              break;
            }
          }
          if (!job->reason.empty()) {
            continue;
          }
          for (const CranedId& craned_id : job->craned_ids) {
            const auto& res_avail = node_state_map.at(craned_id).res_avail;
            if (!(job->allocated_res.EachNodeResMap().at(craned_id) <=
                  res_avail)) {
              job->reason = "Resource";
              break;
            }
          }
        } else {
          for (const CranedId& craned_id : job->craned_ids) {
            const auto& res_avail = resv_node_state_map.at(job->reservation)
                                        .second.at(craned_id)
                                        .res_avail;
            if (!(job->allocated_res.EachNodeResMap().at(craned_id) <=
                  res_avail)) {
              job->reason = "Resource";
              break;
            }
          }
        }
        if (job->reason.empty()) {
          job->reason = "Priority";
        }
      }
    }
  }
}

void JobScheduler::ProcessFinalSteps_(
    std::unordered_set<StepInCtld*> const& steps) {
  PersistAndTransferStepsToMongodb_(steps);
  // CallPluginHookForFinalJobs_(jobs);
}

void JobScheduler::PersistAndTransferStepsToMongodb_(
    std::unordered_set<StepInCtld*> const& steps) {
  if (steps.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginStepVarDbTransaction(&txn_id);
  for (StepInCtld* step : steps) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfStep(txn_id, step->StepDbId(),
                                                       step->RuntimeAttr()))
      CRANE_ERROR("Failed to call UpdateRuntimeAttrOfStep() for step #{}.{}",
                  step->job_id, step->StepId());
  }

  g_embedded_db_client->CommitStepVarDbTransaction(txn_id);

  // Now jobs are in MongoDB.
  if (!g_db_client->InsertSteps(steps)) {
    CRANE_ERROR("Failed to call g_db_client->InsertSteps() ");
    return;
  }

  // Remove jobs in final queue.
  std::vector<step_db_id_t> db_ids;
  for (StepInCtld* step : steps) db_ids.emplace_back(step->StepDbId());

  if (!g_embedded_db_client->PurgeEndedSteps(db_ids)) {
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedSteps() "
        "for final jobs");
  }
}

void JobScheduler::ProcessFinalJobs_(
    const std::unordered_set<JobInCtld*>& jobs) {
  PersistAndTransferJobsToMongodb_(jobs);
  CallPluginHookForFinalJobs_(jobs);
}

void JobScheduler::CallPluginHookForFinalJobs_(
    std::unordered_set<JobInCtld*> const& jobs) {
  if (g_config.Plugin.Enabled && !jobs.empty()) {
    std::vector<crane::grpc::JobInfo> jobs_post_comp;
    for (JobInCtld* job : jobs) {
      crane::grpc::JobInfo t;
      job->SetFieldsOfJobInfo(&t);
      jobs_post_comp.emplace_back(std::move(t));
    }
    g_plugin_client->EndHookAsync(std::move(jobs_post_comp));
  }
}

void JobScheduler::PersistAndTransferJobsToMongodb_(
    std::unordered_set<JobInCtld*> const& jobs) {
  if (jobs.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  for (JobInCtld* job : jobs) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJob(txn_id, job->JobDbId(),
                                                      job->RuntimeAttr()))
      CRANE_ERROR("Failed to call UpdateRuntimeAttrOfJob() for job #{}",
                  job->JobId());
  }

  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  // Now jobs are in MongoDB.
  if (!g_db_client->InsertJobs(jobs)) {
    CRANE_ERROR("Failed to call g_db_client->InsertJobs() ");
    return;
  }

  // Remove jobs in final queue.
  std::unordered_map<job_id_t, job_db_id_t> db_ids;
  for (JobInCtld* job : jobs) db_ids[job->JobId()] = job->JobDbId();

  if (!g_embedded_db_client->PurgeEndedJobs(db_ids)) {
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedJobs() "
        "for final jobs");
  }
}

CraneExpected<void> JobScheduler::HandleUnsetOptionalInJobToCtld(
    JobInCtld* job) {
  if (job->JobToCtld().has_io_meta()) {
    auto* io_meta = job->MutableJobToCtld()->mutable_io_meta();
    if (!io_meta->has_open_mode_append())
      io_meta->set_open_mode_append(g_config.JobFileOpenModeAppend);
  }

  return {};
}

CraneExpected<void> JobScheduler::AcquireJobAttributes(JobInCtld* job) {
  auto part_it = g_config.Partitions.find(job->partition_id);
  if (part_it == g_config.Partitions.end()) {
    CRANE_ERROR("Failed to call AcquireJobAttributes: no such partition {}",
                job->partition_id);
    return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);
  }

  job->partition_priority = part_it->second.priority;

  Config::Partition const& part_meta = part_it->second;
  CRANE_TRACE(
      "Job {} node res:{}, job res:{}, part default_mem_per_cpu:{}, "
      "default_mem_per_node:{}, max_mem_per_cpu:{}, max_mem_per_node:{}",
      job->JobId(), util::ReadableResourceView(job->req_node_res_view),
      util::ReadableResourceView(job->req_task_res_view),
      part_meta.default_mem_per_cpu, part_meta.default_mem_per_node,
      part_meta.max_mem_per_cpu, part_meta.max_mem_per_node);

  bool user_set_mem_per_cpu = job->JobToCtld().has_mem_per_cpu();
  bool user_set_mem_per_node = job->JobToCtld().has_mem_per_node();

  if (user_set_mem_per_cpu && user_set_mem_per_node) {
    CRANE_ERROR(
        "Job {} has both mem_per_cpu and mem_per_node set by user, which is "
        "not allowed.",
        job->JobId());
    return std::unexpected(CraneErrCode::ERR_INVALID_RESOURCE);
  }

  CRANE_ASSERT(job->req_node_res_view.GetCpuCount() == cpu_t{0});

  if (!user_set_mem_per_cpu && !user_set_mem_per_node) {
    if (part_meta.default_mem_per_node != 0) {
      job->req_node_res_view.SetMemoryBytes(part_meta.default_mem_per_node);
      job->req_node_res_view.SetMemorySwBytes(part_meta.default_mem_per_node);
      user_set_mem_per_node = true;
      CRANE_TRACE("default_mem_per_node for job #{} is set to {}", job->JobId(),
                  part_meta.default_mem_per_node);
    } else if (part_meta.default_mem_per_cpu != 0) {
      auto job_mem_per_cpu = part_meta.default_mem_per_cpu;
      job->req_task_res_view.SetMemoryBytes(
          static_cast<double>(job->req_task_res_view.GetCpuCount()) *
          job_mem_per_cpu);
      job->req_task_res_view.SetMemorySwBytes(
          static_cast<double>(job->req_task_res_view.GetCpuCount()) *
          job_mem_per_cpu);
      user_set_mem_per_cpu = true;
      CRANE_TRACE("default_mem_per_cpu for job #{} is set to {}", job->JobId(),
                  job_mem_per_cpu);
    } else {
      CRANE_ERROR(
          "Neither mem_per_cpu nor mem_per_node is set for job #{} and "
          "partition {}, and no default is provided by partition meta.",
          job->JobId(), job->partition_id);
      return std::unexpected(CraneErrCode::ERR_INVALID_RESOURCE);
    }
  }

  if (part_meta.max_mem_per_cpu != 0) {
    if (user_set_mem_per_cpu) {
      auto max_job_mem =
          static_cast<double>(job->req_task_res_view.CpuCountDouble()) *
          part_meta.max_mem_per_cpu;
      if (job->req_task_res_view.GetMemoryBytes() > max_job_mem) {
        job->req_task_res_view.SetMemoryBytes(max_job_mem);
        job->req_task_res_view.SetMemorySwBytes(max_job_mem);
      }
    } else {
      // mem_per_node / (ntasks_per_node * cpus_per_task) <= max_mem_per_cpu
      // ntasks_per_node >= ceil(mem_per_node / (max_mem_per_cpu *
      // cpus_per_task))
      double cpus_per_task = job->req_task_res_view.CpuCountDouble();
      if (cpus_per_task > 0) {
        auto required_min = static_cast<uint32_t>(std::ceil(
            static_cast<double>(job->req_node_res_view.GetMemoryBytes()) /
            (part_meta.max_mem_per_cpu * cpus_per_task)));
        job->ntasks_per_node_min =
            std::max(job->ntasks_per_node_min, required_min);
      }
    }
  }

  if (part_meta.max_mem_per_node != 0) {
    if (user_set_mem_per_node) {
      if (job->req_node_res_view.GetMemoryBytes() >
          part_meta.max_mem_per_node) {
        job->req_node_res_view.SetMemoryBytes(part_meta.max_mem_per_node);
        job->req_node_res_view.SetMemorySwBytes(part_meta.max_mem_per_node);
      }
    } else {
      // ntasks_per_node * task_memory <= max_mem_per_node
      // ntasks_per_node_max <= floor(max_mem_per_node / task_memory)
      auto job_mem = job->req_task_res_view.GetMemoryBytes();
      if (job_mem > 0) {
        auto required_max = static_cast<uint32_t>(
            static_cast<double>(part_meta.max_mem_per_node) / job_mem);
        if (job->ntasks_per_node_max == 0 ||
            job->ntasks_per_node_max > required_max) {
          job->ntasks_per_node_max = required_max;
        }
      }
    }
  }

  // Finalize ntasks_per_node bounds from job distribution constraint.
  // One pass suffices: the second iteration is provably idempotent
  // because N >= K*m holds for any valid job (ntasks >= node_num).
  {
    uint32_t dist_max = job->ntasks - job->node_num + 1;
    if (job->ntasks_per_node_max == 0)
      job->ntasks_per_node_max = dist_max;
    else
      job->ntasks_per_node_max = std::min(job->ntasks_per_node_max, dist_max);

    job->ntasks_per_node_min =
        std::max(job->ntasks_per_node_min,
                 job->ntasks - (job->node_num - 1) * job->ntasks_per_node_max);

    job->ntasks_per_node_max =
        std::min(job->ntasks_per_node_max,
                 job->ntasks - (job->node_num - 1) * job->ntasks_per_node_min);
  }

  if (job->ntasks_per_node_min > job->ntasks_per_node_max) {
    CRANE_ERROR(
        "Job #{}: ntasks_per_node_min ({}) > ntasks_per_node_max ({}), "
        "infeasible constraints.",
        job->JobId(), job->ntasks_per_node_min, job->ntasks_per_node_max);
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  CRANE_TRACE(
      "Job #{} after mem adjust: node res:{}, job res:{}, "
      "ntasks_per_node_min:{}, ntasks_per_node_max:{}",
      job->JobId(), util::ReadableResourceView(job->req_node_res_view),
      util::ReadableResourceView(job->req_task_res_view),
      job->ntasks_per_node_min, job->ntasks_per_node_max);

  job->req_total_res_view = job->req_node_res_view * job->node_num +
                            job->req_task_res_view * job->ntasks;
  CRANE_TRACE("Job #{} total res:{}", job->JobId(),
              util::ReadableResourceView(job->req_total_res_view));

  auto check_qos_result =
      g_account_manager->CheckQosLimitOnJob(job->Username(), job->account, job);
  if (!check_qos_result) {
    CRANE_ERROR("Failed to call CheckQosLimitOnJob: {}",
                CraneErrStr(check_qos_result.error()));
    return check_qos_result;
  }

  if (!job->JobToCtld().nodelist().empty() && job->included_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(job->JobToCtld().nodelist(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVALID_NODE_LIST);

    for (auto&& node : nodes) job->included_nodes.emplace(std::move(node));
  }

  if (!job->JobToCtld().excludes().empty() && job->excluded_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(job->JobToCtld().excludes(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVALID_EX_NODE_LIST);

    for (auto&& node : nodes) job->excluded_nodes.emplace(std::move(node));
  }

  if (!job->JobToCtld().licenses_count().empty()) {
    auto check_licenses_result = g_licenses_manager->CheckLicensesLegal(
        job->JobToCtld().licenses_count(), job->JobToCtld().is_licenses_or());
    if (!check_licenses_result) {
      CRANE_ERROR("Failed to call CheckLicensesLegal: {}",
                  check_licenses_result.error());
      return std::unexpected(CraneErrCode::ERR_LICENSE_LEGAL_FAILED);
    }
  }

  if (g_config.WckeyValid) {
    if (job->MutableJobToCtld()->has_wckey() &&
        !job->MutableJobToCtld()->wckey().empty()) {
      std::string wckey = job->MutableJobToCtld()->wckey();
      auto wckey_scoped_ptr =
          g_account_manager->GetExistedWckeyInfo(wckey, job->Username());
      if (!wckey_scoped_ptr) {
        CRANE_DEBUG("Job wckey '{}' not found in the wckey database, rejected.",
                    wckey);
        return std::unexpected(CraneErrCode::ERR_INVALID_WCKEY);
      }

      job->wckey = wckey;
      // Note: Ignore error from GetExistedDefaultWckeyName since the user's
      // wckey was already validated; the default check is only for marking
    } else {
      // No wckey provided; use the default
      auto result =
          g_account_manager->GetExistedDefaultWckeyName(job->Username());
      job->using_default_wckey = true;
      if (result) job->wckey = result.value();
    }
  } else {
    job->wckey.clear();
  }

  return {};
}

CraneExpected<void> JobScheduler::CheckJobValidity(JobInCtld* job) {
  if (!CheckIfTimeLimitIsValid(job->time_limit))
    return std::unexpected(CraneErrCode::ERR_TIME_TIMIT_BEYOND);

  // Check res req valid
  if (job->req_total_res_view.GetMemoryBytes() == 0) {
    CRANE_DEBUG("Job #{} has zero memory request.", job->JobId());
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }
  if (job->req_task_res_view.CpuCountDouble() == 0) {
    CRANE_DEBUG("Job #{} has zero cpu request.", job->JobId());
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  if (job->deadline_time <= job->SubmitTime())
    return std::unexpected(CraneErrCode::ERR_INVALID_DEADLINE);

  // Check whether the selected partition is able to run this job.
  std::unordered_set<std::string> avail_nodes;
  {
    // Preserve lock ordering.
    auto metas_ptr = g_meta_container->GetPartitionMetasPtr(job->partition_id);

    // Since we do not access the elements in partition_metas_m

    // Check whether the selected partition is able to run this job.
    if (!(job->req_total_res_view <=
          metas_ptr->partition_global_meta.res_total_inc_dead)) {
      CRANE_TRACE(
          "Resource not enough for job #{}. "
          "Partition total: cpu {}, mem: {}, mem+sw: {}, gres: {}",
          job->JobId(),
          metas_ptr->partition_global_meta.res_total_inc_dead.GetCpuCount(),
          util::ReadableMemory(metas_ptr->partition_global_meta
                                   .res_total_inc_dead.GetMemoryBytes()),
          util::ReadableMemory(metas_ptr->partition_global_meta
                                   .res_total_inc_dead.GetMemorySwBytes()),
          util::ReadableGresMap(
              metas_ptr->partition_global_meta.res_total.GetGresMap()));
      return std::unexpected(CraneErrCode::ERR_NO_RESOURCE);
    }

    if (job->node_num > metas_ptr->craned_ids.size()) {
      CRANE_TRACE(
          "Nodes not enough for job #{}. "
          "Partition total Nodes: {}",
          job->JobId(), metas_ptr->craned_ids.size());
      return std::unexpected(CraneErrCode::ERR_INVALID_NODE_NUM);
    }

    if (job->reservation != "") {
      if (!g_meta_container->GetResvMetaMapConstPtr()->contains(
              job->reservation)) {
        CRANE_TRACE("Reservation {} not found for job #{}", job->reservation,
                    job->JobId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }

      auto resv_meta = g_meta_container->GetResvMetaPtr(job->reservation);

      if (resv_meta->part_id != "" && resv_meta->part_id != job->partition_id) {
        CRANE_TRACE("Partition {} not allowed for reservation {} for job #{}",
                    job->partition_id, job->reservation, job->JobId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }

      // if passed, either not in the black list (true, true)
      // or in the white list (false, false)
      if (resv_meta->accounts_black_list ^
          !resv_meta->accounts.contains(job->account)) {
        CRANE_TRACE("Account {} not allowed for reservation {} for job #{}",
                    job->account, job->reservation, job->JobId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }
      if (resv_meta->users_black_list ^
          !resv_meta->users.contains(job->Username())) {
        CRANE_TRACE("User {} not allowed for reservation {} for job #{}",
                    job->Username(), job->reservation, job->JobId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }

      if (!job->included_nodes.empty()) {
        auto reserved_craned_id_list = resv_meta->craned_ids;
        std::unordered_set<std::string> reserved_craned_id_set;
        reserved_craned_id_set.insert(reserved_craned_id_list.begin(),
                                      reserved_craned_id_list.end());
        for (const auto& craned_id : job->included_nodes) {
          if (!reserved_craned_id_set.contains(craned_id)) {
            CRANE_TRACE("Craned {} is not in the reservation {} for job #{}",
                        craned_id, job->reservation, job->JobId());
            return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
          }
        }
      }
    }

    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
    for (const auto& craned_id : metas_ptr->craned_ids) {
      auto craned_meta = craned_meta_map->at(craned_id).GetExclusivePtr();
      if (job->req_node_res_view + job->req_task_res_view <=
              craned_meta->res_total &&
          (job->included_nodes.empty() ||
           job->included_nodes.contains(craned_id)) &&
          (job->excluded_nodes.empty() ||
           !job->excluded_nodes.contains(craned_id)))
        avail_nodes.emplace(craned_meta->static_meta.hostname);

      if (avail_nodes.size() >= job->node_num) break;
    }
  }

  if (job->node_num > avail_nodes.size()) {
    CRANE_TRACE(
        "Resource not enough. Job #{} needs {} nodes, while only {} "
        "nodes satisfy its requirement.",
        job->JobId(), job->node_num, avail_nodes.size());
    return std::unexpected(CraneErrCode::ERR_NO_ENOUGH_NODE);
  }

  return {};
}

CraneExpected<void> JobScheduler::HandleUnsetOptionalInStepToCtld(
    StepInCtld* step) {
  if (step->StepToCtld().has_io_meta()) {
    auto* io_meta = step->MutableStepToCtld()->mutable_io_meta();
    if (!io_meta->has_open_mode_append())
      io_meta->set_open_mode_append(g_config.JobFileOpenModeAppend);
  }
  return {};
}

CraneExpected<void> JobScheduler::AcquireStepAttributes(StepInCtld* step) {
  if (!step->StepToCtld().nodelist().empty() && step->included_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(step->StepToCtld().nodelist(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVALID_NODE_LIST);

    for (auto&& node : nodes) step->included_nodes.emplace(std::move(node));
  }

  if (!step->StepToCtld().excludes().empty() && step->excluded_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(step->StepToCtld().excludes(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVALID_EX_NODE_LIST);

    for (auto&& node : nodes) step->excluded_nodes.emplace(std::move(node));
  }

  auto part_it = g_config.Partitions.find(step->job->partition_id);
  if (part_it != g_config.Partitions.end()) {
    Config::Partition const& part_meta = part_it->second;

    bool user_set_mem_per_cpu = step->StepToCtld().has_mem_per_cpu();
    bool user_set_mem_per_node = step->StepToCtld().has_mem_per_node();

    if (user_set_mem_per_cpu && user_set_mem_per_node) {
      CRANE_ERROR(
          "Step #{}.{} has both mem_per_cpu and mem_per_node set, "
          "which is not allowed.",
          step->job_id, step->StepId());
      return std::unexpected(CraneErrCode::ERR_INVALID_RESOURCE);
    }

    bool no_memory_set = (step->req_node_res_view.GetMemoryBytes() == 0 &&
                          step->req_task_res_view.GetMemoryBytes() == 0);

    if (no_memory_set) {
      if (part_meta.default_mem_per_node != 0) {
        step->req_node_res_view.SetMemoryBytes(part_meta.default_mem_per_node);
        step->req_node_res_view.SetMemorySwBytes(
            part_meta.default_mem_per_node);
        user_set_mem_per_node = true;
      } else if (part_meta.default_mem_per_cpu != 0) {
        auto mem_per_cpu = part_meta.default_mem_per_cpu;
        step->req_task_res_view.SetMemoryBytes(
            static_cast<double>(step->req_task_res_view.GetCpuCount()) *
            mem_per_cpu);
        step->req_task_res_view.SetMemorySwBytes(
            static_cast<double>(step->req_task_res_view.GetCpuCount()) *
            mem_per_cpu);
        user_set_mem_per_cpu = true;
      } else {
        CRANE_ERROR(
            "Step #{}.{}: neither mem_per_cpu nor mem_per_node is "
            "set, and partition {} has no default.",
            step->job_id, step->StepId(), step->job->partition_id);
        return std::unexpected(CraneErrCode::ERR_INVALID_RESOURCE);
      }
    }

    if (part_meta.max_mem_per_cpu != 0) {
      if (user_set_mem_per_cpu) {
        auto max_job_mem =
            static_cast<double>(step->req_task_res_view.CpuCountDouble()) *
            part_meta.max_mem_per_cpu;
        if (step->req_task_res_view.GetMemoryBytes() > max_job_mem) {
          step->req_task_res_view.SetMemoryBytes(max_job_mem);
          step->req_task_res_view.SetMemorySwBytes(max_job_mem);
        }
      } else if (user_set_mem_per_node) {
        double cpus_per_task = step->req_task_res_view.CpuCountDouble();
        if (cpus_per_task > 0) {
          auto required_min = static_cast<uint32_t>(std::ceil(
              static_cast<double>(step->req_node_res_view.GetMemoryBytes()) /
              (part_meta.max_mem_per_cpu * cpus_per_task)));
          step->ntasks_per_node_min =
              std::max(step->ntasks_per_node_min, required_min);
        }
      }
    }

    if (part_meta.max_mem_per_node != 0) {
      if (user_set_mem_per_node) {
        if (step->req_node_res_view.GetMemoryBytes() >
            part_meta.max_mem_per_node) {
          step->req_node_res_view.SetMemoryBytes(part_meta.max_mem_per_node);
          step->req_node_res_view.SetMemorySwBytes(part_meta.max_mem_per_node);
        }
      } else if (user_set_mem_per_cpu) {
        auto job_mem = step->req_task_res_view.GetMemoryBytes();
        if (job_mem > 0) {
          auto required_max = static_cast<uint32_t>(
              static_cast<double>(part_meta.max_mem_per_node) / job_mem);
          if (step->ntasks_per_node_max == 0 ||
              step->ntasks_per_node_max > required_max) {
            step->ntasks_per_node_max = required_max;
          }
        }
      }
    }
  }

  {
    uint32_t dist_max = step->ntasks - step->node_num + 1;
    if (step->ntasks_per_node_max == 0)
      step->ntasks_per_node_max = dist_max;
    else
      step->ntasks_per_node_max = std::min(step->ntasks_per_node_max, dist_max);

    step->ntasks_per_node_min = std::max(
        step->ntasks_per_node_min,
        step->ntasks - (step->node_num - 1) * step->ntasks_per_node_max);

    step->ntasks_per_node_max = std::min(
        step->ntasks_per_node_max,
        step->ntasks - (step->node_num - 1) * step->ntasks_per_node_min);
  }

  if (step->ntasks_per_node_min > step->ntasks_per_node_max) {
    CRANE_ERROR(
        "Step #{}.{}: ntasks_per_node_min ({}) > "
        "ntasks_per_node_max ({}), infeasible constraints.",
        step->job_id, step->StepId(), step->ntasks_per_node_min,
        step->ntasks_per_node_max);
    return std::unexpected(CraneErrCode::ERR_INVALID_RESOURCE);
  }

  step->req_total_res_view = step->req_node_res_view * step->node_num +
                             step->req_task_res_view * step->ntasks;

  return {};
}

CraneExpected<void> JobScheduler::CheckStepValidity(StepInCtld* step) {
  auto* job = step->job;
  if (!CheckIfTimeLimitIsValid(step->time_limit))
    return std::unexpected(CraneErrCode::ERR_TIME_TIMIT_BEYOND);

  if (step->req_total_res_view.GetMemoryBytes() == 0) {
    CRANE_DEBUG("Step #{}.{} has zero memory request.", step->job_id,
                step->StepId());
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }
  if (step->req_task_res_view.GetCpuCount() == cpu_t{0}) {
    CRANE_DEBUG("Step #{}.{} has zero cpu request.", step->job_id,
                step->StepId());
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  if (job->uid != step->uid) {
    return std::unexpected{CraneErrCode::ERR_PERMISSION_DENIED};
  }

  if (step->type == crane::grpc::JobType::Container) {
    // Check if step is send to a job not supporting container
    if (job->type != crane::grpc::JobType::Container)
      return std::unexpected{CraneErrCode::ERR_INVALID_PARAM};
    // Copy pod_meta for step
    step->pod_meta = job->pod_meta;
  }

  std::unordered_set<std::string> avail_nodes;
  for (const auto& craned_id : job->CranedIds()) {
    const auto& job_res = job->AllocatedRes();
    if (step->req_node_res_view + step->req_task_res_view <=
            job_res.At(craned_id) &&
        (step->included_nodes.empty() ||
         step->included_nodes.contains(craned_id)) &&
        (step->excluded_nodes.empty() ||
         !step->excluded_nodes.contains(craned_id)))
      avail_nodes.emplace(craned_id);

    if (avail_nodes.size() >= step->node_num) break;
  }

  if (step->node_num > avail_nodes.size()) {
    CRANE_TRACE(
        "Resource not enough. Step #{}.{} needs {} nodes, while only {} "
        "nodes in job satisfy its requirement.",
        step->job_id, step->StepId(), step->node_num, avail_nodes.size());
    return std::unexpected(CraneErrCode::ERR_NO_ENOUGH_NODE);
  }

  if (job->uid != step->uid) {
    return std::unexpected{CraneErrCode::ERR_PERMISSION_DENIED};
  }

  if (!(step->req_total_res_view <= job->req_total_res_view))
    return std::unexpected{CraneErrCode::ERR_STEP_RES_BEYOND};

  return {};
}

void JobScheduler::TerminateJobsOnCraned(const CranedId& craned_id,
                                         uint32_t exit_code) {
  CRANE_TRACE("Terminate jobs on craned {}", craned_id);

  // The order of LockGuards matters.
  LockGuard indexes_guard(&m_job_indexes_mtx_);

  auto it = m_node_to_jobs_map_.find(craned_id);
  if (it != m_node_to_jobs_map_.end()) {
    // m_node_to_jobs_map_[craned_id] will be cleaned in
    // StepStatusChangeNoLock_. Do not clean it here and make a copy of
    // it->second.
    std::vector<job_id_t> job_ids(it->second.begin(), it->second.end());

    for (job_id_t job_id : job_ids)
      StepStatusChangeAsync(job_id, kDaemonStepId, craned_id,
                            crane::grpc::JobStatus::Failed, exit_code,
                            "Terminated",
                            google::protobuf::util::TimeUtil::GetCurrentTime());
  } else {
    CRANE_TRACE("No job is executed by craned {}. Ignore cleaning step...",
                craned_id);
  }
}

void MultiFactorPriority::GetOrderedJobPtrVec(
    const absl::Time& now,
    const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs,
    const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
    size_t limit, std::vector<PdJobInScheduler*>& job_ptr_vec) {
  CalculateFactorBound_(pending_jobs, running_jobs, now);

  job_ptr_vec.reserve(pending_jobs.size());
  for (const auto& job : pending_jobs) {
    if (job->priority == 0.0) {
      job->priority = CalculatePriority_(job.get(), now);
    }
    job_ptr_vec.emplace_back(job.get());
  }

  std::ranges::sort(job_ptr_vec, [](PdJobInScheduler* a, PdJobInScheduler* b) {
    return a->priority > b->priority;
  });

  if (job_ptr_vec.size() > limit) {
    for (int i = limit; i < job_ptr_vec.size(); i++) {
      job_ptr_vec[i]->reason = "Priority";
    }
    job_ptr_vec.resize(limit);
  }
}

void MultiFactorPriority::CalculateFactorBound_(
    const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs,
    const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
    const absl::Time& now) {
  FactorBound& bound = m_factor_bound_;

  // Initialize the values of each max and min
  bound.age_max = 0;
  bound.age_min = std::numeric_limits<uint64_t>::max();

  bound.qos_priority_max = 0;
  bound.qos_priority_min = std::numeric_limits<uint32_t>::max();

  bound.part_priority_max = 0;
  bound.part_priority_min = std::numeric_limits<uint32_t>::max();

  bound.node_num_max = 0;
  bound.node_num_min = std::numeric_limits<uint32_t>::max();

  bound.mem_alloc_max = 0;
  bound.mem_alloc_min = std::numeric_limits<uint64_t>::max();

  bound.cpus_alloc_max = 0;
  bound.cpus_alloc_min = std::numeric_limits<double>::max();

  bound.service_val_max = 0;
  bound.service_val_min = std::numeric_limits<uint32_t>::max();

  bound.acc_service_val_map.clear();

  for (const auto& job : pending_jobs) {
    uint64_t age = absl::ToInt64Seconds(now - job->submit_time);
    age = std::min(age, g_config.PriorityConfig.MaxAge);

    bound.acc_service_val_map[job->account] = 0.0;

    bound.age_min = std::min(age, bound.age_min);
    bound.age_max = std::max(age, bound.age_max);

    uint32_t nodes_req = job->node_num;
    bound.node_num_min = std::min(nodes_req, bound.node_num_min);
    bound.node_num_max = std::max(nodes_req, bound.node_num_max);

    uint64_t job_mem_req = job->req_total_res_view.GetMemoryBytes();
    bound.mem_alloc_min = std::min(job_mem_req, bound.mem_alloc_min);
    bound.mem_alloc_max = std::max(job_mem_req, bound.mem_alloc_max);

    double job_cpus_req = job->req_total_res_view.CpuCountDouble();
    bound.cpus_alloc_min = std::min(job_cpus_req, bound.cpus_alloc_min);
    bound.cpus_alloc_max = std::max(job_cpus_req, bound.cpus_alloc_max);

    uint32_t qos_priority = job->qos_priority;
    bound.qos_priority_min = std::min(qos_priority, bound.qos_priority_min);
    bound.qos_priority_max = std::max(qos_priority, bound.qos_priority_max);

    uint32_t part_priority = job->partition_priority;
    bound.part_priority_min = std::min(part_priority, bound.part_priority_min);
    bound.part_priority_max = std::max(part_priority, bound.part_priority_max);
  }

  for (const auto& job : running_jobs) {
    uint32_t nodes_alloc = job->node_num;
    bound.node_num_min = std::min(nodes_alloc, bound.node_num_min);
    bound.node_num_max = std::max(nodes_alloc, bound.node_num_max);

    uint64_t mem_alloc = job->allocated_res_view.GetMemoryBytes();
    bound.mem_alloc_min = std::min(mem_alloc, bound.mem_alloc_min);
    bound.mem_alloc_max = std::max(mem_alloc, bound.mem_alloc_max);

    double cpus_alloc = job->allocated_res_view.CpuCountDouble();
    bound.cpus_alloc_min = std::min(cpus_alloc, bound.cpus_alloc_min);
    bound.cpus_alloc_max = std::max(cpus_alloc, bound.cpus_alloc_max);

    uint32_t qos_priority = job->qos_priority;
    bound.qos_priority_min = std::min(qos_priority, bound.qos_priority_min);
    bound.qos_priority_max = std::max(qos_priority, bound.qos_priority_max);

    uint32_t part_priority = job->partition_priority;
    bound.part_priority_min = std::min(part_priority, bound.part_priority_min);
    bound.part_priority_max = std::max(part_priority, bound.part_priority_max);
  }

  for (const auto& job : running_jobs) {
    double service_val = 0;
    if (bound.cpus_alloc_max > bound.cpus_alloc_min)
      service_val +=
          1.0 *
          (job->allocated_res_view.CpuCountDouble() - bound.cpus_alloc_min) /
          (bound.cpus_alloc_max - bound.cpus_alloc_min);
    else
      // += 1.0 here rather than 0.0 in case that the final service_val is 0.
      // If the final service_val is 0, the running time of the job will not
      // be ruled out in calculation. We must avoid that.
      service_val += 1.0;

    if (bound.node_num_max > bound.node_num_min)
      service_val += 1.0 * (job->node_num - bound.node_num_min) /
                     (bound.node_num_max - bound.node_num_min);
    else
      service_val += 1.0;

    if (bound.mem_alloc_max > bound.mem_alloc_min)
      service_val +=
          1.0 *
          static_cast<double>(job->allocated_res_view.GetMemoryBytes() -
                              bound.mem_alloc_min) /
          static_cast<double>(bound.mem_alloc_max - bound.mem_alloc_min);
    else
      service_val += 1.0;

    uint64_t run_time = ToInt64Seconds(now - job->start_time);
    bound.acc_service_val_map[job->account] +=
        service_val * static_cast<double>(run_time);
  }

  for (const auto& [acc_name, ser_val] : bound.acc_service_val_map) {
    bound.service_val_min = std::min(ser_val, bound.service_val_min);
    bound.service_val_max = std::max(ser_val, bound.service_val_max);
  }
}

double MultiFactorPriority::CalculatePriority_(PdJobInScheduler* job,
                                               const absl::Time& now) const {
  FactorBound const& bound = m_factor_bound_;

  uint64_t job_age = ToInt64Seconds(now - job->submit_time);
  job_age = std::min(job_age, g_config.PriorityConfig.MaxAge);

  uint32_t job_qos_priority = job->qos_priority;
  uint32_t job_part_priority = job->partition_priority;
  uint32_t job_nodes_alloc = job->node_num;
  uint64_t job_mem_alloc = job->req_total_res_view.GetMemoryBytes();
  double job_cpus_alloc = job->req_total_res_view.CpuCountDouble();
  double job_service_val = bound.acc_service_val_map.at(job->account);

  double qos_factor{0};
  double age_factor{0};
  double partition_factor{0};
  double job_size_factor{0};
  double fair_share_factor{0};

  // age_factor
  if (bound.age_max > bound.age_min)
    age_factor = 1.0 * static_cast<double>(job_age - bound.age_min) /
                 static_cast<double>(bound.age_max - bound.age_min);

  // qos_factor
  if (bound.qos_priority_max > bound.qos_priority_min)
    qos_factor = 1.0 * (job_qos_priority - bound.qos_priority_min) /
                 (bound.qos_priority_max - bound.qos_priority_min);

  // partition_factor
  if (bound.part_priority_max > bound.part_priority_min)
    partition_factor = 1.0 * (job_part_priority - bound.part_priority_min) /
                       (bound.part_priority_max - bound.part_priority_min);

  // job_size_factor
  if (bound.cpus_alloc_max > bound.cpus_alloc_min)
    job_size_factor += 1.0 * (job_cpus_alloc - bound.cpus_alloc_min) /
                       (bound.cpus_alloc_max - bound.cpus_alloc_min);
  if (bound.node_num_max > bound.node_num_min)
    job_size_factor += 1.0 * (job_nodes_alloc - bound.node_num_min) /
                       (bound.node_num_max - bound.node_num_min);
  if (bound.mem_alloc_max > bound.mem_alloc_min)
    job_size_factor +=
        1.0 * static_cast<double>(job_mem_alloc - bound.mem_alloc_min) /
        static_cast<double>(bound.mem_alloc_max - bound.mem_alloc_min);
  if (g_config.PriorityConfig.FavorSmall)
    job_size_factor = 1.0 - job_size_factor / 3;
  else
    job_size_factor /= 3.0;

  // fair_share_factor
  if (bound.service_val_max > bound.service_val_min)
    fair_share_factor =
        1.0 - (job_service_val - bound.service_val_min) /
                  (bound.service_val_max - bound.service_val_min);

  double priority =
      g_config.PriorityConfig.WeightAge * age_factor +
      g_config.PriorityConfig.WeightPartition * partition_factor +
      g_config.PriorityConfig.WeightJobSize * job_size_factor +
      g_config.PriorityConfig.WeightFairShare * fair_share_factor +
      g_config.PriorityConfig.WeightQoS * qos_factor;

  return priority;
}
}  // namespace Ctld
