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

#include "TaskScheduler.h"

#include "AccountManager.h"
#include "AccountMetaContainer.h"
#include "CranedMetaContainer.h"
#include "CtldPublicDefs.h"
#include "EmbeddedDbClient.h"
#include "RpcService/CranedKeeper.h"
#include "crane/PluginClient.h"
#include "protos/Crane.pb.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {
using namespace std::chrono_literals;

TaskScheduler::TaskScheduler() {
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

TaskScheduler::~TaskScheduler() {
  m_thread_stop_ = true;
  if (m_schedule_thread_.joinable()) m_schedule_thread_.join();
  if (m_task_release_thread_.joinable()) m_task_release_thread_.join();
  if (m_task_cancel_thread_.joinable()) m_task_cancel_thread_.join();
  if (m_task_submit_thread_.joinable()) m_task_submit_thread_.join();
  if (m_task_status_change_thread_.joinable())
    m_task_status_change_thread_.join();
  if (m_resv_clean_thread_.joinable()) m_resv_clean_thread_.join();
  m_rpc_worker_pool_->wait();
  m_rpc_worker_pool_.reset();
}

bool TaskScheduler::Init() {
  using crane::grpc::TaskInEmbeddedDb;

  bool ok;
  CraneErrCode err;

  EmbeddedDbClient::DbSnapshot snapshot;
  ok = g_embedded_db_client->RetrieveLastSnapshot(&snapshot);
  if (!ok) {
    CRANE_ERROR("Failed to retrieve embedded DB snapshot!");
    return false;
  }

  auto& running_queue = snapshot.running_queue;

  if (!running_queue.empty()) {
    CRANE_INFO("{} running task(s) recovered.", running_queue.size());

    for (auto&& [task_db_id, task_in_embedded_db] : running_queue) {
      auto task = std::make_unique<TaskInCtld>();
      task->SetFieldsByTaskToCtld(task_in_embedded_db.task_to_ctld());
      // Must be called after SetFieldsByTaskToCtld!
      task->SetFieldsByRuntimeAttr(task_in_embedded_db.runtime_attr());
      task_id_t task_id = task->TaskId();

      CRANE_TRACE("Restore task #{} from embedded running queue.",
                  task->TaskId());

      auto result = AcquireTaskAttributes(task.get());
      if (!result || task->type == crane::grpc::Interactive) {
        task->SetStatus(crane::grpc::Failed);
        ok = g_embedded_db_client->UpdateRuntimeAttrOfTask(0, task_db_id,
                                                           task->RuntimeAttr());
        if (!ok) {
          CRANE_ERROR(
              "UpdateRuntimeAttrOfTask failed for task #{} when "
              "mark the task as FAILED.",
              task_id);
        }
        if (!result)
          CRANE_INFO(
              "Failed to acquire task attributes for restored running task "
              "#{}. "
              "Error Code: {}. "
              "Mark it as FAILED and move it to the ended queue.",
              task_id, CraneErrStr(result.error()));
        else {
          CRANE_INFO("Mark running interactive task {} as FAILED.", task_id);

          ok = g_db_client->InsertJob(task.get());
          if (!ok) {
            CRANE_ERROR(
                "InsertJob failed for task #{} "
                "when recovering running queue.",
                task->TaskId());
          }

          ok = g_embedded_db_client->PurgeEndedTasks(
              {{task->TaskId(), task_db_id}});
          if (!ok) {
            CRANE_ERROR(
                "PurgeEndedTasks failed for task #{} when recovering "
                "running queue.",
                task->TaskId());
          }
        }

        // Move this problematic task into ended queue and
        // process next task.
        continue;
      }
      PutRecoveredTaskIntoRunningQueueLock_(std::move(task));
    }
  }

  // Process the pending tasks in the embedded pending queue.
  auto& pending_queue = snapshot.pending_queue;
  if (!pending_queue.empty()) {
    CRANE_INFO("{} pending task(s) recovered.", pending_queue.size());

    for (auto&& [task_db_id, task_in_embedded_db] : pending_queue) {
      auto task = std::make_unique<TaskInCtld>();
      task->SetFieldsByTaskToCtld(task_in_embedded_db.task_to_ctld());
      // Must be called after SetFieldsByTaskToCtld!
      task->SetFieldsByRuntimeAttr(task_in_embedded_db.runtime_attr());

      task_id_t task_id = task->TaskId();

      CRANE_TRACE("Restore task #{} from embedded pending queue.",
                  task->TaskId());

      bool mark_task_as_failed = false;

      if (task->type == crane::grpc::Interactive) {
        CRANE_INFO("Mark interactive task #{} as FAILED", task_id);
        mark_task_as_failed = true;
      }

      if (!mark_task_as_failed && !(AcquireTaskAttributes(task.get()))) {
        CRANE_ERROR("AcquireTaskAttributes failed for task #{}", task_id);
        mark_task_as_failed = true;
      }

      if (!mark_task_as_failed && !CheckTaskValidity(task.get())) {
        CRANE_ERROR("CheckTaskValidity failed for task #{}", task_id);
        mark_task_as_failed = true;
      }

      if (!mark_task_as_failed) {
        RequeueRecoveredTaskIntoPendingQueueLock_(std::move(task));
      } else {
        // If a batch task failed to requeue the task into pending queue due to
        // insufficient resource or other reasons or the task is an interactive
        // task , Mark it as FAILED and move it to the ended queue.
        CRANE_INFO(
            "Failed to requeue task #{}. Mark it as FAILED and "
            "move it to the ended queue.",
            task_id);
        task->SetStatus(crane::grpc::Failed);
        ok = g_embedded_db_client->UpdateRuntimeAttrOfTask(0, task_db_id,
                                                           task->RuntimeAttr());
        if (!ok) {
          CRANE_ERROR(
              "UpdateRuntimeAttrOfTask failed for task #{} when "
              "mark the task as FAILED.",
              task_id);
        }

        ok = g_db_client->InsertJob(task.get());
        if (!ok) {
          CRANE_ERROR(
              "InsertJob failed for task #{} when recovering pending "
              "queue.",
              task->TaskId());
        }

        ok = g_embedded_db_client->PurgeEndedTasks(
            {{task->TaskId(), task->TaskDbId()}});
        if (!ok) {
          CRANE_ERROR(
              "PurgeEndedTasks failed for task #{} when recovering "
              "pending queue.",
              task->TaskId());
        }
      }
    }
  }

  if (!snapshot.final_queue.empty()) {
    CRANE_INFO("{} final task(s) might not have been put to mongodb.",
               snapshot.final_queue.size());

    std::unordered_map<job_id_t, task_db_id_t> db_ids;
    for (auto& [db_id, task_in_embedded_db] : snapshot.final_queue) {
      task_id_t task_id = task_in_embedded_db.runtime_attr().task_id();
      ok = g_db_client->CheckTaskDbIdExisted(db_id);
      if (!ok) {
        if (!g_db_client->InsertRecoveredJob(task_in_embedded_db)) {
          CRANE_ERROR(
              "Failed to call g_db_client->InsertRecoveredJob() "
              "for task #{}",
              task_id);
        }
      }

      db_ids[task_id] = db_id;
    }

    ok = g_embedded_db_client->PurgeEndedTasks(db_ids);
    if (!ok) {
      CRANE_ERROR("Failed to call g_embedded_db_client->PurgeEndedTasks()");
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
      crane::grpc::TaskStatus::Completed,
      crane::grpc::TaskStatus::Failed,
      crane::grpc::TaskStatus::ExceedTimeLimit,
      crane::grpc::TaskStatus::Cancelled,
      crane::grpc::TaskStatus::OutOfMemory,
  };

  std::vector<job_id_t> invalid_jobs;
  auto mark_job_invalid = [&invalid_jobs](TaskInCtld* job) {
    job_id_t job_id = job->TaskId();
    CRANE_ERROR("[Job #{}] Running job without step, mark the job as FAILED!",
                job_id);
    job->SetStatus(crane::grpc::Failed);
    auto ok = g_embedded_db_client->UpdateRuntimeAttrOfTask(0, job->TaskDbId(),
                                                            job->RuntimeAttr());
    if (!ok) {
      CRANE_ERROR(
          "[Job #{}] UpdateRuntimeAttrOfTask failed when "
          "mark the job as FAILED.",
          job_id);
    }

    ok = g_db_client->InsertJob(job);
    if (!ok) {
      CRANE_ERROR(
          "InsertJob failed for task #{} when recovering pending "
          "queue.",
          job_id);
    }

    ok = g_embedded_db_client->PurgeEndedTasks(
        {{job->TaskId(), job->TaskDbId()}});
    if (!ok) {
      CRANE_ERROR(
          "PurgeEndedTasks failed for task #{} when recovering "
          "pending queue.",
          job_id);
    }
    invalid_jobs.push_back(job_id);
  };
  // When store, write/purge step info first, then job info.
  // When read, iterate by job.
  for (auto& [job_id, job] : m_running_task_map_) {
    auto it = step_snapshot.steps.find(job_id);
    if (it == step_snapshot.steps.end()) {
      mark_job_invalid(job.get());
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

      StepInCtld* step;
      if (step_type == crane::grpc::StepType::DAEMON) {
        step = new DaemonStepInCtld();
      } else {
        step = new CommonStepInCtld();
      }

      step->RecoverFromDb(*job, step_info);
      if (auto err_expt = AcquireStepAttributes(*job, step);
          !err_expt.has_value()) {
        CRANE_ERROR(
            "[Step #{}{}] AcquireStepAttributes failed: {}, step dropped!",
            job_id, step_id, CraneErrStr(err_expt.error()));
        delete step;
        invalid_steps[job_id].emplace_back(std::move(step_info));

        continue;
      }

      if (auto err_expt = CheckStepValidity(*job, step);
          !err_expt.has_value()) {
        CRANE_ERROR("[Step #{}{}] CheckStepValidity failed: {}, step dropped!",
                    job_id, step_id, CraneErrStr(err_expt.error()));
        delete step;
        invalid_steps[job_id].emplace_back(std::move(step_info));
        continue;
      }

      if (step_status == crane::grpc::TaskStatus::Pending) {
        // Not support to recover pending step now. All pending steps are crun
        // which can not recover now.
        delete step;
        invalid_steps[job_id].emplace_back(std::move(step_info));
        continue;
      }

      if (step_type == crane::grpc::StepType::DAEMON) {
        std::unique_ptr<DaemonStepInCtld> step_ptr(
            dynamic_cast<DaemonStepInCtld*>(step));

        job->SetDaemonStep(std::move(step_ptr));
        CRANE_INFO("Daemon step recovered for job #{}", job->TaskId());

      } else if (step_type == crane::grpc::StepType::PRIMARY) {
        std::unique_ptr<CommonStepInCtld> step_ptr(
            dynamic_cast<CommonStepInCtld*>(step));

        job->SetPrimaryStep(std::move(step_ptr));
        CRANE_INFO("Primary step recovered for job #{}", job->TaskId());

      } else {
        std::unique_ptr<CommonStepInCtld> step_ptr(
            dynamic_cast<CommonStepInCtld*>(step));

        job->AddStep(std::move(step_ptr));
        CRANE_INFO("Common step {} recovered for job #{}", job->TaskId(),
                   step->StepId());
      }
    }

    if (!job->PrimaryStep() && !job->DaemonStep() && job->Steps().empty())
      mark_job_invalid(job.get());
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

  for (auto& job_id : invalid_jobs) {
    m_running_task_map_.erase(job_id);
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
  m_task_timer_handle_ = uvw_release_loop->resource<uvw::timer_handle>();
  m_task_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        CleanTaskTimerCb_();
      });
  m_task_timer_handle_->start(
      std::chrono::milliseconds(kTaskHoldTimerTimeoutMs * 3),
      std::chrono::milliseconds(kTaskHoldTimerTimeoutMs));

  m_task_timeout_async_handle_ =
      uvw_release_loop->resource<uvw::async_handle>();
  m_task_timeout_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        TaskTimerAsyncCb_();
      });

  m_clean_task_timer_queue_handle_ =
      uvw_release_loop->resource<uvw::async_handle>();
  m_clean_task_timer_queue_handle_->on<uvw::async_event>(
      [this, loop = uvw_release_loop](const uvw::async_event&,
                                      uvw::async_handle&) {
        CleanTaskTimerQueueCb_(loop);
      });

  m_task_release_thread_ = std::thread(
      [this, loop = uvw_release_loop]() { ReleaseTaskThread_(loop); });

  std::shared_ptr<uvw::loop> uvw_cancel_loop = uvw::loop::create();
  m_cancel_task_timer_handle_ = uvw_cancel_loop->resource<uvw::timer_handle>();
  m_cancel_task_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        CancelTaskTimerCb_();
      });
  m_cancel_task_timer_handle_->start(
      std::chrono::milliseconds(kCancelTaskTimeoutMs * 3),
      std::chrono::milliseconds(kCancelTaskTimeoutMs));

  m_cancel_task_async_handle_ = uvw_cancel_loop->resource<uvw::async_handle>();
  m_cancel_task_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CancelTaskAsyncCb_();
      });

  m_clean_cancel_queue_handle_ = uvw_cancel_loop->resource<uvw::async_handle>();
  m_clean_cancel_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanCancelQueueCb_();
      });

  m_task_cancel_thread_ = std::thread(
      [this, loop = std::move(uvw_cancel_loop)]() { CancelTaskThread_(loop); });

  std::shared_ptr<uvw::loop> uvw_submit_loop = uvw::loop::create();
  m_submit_task_timer_handle_ = uvw_submit_loop->resource<uvw::timer_handle>();
  m_submit_task_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        SubmitTaskTimerCb_();
      });
  m_submit_task_timer_handle_->start(
      std::chrono::milliseconds(kSubmitTaskTimeoutMs * 3),
      std::chrono::milliseconds(kSubmitTaskTimeoutMs));

  m_submit_task_async_handle_ = uvw_submit_loop->resource<uvw::async_handle>();
  m_submit_task_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        SubmitTaskAsyncCb_();
      });

  m_clean_submit_queue_handle_ = uvw_submit_loop->resource<uvw::async_handle>();
  m_clean_submit_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanSubmitQueueCb_();
      });

  m_task_submit_thread_ = std::thread(
      [this, loop = std::move(uvw_submit_loop)]() { SubmitTaskThread_(loop); });

  std::shared_ptr<uvw::loop> uvw_task_status_change_loop = uvw::loop::create();
  m_task_status_change_timer_handle_ =
      uvw_task_status_change_loop->resource<uvw::timer_handle>();
  m_task_status_change_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        TaskStatusChangeTimerCb_();
      });
  m_task_status_change_timer_handle_->start(
      std::chrono::milliseconds(kTaskStatusChangeTimeoutMS * 3),
      std::chrono::milliseconds(kTaskStatusChangeTimeoutMS));

  m_task_status_change_async_handle_ =
      uvw_task_status_change_loop->resource<uvw::async_handle>();
  m_task_status_change_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        TaskStatusChangeAsyncCb_();
      });

  m_clean_task_status_change_handle_ =
      uvw_task_status_change_loop->resource<uvw::async_handle>();
  m_clean_task_status_change_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanTaskStatusChangeQueueCb_();
      });

  m_task_status_change_thread_ =
      std::thread([this, loop = std::move(uvw_task_status_change_loop)]() {
        TaskStatusChangeThread_(loop);
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

  // Start schedule thread first.
  m_schedule_thread_ = std::thread([this] { ScheduleThread_(); });

  return true;
}

void TaskScheduler::RequeueRecoveredTaskIntoPendingQueueLock_(
    std::unique_ptr<TaskInCtld> task) {
  CRANE_ASSERT_MSG(
      g_account_meta_container->TryMallocQosResource(*task) ==
          CraneErrCode::SUCCESS,
      fmt::format(
          "ApplyQosLimitOnTask failed when recovering pending task #{}.",
          task->TaskId()));
  // The order of LockGuards matters.
  LockGuard pending_guard(&m_pending_task_map_mtx_);
  m_pending_task_map_.emplace(task->TaskId(), std::move(task));
}

void TaskScheduler::PutRecoveredTaskIntoRunningQueueLock_(
    std::unique_ptr<TaskInCtld> task) {
  auto res = g_account_meta_container->TryMallocQosResource(*task);
  CRANE_ASSERT_MSG(
      res == CraneErrCode::SUCCESS,
      fmt::format(
          "ApplyQosLimitOnTask failed when recovering running task #{}.",
          task->TaskId()));
  for (const CranedId& craned_id : task->CranedIds())
    g_meta_container->MallocResourceFromNode(craned_id, task->TaskId(),
                                             task->AllocatedRes());
  if (!task->reservation.empty()) {
    g_meta_container->MallocResourceFromResv(task->reservation, task->TaskId(),
                                             task->AllocatedRes());
  }

  // The order of LockGuards matters.
  LockGuard running_guard(&m_running_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  for (const CranedId& craned_id : task->CranedIds())
    m_node_to_tasks_map_[craned_id].emplace(task->TaskId());

  m_running_task_map_.emplace(task->TaskId(), std::move(task));
}

void TaskScheduler::ReleaseTaskThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("ReleaseTaskThr");

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

void TaskScheduler::CancelTaskThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("CancelTaskThr");

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

void TaskScheduler::SubmitTaskThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("SubmitTaskThr");

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

void TaskScheduler::TaskStatusChangeThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("TaskStatChThr");

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
        "Failed to start the idle event in TaskStatusChangeWithReasonAsync "
        "loop.");
  }

  uvw_loop->run();
}

void TaskScheduler::CleanResvThread_(
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

void TaskScheduler::ScheduleThread_() {
  util::SetCurrentThreadName("ScheduleThread");

  std::chrono::steady_clock::time_point schedule_begin;
  std::chrono::steady_clock::time_point schedule_end;
  size_t num_tasks_single_schedule;
  size_t num_tasks_single_execution;

  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  while (!m_thread_stop_) {
    // Note: In other parts of code, we must avoid the happening of the
    // situation where m_running_task_map_mtx is acquired and then
    // m_pending_task_map_mtx_ needs to be acquired. Deadlock may happen under
    // such a situation.
    m_pending_task_map_mtx_.Lock();
    if (!m_pending_task_map_.empty()) {  // all_part_metas is locked here.
      // Running map must be locked before g_meta_container's lock.
      // Otherwise, DEADLOCK may happen because TaskStatusChange() locks running
      // map first and then locks g_meta_container.

      // Truncated by 1s.
      // We use the time now as the base time across the whole algorithm.
      absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

      std::vector<std::unique_ptr<PdJobInScheduler>> pending_jobs;
      pending_jobs.reserve(m_pending_task_map_.size());
      for (auto& it : m_pending_task_map_) {
        const auto& job = it.second;
        if (job->Held()) {
          job->pending_reason = "Held";
          continue;
        }
        if (job->begin_time > now) {
          job->pending_reason = "BeginTime";
          continue;
        }
        pending_jobs.emplace_back(
            std::make_unique<PdJobInScheduler>(job.get()));
      }

      // ScheduleThread_ is the only thread move jobs from pending to
      // running, so it's safe to release m_pending_task_map_mtx_ before
      // locking m_running_task_map_mtx_.
      m_pending_task_map_mtx_.Unlock();

      m_running_task_map_mtx_.Lock();

      std::vector<std::unique_ptr<RnJobInScheduler>> running_jobs;
      running_jobs.reserve(m_running_task_map_.size());
      for (auto& it : m_running_task_map_) {
        running_jobs.emplace_back(
            std::make_unique<RnJobInScheduler>(it.second.get()));
      }

      // ScheduleThread_ is the only thread start jobs, so it's safe to release
      // m_running_task_map_mtx_ before monitoring resources on nodes.
      m_running_task_map_mtx_.Unlock();

      schedule_begin = std::chrono::steady_clock::now();
      num_tasks_single_schedule = std::min((size_t)g_config.ScheduledBatchSize,
                                           m_pending_task_map_.size());

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

      std::vector<std::unique_ptr<TaskInCtld>> jobs_to_run;

      LockGuard pending_guard(&m_pending_task_map_mtx_);

      for (auto& job_in_scheduler : pending_jobs) {
        auto it = m_pending_task_map_.find(job_in_scheduler->job_id);
        if (it != m_pending_task_map_.end()) {
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

          PartitionId const& partition_id = job->partition_id;

          job->SetEndTime(end_time);
          job->SetCranedIds(std::move(job_in_scheduler->craned_ids));
          job->SetAllocatedRes(std::move(job_in_scheduler->allocated_res));
          job->allocated_res_view.SetToZero();
          job->allocated_res_view += job->AllocatedRes();
          job->nodes_alloc = job->CranedIds().size();
          job->SetStatus(crane::grpc::TaskStatus::Configuring);

          job->allocated_craneds_regex =
              util::HostNameListToStr(job->CranedIds());

          for (CranedId const& craned_id : job->CranedIds())
            g_meta_container->MallocResourceFromNode(craned_id, job->TaskId(),
                                                     job->AllocatedRes());
          if (job->reservation != "") {
            g_meta_container->MallocResourceFromResv(
                job->reservation, job->TaskId(), job->AllocatedRes());
          }

          if (job->ShouldLaunchOnAllNodes()) {
            for (auto const& craned_id : job->CranedIds())
              job->executing_craned_ids.emplace_back(craned_id);
          } else {
            job->executing_craned_ids.emplace_back(job->CranedIds().front());
          }

          jobs_to_run.push_back(std::move(job));
          m_pending_task_map_.erase(it);
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

      num_tasks_single_execution = jobs_to_run.size();

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Set job fields costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      // Now we have the ownerships of to-run jobs in jobs_to_run. Add task
      // ids to node maps immediately before CreateCgroupForTasks to ensure
      // that if a CraneD crash, the callback of CranedKeeper can call
      // TerminateTasksOnCraned in which m_node_to_tasks_map_ will be searched
      // and send TerminateTasksOnCraned to appropriate CraneD to release the
      // cgroups.

      // NOTE: If unlock pending_map here, jobs may be unable to be find
      // before transferring to running_map or DB.

      m_task_indexes_mtx_.Lock();
      for (auto& job : jobs_to_run) {
        for (CranedId const& craned_id : job->CranedIds())
          m_node_to_tasks_map_[craned_id].emplace(job->TaskId());
      }
      m_task_indexes_mtx_.Unlock();

      // RPC is time-consuming. Clustering rpc to one craned for performance.
      HashMap<CranedId, std::vector<crane::grpc::JobToD>> craned_alloc_job_map;

      // Job primary steps
      std::unordered_map<CranedId, std::vector<crane::grpc::StepToD>>
          craned_alloc_steps;
      std::vector<StepInCtld*> step_in_ctld_vec;

      Mutex thread_pool_mtx;
      HashSet<task_id_t> failed_task_id_set;

      for (auto& job : jobs_to_run) {
        job->SetPrimaryStepStatus(crane::grpc::TaskStatus::Invalid);
        std::unique_ptr daemon_step = std::make_unique<DaemonStepInCtld>();
        daemon_step->InitFromJob(*job);
        step_in_ctld_vec.push_back(daemon_step.get());
        job->SetDaemonStep(std::move(daemon_step));
      }

      if (!g_embedded_db_client->AppendSteps(step_in_ctld_vec)) {
        for (auto& job : jobs_to_run) {
          failed_task_id_set.insert(job->TaskId());
        }
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

      std::latch alloc_job_latch(craned_alloc_job_map.size());
      for (auto&& iter : craned_alloc_job_map) {
        CranedId const& craned_id = iter.first;
        std::vector<crane::grpc::JobToD>& jobs = iter.second;

        m_rpc_worker_pool_->detach_task([&]() {
          auto stub = g_craned_keeper->GetCranedStub(craned_id);
          CRANE_TRACE("Send AllocJobs for {} tasks to {}", jobs.size(),
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
              failed_task_id_set.emplace(job_to_d.job_id());
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
            failed_task_id_set.emplace(job_to_d.job_id());
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
              failed_task_id_set.emplace(step_to_d.job_id());
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
                      CraneErrStr(err), craned_id);

          thread_pool_mtx.Lock();
          for (const auto& step_to_d : steps)
            failed_task_id_set.emplace(step_to_d.job_id());
          thread_pool_mtx.Unlock();

          // If tasks in task_uid_pairs failed to start,
          // they will be moved to the completed tasks and do the following
          // steps:
          // 1. call g_meta_container->FreeResources() for the failed tasks.
          // 2. Release all cgroups related to these failed tasks.
          // 3. Move these tasks to the completed queue.
          CRANE_ERROR("Craned #{} failed when AllocSteps.", craned_id);

          alloc_step_latch.count_down();
        });
      }
      alloc_step_latch.wait();

      std::vector<std::unique_ptr<TaskInCtld>> jobs_created;
      std::vector<std::unique_ptr<TaskInCtld>> jobs_failed;

      for (auto& job : jobs_to_run) {
        if (failed_task_id_set.contains(job->TaskId())) {
          jobs_failed.emplace_back(std::move(job));
        } else {
          jobs_created.emplace_back(std::move(job));
        }
      }

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "CreateCgroupForJobs costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      // Now we have the ownerships of succeeded jobs in `jobs_created` and
      // the ownerships of failed jobs in `jobs_failed`.
      // For successfully created jobs, add them to m_node_to_tasks_map_.
      // For failed jobs, free all the resource and move them to the completed
      // queue.

      // Move jobs into running queue.
      txn_id_t txn_id{0};
      bool ok = g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
      if (!ok) {
        CRANE_ERROR(
            "TaskScheduler failed to start transaction when scheduling.");
      }

      for (auto& job : jobs_created) {
        // IMPORTANT: job must be put into running_task_map before any
        // time-consuming operation, otherwise TaskStatusChange RPC will come
        // earlier before job is put into running_task_map.
        g_embedded_db_client->UpdateRuntimeAttrOfTask(txn_id, job->TaskDbId(),
                                                      job->RuntimeAttr());
      }

      ok = g_embedded_db_client->CommitVariableDbTransaction(txn_id);
      if (!ok) {
        CRANE_ERROR("Embedded database failed to commit manual transaction.");
      }

      // Set succeed tasks status and do callbacks.
      for (auto& job : jobs_created) {
        if (job->type == crane::grpc::Interactive) {
          const auto& meta = std::get<InteractiveMetaInTask>(job->meta);
          std::get<InteractiveMetaInTask>(job->meta).cb_task_res_allocated(
              job->TaskId(), job->allocated_craneds_regex, job->CranedIds());
        }

        // The ownership of TaskInCtld is transferred to the running queue.
        m_running_task_map_mtx_.Lock();
        m_running_task_map_.emplace(job->TaskId(), std::move(job));
        m_running_task_map_mtx_.Unlock();
      }

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Move tasks into running queue costed {} ms",
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
          "Scheduling {} pending tasks. {} get scheduled. Time elapsed: {}ms",
          num_tasks_single_schedule, num_tasks_single_execution,
          std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end -
                                                                schedule_begin)
              .count());

      // Note: If unlock pending_map here, jobs may be unable to be find
      // before transferring to DB.
      if (!jobs_failed.empty()) {
        // Then handle failed tasks in `jobs_failed_to_create_cg` if there's
        // any.
        begin = std::chrono::steady_clock::now();

        for (auto& job : jobs_failed) {
          for (CranedId const& craned_id : job->CranedIds())
            g_meta_container->FreeResourceFromNode(craned_id, job->TaskId());
          if (job->reservation != "")
            g_meta_container->FreeResourceFromResv(job->reservation,
                                                   job->TaskId());
          g_account_meta_container->FreeQosResource(*job);
        }

        // Move failed jobs to the completed queue.
        std::unordered_set<TaskInCtld*> failed_job_raw_ptrs;
        for (auto& job : jobs_failed) {
          failed_job_raw_ptrs.emplace(job.get());

          job->SetStatus(crane::grpc::Failed);
          job->SetExitCode(ExitCode::EC_CGROUP_ERR);
          job->SetEndTime(absl::Now());
        }
        ProcessFinalTasks_(failed_job_raw_ptrs);

        // Failed jobs have been handled properly. Free them explicitly.
        jobs_failed.clear();

        end = std::chrono::steady_clock::now();
        CRANE_TRACE(
            "Handling failed jobs costed {} ms",
            std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                .count());
      }

    } else {
      m_pending_map_cached_size_.store(m_pending_task_map_.size(),
                                       std::memory_order::release);
      m_pending_task_map_mtx_.Unlock();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kTaskScheduleIntervalMs));
  }
}

std::future<task_id_t> TaskScheduler::SubmitTaskAsync(
    std::unique_ptr<TaskInCtld> task) {
  std::promise<task_id_t> promise;
  std::future<task_id_t> future = promise.get_future();

  m_submit_task_queue_.enqueue({std::move(task), std::move(promise)});
  m_submit_task_async_handle_->send();

  return std::move(future);
}

std::future<CraneErrCode> TaskScheduler::HoldReleaseTaskAsync(task_id_t task_id,
                                                              int64_t secs) {
  std::promise<CraneErrCode> promise;
  std::future<CraneErrCode> future = promise.get_future();

  m_task_timer_queue_.enqueue(
      {std::make_pair(task_id, secs), std::move(promise)});
  m_task_timeout_async_handle_->send();

  return std::move(future);
}

CraneErrCode TaskScheduler::ChangeTaskTimeLimit(task_id_t task_id,
                                                int64_t secs) {
  if (!CheckIfTimeLimitSecIsValid(secs)) return CraneErrCode::ERR_INVALID_PARAM;

  std::vector<CranedId> craned_ids;

  {
    LockGuard pending_guard(&m_pending_task_map_mtx_);
    LockGuard running_guard(&m_running_task_map_mtx_);

    TaskInCtld* task;
    bool found = false;

    auto pd_iter = m_pending_task_map_.find(task_id);
    if (pd_iter != m_pending_task_map_.end()) {
      found = true, task = pd_iter->second.get();

      if (task->reservation != "") {
        auto resv_end_time =
            g_meta_container->GetResvMetaPtr(task->reservation)->end_time;
        if (resv_end_time <= absl::Now() + absl::Seconds(secs)) {
          CRANE_DEBUG("Task #{}'s time limit exceeds reservation end time",
                      task_id);
          return CraneErrCode::ERR_INVALID_PARAM;
        }
      }
    }
    if (!found) {
      auto rn_iter = m_running_task_map_.find(task_id);
      if (rn_iter != m_running_task_map_.end()) {
        found = true, task = rn_iter->second.get();
        craned_ids = task->executing_craned_ids;

        if (task->reservation != "") {
          const auto& reservation_meta =
              g_meta_container->GetResvMetaPtr(task->reservation);
          auto resv_end_time = reservation_meta->end_time;
          if (resv_end_time <= task->StartTime() + absl::Seconds(secs)) {
            CRANE_DEBUG("Task #{}'s time limit exceeds reservation end time",
                        task_id);
            return CraneErrCode::ERR_INVALID_PARAM;
          }
        }
      }
    }

    if (!found) {
      CRANE_DEBUG("Task #{} not in Pd/Rn queue for time limit change!",
                  task_id);
      return CraneErrCode::ERR_NON_EXISTENT;
    }

    task->time_limit = absl::Seconds(secs);
    task->MutableTaskToCtld()->mutable_time_limit()->set_seconds(secs);
    g_embedded_db_client->UpdateTaskToCtldIfExists(0, task->TaskDbId(),
                                                   task->TaskToCtld());
  }

  // Only send request to the executing node
  for (const CranedId& craned_id : craned_ids) {
    auto stub = g_craned_keeper->GetCranedStub(craned_id);
    if (stub && !stub->Invalid()) {
      CraneErrCode err = stub->ChangeJobTimeLimit(task_id, secs);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("Failed to change time limit of task #{} on Node {}",
                    task_id, craned_id);
        return err;
      }
    }
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode TaskScheduler::ChangeTaskPriority(task_id_t task_id,
                                               double priority) {
  m_pending_task_map_mtx_.Lock();

  auto pd_iter = m_pending_task_map_.find(task_id);
  if (pd_iter == m_pending_task_map_.end()) {
    m_pending_task_map_mtx_.Unlock();
    CRANE_TRACE("Task #{} not in Pd queue for priority change", task_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  pd_iter->second->mandated_priority = priority;
  m_pending_task_map_mtx_.Unlock();
  return CraneErrCode::SUCCESS;
}

CraneErrCode TaskScheduler::ChangeTaskExtraAttrs(
    task_id_t task_id, const std::string& new_extra_attr) {
  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard running_guard(&m_running_task_map_mtx_);

  TaskInCtld* task;
  bool found = false;

  auto pd_iter = m_pending_task_map_.find(task_id);
  if (pd_iter != m_pending_task_map_.end()) {
    found = true, task = pd_iter->second.get();
  }
  if (!found) {
    auto rn_iter = m_running_task_map_.find(task_id);
    if (rn_iter != m_running_task_map_.end()) {
      found = true, task = rn_iter->second.get();
    }
  }

  if (!found) {
    CRANE_DEBUG("Task #{} not in Pd/Rn queue for extra attribute change!",
                task_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  task->extra_attr = new_extra_attr;
  task->MutableTaskToCtld()->set_extra_attr(new_extra_attr);
  g_embedded_db_client->UpdateTaskToCtldIfExists(0, task->TaskDbId(),
                                                 task->TaskToCtld());
  return CraneErrCode::SUCCESS;
}

CraneExpected<std::future<task_id_t>> TaskScheduler::SubmitTaskToScheduler(
    std::unique_ptr<TaskInCtld> task) {
  if (!task->password_entry->Valid()) {
    CRANE_DEBUG("Uid {} not found on the controller node", task->uid);
    return std::unexpected(CraneErrCode::ERR_INVALID_UID);
  }
  task->SetUsername(task->password_entry->Username());

  {  // Limit the lifecycle of user_scoped_ptr
    auto user_scoped_ptr =
        g_account_manager->GetExistedUserInfo(task->Username());
    if (!user_scoped_ptr) {
      CRANE_DEBUG("User '{}' not found in the account database",
                  task->Username());
      return std::unexpected(CraneErrCode::ERR_INVALID_USER);
    }

    if (task->account.empty()) {
      task->account = user_scoped_ptr->default_account;
      task->MutableTaskToCtld()->set_account(user_scoped_ptr->default_account);
    } else {
      if (!user_scoped_ptr->account_to_attrs_map.contains(task->account)) {
        CRANE_DEBUG(
            "Account '{}' is not in the user account list when submitting the "
            "task",
            task->account);
        return std::unexpected(CraneErrCode::ERR_USER_ACCOUNT_MISMATCH);
      }
    }
  }

  if (!g_account_manager->CheckUserPermissionToPartition(
          task->Username(), task->account, task->partition_id)) {
    CRANE_DEBUG(
        "User '{}' doesn't have permission to use partition '{}' when using "
        "account '{}'",
        task->Username(), task->partition_id, task->account);
    return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);
  }

  auto enable_res = g_account_manager->CheckIfUserOfAccountIsEnabled(
      task->Username(), task->account);
  if (!enable_res) {
    return std::unexpected(enable_res.error());
  }

  auto result = g_meta_container->CheckIfAccountIsAllowedInPartition(
      task->partition_id, task->account);
  if (!result) return std::unexpected(result.error());

  task->SetSubmitTime(absl::Now());

  result = TaskScheduler::HandleUnsetOptionalInTaskToCtld(task.get());
  if (result) result = TaskScheduler::AcquireTaskAttributes(task.get());
  if (result) result = TaskScheduler::CheckTaskValidity(task.get());
  if (result) {
    auto res = g_account_meta_container->TryMallocQosResource(*task);
    if (res != CraneErrCode::SUCCESS) {
      CRANE_ERROR("The requested QoS resources have reached the user's limit.");
      return std::unexpected(res);
    }
    std::future<task_id_t> future =
        g_task_scheduler->SubmitTaskAsync(std::move(task));
    return {std::move(future)};
  }

  return std::unexpected(result.error());
}

CraneErrCode TaskScheduler::SetHoldForTaskInRamAndDb_(task_id_t task_id,
                                                      bool hold) {
  m_pending_task_map_mtx_.Lock();

  auto pd_iter = m_pending_task_map_.find(task_id);
  if (pd_iter == m_pending_task_map_.end()) {
    m_pending_task_map_mtx_.Unlock();
    CRANE_TRACE("Task #{} not in Pd queue for hold/release", task_id);
    return CraneErrCode::ERR_NON_EXISTENT;
  }

  TaskInCtld* task = pd_iter->second.get();
  task->SetHeld(hold);

  // Copy persisted data to prevent inconsistency.
  task_db_id_t db_id = task->TaskDbId();
  auto runtime_attr = task->RuntimeAttr();

  m_pending_task_map_mtx_.Unlock();

  if (!g_embedded_db_client->UpdateRuntimeAttrOfTaskIfExists(0, db_id,
                                                             runtime_attr))
    CRANE_ERROR("Failed to update runtime attr of task #{} to DB", task_id);

  return CraneErrCode::SUCCESS;
}

CraneErrCode TaskScheduler::TerminateRunningTaskNoLock_(TaskInCtld* task) {
  task_id_t task_id = task->TaskId();

  bool need_to_be_terminated = false;
  if (task->type == crane::grpc::Interactive) {
    auto& meta = std::get<InteractiveMetaInTask>(task->meta);
    if (!meta.has_been_terminated_on_craned) {
      meta.has_been_terminated_on_craned = true;
      need_to_be_terminated = true;
    }
  } else {
    need_to_be_terminated = true;
  }

  if (need_to_be_terminated) {
    for (CranedId const& craned_id : task->executing_craned_ids) {
      m_cancel_task_queue_.enqueue(
          CancelRunningTaskQueueElem{.job_id = task_id,
                                     .step_id = kDaemonStepId,
                                     .craned_id = craned_id});
      m_cancel_task_async_handle_->send();
    }
  }

  return CraneErrCode::SUCCESS;
}

crane::grpc::CancelTaskReply TaskScheduler::CancelPendingOrRunningTask(
    const crane::grpc::CancelTaskRequest& request) {
  crane::grpc::CancelTaskReply reply;

  uint32_t operator_uid = request.operator_uid();

  // When an ordinary user tries to cancel jobs, they are automatically filtered
  // to their own jobs.
  std::string filter_uname = request.filter_username();
  if (filter_uname.empty() &&
      !g_account_manager->CheckUidIsAdmin(operator_uid)) {
    PasswordEntry entry(operator_uid);
    filter_uname = entry.Username();
  }

  auto rng_filter_state = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    return request.filter_state() == crane::grpc::Invalid ||
           task->Status() == request.filter_state();
  };

  auto rng_filter_partition = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    return request.filter_partition().empty() ||
           task->partition_id == request.filter_partition();
  };

  auto rng_filter_account = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    return request.filter_account().empty() ||
           task->account == request.filter_account();
  };

  auto rng_filter_task_name = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    return request.filter_task_name().empty() ||
           task->name == request.filter_task_name();
  };

  auto rng_filter_user_name = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    return filter_uname.empty() || task->Username() == filter_uname;
  };

  std::unordered_set<uint32_t> filter_task_ids_set(
      request.filter_task_ids().begin(), request.filter_task_ids().end());
  auto rng_filer_task_ids = [&](auto& it) {
    if (request.filter_task_ids().empty()) return true;

    std::unique_ptr<TaskInCtld>& task = it.second;

    auto iter = filter_task_ids_set.find(task->TaskId());
    if (iter == filter_task_ids_set.end()) return false;

    filter_task_ids_set.erase(iter);
    return true;
  };

  std::unordered_set<std::string> filter_nodes_set(
      std::begin(request.filter_nodes()), std::end(request.filter_nodes()));
  auto rng_filter_nodes = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    if (request.filter_nodes().empty()) return true;

    for (const auto& node : task->CranedIds())
      if (filter_nodes_set.contains(node)) return true;

    return false;
  };

  auto rng_transformer_id = [](auto& it) { return it.first; };

  auto fn_cancel_pending_task = [&](task_id_t task_id) {
    CRANE_TRACE("Cancelling pending task #{}", task_id);

    auto it = m_pending_task_map_.find(task_id);
    CRANE_ASSERT(it != m_pending_task_map_.end());
    TaskInCtld* task = it->second.get();

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        operator_uid, task->Username(), false);
    if (!result) {
      reply.add_not_cancelled_tasks(task_id);
      reply.add_not_cancelled_reasons("Permission Denied");
    } else {
      reply.add_cancelled_tasks(task_id);

      m_cancel_task_queue_.enqueue(
          CancelPendingTaskQueueElem{std::move(it->second)});
      m_cancel_task_async_handle_->send();

      m_pending_task_map_.erase(it);
    }
  };

  auto fn_cancel_running_task = [&](auto& it) {
    task_id_t task_id = it.first;
    TaskInCtld* task = it.second.get();

    CRANE_TRACE("Cancelling running task #{}", task_id);

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        operator_uid, task->Username(), false);
    if (!result) {
      reply.add_not_cancelled_tasks(task_id);
      reply.add_not_cancelled_reasons("Permission Denied");
    } else {
      if (task->type == crane::grpc::Interactive) {
        auto& meta = std::get<InteractiveMetaInTask>(task->meta);
        if (!meta.has_been_cancelled_on_front_end) {
          meta.has_been_cancelled_on_front_end = true;
          meta.cb_task_cancel(task_id);
        }
        reply.add_cancelled_tasks(task_id);
      } else {
        CraneErrCode err = TerminateRunningTaskNoLock_(task);
        if (err == CraneErrCode::SUCCESS) {
          reply.add_cancelled_tasks(task_id);
        } else {
          reply.add_not_cancelled_tasks(task_id);
          reply.add_not_cancelled_reasons(CraneErrStr(err).data());
        }
      }
    }
  };

  auto joined_filters = ranges::views::filter(rng_filter_state) |
                        ranges::views::filter(rng_filter_partition) |
                        ranges::views::filter(rng_filter_account) |
                        ranges::views::filter(rng_filter_user_name) |
                        ranges::views::filter(rng_filter_task_name) |
                        ranges::views::filter(rng_filer_task_ids) |
                        ranges::views::filter(rng_filter_nodes);

  std::vector<task_id_t> to_cancel_pd_task_ids;

  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard running_guard(&m_running_task_map_mtx_);

  auto pending_task_id_rng = m_pending_task_map_ | joined_filters |
                             ranges::views::transform(rng_transformer_id);

  // Evaluate immediately. fn_cancel_pending_task will change the contents
  // of m_pending_task_map_ and invalidate the end() of pending_task_id_rng.
  to_cancel_pd_task_ids =
      pending_task_id_rng | ranges::to<std::vector<task_id_t>>;
  ranges::for_each(to_cancel_pd_task_ids, fn_cancel_pending_task);

  auto running_task_rng = m_running_task_map_ | joined_filters;
  ranges::for_each(running_task_rng, fn_cancel_running_task);

  // We want to show error message for non-existent task ids.
  for (const auto& id : filter_task_ids_set) {
    reply.add_not_cancelled_tasks(id);
    reply.add_not_cancelled_reasons("Not Found");
  }

  return reply;
}

crane::grpc::AttachInContainerTaskReply TaskScheduler::AttachInContainerTask(
    const crane::grpc::AttachInContainerTaskRequest& request) {
  crane::grpc::AttachInContainerTaskReply response;

  CranedId target_craned_id;
  {
    LockGuard pending_guard(&m_pending_task_map_mtx_);
    LockGuard running_guard(&m_running_task_map_mtx_);

    task_id_t task_id = request.task_id();
    auto pd_it = m_pending_task_map_.find(task_id);
    if (pd_it != m_pending_task_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
      err->set_description("Task is still pending. Try again later.");
      response.set_ok(false);
      return response;
    }

    auto rn_it = m_running_task_map_.find(task_id);
    if (rn_it == m_running_task_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested task is not running.");
      response.set_ok(false);
      return response;
    }

    TaskInCtld* task = rn_it->second.get();
    if (!task->IsContainer()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested task is not a container task.");
      response.set_ok(false);
      return response;
    }

    // If tty is requested, tty must be enabled when creating the container
    if (request.tty() && !std::get<ContainerMetaInTask>(task->meta).tty) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description(
          "TTY not enabled when creating this container task.");
      response.set_ok(false);
      return response;
    }

    // If stdin is requested, stdin must be enabled when creating the container
    if (request.stdin() && !std::get<ContainerMetaInTask>(task->meta).stdin) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description(
          "STDIN not opened when creating this container task.");
      response.set_ok(false);
      return response;
    }

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        request.uid(), task->Username(), false);
    if (!result) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_PERMISSION_USER);
      err->set_description("Insufficient permission to attach.");
      response.set_ok(false);
      return response;
    }

    // TODO: Handle multiple nodes after step/task is implemented.
    if (task->executing_craned_ids.size() != 1) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_GENERIC_FAILURE);
      err->set_description("Multiple-node task not supported yet.");
      response.set_ok(false);
      return response;
    }

    target_craned_id = task->executing_craned_ids.front();
  }

  auto stub = g_craned_keeper->GetCranedStub(target_craned_id);
  if (stub == nullptr || stub->Invalid()) {
    auto* err = response.mutable_status();
    err->set_code(CraneErrCode::ERR_RPC_FAILURE);
    err->set_description("Craned is not available.");
    response.set_ok(false);
    return response;
  }

  return stub->AttachInContainerTask(request);
}

crane::grpc::ExecInContainerTaskReply TaskScheduler::ExecInContainerTask(
    const crane::grpc::ExecInContainerTaskRequest& request) {
  crane::grpc::ExecInContainerTaskReply response;

  CranedId target_craned_id;
  {
    LockGuard pending_guard(&m_pending_task_map_mtx_);
    LockGuard running_guard(&m_running_task_map_mtx_);

    task_id_t task_id = request.task_id();
    auto pd_it = m_pending_task_map_.find(task_id);
    if (pd_it != m_pending_task_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_CRI_CONTAINER_NOT_READY);
      err->set_description("Task is still pending. Try again later.");
      response.set_ok(false);
      return response;
    }

    auto rn_it = m_running_task_map_.find(task_id);
    if (rn_it == m_running_task_map_.end()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested task is not running.");
      response.set_ok(false);
      return response;
    }

    TaskInCtld* task = rn_it->second.get();
    if (!task->IsContainer()) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_INVALID_PARAM);
      err->set_description("Requested task is not a container task.");
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

    auto result = g_account_manager->CheckIfUidHasPermOnUser(
        request.uid(), task->Username(), false);
    if (!result) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_PERMISSION_USER);
      err->set_description("Insufficient permission to exec.");
      response.set_ok(false);
      return response;
    }

    // TODO: Handle multiple nodes after step/task is implemented.
    if (task->executing_craned_ids.size() != 1) {
      auto* err = response.mutable_status();
      err->set_code(CraneErrCode::ERR_GENERIC_FAILURE);
      err->set_description("Multiple-node task not supported yet.");
      response.set_ok(false);
      return response;
    }

    target_craned_id = task->executing_craned_ids.front();
  }

  auto stub = g_craned_keeper->GetCranedStub(target_craned_id);
  if (stub == nullptr || stub->Invalid()) {
    auto* err = response.mutable_status();
    err->set_code(CraneErrCode::ERR_RPC_FAILURE);
    err->set_description("Craned is not available.");
    response.set_ok(false);
    return response;
  }

  return stub->ExecInContainerTask(request);
}

crane::grpc::CreateReservationReply TaskScheduler::CreateResv(
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

std::expected<void, std::string> TaskScheduler::CreateResv_(
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
  ResourceV2 allocated_res;
  {
    LockGuard running_guard(&m_running_task_map_mtx_);
    std::vector<CranedId> nodes_not_found;
    std::vector<CranedId> nodes_conflicted;

    for (CranedId const& craned_id : craned_ids) {
      auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
      if (!craned_meta) {
        nodes_not_found.emplace_back(craned_id);
        continue;
      }

      bool failed = false;
      for (task_id_t task_id :
           craned_meta->rn_task_res_map | std::views::keys) {
        const auto& task = m_running_task_map_.at(task_id);
        absl::Time task_end_time = task->StartTime() + task->time_limit;

        if (task_end_time > start_time) {
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
  auto resv_meta_map = g_meta_container->GetResvMetaMapPtr();
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

crane::grpc::DeleteReservationReply TaskScheduler::DeleteResv(
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

std::expected<void, std::string> TaskScheduler::DeleteResvMeta_(
    const ResvId& resv_id) {
  g_meta_container->LockResReduceEvents();
  auto resv_meta_map = g_meta_container->GetResvMetaMapPtr();
  CRANE_TRACE("Deleting reservation {}", resv_id);
  if (!resv_meta_map->contains(resv_id)) {
    g_meta_container->UnlockResReduceEvents();
    return std::unexpected(fmt::format("Reservation {} not found", resv_id));
  }

  const auto& resv_meta = resv_meta_map->at(resv_id).GetExclusivePtr();

  if (!resv_meta->rn_job_res_map.empty()) {
    g_meta_container->UnlockResReduceEvents();
    return std::unexpected(fmt::format(
        "Not allowed to delete reservation {} with running tasks", resv_id));
  }

  const auto craned_ids = std::move(resv_meta->craned_ids);
  resv_meta_map->erase(resv_id);
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

void TaskScheduler::CleanTaskTimerCb_() {
  m_clean_task_timer_queue_handle_->send();
}

void TaskScheduler::TaskTimerAsyncCb_() {
  if (m_task_timer_queue_.size_approx() >= kTaskHoldTimerBatchNum) {
    m_clean_task_timer_queue_handle_->send();
  }
}

void TaskScheduler::CleanTaskTimerQueueCb_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  // It's ok to use an approximate size.
  size_t approximate_size = m_task_timer_queue_.size_approx();

  std::vector<TaskTimerQueueElem> timer_to_create;
  timer_to_create.resize(approximate_size);

  size_t actual_size = m_task_timer_queue_.try_dequeue_bulk(
      timer_to_create.begin(), approximate_size);

  timer_to_create.resize(actual_size);

  for (auto& [req, promise] : timer_to_create) {
    const auto& [task_id, secs] = req;

    // If any timer for the task exists, remove it.
    auto timer_it = m_task_timer_handles_.find(task_id);
    if (timer_it != m_task_timer_handles_.end()) {
      timer_it->second->close();
      m_task_timer_handles_.erase(timer_it);
    }

    CraneErrCode err;
    if (secs == 0) {  // Remove timer
      CRANE_TRACE("Remove hold constraint timer for task #{}.", task_id);
      err = SetHoldForTaskInRamAndDb_(task_id, false);
    } else if (secs == std::numeric_limits<int64_t>::max()) {
      CRANE_TRACE("Add a hold constraint for task #{} without timer.", task_id);
      err = SetHoldForTaskInRamAndDb_(task_id, true);
    } else {  // Set timer
      CRANE_TRACE("Add a hold constraint for task #{} with {}s timer.", task_id,
                  secs);

      auto on_timer_cb = [this, task_id](const uvw::timer_event&,
                                         uvw::timer_handle& handle) {
        CraneErrCode err = SetHoldForTaskInRamAndDb_(task_id, false);
        if (err != CraneErrCode::SUCCESS)
          CRANE_ERROR("Failed to release task #{} after hold.", task_id);

        handle.close();
        m_task_timer_handles_.erase(task_id);
      };

      err = SetHoldForTaskInRamAndDb_(task_id, true);

      auto task_timer_handle_ = uvw_loop->resource<uvw::timer_handle>();
      task_timer_handle_->on<uvw::timer_event>(std::move(on_timer_cb));
      task_timer_handle_->start(std::chrono::seconds(secs), 0s);
      m_task_timer_handles_[task_id] = std::move(task_timer_handle_);
    }

    promise.set_value(err);
  }
}

void TaskScheduler::CleanResvTimerQueueCb_(
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

void TaskScheduler::CancelTaskTimerCb_() {
  m_clean_cancel_queue_handle_->send();
}

void TaskScheduler::CancelTaskAsyncCb_() {
  if (m_cancel_task_queue_.size_approx() >= kCancelTaskBatchNum) {
    m_clean_cancel_queue_handle_->send();
  }
}

void TaskScheduler::CleanCancelQueueCb_() {
  // It's ok to use an approximate size.
  size_t approximate_size = m_cancel_task_queue_.size_approx();
  std::vector<CancelTaskQueueElem> tasks_to_cancel;
  tasks_to_cancel.resize(approximate_size);

  // Carry the ownership of TaskInCtld for automatic destruction.
  std::vector<std::unique_ptr<TaskInCtld>> pending_task_ptr_vec;
  HashMap<CranedId, std::unordered_map<job_id_t, std::set<step_id_t>>>
      running_task_craned_id_map;

  size_t actual_size = m_cancel_task_queue_.try_dequeue_bulk(
      tasks_to_cancel.begin(), approximate_size);

  // To cancel a task, there is two cases:
  // For pending task, we just need to set the task status to Cancelled.
  // For running task, we need to send a TerminateTasks RPC to the craned.
  for (auto& elem : tasks_to_cancel) {
    std::visit(  //
        VariantVisitor{
            [&](CancelPendingTaskQueueElem& pd_elem) {
              pending_task_ptr_vec.emplace_back(std::move(pd_elem.task));
            },
            [&](CancelRunningTaskQueueElem& rn_elem) {
              running_task_craned_id_map[rn_elem.craned_id][rn_elem.job_id]
                  .insert(rn_elem.step_id);
            }},
        elem);
  }

  auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
  for (auto&& [craned_id, steps] : running_task_craned_id_map) {
    if (!g_meta_container->CheckCranedOnline(craned_id)) {
      for (auto [job_id, step_ids] : steps) {
        for (auto step_id : step_ids)
          StepStatusChangeAsync(job_id, step_id, craned_id,
                                crane::grpc::TaskStatus::Cancelled,
                                ExitCode::EC_TERMINATED, "", now);
      }
      continue;
    }
    g_thread_pool->detach_task([id = craned_id, steps_to_cancel = steps]() {
      CRANE_TRACE("Craned {} is going to cancel [{}].", id,
                  util::JobStepsToString(steps_to_cancel));
      auto stub = g_craned_keeper->GetCranedStub(id);

      if (stub && !stub->Invalid()) stub->TerminateSteps(steps_to_cancel);
    });
  }

  if (pending_task_ptr_vec.empty()) return;

  for (auto& task : pending_task_ptr_vec) {
    task->SetStatus(crane::grpc::Cancelled);
    task->SetEndTime(absl::Now());
    g_account_meta_container->FreeQosResource(*task);

    if (task->type == crane::grpc::Interactive) {
      auto& meta = std::get<InteractiveMetaInTask>(task->meta);
      // Cancel request may not come from crun/calloc but from ccancel,
      // ask them to exit
      if (!meta.has_been_cancelled_on_front_end) {
        meta.has_been_cancelled_on_front_end = true;
        g_thread_pool->detach_task([cb = meta.cb_task_cancel,
                                    task_id = task->TaskId()] { cb(task_id); });
      } else {
        // Cancel request from crun/calloc, reply CompletionAck
        g_thread_pool->detach_task(
            [cb = meta.cb_task_completed, task_id = task->TaskId()] {
              cb(task_id, true);
            });
      }
    }
  }

  std::unordered_set<TaskInCtld*> pd_task_raw_ptrs;
  for (auto& task : pending_task_ptr_vec) pd_task_raw_ptrs.emplace(task.get());
  ProcessFinalTasks_(pd_task_raw_ptrs);
}

void TaskScheduler::SubmitTaskTimerCb_() {
  m_clean_submit_queue_handle_->send();
}

void TaskScheduler::SubmitTaskAsyncCb_() {
  if (m_submit_task_queue_.size_approx() >= kSubmitTaskBatchNum)
    m_clean_submit_queue_handle_->send();
}

void TaskScheduler::CleanSubmitQueueCb_() {
  using SubmitQueueElem =
      std::pair<std::unique_ptr<TaskInCtld>, std::promise<task_id_t>>;

  // It's ok to use an approximate size.
  size_t approximate_size = m_submit_task_queue_.size_approx();

  std::vector<SubmitQueueElem> accepted_tasks;
  std::vector<TaskInCtld*> accepted_task_ptrs;
  std::vector<SubmitQueueElem> rejected_tasks;

  size_t map_size = m_pending_map_cached_size_.load(std::memory_order_acquire);
  size_t accepted_size;
  size_t rejected_size;

  if (g_config.RejectTasksBeyondCapacity) {
    accepted_size =
        std::min(approximate_size, g_config.PendingQueueMaxSize - map_size);
    rejected_size = approximate_size - accepted_size;
  } else {
    accepted_size = approximate_size;
    rejected_size = 0;
  }

  size_t accepted_actual_size;
  size_t rejected_actual_size;

  // Accept tasks within queue capacity.
  do {
    if (accepted_size == 0) break;
    accepted_tasks.resize(accepted_size);

    accepted_actual_size = m_submit_task_queue_.try_dequeue_bulk(
        accepted_tasks.begin(), accepted_size);
    if (accepted_actual_size == 0) break;

    accepted_task_ptrs.reserve(accepted_actual_size);

    // The order of element inside the bulk is reverse.
    for (uint32_t i = 0; i < accepted_tasks.size(); i++) {
      uint32_t pos = accepted_tasks.size() - 1 - i;
      auto* task = accepted_tasks[pos].first.get();
      // Add the task to the pending task queue.
      task->SetStatus(crane::grpc::Pending);
      accepted_task_ptrs.emplace_back(task);
    }

    if (!g_embedded_db_client->AppendTasksToPendingAndAdvanceTaskIds(
            accepted_task_ptrs)) {
      CRANE_ERROR("Failed to append a batch of tasks to embedded db queue.");
      for (auto& pair : accepted_tasks) {
        g_account_meta_container->FreeQosResource(*pair.first);
        pair.second /*promise*/.set_value(0);
      }
      break;
    }

    m_pending_task_map_mtx_.Lock();

    for (uint32_t i = 0; i < accepted_tasks.size(); i++) {
      uint32_t pos = accepted_tasks.size() - 1 - i;
      task_id_t id = accepted_tasks[pos].first->TaskId();
      auto& task_id_promise = accepted_tasks[pos].second;

      m_pending_task_map_.emplace(id, std::move(accepted_tasks[pos].first));
      task_id_promise.set_value(id);
    }

    m_pending_map_cached_size_.store(m_pending_task_map_.size(),
                                     std::memory_order_release);
    m_pending_task_map_mtx_.Unlock();
  } while (false);

  // Reject tasks beyond queue capacity
  do {
    if (rejected_size == 0) break;
    rejected_tasks.resize(rejected_size);

    rejected_actual_size = m_submit_task_queue_.try_dequeue_bulk(
        rejected_tasks.begin(), rejected_size);
    if (rejected_actual_size == 0) break;

    CRANE_TRACE("Rejecting {} tasks...", rejected_actual_size);
    for (size_t i = 0; i < rejected_actual_size; i++) {
      g_account_meta_container->FreeQosResource(*rejected_tasks[i].first);
      rejected_tasks[i].second.set_value(0);
    }
  } while (false);
}

void TaskScheduler::StepStatusChangeAsync(
    job_id_t job_id, step_id_t step_id, const CranedId& craned_index,
    crane::grpc::TaskStatus new_status, uint32_t exit_code, std::string reason,
    google::protobuf::Timestamp timestamp) {
  m_task_status_change_queue_.enqueue({.job_id = job_id,
                                       .step_id = step_id,
                                       .exit_code = exit_code,
                                       .new_status = new_status,
                                       .craned_index = craned_index,
                                       .reason = reason,
                                       .timestamp = timestamp});
  m_task_status_change_async_handle_->send();
}

void TaskScheduler::TaskStatusChangeTimerCb_() {
  m_clean_task_status_change_handle_->send();
}

void TaskScheduler::TaskStatusChangeAsyncCb_() {
  if (m_task_status_change_queue_.size_approx() >= kTaskStatusChangeBatchNum)
    m_clean_task_status_change_handle_->send();
}

void TaskScheduler::CleanTaskStatusChangeQueueCb_() {
  size_t approximate_size = m_task_status_change_queue_.size_approx();

  std::vector<TaskStatusChangeArg> args;
  args.resize(approximate_size);

  size_t actual_size = m_task_status_change_queue_.try_dequeue_bulk(
      args.begin(), approximate_size);
  if (actual_size == 0) return;
  args.resize(actual_size);

  CRANE_TRACE("Cleaning {} TaskStatusChanges...", actual_size);

  StepStatusChangeContext context{};
  context.rn_step_raw_ptrs.reserve(actual_size);
  context.step_ptrs.reserve(actual_size);
  context.step_raw_ptrs.reserve(actual_size);

  context.job_ptrs.reserve(actual_size);
  context.job_raw_ptrs.reserve(actual_size);
  context.rn_job_raw_ptrs.reserve(actual_size);

  LockGuard running_guard(&m_running_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  for (const auto& [task_id, step_id, exit_code, new_status, craned_index,
                    reason, timestamp] : args) {
    auto iter = m_running_task_map_.find(task_id);
    if (iter == m_running_task_map_.end()) {
      CRANE_WARN(
          "[Job #{}] Ignoring unknown job in CleanTaskStatusChangeQueueCb_.",
          task_id);
      continue;
    }

    // Free job allocation
    std::optional<std::pair<crane::grpc::TaskStatus, uint32_t /*exit code*/>>
        job_finished_status{std::nullopt};

    std::unique_ptr<TaskInCtld>& task = iter->second;
    if (task->DaemonStep() && step_id == task->DaemonStep()->StepId()) {
      CRANE_TRACE(
          "[Step #{}.{}] Daemon step status change received, status: {}.",
          task->TaskId(), step_id, new_status);
      auto* step = task->DaemonStep();
      job_finished_status = step->StepStatusChange(
          new_status, exit_code, reason, craned_index, &context);
    } else {
      CommonStepInCtld* step = task->GetStep(step_id);
      if (step == nullptr) {
        CRANE_WARN("[Step #{}.{}] Ignoring unknown step in TaskStatusChange.",
                   task_id, step_id);
        continue;
      }
      CRANE_TRACE("[Step #{}.{}] Step status change received, status: {}.",
                  task_id, step_id, new_status);
      step->StepStatusChange(new_status, exit_code, reason, craned_index,
                             &context);
    }

    if (job_finished_status.has_value()) {
      if (task->type == crane::grpc::Interactive) {
        auto& meta = std::get<InteractiveMetaInTask>(task->meta);
        // if (meta.interactive_type == crane::grpc::Crun) {  // Crun
        //   if (++meta.status_change_cnt < task->executing_craned_ids.size()) {
        //     CRANE_TRACE(
        //         "{}/{} TaskStatusChanges of Crun task #{} were received. "
        //         "Keep waiting...",
        //         meta.status_change_cnt, task->executing_craned_ids.size(),
        //         task->TaskId());
        //     continue;
        //   }
        // }

        // TaskStatusChange may indicate the time limit has been reached and
        // the task has been terminated. No more TerminateTask RPC should be
        // sent to the craned node if any further CancelTask or
        // TaskCompletionRequest RPC is received.

        // Task end triggered by craned.
        if (!meta.has_been_cancelled_on_front_end) {
          meta.has_been_cancelled_on_front_end = true;
          meta.cb_task_cancel(task->TaskId());
          // Completion ack will send in grpc server triggered by task complete
          // req
          meta.cb_task_completed(task->TaskId(), false);
        } else {
          // Send Completion Ack to frontend now.
          meta.cb_task_completed(task->TaskId(), true);
        }
      }

      task->SetStatus(job_finished_status.value().first);
      task->SetExitCode(job_finished_status.value().second);
      task->SetEndTime(absl::FromUnixSeconds(timestamp.seconds()) +
                       absl::Nanoseconds(timestamp.nanos()));

      for (CranedId const& craned_id : task->CranedIds()) {
        auto node_to_task_map_it = m_node_to_tasks_map_.find(craned_id);
        if (node_to_task_map_it == m_node_to_tasks_map_.end()) [[unlikely]] {
          CRANE_ERROR("Failed to find craned_id {} in m_node_to_tasks_map_",
                      craned_id);
        } else {
          node_to_task_map_it->second.erase(task_id);
          if (node_to_task_map_it->second.empty()) {
            m_node_to_tasks_map_.erase(node_to_task_map_it);
          }
        }
      }

      for (CranedId const& craned_id : task->CranedIds()) {
        g_meta_container->FreeResourceFromNode(craned_id, task_id);
      }
      if (task->reservation != "")
        g_meta_container->FreeResourceFromResv(task->reservation,
                                               task->TaskId());
      g_account_meta_container->FreeQosResource(*task);

      context.job_raw_ptrs.insert(task.get());
      context.job_ptrs.emplace(std::move(task));

      // As for now, task status change includes only
      // Pending / Running -> Completed / Failed / Cancelled.
      // It means all task status changes will put the task into mongodb,
      // so we don't have any branch code here and just put it into mongodb.

      CRANE_TRACE("[Job #{}] Completed with status {}.", task_id,
                  job_finished_status.value());
      m_running_task_map_.erase(iter);
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
              "Failed to AllocSteps for [{}] tasks on Node {}: Rpc failure",
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
            "Failed to AllocSteps for [{}] tasks on Node {}: Craned down",
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
              crane::grpc::TaskStatus::Failed, ExitCode::EC_CRANED_DOWN,
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
          CRANE_ERROR("Failed to FreeSteps for [{}] steps on Node {}: {}",
                      util::JobStepsToString(
                          context.craned_step_free_map.at(craned_id)),
                      craned_id, CraneErrStr(err));
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
                  job_id, step_id, craned_id, crane::grpc::TaskStatus::Failed,
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
                job_id, step_id, craned_id, crane::grpc::TaskStatus::Failed,
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
                  "Failed to TerminateOrphanedSteps for [{}] tasks on Node {}",
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
              CRANE_ERROR("Failed to TerminateSteps for [{}] tasks on Node {}",
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
              job_id, kDaemonStepId, craned_id, crane::grpc::TaskStatus::Failed,
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

  // When store, write step info first, then job info.
  auto ok = g_embedded_db_client->BeginStepVarDbTransaction(&txn_id);
  if (!ok) {
    CRANE_ERROR(
        "TaskScheduler failed to start step transaction when clean step status "
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
        "TaskScheduler failed to commit step transaction when clean step "
        "status change.");
  }

  ProcessFinalSteps_(context.step_raw_ptrs);

  ok = g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  if (!ok) {
    CRANE_ERROR(
        "TaskScheduler failed to start job transaction when clean step status "
        "change.");
  }
  // Jobs will update in embedded db
  for (auto* job : context.rn_job_raw_ptrs) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfTask(txn_id, job->TaskDbId(),
                                                       job->RuntimeAttr()))
      CRANE_ERROR("[Job #{}] Failed to call UpdateRuntimeAttrOfTask()",
                  job->TaskId());
  }

  ok = g_embedded_db_client->CommitVariableDbTransaction(txn_id);
  if (!ok) {
    CRANE_ERROR(
        "TaskScheduler failed to commit job transaction when clean  step "
        "status change.");
  }
  ProcessFinalTasks_(context.job_raw_ptrs);
}

void TaskScheduler::QueryTasksInRam(
    const crane::grpc::QueryTasksInfoRequest* request,
    crane::grpc::QueryTasksInfoReply* response) {
  auto now = absl::Now();

  auto* task_list = response->mutable_task_info_list();
  auto append_fn = [&](auto& it) {
    TaskInCtld& task = *it.second;
    auto* task_it = task_list->Add();
    task.SetFieldsOfTaskInfo(task_it);
    task_it->mutable_elapsed_time()->set_seconds(
        ToInt64Seconds(now - task.StartTime()));
  };

  auto task_rng_filter_time = [&](auto& it) {
    TaskInCtld& task = *it.second;
    bool has_submit_time_interval = request->has_filter_submit_time_interval();
    bool has_start_time_interval = request->has_filter_start_time_interval();
    bool has_end_time_interval = request->has_filter_end_time_interval();

    bool valid = true;
    if (has_submit_time_interval) {
      const auto& interval = request->filter_submit_time_interval();
      valid &= !interval.has_lower_bound() ||
               task.RuntimeAttr().submit_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               task.RuntimeAttr().submit_time() <= interval.upper_bound();
    }

    if (has_start_time_interval) {
      const auto& interval = request->filter_start_time_interval();
      valid &= !interval.has_lower_bound() ||
               task.RuntimeAttr().start_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               task.RuntimeAttr().start_time() <= interval.upper_bound();
    }

    if (has_end_time_interval) {
      const auto& interval = request->filter_end_time_interval();
      valid &= !interval.has_lower_bound() ||
               task.RuntimeAttr().end_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               task.RuntimeAttr().end_time() <= interval.upper_bound();
    }

    return valid;
  };

  bool no_accounts_constraint = request->filter_accounts().empty();
  std::unordered_set<std::string> req_accounts(
      request->filter_accounts().begin(), request->filter_accounts().end());
  auto task_rng_filter_account = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_accounts_constraint || req_accounts.contains(task.account);
  };

  bool no_username_constraint = request->filter_users().empty();
  std::unordered_set<std::string> req_users(request->filter_users().begin(),
                                            request->filter_users().end());
  auto task_rng_filter_username = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_username_constraint || req_users.contains(task.Username());
  };

  bool no_qos_constraint = request->filter_qos().empty();
  std::unordered_set<std::string> req_qos(request->filter_qos().begin(),
                                          request->filter_qos().end());
  auto task_rng_filter_qos = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_qos_constraint || req_qos.contains(task.qos);
  };

  bool no_task_names_constraint = request->filter_task_names().empty();
  std::unordered_set<std::string> req_task_names(
      request->filter_task_names().begin(), request->filter_task_names().end());
  auto task_rng_filter_name = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_task_names_constraint ||
           req_task_names.contains(task.TaskToCtld().name());
  };

  bool no_partitions_constraint = request->filter_partitions().empty();
  std::unordered_set<std::string> req_partitions(
      request->filter_partitions().begin(), request->filter_partitions().end());
  auto task_rng_filter_partition = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_partitions_constraint ||
           req_partitions.contains(task.partition_id);
  };

  bool no_task_ids_constraint = request->filter_task_ids().empty();
  std::unordered_set<uint32_t> req_task_ids(request->filter_task_ids().begin(),
                                            request->filter_task_ids().end());
  auto task_rng_filter_id = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_task_ids_constraint || req_task_ids.contains(task.TaskId());
  };

  bool no_task_states_constraint = request->filter_task_states().empty();
  std::unordered_set<int> req_task_states(request->filter_task_states().begin(),
                                          request->filter_task_states().end());
  auto task_rng_filter_state = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_task_states_constraint ||
           req_task_states.contains(task.RuntimeAttr().status());
  };

  bool no_task_types_constraint = request->filter_task_types().empty();
  std::unordered_set<int> req_task_types(request->filter_task_types().begin(),
                                         request->filter_task_types().end());
  auto task_rng_filter_task_type = [&](auto& it) {
    TaskInCtld& task = *it.second;
    return no_task_types_constraint || req_task_types.contains(task.type);
  };

  auto pending_rng = m_pending_task_map_ | ranges::views::all;
  auto running_rng = m_running_task_map_ | ranges::views::all;
  auto pd_r_rng = ranges::views::concat(pending_rng, running_rng);

  size_t num_limit = request->num_limit() == 0 ? kDefaultQueryTaskNumLimit
                                               : request->num_limit();

  auto filtered_rng = pd_r_rng |
                      ranges::views::filter(task_rng_filter_account) |
                      ranges::views::filter(task_rng_filter_name) |
                      ranges::views::filter(task_rng_filter_partition) |
                      ranges::views::filter(task_rng_filter_id) |
                      ranges::views::filter(task_rng_filter_state) |
                      ranges::views::filter(task_rng_filter_username) |
                      ranges::views::filter(task_rng_filter_time) |
                      ranges::views::filter(task_rng_filter_qos) |
                      ranges::views::filter(task_rng_filter_task_type) |
                      ranges::views::take(num_limit);

  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard running_guard(&m_running_task_map_mtx_);

  ranges::for_each(filtered_rng, append_fn);
}

void TaskScheduler::QueryRnJobOnCtldForNodeConfig(
    const CranedId& craned_id, crane::grpc::ConfigureCranedRequest* req) {
  LockGuard running_job_guard(&m_running_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  auto it = m_node_to_tasks_map_.find(craned_id);
  if (it == m_node_to_tasks_map_.end()) return;

  auto& job_steps = *req->mutable_job_steps();

  for (const auto& job_id : it->second) {
    auto job_it = m_running_task_map_.find(job_id);
    if (job_it == m_running_task_map_.end()) continue;
    auto* daemon_step = job_it->second->DaemonStep();
    *job_steps[job_id].mutable_job() = daemon_step->GetJobToD(craned_id);
    auto& steps = *job_steps[job_id].mutable_steps();
    auto& step_status = *job_steps[job_id].mutable_step_status();
    steps[daemon_step->StepId()] = daemon_step->GetStepToD(craned_id);
    step_status[daemon_step->StepId()] = daemon_step->Status();
    if (job_it->second->PrimaryStep() &&
        std::ranges::contains(job_it->second->PrimaryStep()->ExecutionNodes(),
                              craned_id)) {
      const auto* step = job_it->second->PrimaryStep();
      steps[step->StepId()] = step->GetStepToD(craned_id);
      step_status[step->StepId()] = step->Status();
    }
    for (const auto& step : job_it->second->Steps() | std::views::values)
      if (std::ranges::contains(step->ExecutionNodes(), craned_id)) {
        steps[step->StepId()] = step->GetStepToD(craned_id);
        step_status[step->StepId()] = step->Status();
      }
  }
}

void TaskScheduler::TerminateOrphanedSteps(
    const std::unordered_map<job_id_t, std::set<step_id_t>>& steps,
    const CranedId& excluded_node) {
  CRANE_INFO("Terminate orphaned steps: [{}] synced by {}.",
             util::JobStepsToString(steps), excluded_node);

  // Now we just terminate all job and task.
  std::unordered_map<CranedId,
                     std::unordered_map<job_id_t, std::set<step_id_t>>>
      craned_steps_map;
  {
    LockGuard running_job_guard(&m_running_task_map_mtx_);
    LockGuard indexes_guard(&m_task_indexes_mtx_);
    for (const auto& [job_id, step_ids] : steps) {
      auto job_it = m_running_task_map_.find(job_id);
      if (job_it == m_running_task_map_.end()) {
        CRANE_WARN("Job {} not found in running task map.", job_id);
        continue;
      }
      auto& job = job_it->second;
      for (const auto& step_id : step_ids) {
        StepInCtld* step{nullptr};
        if (auto daemon_step = job->DaemonStep();
            daemon_step && daemon_step->StepId() == step_id) {
          step = daemon_step;
        } else if (auto primary_step = job->PrimaryStep();
                   primary_step && step_id == primary_step->StepId()) {
          step = primary_step;
        } else {
          step = job->GetStep(step_id);
        }
        if (!step) {
          CRANE_WARN("[Step #{}.{}] Step not found in job.", step_id, job_id);
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
  uint32_t node_num_limit;
  if constexpr (kAlgoRedundantNode) {
    node_num_limit = std::min(job->node_num + 10, job->node_num * 2);
  } else {
    node_num_limit = job->node_num;
  }

  std::vector<NodeState*> nodes_to_sched;

  absl::Time earliest_end_time = now + job->time_limit;

  for (const auto& node_state :
       m_node_selector_->GetOrderedNodesSet() | std::views::values) {
    const auto& craned_id = node_state->craned_id;
    auto& time_avail_res_map = node_state->time_avail_res_map;
    // Number of tasks is not less than map size.
    // When condition is true, the craned has too many tasks.
    if (time_avail_res_map.size() >= kAlgoMaxTaskNumPerNode) {
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

    if (!(job->requested_node_res_view <=
          m_node_selector_->GetNodeState(craned_id)->res_total)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Job #{} needs more resource than that of craned {}. "
            "Skipping this craned.",
            job->job_id, craned_id);
      }
      continue;
    }

    if (job->exclusive) {
      job->requested_node_res_view.SetToZero();
      job->requested_node_res_view += node_state->res_total;
    }

    if constexpr (kAlgoRedundantNode) {
      nodes_to_sched.emplace_back(node_state);
      if (nodes_to_sched.size() >= node_num_limit) break;
    } else {
      // Given N as the required number of nodes,
      // all the nodes that is able to run the task at some time-point will be
      // iterated and the first N nodes will be in the craned_ids_.
      if (nodes_to_sched.size() < node_num_limit)
        nodes_to_sched.emplace_back(node_state);

      // Find all possible nodes that can run the task now.
      ResourceInNode feasible_res;
      bool ok = job->requested_node_res_view.GetFeasibleResourceInNode(
          node_state->res_avail, &feasible_res);
      if (ok) {
        bool is_node_satisfied_now = true;
        for (const auto& [time, res] : time_avail_res_map) {
          if (time >= earliest_end_time) break;
          if (!(feasible_res <= res)) is_node_satisfied_now = false;
        }

        if (is_node_satisfied_now) {
          job->craned_ids.emplace_back(craned_id);
          job->allocated_res.AddResourceInNode(craned_id, feasible_res);
          if (job->craned_ids.size() >= job->node_num) {
            job->start_time = now;
            return true;
          }
        }
      }
    }
  }

  if (nodes_to_sched.size() < job->node_num) {
    CRANE_TRACE(
        "Only {} nodes are available for job #{} but {} nodes are required.",
        nodes_to_sched.size(), job->job_id, job->node_num);
    return false;
  }

  job->allocated_res.SetToZero();

  for (const auto& node_state : nodes_to_sched) {
    ResourceInNode feasible_res;

    // TODO: get feasible resource randomly (may cause start time change
    //       rapidly)
    bool ok = job->requested_node_res_view.GetFeasibleResourceInNode(
        node_state->res_avail, &feasible_res);
    if (!ok) {
      ok = job->requested_node_res_view.GetFeasibleResourceInNode(
          node_state->res_total, &feasible_res);
    }
    if (!ok) {
      CRANE_ERROR(
          "Job #{} needs more resource than that of craned {}. "
          "Craned resource might have been changed.",
          job->job_id, node_state->craned_id);
      return false;
    }

    job->allocated_res.AddResourceInNode(node_state->craned_id, feasible_res);
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

  // For nodes in partition, reservations and running jobs on node are collected
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
          now + absl::Seconds(1));  // In case TaskStatusChange is delayed
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
              "Reservation {} of running job #{} not found in resv_node_state_"
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

  // Schedule pending tasks
  // TODO: do it in parallel
  for (const auto& job : job_ptr_vec) {
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

void TaskScheduler::ProcessFinalSteps_(
    std::unordered_set<StepInCtld*> const& steps) {
  PersistAndTransferStepsToMongodb_(steps);
  // CallPluginHookForFinalTasks_(tasks);
}

void TaskScheduler::PersistAndTransferStepsToMongodb_(
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

  // Now tasks are in MongoDB.
  if (!g_db_client->InsertSteps(steps)) {
    CRANE_ERROR("Failed to call g_db_client->InsertSteps() ");
    return;
  }

  // Remove tasks in final queue.
  std::vector<step_db_id_t> db_ids;
  for (StepInCtld* step : steps) db_ids.emplace_back(step->StepDbId());

  if (!g_embedded_db_client->PurgeEndedSteps(db_ids)) {
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedSteps() "
        "for final tasks");
  }
}

void TaskScheduler::ProcessFinalTasks_(
    const std::unordered_set<TaskInCtld*>& tasks) {
  PersistAndTransferTasksToMongodb_(tasks);
  CallPluginHookForFinalTasks_(tasks);
}

void TaskScheduler::CallPluginHookForFinalTasks_(
    std::unordered_set<TaskInCtld*> const& tasks) {
  if (g_config.Plugin.Enabled && !tasks.empty()) {
    std::vector<crane::grpc::TaskInfo> tasks_post_comp;
    for (TaskInCtld* task : tasks) {
      crane::grpc::TaskInfo t;
      task->SetFieldsOfTaskInfo(&t);
      tasks_post_comp.emplace_back(std::move(t));
    }
    g_plugin_client->EndHookAsync(std::move(tasks_post_comp));
  }
}

void TaskScheduler::PersistAndTransferTasksToMongodb_(
    std::unordered_set<TaskInCtld*> const& tasks) {
  if (tasks.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  for (TaskInCtld* task : tasks) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfTask(txn_id, task->TaskDbId(),
                                                       task->RuntimeAttr()))
      CRANE_ERROR("Failed to call UpdateRuntimeAttrOfTask() for task #{}",
                  task->TaskId());
  }

  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  // Now tasks are in MongoDB.
  if (!g_db_client->InsertJobs(tasks)) {
    CRANE_ERROR("Failed to call g_db_client->InsertJobs() ");
    return;
  }

  // Remove tasks in final queue.
  std::unordered_map<job_id_t, task_db_id_t> db_ids;
  for (TaskInCtld* task : tasks) db_ids[task->TaskId()] = task->TaskDbId();

  if (!g_embedded_db_client->PurgeEndedTasks(db_ids)) {
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedTasks() "
        "for final tasks");
  }
}

CraneExpected<void> TaskScheduler::HandleUnsetOptionalInTaskToCtld(
    TaskInCtld* task) {
  if (task->IsBatch()) {
    auto* batch_meta = task->MutableTaskToCtld()->mutable_batch_meta();
    if (!batch_meta->has_open_mode_append())
      batch_meta->set_open_mode_append(g_config.JobFileOpenModeAppend);
  }

  return {};
}

CraneExpected<void> TaskScheduler::AcquireTaskAttributes(TaskInCtld* task) {
  auto part_it = g_config.Partitions.find(task->partition_id);
  if (part_it == g_config.Partitions.end()) {
    CRANE_ERROR("Failed to call AcquireTaskAttributes: {}",
                CraneErrStr(CraneErrCode::ERR_INVALID_PARTITION));
    return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);
  }

  task->partition_priority = part_it->second.priority;

  Config::Partition const& part_meta = part_it->second;

  // Calculate task memory value based on MEM_PER_CPU and user-set memory.
  AllocatableResource& task_alloc_res =
      task->requested_node_res_view.GetAllocatableRes();
  double core_double = static_cast<double>(task_alloc_res.cpu_count);

  double task_mem_per_cpu = (double)task_alloc_res.memory_bytes / core_double;
  if (task_alloc_res.memory_bytes == 0) {
    // If a task leaves its memory bytes to 0,
    // use the partition's default value.
    task_mem_per_cpu = part_meta.default_mem_per_cpu;
  } else if (part_meta.max_mem_per_cpu != 0) {
    // If a task sets its memory bytes,
    // check if memory/core ratio is greater than the partition's maximum
    // value.
    task_mem_per_cpu =
        std::min(task_mem_per_cpu, (double)part_meta.max_mem_per_cpu);
  }
  uint64_t mem_bytes = core_double * task_mem_per_cpu;

  task->requested_node_res_view.GetAllocatableRes().memory_bytes = mem_bytes;
  task->requested_node_res_view.GetAllocatableRes().memory_sw_bytes = mem_bytes;

  auto check_qos_result = g_account_manager->CheckQosLimitOnTask(
      task->Username(), task->account, task);
  if (!check_qos_result) {
    CRANE_ERROR("Failed to call CheckQosLimitOnTask: {}",
                CraneErrStr(check_qos_result.error()));
    return check_qos_result;
  }

  if (!task->TaskToCtld().nodelist().empty() && task->included_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(task->TaskToCtld().nodelist(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVALID_NODE_LIST);

    for (auto&& node : nodes) task->included_nodes.emplace(std::move(node));
  }

  if (!task->TaskToCtld().excludes().empty() && task->excluded_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(task->TaskToCtld().excludes(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVALID_EX_NODE_LIST);

    for (auto&& node : nodes) task->excluded_nodes.emplace(std::move(node));
  }

  return {};
}

CraneExpected<void> TaskScheduler::CheckTaskValidity(TaskInCtld* task) {
  if (!CheckIfTimeLimitIsValid(task->time_limit))
    return std::unexpected(CraneErrCode::ERR_TIME_TIMIT_BEYOND);

  // Check whether the selected partition is able to run this task.
  std::unordered_set<std::string> avail_nodes;
  {
    // Preserve lock ordering.
    auto metas_ptr = g_meta_container->GetPartitionMetasPtr(task->partition_id);

    // Since we do not access the elements in partition_metas_m

    // Check whether the selected partition is able to run this task.
    if (!(task->requested_node_res_view * task->node_num <=
          metas_ptr->partition_global_meta.res_total_inc_dead)) {
      CRANE_TRACE(
          "Resource not enough for task #{}. "
          "Partition total: cpu {}, mem: {}, mem+sw: {}, gres: {}",
          task->TaskId(),
          metas_ptr->partition_global_meta.res_total_inc_dead
              .GetAllocatableRes()
              .cpu_count,
          util::ReadableMemory(
              metas_ptr->partition_global_meta.res_total_inc_dead
                  .GetAllocatableRes()
                  .memory_bytes),
          util::ReadableMemory(
              metas_ptr->partition_global_meta.res_total_inc_dead
                  .GetAllocatableRes()
                  .memory_sw_bytes),
          util::ReadableTypedDeviceMap(
              metas_ptr->partition_global_meta.res_total.GetDeviceMap()));
      return std::unexpected(CraneErrCode::ERR_NO_RESOURCE);
    }

    if (task->node_num > metas_ptr->craned_ids.size()) {
      CRANE_TRACE(
          "Nodes not enough for task #{}. "
          "Partition total Nodes: {}",
          task->TaskId(), metas_ptr->craned_ids.size());
      return std::unexpected(CraneErrCode::ERR_INVALID_NODE_NUM);
    }

    if (task->reservation != "") {
      if (!g_meta_container->GetResvMetaMapConstPtr()->contains(
              task->reservation)) {
        CRANE_TRACE("Reservation {} not found for task #{}", task->reservation,
                    task->TaskId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }

      auto resv_meta = g_meta_container->GetResvMetaPtr(task->reservation);

      if (resv_meta->part_id != "" &&
          resv_meta->part_id != task->partition_id) {
        CRANE_TRACE("Partition {} not allowed for reservation {} for task #{}",
                    task->partition_id, task->reservation, task->TaskId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }

      // if passed, either not in the black list (true, true)
      // or in the white list (false, false)
      if (resv_meta->accounts_black_list ^
          !resv_meta->accounts.contains(task->account)) {
        CRANE_TRACE("Account {} not allowed for reservation {} for task #{}",
                    task->account, task->reservation, task->TaskId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }
      if (resv_meta->users_black_list ^
          !resv_meta->users.contains(task->Username())) {
        CRANE_TRACE("User {} not allowed for reservation {} for task #{}",
                    task->Username(), task->reservation, task->TaskId());
        return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
      }

      if (!task->included_nodes.empty()) {
        auto reserved_craned_id_list = resv_meta->craned_ids;
        std::unordered_set<std::string> reserved_craned_id_set;
        reserved_craned_id_set.insert(reserved_craned_id_list.begin(),
                                      reserved_craned_id_list.end());
        for (const auto& craned_id : task->included_nodes) {
          if (!reserved_craned_id_set.contains(craned_id)) {
            CRANE_TRACE("Craned {} is not in the reservation {} for task #{}",
                        craned_id, task->reservation, task->TaskId());
            return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
          }
        }
      }
    }

    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
    for (const auto& craned_id : metas_ptr->craned_ids) {
      auto craned_meta = craned_meta_map->at(craned_id).GetExclusivePtr();
      if (task->requested_node_res_view <= craned_meta->res_total &&
          (task->included_nodes.empty() ||
           task->included_nodes.contains(craned_id)) &&
          (task->excluded_nodes.empty() ||
           !task->excluded_nodes.contains(craned_id)))
        avail_nodes.emplace(craned_meta->static_meta.hostname);

      if (avail_nodes.size() >= task->node_num) break;
    }
  }

  if (task->node_num > avail_nodes.size()) {
    CRANE_TRACE(
        "Resource not enough. Task #{} needs {} nodes, while only {} "
        "nodes satisfy its requirement.",
        task->TaskId(), task->node_num, avail_nodes.size());
    return std::unexpected(CraneErrCode::ERR_NO_ENOUGH_NODE);
  }

  return {};
}

CraneExpected<void> TaskScheduler::AcquireStepAttributes(const TaskInCtld& task,
                                                         StepInCtld* step) {
  auto part_it = g_config.Partitions.find(task.partition_id);
  if (part_it == g_config.Partitions.end()) {
    CRANE_ERROR("Failed to call AcquireStepAttributes: {}",
                CraneErrStr(CraneErrCode::ERR_INVALID_PARTITION));
    return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);
  }

  Config::Partition const& part_meta = part_it->second;

  // Calculate step memory value based on MEM_PER_CPU and user-set memory.
  AllocatableResource& step_alloc_res =
      step->requested_node_res_view.GetAllocatableRes();
  double core_double = static_cast<double>(step_alloc_res.cpu_count);
  // FIXME: core_double must greater than 0, so as job.

  double step_mem_per_cpu = (double)step_alloc_res.memory_bytes / core_double;
  if (step_alloc_res.memory_bytes == 0) {
    // If a step leaves its memory bytes to 0,
    // use the partition's default value.
    step_mem_per_cpu = part_meta.default_mem_per_cpu;
  } else if (part_meta.max_mem_per_cpu != 0) {
    // If a step sets its memory bytes,
    // check if memory/core ratio is greater than the partition's maximum
    // value.
    step_mem_per_cpu =
        std::min(step_mem_per_cpu, (double)part_meta.max_mem_per_cpu);
  }
  uint64_t mem_bytes = core_double * step_mem_per_cpu;

  step->requested_node_res_view.GetAllocatableRes().memory_bytes = mem_bytes;
  step->requested_node_res_view.GetAllocatableRes().memory_sw_bytes = mem_bytes;
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

  return {};
}

CraneExpected<void> TaskScheduler::CheckStepValidity(const TaskInCtld& task,
                                                     StepInCtld* step) {
  if (!CheckIfTimeLimitIsValid(step->time_limit))
    return std::unexpected(CraneErrCode::ERR_TIME_TIMIT_BEYOND);

  return {};
}

void TaskScheduler::TerminateTasksOnCraned(const CranedId& craned_id,
                                           uint32_t exit_code) {
  CRANE_TRACE("Terminate tasks on craned {}", craned_id);

  // The order of LockGuards matters.
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  auto it = m_node_to_tasks_map_.find(craned_id);
  if (it != m_node_to_tasks_map_.end()) {
    // m_node_to_tasks_map_[craned_id] will be cleaned in
    // TaskStatusChangeNoLock_. Do not clean it here and make a copy of
    // it->second.
    std::vector<task_id_t> task_ids(it->second.begin(), it->second.end());

    for (task_id_t task_id : task_ids)
      StepStatusChangeAsync(task_id, kDaemonStepId, craned_id,
                            crane::grpc::TaskStatus::Failed, exit_code,
                            "Terminated",
                            google::protobuf::util::TimeUtil::GetCurrentTime());
  } else {
    CRANE_TRACE("No task is executed by craned {}. Ignore cleaning step...",
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

    uint64_t job_mem_req = job->requested_node_res_view.MemoryBytes();
    bound.mem_alloc_min = std::min(job_mem_req, bound.mem_alloc_min);
    bound.mem_alloc_max = std::max(job_mem_req, bound.mem_alloc_max);

    double job_cpus_req =
        static_cast<double>(job->requested_node_res_view.CpuCount());
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

    uint64_t mem_alloc = job->allocated_res_view.MemoryBytes();
    bound.mem_alloc_min = std::min(mem_alloc, bound.mem_alloc_min);
    bound.mem_alloc_max = std::max(mem_alloc, bound.mem_alloc_max);

    double cpus_alloc = job->allocated_res_view.CpuCount();
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
          1.0 * (job->allocated_res_view.CpuCount() - bound.cpus_alloc_min) /
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
          static_cast<double>(job->allocated_res_view.MemoryBytes() -
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
  uint64_t job_mem_alloc = job->requested_node_res_view.MemoryBytes();
  double job_cpus_alloc =
      static_cast<double>(job->requested_node_res_view.CpuCount());
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
      g_config.PriorityConfig.WeightQOS * qos_factor;

  return priority;
}
}  // namespace Ctld
