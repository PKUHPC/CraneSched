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
      std::make_unique<MinLoadFirst>(m_priority_sorter_.get());
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

          ok = g_embedded_db_client->PurgeEndedTasks({task_db_id});
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

      if (task->type != crane::grpc::Batch) {
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

        std::vector<task_db_id_t> db_ids{task_db_id};
        ok = g_embedded_db_client->PurgeEndedTasks(db_ids);
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

    std::vector<task_db_id_t> db_ids;
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

      db_ids.emplace_back(db_id);
    }

    ok = g_embedded_db_client->PurgeEndedTasks(db_ids);
    if (!ok) {
      CRANE_ERROR("Failed to call g_embedded_db_client->PurgeEndedTasks()");
    }
  }

  EmbeddedDbClient::StepDbSnapshot step_snapshot;
  ok = g_embedded_db_client->RetrieveLastStepInfo(&step_snapshot);
  if (!ok) {
    CRANE_ERROR("Failed to retrieve embedded DB step snapshot!");
    return false;
  }
  for (auto&& [job_id, steps] : step_snapshot.steps) {
    // Only running jobs have steps
    if (!m_running_task_map_.contains(job_id)) {
      CRANE_WARN(
          "Job #{} is not running, but has steps! Related steps dropped.",
          job_id);
      continue;
    }
    auto& job = *m_running_task_map_.at(job_id);
    for (auto&& step_info : steps) {
      StepInCtld* step;
      auto step_type = step_info.runtime_attr().step_type();
      if (step_type == crane::grpc::StepType::INVALID) {
        CRANE_ERROR("Invalid step type, ignored!");
        continue;
      }
      if (step_type == crane::grpc::StepType::DAEMON) {
        step = new DaemonStepInCtld();
      } else {
        step = new CommonStepInCtld();
      }
      step->RecoverFromDb(job, step_info);
      AcquireStepAttributes(job, step);
      CheckStepValidity(job, step);
      if (step_type == crane::grpc::StepType::DAEMON) {
        std::unique_ptr<DaemonStepInCtld> step_ptr(
            dynamic_cast<DaemonStepInCtld*>(step));
        job.SetDaemonStep(std::move(step_ptr));
        CRANE_INFO("Daemon step recovered for job #{}", job.TaskId());
      } else if (step_type == crane::grpc::StepType::PRIMARY) {
        std::unique_ptr<CommonStepInCtld> step_ptr(
            dynamic_cast<CommonStepInCtld*>(step));
        CRANE_INFO("Primary step recovered for job #{}", job.TaskId());
        job.SetPrimaryStep(std::move(step_ptr));
      } else {
        std::unique_ptr<CommonStepInCtld> step_ptr(
            dynamic_cast<CommonStepInCtld*>(step));
        CRANE_INFO("Common step {} recovered for job #{}", job.TaskId(),
                   step->StepId());
        job.AddStep(std::move(step_ptr));
      }
    }
  }

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
    g_meta_container->MallocResourceFromResv(
        task->reservation, task->TaskId(),
        {task->EndTime(), task->AllocatedRes()});
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

void TaskScheduler::StepExecThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("StepExecThr");

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
    CRANE_ERROR("Failed to start the idle event in StepExec loop.");
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
      m_running_task_map_mtx_.Lock();

      schedule_begin = std::chrono::steady_clock::now();
      num_tasks_single_schedule = std::min((size_t)g_config.ScheduledBatchSize,
                                           m_pending_task_map_.size());

      begin = std::chrono::steady_clock::now();

      std::list<INodeSelectionAlgo::NodeSelectionResult> selection_result_list;
      m_node_selection_algo_->NodeSelect(
          m_running_task_map_, &m_pending_task_map_, &selection_result_list);

      // Update cached pending map size
      m_pending_map_cached_size_.store(m_pending_task_map_.size(),
                                       std::memory_order::release);

      m_running_task_map_mtx_.Unlock();
      m_pending_task_map_mtx_.Unlock();

      num_tasks_single_execution = selection_result_list.size();

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "NodeSelect costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();
      for (auto& it : selection_result_list) {
        auto& task = it.first;
        PartitionId const& partition_id = task->partition_id;

        task->SetStatus(crane::grpc::TaskStatus::Configuring);
        task->SetCranedIds(std::move(it.second));
        task->nodes_alloc = task->CranedIds().size();

        // CRANE_DEBUG(
        // "Task #{} is allocated to partition {} and craned nodes: {}",
        // task->TaskId(), partition_id, fmt::join(task->CranedIds(), ", "));

        task->allocated_craneds_regex =
            util::HostNameListToStr(task->CranedIds());

        if (task->ShouldLaunchOnAllNodes()) {
          for (auto const& craned_id : task->CranedIds())
            task->executing_craned_ids.emplace_back(craned_id);
        } else
          task->executing_craned_ids.emplace_back(task->CranedIds().front());
      }

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Set task fields costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      m_task_indexes_mtx_.Lock();
      for (auto& it : selection_result_list) {
        auto& task = it.first;
        for (CranedId const& craned_id : task->CranedIds())
          m_node_to_tasks_map_[craned_id].emplace(task->TaskId());
      }
      m_task_indexes_mtx_.Unlock();

      // RPC is time-consuming. Clustering rpc to one craned for performance.
      HashMap<CranedId, std::vector<crane::grpc::JobToD>>
          craned_daemon_step_map;

      // Job primary steps
      std::unordered_map<CranedId, std::vector<crane::grpc::StepToD>>
          craned_alloc_steps;
      std::vector<StepInCtld*> step_in_ctld_vec;

      for (auto& it : selection_result_list) {
        auto& task = it.first;
        std::unique_ptr daemon_step = std::make_unique<DaemonStepInCtld>();
        daemon_step->InitFromJob(*task);
        step_in_ctld_vec.push_back(daemon_step.get());
        task->SetDaemonStep(std::move(daemon_step));
      }
      if (!g_embedded_db_client->AppendSteps(step_in_ctld_vec)) {
        // TODO: Handle this failure
        CRANE_ERROR("Failed to append steps to embedded database.");
      }
      for (auto& it : selection_result_list) {
        auto& task = it.first;
        auto* daemon_step = task->DaemonStep();
        for (CranedId const& craned_id : task->CranedIds()) {
          craned_daemon_step_map[craned_id].push_back(
              daemon_step->GetJobToD(craned_id));
        }
        for (const auto& craned_id : daemon_step->CranedIds())
          craned_alloc_steps[craned_id].emplace_back(
              daemon_step->GetStepToD(craned_id));
      }

      Mutex thread_pool_mtx;
      HashSet<CranedId> failed_craned_set;
      HashSet<task_id_t> failed_task_id_set;

      absl::BlockingCounter bl(craned_daemon_step_map.size());
      for (auto&& iter : craned_daemon_step_map) {
        CranedId const& craned_id = iter.first;
        std::vector<crane::grpc::JobToD>& jobs = iter.second;

        g_thread_pool->detach_task([&]() {
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
            failed_craned_set.emplace(craned_id);
            for (const auto& job_to_d : jobs)
              failed_task_id_set.emplace(job_to_d.job_id());
            bl.DecrementCount();
            return;
          }

          auto err = stub->AllocJobs(jobs);
          if (err == CraneErrCode::SUCCESS) {
            bl.DecrementCount();
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

          failed_craned_set.emplace(craned_id);
          for (const auto& job_to_d : jobs)
            failed_task_id_set.emplace(job_to_d.job_id());

          thread_pool_mtx.Unlock();

          // If tasks in task_uid_pairs failed to start,
          // they will be moved to the completed tasks and do the following
          // steps:
          // 1. call g_meta_container->FreeResources() for the failed tasks.
          // 2. Release all cgroups related to these failed tasks.
          // 3. Move these tasks to the completed queue.
          CRANE_ERROR("Craned #{} failed when AllocJobs.", craned_id);

          bl.DecrementCount();
        });
      }
      bl.Wait();

      std::latch alloc_step_latch(craned_alloc_steps.size());
      for (const auto& craned_id : craned_alloc_steps | std::views::keys) {
        g_thread_pool->detach_task([&]() {
          auto& steps = craned_alloc_steps[craned_id];
          auto stub = g_craned_keeper->GetCranedStub(craned_id);
          CRANE_TRACE(
              "Send AllocSteps for {} tasks to {}",
              absl::StrJoin(
                  steps | std::views::transform([](const auto& step_to_d) {
                    return util::StepIdsToString(step_to_d.job_id(),
                                                 step_to_d.step_id());
                  }),
                  ","),
              craned_id);
          if (stub == nullptr || stub->Invalid()) {
            alloc_step_latch.count_down();
            return;
          }

          auto err = stub->AllocSteps(steps);
          if (err == CraneErrCode::SUCCESS) {
            alloc_step_latch.count_down();
            return;
          }

          thread_pool_mtx.Lock();

          failed_craned_set.emplace(craned_id);
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

      std::list<INodeSelectionAlgo::NodeSelectionResult> failed_result_list;
      for (auto it = selection_result_list.begin();
           it != selection_result_list.end();) {
        auto& task = it->first;
        if (failed_task_id_set.contains(task->TaskId())) {
          failed_result_list.emplace_back(std::move(*it));
          it = selection_result_list.erase(it);
        } else
          it = std::next(it);
      }

      // Now we have the ownerships of succeeded tasks in
      // `selection_result_list` and the ownerships of failed tasks in
      // `failed_result_list`.

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "CreateCgroupForTasks costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      // Move tasks into running queue.
      txn_id_t txn_id{0};
      bool ok = g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
      if (!ok) {
        CRANE_ERROR(
            "TaskScheduler failed to start transaction when scheduling.");
      }

      for (auto& it : selection_result_list) {
        auto& task = it.first;

        // IMPORTANT: task must be put into running_task_map before any
        //  time-consuming operation, otherwise TaskStatusChange RPC will come
        //  earlier before task is put into running_task_map.
        g_embedded_db_client->UpdateRuntimeAttrOfTask(txn_id, task->TaskDbId(),
                                                      task->RuntimeAttr());
      }

      ok = g_embedded_db_client->CommitVariableDbTransaction(txn_id);
      if (!ok) {
        CRANE_ERROR("Embedded database failed to commit manual transaction.");
      }

      // TODO: Persistent daemon step.

      // Set succeed tasks status and do callbacks.
      for (auto& it : selection_result_list) {
        auto& task = it.first;
        if (task->type == crane::grpc::Interactive) {
          const auto& meta = std::get<InteractiveMetaInTask>(task->meta);
          std::get<InteractiveMetaInTask>(task->meta)
              .cb_task_res_allocated(task->TaskId(),
                                     task->allocated_craneds_regex,
                                     task->CranedIds());
        }

        // The ownership of TaskInCtld is transferred to the running queue.
        m_running_task_map_mtx_.Lock();
        m_running_task_map_.emplace(task->TaskId(), std::move(task));
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
      for (auto const& craned_id : craned_daemon_step_map | std::views::keys) {
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

      if (!failed_result_list.empty()) {
        // Then handle failed tasks in failed_result_list if there's any.
        begin = std::chrono::steady_clock::now();

        for (auto& it : failed_result_list) {
          auto& task = it.first;
          for (CranedId const& craned_id : task->CranedIds())
            g_meta_container->FreeResourceFromNode(craned_id, task->TaskId());
          if (task->reservation != "")
            g_meta_container->FreeResourceFromResv(task->reservation,
                                                   task->TaskId());
          g_account_meta_container->FreeQosResource(*task);
        }

        // Move failed tasks to the completed queue.
        std::vector<TaskInCtld*> failed_task_raw_ptrs;
        for (auto& it : failed_result_list) {
          auto& task = it.first;
          failed_task_raw_ptrs.emplace_back(task.get());

          task->SetStatus(crane::grpc::Failed);
          task->SetExitCode(ExitCode::kExitCodeRpcError);
          task->SetEndTime(absl::Now());
        }
        ProcessFinalTasks_(failed_task_raw_ptrs);

        // Failed tasks have been handled properly. Free them explicitly.
        failed_result_list.clear();

        end = std::chrono::steady_clock::now();
        CRANE_TRACE(
            "Handling failed tasks costed {} ms",
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

void TaskScheduler::SetNodeSelectionAlgo(
    std::unique_ptr<INodeSelectionAlgo> algo) {
  m_node_selection_algo_ = std::move(algo);
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
        auto resv_end_time = g_meta_container->GetResvMetaPtr(task->reservation)
                                 ->logical_part.end_time;
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
          auto resv_end_time = reservation_meta->logical_part.end_time;
          if (resv_end_time <= task->StartTime() + absl::Seconds(secs)) {
            CRANE_DEBUG("Task #{}'s time limit exceeds reservation end time",
                        task_id);
            return CraneErrCode::ERR_INVALID_PARAM;
          }
          reservation_meta->logical_part.rn_task_res_map[task_id].end_time =
              task->StartTime() + absl::Seconds(secs);
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

  PartitionId partition = request.partition();
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

  ResvId resv_name = request.reservation_name();
  auto resv_meta_map = g_meta_container->GetResvMetaMapPtr();
  if (resv_meta_map->contains(resv_name)) {
    return std::unexpected("Reservation name already exists");
  }

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

      for (const auto& resv :
           craned_meta->resv_in_node_map | std::views::values) {
        if (resv.start_time < end_time && resv.end_time > start_time) {
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
      return std::unexpected(failed_msg);
    }
  }

  ResvMeta resv{
      .name = resv_name,
      .part_id = partition,
      .logical_part =
          LogicalPartition{
              .start_time = start_time,
              .end_time = end_time,
              .res_total = allocated_res,
              .res_avail = allocated_res,
              .res_in_use = ResourceV2(),
              .craned_ids = std::move(craned_ids),
          },
      .accounts_black_list = allowed_accounts.empty(),
      .users_black_list = allowed_users.empty(),
      .accounts = allowed_accounts.empty() ? std::move(denied_accounts)
                                           : std::move(allowed_accounts),
      .users = allowed_users.empty() ? std::move(denied_users)
                                     : std::move(allowed_users),
  };

  const auto& [it, ok] = resv_meta_map->emplace(resv_name, std::move(resv));
  if (!ok) {
    CRANE_ERROR("Failed to insert reservation meta for reservation {}",
                resv_name);
    return std::unexpected(fmt::format(
        "Failed to insert reservation meta for reservation {}", resv_name));
  }
  for (auto& craned_meta : craned_meta_vec) {
    const auto& [it, ok] = craned_meta->resv_in_node_map.emplace(
        resv_name,
        CranedMeta::ResvInNode{.start_time = start_time,
                               .end_time = end_time,
                               .res_total = craned_meta->static_meta.res});
    if (!ok) {
      CRANE_ERROR("Failed to insert reservation resource to {}",
                  craned_meta->static_meta.hostname);
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

crane::grpc::DeleteReservationReply TaskScheduler::DeleteResv(
    const crane::grpc::DeleteReservationRequest& request) {
  crane::grpc::DeleteReservationReply reply;

  ResvId resv_name = request.reservation_name();
  auto resv_meta_map = g_meta_container->GetResvMetaMapPtr();

  auto res = DeleteResvMeta_(resv_meta_map, resv_name);
  if (res.has_value()) {
    reply.set_ok(true);
  } else {
    reply.set_ok(false);
    reply.set_reason(res.error());
  }

  return reply;
}

std::expected<void, std::string> TaskScheduler::DeleteResvMeta_(
    CranedMetaContainer::ResvMetaMapPtr& resv_meta_map, const ResvId& resv_id) {
  CRANE_TRACE("Deleting reservation {}", resv_id);
  if (!resv_meta_map->contains(resv_id)) {
    return std::unexpected(fmt::format("Reservation {} not found", resv_id));
  }

  const auto& resv_meta = resv_meta_map->at(resv_id).GetExclusivePtr();

  if (!resv_meta->logical_part.rn_task_res_map.empty()) {
    return std::unexpected(fmt::format(
        "Not allowed to delete reservation {} with running tasks", resv_id));
  }

  for (const auto& craned_id : resv_meta->logical_part.craned_ids) {
    auto craned_meta_ptr = g_meta_container->GetCranedMetaPtr(craned_id);
    if (!craned_meta_ptr) {
      CRANE_ERROR("Node {} not found when deleting reservation {}", craned_id,
                  resv_id);
      continue;
    }

    auto& reservation_resource_map = craned_meta_ptr->resv_in_node_map;
    if (!reservation_resource_map.contains(resv_id)) {
      CRANE_ERROR(
          "Reservation not found on node {} when deleting reservation {}",
          craned_id, resv_id);
      continue;
    }
    reservation_resource_map.erase(resv_id);
  }

  resv_meta_map->erase(resv_id);

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
      auto reservation_meta_map = g_meta_container->GetResvMetaMapPtr();
      auto err = DeleteResvMeta_(reservation_meta_map, reservation_id);

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

  for (auto&& [craned_id, steps] : running_task_craned_id_map) {
    if (!g_meta_container->CheckCranedOnline(craned_id)) {
      for (auto [job_id, step_ids] : steps) {
        for (auto step_id : step_ids)
          StepStatusChangeAsync(job_id, step_id, craned_id,
                                crane::grpc::TaskStatus::Cancelled,
                                ExitCode::kExitCodeTerminated, "");
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

  std::vector<TaskInCtld*> pd_task_raw_ptrs;
  for (auto& task : pending_task_ptr_vec)
    pd_task_raw_ptrs.emplace_back(task.get());
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

void TaskScheduler::StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                                          const CranedId& craned_index,
                                          crane::grpc::TaskStatus new_status,
                                          uint32_t exit_code,
                                          std::string reason) {
  m_task_status_change_queue_.enqueue({.job_id = job_id,
                                       .step_id = step_id,
                                       .exit_code = exit_code,
                                       .new_status = new_status,
                                       .craned_index = craned_index,
                                       .reason = reason});
  m_task_status_change_async_handle_->send();
}

void TaskScheduler::TaskStatusChangeTimerCb_() {
  m_clean_task_status_change_handle_->send();
}

void TaskScheduler::TaskStatusChangeAsyncCb_() {
  if (m_task_status_change_queue_.size_approx() >= kTaskStatusChangeBatchNum)
    m_clean_task_status_change_handle_->send();
}

std::optional<crane::grpc::TaskStatus>
TaskScheduler::DaemonStepStatusChangeHandler_(
    crane::grpc::TaskStatus new_status, TaskInCtld* job,
    const CranedId& craned_id, StepStatusChangeContext* context) {
  using util::StepStatusToString;
  bool all_node_ready{false};
  bool job_finished{false};
  auto* step = job->DaemonStep();
  CRANE_TRACE("[Step #{}.{}] current status {}, got new status {} from {}",
              step->job_id, step->StepId(), StepStatusToString(step->Status()),
              StepStatusToString(new_status), craned_id);
  switch (step->Status()) {
  case crane::grpc::TaskStatus::Configuring:
    // Configuring -> Failed / Running
    step->NodeConfigured(craned_id);
    if (new_status != crane::grpc::TaskStatus::Running) {
      step->SetConfigureFailedStatus(new_status);
    }
    if (step->AllNodesConfigured()) {
      if (step->ConfigureFailed())
        job_finished = true;
      else {
        CRANE_TRACE("[Step #{}.{}] CONFIGURING->Running", step->job_id,
                    step->StepId());
        step->SetStatus(crane::grpc::TaskStatus::Running);

        context->rn_step_raw_ptrs.push_back(step);
        std::unique_ptr primary_step = std::make_unique<CommonStepInCtld>();
        primary_step->InitPrimaryStepFromJob(*job);
        // TODO: Aggregate primary step creation
        g_embedded_db_client->AppendSteps({primary_step.get()});
        job->SetPrimaryStep(std::move(primary_step));
        for (auto& node_id : job->PrimaryStep()->ExecutionNodes()) {
          context->craned_step_alloc_map[node_id].emplace_back(
              job->PrimaryStep()->GetStepToD(node_id));
        }
      }
    }
    break;
  case crane::grpc::TaskStatus::Running:
    // Running -> Completed / Failed

    step->NodeFinish(craned_id);
    if (new_status != crane::grpc::TaskStatus::Completed) {
      step->SetFinishFailedStatus(new_status);
    }
    job_finished = step->AllNodesFinished();
    break;

  default: {
    CRANE_ASSERT_MSG(
        false, fmt::format("Invalid step status, current: {}, new status: {}",
                           StepStatusToString(step->Status()),
                           StepStatusToString(new_status)));
    std::unreachable();
  }
  }

  if (job_finished) {
    if (step->ConfigureFailed()) {
      step->SetStatus(step->ConfigureFailedStatus());
      CRANE_INFO("[Step #{}.{}] ConfigureFailed with status {}.", step->job_id,
                 step->StepId(), StepStatusToString(step->Status()));
      // Job finish with failed status
      job->SetPrimaryStepStatus(step->Status());
      // Daemon step failed to configure, terminate all daemon step,
      for (const auto& node : step->CranedIds()) {
        context->craned_orphaned_steps[node][step->job_id].emplace(
            step->StepId());
      }
    } else {
      if (step->FinishWithFailedStatus()) {
        step->SetStatus(step->FinishFailedStatus());
      } else {
        step->SetStatus(crane::grpc::TaskStatus::Completed);
      }
      CRANE_INFO("[Step #{}.{}] FINISHED with status {}.", step->job_id,
                 step->StepId(), StepStatusToString(step->Status()));
    }
    context->step_raw_ptrs.push_back(step);
    context->step_ptrs.emplace_back(job->ReleaseDaemonStep());
    return job->PrimaryStepStatus();
  }
  return std::nullopt;
}

void TaskScheduler::CommonStepStatusChangeHandler_(
    crane::grpc::TaskStatus new_status, TaskInCtld* job, step_id_t step_id,
    const CranedId& craned_id, StepStatusChangeContext* context) {
  using util::StepStatusToString;
  // PrimaryStep
  bool primary_step =
      job->PrimaryStep() && step_id == job->PrimaryStep()->StepId();
  CommonStepInCtld* step{nullptr};
  if (primary_step) {
    step = job->PrimaryStep();
  } else {
    step = job->GetStep(step_id);
  }

  /**
   * Step final status
   * finished: step configured successfully, got all step execution status
   * change
   * configure_failed: step failed to configure, terminate the step
   * For both status, job finish if primary step.
   */

  bool step_finished{false};
  // Step failed to configure, terminate step
  bool step_configure_failed{false};

  CRANE_TRACE("[Step #{}.{}] current status {}, got new status {} from {}",
              step->job_id, step->StepId(), StepStatusToString(step->Status()),
              StepStatusToString(new_status), craned_id);
  if (step->Status() == crane::grpc::TaskStatus::Configuring) {
    // Configuring -> Running / Failed / Cancelled,
    step->NodeConfigured(craned_id);
    if (new_status != crane::grpc::TaskStatus::Running) {
      step->SetConfigureFailedStatus(new_status);
    }
    if (step->AllNodesConfigured()) {
      if (step->ConfigureFailed()) {
        // Configuring -> Failed
        step_configure_failed = true;
      } else {
        // Configuring -> Running
        // All supervisor ready without failure, start execution.
        CRANE_INFO("[Step #{}.{}] is ready to run.", step->job_id,
                   step->StepId());
        step->SetStatus(crane::grpc::TaskStatus::Running);

        // Primary:Update job status when primary step is Running.
        if (primary_step) {
          job->SetStatus(crane::grpc::TaskStatus::Running);
          context->rn_job_raw_ptrs.push_back(job);
        }

        // Launch step execution
        for (auto& node : step->ExecutionNodes()) {
          context->craned_step_exec_map[node][step->job_id].insert(
              step->StepId());
        }
        context->rn_step_raw_ptrs.push_back(step);
      }
    }
  } else if (step->Status() == crane::grpc::TaskStatus::Running) {
    // Running -> Completed / Failed / Cancelled,
    // Primary: the job is completed.

    step->NodeFinish(craned_id);
    if (new_status != crane::grpc::TaskStatus::Completed) {
      step->SetFinishFailedStatus(new_status);
    }
    step_finished = step->AllNodesFinished();

  } else {
    CRANE_ASSERT_MSG(
        false, fmt::format("Invalid step status, current: {}, new status: {}",
                           StepStatusToString(step->Status()),
                           StepStatusToString(new_status)));
    std::unreachable();
  }

  // Step finish: configure failed or execution status change
  if (step_finished || step_configure_failed) {
    if (step->ConfigureFailed()) {
      CRANE_INFO("[Step #{}.{}] CONFIGURE_FAILED.", step->job_id,
                 step->StepId());
      // CONFIGURE_FAILED
      step->SetStatus(step->ConfigureFailedStatus());
      // Step failed to configure, terminate this step
      for (const auto& node : step->ExecutionNodes()) {
        if (node != craned_id)
          context->craned_orphaned_steps[node][job->TaskId()].emplace(step_id);
      }

      if (primary_step) {
        // Primary step CONFIGURE_FAILED, free daemon step, will send status
        // change.
        for (const auto& node : job->DaemonStep()->CranedIds()) {
          context->craned_jobs_to_free[node].emplace_back(job->TaskId());
        }
      }
    } else {
      // Step COMPLETED
      if (primary_step) {
        // Primary step finish, free daemon step, will send status change.
        for (const auto& node : job->DaemonStep()->CranedIds()) {
          context->craned_jobs_to_free[node].emplace_back(job->TaskId());
        }

        // Cancel all other step with CANCELED status
        for (const auto& comm_step : job->Steps() | std::views::values) {
          for (const auto& node : comm_step->ExecutionNodes()) {
            context->craned_cancel_steps[node][comm_step->job_id].emplace(
                comm_step->StepId());
          }
        }
      }
      if (step->ConfigureFailed()) {
        CRANE_INFO("[Step #{}.{}] CONFIGURE_FAILED.", step->job_id,
                   step->StepId());
        step->SetStatus(step->ConfigureFailedStatus());
      }
      if (step->FinishWithFailedStatus()) {
        step->SetStatus(step->FinishFailedStatus());
      } else {
        step->SetStatus(crane::grpc::TaskStatus::Completed);
      }
      CRANE_INFO("[Step #{}.{}] FINISHED with status {}.", step->job_id,
                 step->StepId(), util::StepStatusToString(step->Status()));
    }

    context->step_raw_ptrs.push_back(step);
    if (primary_step) {
      job->SetPrimaryStepStatus(step->Status());
      context->rn_job_raw_ptrs.push_back(job);
      context->step_ptrs.emplace_back(job->ReleasePrimaryStep());
    } else {
      context->step_ptrs.emplace_back(job->EraseStep(step_id));
    }
  }

  return;
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
                    reason] : args) {
    auto iter = m_running_task_map_.find(task_id);
    if (iter == m_running_task_map_.end()) {
      CRANE_WARN(
          "[Job #{}] Ignoring unknown job in CleanTaskStatusChangeQueueCb_.",
          task_id);
      continue;
    }

    // Free job allocation
    std::optional<crane::grpc::TaskStatus> job_finished_status{std::nullopt};

    std::unique_ptr<TaskInCtld>& task = iter->second;
    if (step_id == kDaemonStepId) {
      // job finish if all daemon step sent complete status or some daemon step
      // failed to configure.
      job_finished_status = DaemonStepStatusChangeHandler_(
          new_status, task.get(), craned_index, &context);
    } else {
      CommonStepStatusChangeHandler_(new_status, task.get(), step_id,
                                     craned_index, &context);
    }

    if (job_finished_status.has_value()) {
      // Job status set CommonStepStatusChangeHandler_
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

      task->SetStatus(job_finished_status.value());
      task->SetExitCode(exit_code);
      task->SetEndTime(absl::Now());

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

      context.job_raw_ptrs.emplace_back(task.get());
      context.job_ptrs.emplace_back(std::move(task));

      // As for now, task status change includes only
      // Pending / Running -> Completed / Failed / Cancelled.
      // It means all task status changes will put the task into mongodb,
      // so we don't have any branch code here and just put it into mongodb.

      CRANE_TRACE("[Job #{}] Completed with status {}.", task_id,
                  util::StepStatusToString(job_finished_status.value()));
      m_running_task_map_.erase(iter);
    }
  }
  // Maybe put following operations into thread pool.

  std::latch alloc_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_step_alloc_map.size())};

  for (const auto& craned_id : context.craned_step_alloc_map | std::views::keys)
    g_thread_pool->detach_task([this, &alloc_step_latch, &craned_id, &context] {
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
        for (const auto& [job_id, step_ids] :
             context.craned_step_exec_map.at(craned_id)) {
          for (const auto& step_id : step_ids)
            StepStatusChangeWithReasonAsync(
                job_id, step_id, craned_id, crane::grpc::TaskStatus::Failed,
                ExitCode::kExitCodeCranedDown, "CranedDown");
        }
      }
      alloc_step_latch.count_down();
    });

  std::latch exec_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_step_exec_map.size())};
  for (const auto& craned_id :
       context.craned_step_exec_map | std::views::keys) {
    g_thread_pool->detach_task([this, &exec_step_latch, &craned_id,
                                &context]() {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);

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
                  ExitCode::kExitCodeRpcError, "ExecRpcError");
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
                ExitCode::kExitCodeCranedDown, "CranedDown");
        }
      }
      exec_step_latch.count_down();
    });
  }

  std::latch orphaned_step_latch{
      static_cast<std::ptrdiff_t>(context.craned_orphaned_steps.size())};
  for (const auto& craned_id :
       context.craned_orphaned_steps | std::views::keys) {
    g_thread_pool->detach_task([&orphaned_step_latch, &craned_id, &context]() {
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
    g_thread_pool->detach_task([&cancel_step_latch, &craned_id, &context]() {
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
    g_thread_pool->detach_task([&free_job_latch, craned_id, &context]() {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);

      // If the craned is down, just ignore it.
      if (stub && !stub->Invalid()) {
        const auto& jobs = context.craned_jobs_to_free.at(craned_id);
        auto err = stub->FreeJobs(jobs);
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("Failed to FreeJobs for [{}] on Node {}",
                      absl::StrJoin(jobs, ","), craned_id);
        }
      }
      free_job_latch.count_down();
    });
  }

  alloc_step_latch.wait();
  exec_step_latch.wait();
  orphaned_step_latch.wait();
  cancel_step_latch.wait();
  free_job_latch.wait();

  txn_id_t txn_id;
  g_embedded_db_client->BeginVariableDbTransaction(&txn_id);

  // Jobs will update in embedded db
  for (auto* job : context.rn_job_raw_ptrs) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfTask(txn_id, job->TaskDbId(),
                                                       job->RuntimeAttr()))
      CRANE_ERROR("[Job #{}]Failed to call UpdateRuntimeAttrOfTask()",
                  job->TaskId());
  }

  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  g_embedded_db_client->BeginStepVarDbTransaction(&txn_id);
  // Steps will update in embedded db
  for (auto* step : context.rn_step_raw_ptrs) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfStep(txn_id, step->StepDbId(),
                                                       step->RuntimeAttr()))
      CRANE_ERROR("[Job #{}.{}]Failed to call UpdateRuntimeAttrOfStep()",
                  step->job_id, step->StepId());
  }
  g_embedded_db_client->CommitStepVarDbTransaction(txn_id);

  ProcessFinalSteps_(context.step_raw_ptrs);
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

    *job_steps[job_id].mutable_job() =
        job_it->second->DaemonStep()->GetJobToD(craned_id);
    auto& steps = *job_steps[job_id].mutable_steps();
    steps[job_it->second->DaemonStep()->StepId()] =
        job_it->second->DaemonStep()->GetStepToD(craned_id);
    if (job_it->second->PrimaryStep() &&
        std::ranges::contains(job_it->second->PrimaryStep()->ExecutionNodes(),
                              craned_id)) {
      const auto* step = job_it->second->PrimaryStep();
      steps[step->StepId()] = step->GetStepToD(craned_id);
    }
    for (const auto& step : job_it->second->Steps() | std::views::values)
      if (std::ranges::contains(step->ExecutionNodes(), craned_id))
        steps[step->StepId()] = step->GetStepToD(craned_id);
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
        if (step_id == kDaemonStepId) {
          step = job->DaemonStep();
        } else if (step_id == job->PrimaryStep()->StepId()) {
          step = job->PrimaryStep();
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

  for (const auto& [craned_id, craned_steps] : craned_steps_map) {
    for (const auto& [job_id, step_ids] : craned_steps) {
      for (const auto& step_id : step_ids) {
        StepStatusChangeAsync(job_id, step_id, craned_id,
                              crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeCranedDown, "Craned down");
      }
    }
  }
  for (const auto& [craned_id, craned_steps] : craned_steps_map) {}
}

void MinLoadFirst::NodeSelectionInfo::InitCostAndTimeAvailResMap(
    const CranedId& craned_id, const ResourceInNode& res_total,
    const ResourceInNode& res_avail, const absl::Time& now,
    const std::vector<std::pair<absl::Time, const ResourceInNode*>>&
        running_tasks,
    const absl::flat_hash_map<ResvId, CranedMeta::ResvInNode>* resv_map) {
  struct ResChange {
    absl::Time time;
    const ResourceInNode* res;
    bool is_alloc;
  };
  std::vector<ResChange> changes;

  uint64_t& cost = m_node_cost_map_[craned_id];
  for (const auto& [end_time, res] : running_tasks) {
    UpdateCostFunc(cost, now, end_time, *res, res_total);
    changes.emplace_back(ResChange{end_time, res, false});
  }

  if (resv_map != nullptr && !resv_map->empty()) {
    absl::Time first_resv_time = absl::InfiniteFuture();
    for (const auto& [resv_id, resv] : *resv_map) {
      const absl::Time& end_time = resv.end_time;
      if (end_time < now) {  // Already expired
        continue;
      }
      absl::Time start_time = std::max(now, resv.start_time);
      UpdateCostFunc(cost, start_time, end_time, resv.res_total, res_total);
      changes.emplace_back(ResChange{start_time, &resv.res_total, true});
      changes.emplace_back(ResChange{end_time, &resv.res_total, false});
      if (start_time < first_resv_time) {
        first_resv_time = start_time;
      }
    }
    m_first_resv_time_map_[craned_id] = first_resv_time;
  }

  m_cost_node_id_set_.emplace(cost, craned_id);
  m_node_res_total_map_[craned_id] = res_total;

  std::sort(changes.begin(), changes.end(),
            [](const ResChange& lhs, const ResChange& rhs) {
              return lhs.time == rhs.time
                         ? lhs.is_alloc <
                               rhs.is_alloc  // release before  allocation
                         : lhs.time < rhs.time;
            });
  TimeAvailResMap& time_avail_res_map = m_node_time_avail_res_map_[craned_id];
  {
    auto [cur_iter, ok] = time_avail_res_map.emplace(now, res_avail);

    for (const auto& change : changes) {
      if (change.time != cur_iter->first) {
        std::tie(cur_iter, ok) =
            time_avail_res_map.emplace(change.time, cur_iter->second);
        if constexpr (kAlgoTraceOutput) {
          CRANE_TRACE(
              "Insert duration [now+{}s, inf) with resource: "
              "cpu: {}, mem: {}, gres: {}",
              absl::ToInt64Seconds(cur_iter->first - now),
              cur_iter->second.allocatable_res.cpu_count,
              cur_iter->second.allocatable_res.memory_bytes,
              util::ReadableDresInNode(cur_iter->second));
        }
      }
      if (change.is_alloc) {
        cur_iter->second -= *(change.res);
      } else {
        cur_iter->second += *(change.res);
      }

      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Craned {} res_avail at now + {}s: cpu: {}, mem: {}, gres: "
            "{}; ",
            craned_id, absl::ToInt64Seconds(cur_iter->first - now),
            cur_iter->second.allocatable_res.cpu_count,
            cur_iter->second.allocatable_res.memory_bytes,
            util::ReadableDresInNode(cur_iter->second));
      }
    }
  }

  if constexpr (kAlgoTraceOutput) {
    std::string str;
    str.append(fmt::format("Node {}: ", craned_id));
    auto prev_iter = time_avail_res_map.begin();
    auto iter = std::next(prev_iter);
    for (; iter != time_avail_res_map.end(); prev_iter++, iter++) {
      str.append(
          fmt::format("[ now+{}s , now+{}s ) Available allocatable "
                      "res: cpu core {}, mem {}, gres {}",
                      absl::ToInt64Seconds(prev_iter->first - now),
                      absl::ToInt64Seconds(iter->first - now),
                      prev_iter->second.allocatable_res.cpu_count,
                      prev_iter->second.allocatable_res.memory_bytes,
                      util::ReadableDresInNode(prev_iter->second)));
    }
    str.append(
        fmt::format("[ now+{}s , inf ) Available allocatable "
                    "res: cpu core {}, mem {}, gres {}",
                    absl::ToInt64Seconds(prev_iter->first - now),
                    prev_iter->second.allocatable_res.cpu_count,
                    prev_iter->second.allocatable_res.memory_bytes,
                    util::ReadableDresInNode(prev_iter->second)));
    CRANE_TRACE("{}", str);
  }
}

void MinLoadFirst::CalculateNodeSelectionInfoOfPartition_(
    const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::Time now, const PartitionId& partition_id,
    const std::unordered_set<CranedId>& craned_ids,
    const CranedMetaContainer::CranedMetaRawMap& craned_meta_map,
    NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_selection_info_ref = *node_selection_info;

  for (const auto& craned_id : craned_ids) {
    auto& craned_meta_ptr = craned_meta_map.at(craned_id);
    auto craned_meta = craned_meta_ptr.GetExclusivePtr();

    // An offline craned shouldn't be scheduled.
    if (!craned_meta->alive || craned_meta->drain) continue;

    std::vector<std::pair<absl::Time, const ResourceInNode*>>
        end_time_task_res_vec;

    for (const auto& [task_id, res] : craned_meta->rn_task_res_map) {
      const auto& task = running_tasks.at(task_id);
      absl::Time end_time = std::max(task->StartTime() + task->time_limit,
                                     now + absl::Seconds(1));
      if (task->reservation == "") {  // task using reserved resource is not
                                      // considered here.
        // For some completing tasks,
        // task->StartTime() + task->time_limit <= absl::Now().
        // In this case,
        // max(task->StartTime() + task->time_limit, now + absl::Seconds(1))
        // should be taken for end time,
        // otherwise, tasks might be scheduled and executed even when
        // res_avail = 0 and will cause a severe error where res_avail < 0.
        end_time_task_res_vec.emplace_back(end_time, &res);
      }
    }

    if constexpr (kAlgoTraceOutput) {
      std::vector<std::pair<absl::Time, task_id_t>> end_time_task_id_vec;
      for (const auto& [task_id, res] : craned_meta->rn_task_res_map) {
        const auto& task = running_tasks.at(task_id);
        absl::Time end_time = std::max(task->StartTime() + task->time_limit,
                                       now + absl::Seconds(1));
        if (task->reservation == "") {
          end_time_task_id_vec.emplace_back(end_time, task_id);
        }
      }

      std::string running_task_ids_str;
      for (const auto& [end_time, task_id] : end_time_task_id_vec)
        running_task_ids_str.append(fmt::format("{}, ", task_id));
      CRANE_TRACE("Craned node {} has running non-reservation tasks: {}",
                  craned_id, running_task_ids_str);

      std::sort(end_time_task_id_vec.begin(), end_time_task_id_vec.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.first < rhs.first;
                });
      if (!end_time_task_id_vec.empty()) {
        std::string str;
        str.append(
            fmt::format("Partition {}, Craned {}: ", partition_id, craned_id));
        for (auto [end_time, task_id] : end_time_task_id_vec) {
          str.append(fmt::format("Task #{} ends after {}s, ", task_id,
                                 absl::ToInt64Seconds(end_time - now)));
        }
        CRANE_TRACE("{}", str);
      }
    }

    node_selection_info_ref.InitCostAndTimeAvailResMap(
        craned_id, craned_meta->res_total, craned_meta->res_avail, now,
        end_time_task_res_vec, &craned_meta->resv_in_node_map);
  }
}

void MinLoadFirst::CalculateNodeSelectionInfoOfReservation_(
    const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::Time now, const ResvMeta* resv_meta,
    const CranedMetaContainer::CranedMetaRawMap& craned_meta_map,
    NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_selection_info_ref = *node_selection_info;

  // Sort all running task in this node by ending time.
  std::unordered_map<CranedId,
                     std::vector<std::pair<absl::Time, const ResourceInNode*>>>
      node_time_res_vec_map;

  for (const auto& craned_id : resv_meta->logical_part.craned_ids) {
    auto craned_meta_ptr = craned_meta_map.at(craned_id).GetExclusivePtr();
    if (!craned_meta_ptr->alive || craned_meta_ptr->drain) continue;

    node_time_res_vec_map[craned_id];
  }

  for (const auto& res :
       resv_meta->logical_part.rn_task_res_map | std::views::values) {
    const auto& [expected_end_time, alloc_res] = res;
    absl::Time end_time = std::max(expected_end_time, now + absl::Seconds(1));
    const auto& each_node_res_map = alloc_res.EachNodeResMap();
    for (const auto& [craned_id, res_in_node] : each_node_res_map) {
      auto iter = node_time_res_vec_map.find(craned_id);
      if (iter != node_time_res_vec_map.end()) {
        iter->second.emplace_back(end_time, &res_in_node);
      }
    }
  }

  // TODO: Move out to reduce the scope of the lock of crane_meta_map
  for (auto& [craned_id, time_res_vec] : node_time_res_vec_map) {
    auto res_avail_iter =
        resv_meta->logical_part.res_avail.EachNodeResMap().find(craned_id);
    node_selection_info_ref.InitCostAndTimeAvailResMap(
        craned_id, resv_meta->logical_part.res_total.at(craned_id),
        res_avail_iter !=
                resv_meta->logical_part.res_avail.EachNodeResMap().end()
            ? res_avail_iter->second
            : ResourceInNode(),
        now, node_time_res_vec_map[craned_id], nullptr);

    auto& time_avail_res_map =
        node_selection_info_ref.GetTimeAvailResMap(craned_id);

    time_avail_res_map[resv_meta->logical_part.end_time].SetToZero();
  }
}

bool MinLoadFirst::CalculateRunningNodesAndStartTime_(
    const NodeSelectionInfo& node_selection_info,
    const util::Synchronized<PartitionMeta>& partition_meta_ptr,
    const CranedMetaContainer::CranedMetaRawMap& craned_meta_map,
    TaskInCtld* task, absl::Time now, std::list<CranedId>* craned_ids,
    absl::Time* start_time) {
  uint32_t node_num_limit;
  if constexpr (kAlgoRedundantNode) {
    node_num_limit = std::min(task->node_num + 10, task->node_num * 2);
  } else {
    node_num_limit = task->node_num;
  }

  std::vector<CranedId> craned_indexes_;
  std::vector<CranedId> ready_craned_indexes_;

  ResourceV2 allocated_res;
  task->allocated_res_view.SetToZero();

  absl::Time earliest_end_time = now + task->time_limit;
  ResourceView requested_node_res_view;

  for (const auto& craned_index :
       node_selection_info.GetCostNodeIdSet() | std::views::values) {
    if (!partition_meta_ptr.GetExclusivePtr()->craned_ids.contains(
            craned_index)) {
      // TODO: Performance issue! We can use cached available node set
      //  for the task when checking task validity in TaskScheduler.
      continue;
    }
    auto& time_avail_res_map =
        node_selection_info.GetTimeAvailResMap(craned_index);
    // Number of tasks is not less than map size.
    // When condition is true, the craned has too many tasks.
    if (time_avail_res_map.size() >= kAlgoMaxTaskNumPerNode) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE("Craned {} has too many tasks. Skipping this craned.",
                    craned_index);
      }
      continue;
    }

    if (!task->included_nodes.empty() &&
        !task->included_nodes.contains(craned_index)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Craned {} is not in the nodelist of task #{}. "
            "Skipping this craned.",
            craned_index, task->TaskId());
      }
      continue;
    }

    if (!task->excluded_nodes.empty() &&
        task->excluded_nodes.contains(craned_index)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE("Task #{} excludes craned {}. Skipping this craned.",
                    task->TaskId(), craned_index);
      }
      continue;
    }

    auto craned_meta = craned_meta_map.at(craned_index).GetExclusivePtr();
    if (task->reservation != "") {
      auto iter = craned_meta->resv_in_node_map.find(task->reservation);
      if (iter == craned_meta->resv_in_node_map.end() ||
          !(task->requested_node_res_view <= iter->second.res_total) ||
          (task->TaskToCtld().exclusive() &&
           !(craned_meta->res_total <= iter->second.res_total)) ||
          now + task->time_limit > iter->second.end_time) {
        continue;
      }
    } else {
      if (!(task->requested_node_res_view <= craned_meta->res_total)) {
        if constexpr (kAlgoTraceOutput) {
          CRANE_TRACE(
              "Task #{} needs more resource than that of craned {}. "
              "Skipping this craned.",
              task->TaskId(), craned_index);
        }
        continue;
      }
    }

    if (task->TaskToCtld().exclusive()) {
      requested_node_res_view.SetToZero();
      requested_node_res_view += craned_meta->res_total;
    } else {
      requested_node_res_view = task->requested_node_res_view;
    }

    if constexpr (kAlgoRedundantNode) {
      craned_indexes_.emplace_back(craned_index);
      if (craned_indexes_.size() >= node_num_limit) break;
    } else {
      // Given N as the required number of nodes,
      // all the nodes that is able to run the task at some time-point will be
      // iterated and the first N nodes will be in the craned_indexes_.
      if (craned_indexes_.size() < node_num_limit)
        craned_indexes_.emplace_back(craned_index);

      // Find all possible nodes that can run the task now.
      // TODO: Performance issue! Consider speeding up with multiple threads.
      ResourceInNode feasible_res;
      bool ok = requested_node_res_view.GetFeasibleResourceInNode(
          craned_meta->res_avail, &feasible_res);
      if (ok) {
        bool is_node_satisfied_now = true;
        for (const auto& [time, res] : time_avail_res_map) {
          if (time >= earliest_end_time) break;
          if (!(feasible_res <= res)) is_node_satisfied_now = false;
        }

        if (is_node_satisfied_now) {
          ready_craned_indexes_.emplace_back(craned_index);
          allocated_res.AddResourceInNode(craned_index, feasible_res);
          task->allocated_res_view += feasible_res;
          if (ready_craned_indexes_.size() >= task->node_num) {
            task->SetAllocatedRes(std::move(allocated_res));
            *start_time = now;
            for (const CranedId& ready_craned_index : ready_craned_indexes_) {
              craned_ids->emplace_back(ready_craned_index);
            }
            return true;
          }
        }
      }
    }
  }

  if (craned_indexes_.size() < task->node_num) return false;

  allocated_res.SetToZero();
  task->allocated_res_view.SetToZero();

  for (const auto& craned_id : craned_indexes_) {
    const auto& craned_meta = craned_meta_map.at(craned_id).GetExclusivePtr();
    ResourceInNode feasible_res;

    // TODO: get feasible resource randomly (may cause start time change
    //       rapidly)
    bool ok = requested_node_res_view.GetFeasibleResourceInNode(
        craned_meta->res_avail, &feasible_res);
    if (!ok) {
      ok = requested_node_res_view.GetFeasibleResourceInNode(
          craned_meta->res_total, &feasible_res);
    }
    if (!ok) {
      CRANE_DEBUG(
          "Task #{} needs more resource than that of craned {}. "
          "Craned resource might have been changed.",
          task->TaskId(), craned_id);
      return false;
    }

    allocated_res.AddResourceInNode(craned_id, feasible_res);
    task->allocated_res_view += feasible_res;
  }

  task->SetAllocatedRes(std::move(allocated_res));

  EarliestStartSubsetSelector scheduler(task, node_selection_info,
                                        craned_indexes_);
  return scheduler.CalcEarliestStartTime(task, now, start_time, craned_ids);
}

void MinLoadFirst::NodeSelect(
    const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  std::unordered_map<PartitionId, NodeSelectionInfo> part_id_node_info_map;

  // Truncated by 1s.
  // We use the time now as the base time across the whole algorithm.
  absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

  std::unordered_map<ResvId, NodeSelectionInfo> resv_id_node_info_map;

  {
    auto reservation_meta_map = g_meta_container->GetResvMetaMapPtr();
    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
    std::vector<ResvId> expired_reservations;
    for (auto& [reservation_id, reservation_meta] : *reservation_meta_map) {
      auto resv_meta = reservation_meta.GetExclusivePtr();
      if (now < resv_meta->logical_part.start_time) continue;
      if (now >= resv_meta->logical_part.end_time) {
        CRANE_WARN("Reservation {} expired but not cleaned up", reservation_id);
        continue;
      }
      CalculateNodeSelectionInfoOfReservation_(
          running_tasks, now, resv_meta.get(), *craned_meta_map,
          &resv_id_node_info_map[reservation_id]);
    }
  }

  {
    auto all_partitions_meta_map =
        g_meta_container->GetAllPartitionsMetaMapConstPtr();
    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();

    // Calculate NodeSelectionInfo for all partitions
    for (auto& [partition_id, partition_metas] : *all_partitions_meta_map) {
      auto part_meta_ptr = partition_metas.GetExclusivePtr();
      std::unordered_set<CranedId> craned_ids = part_meta_ptr->craned_ids;

      NodeSelectionInfo& node_info_in_a_partition =
          part_id_node_info_map[partition_id];

      CalculateNodeSelectionInfoOfPartition_(running_tasks, now, partition_id,
                                             craned_ids, *craned_meta_map,
                                             &node_info_in_a_partition);
    }
  }

  std::vector<task_id_t> task_id_vec;
  task_id_vec = m_priority_sorter_->GetOrderedTaskIdList(
      *pending_task_map, running_tasks, g_config.ScheduledBatchSize, now);
  // Now we know, on every node, the # of running tasks (which
  //  doesn't include those we select as the incoming running tasks in the
  //  following code) and how many resources are available at the end of each
  //  task.
  // Iterate over all the pending tasks and select the available node for the
  //  task to run in its partition.
  for (task_id_t task_id : task_id_vec) {
    auto pending_task_it = pending_task_map->find(task_id);
    auto& task = pending_task_it->second;

    PartitionId part_id = task->partition_id;

    const auto& reservation_id = task->reservation;
    NodeSelectionInfo* node_info_ptr = nullptr;
    if (reservation_id == "")
      node_info_ptr = &part_id_node_info_map.at(part_id);
    else {
      auto iter = resv_id_node_info_map.find(reservation_id);
      if (iter == resv_id_node_info_map.end()) {
        task->pending_reason = "Unavailable Reservation";
        continue;
      } else {
        node_info_ptr = &iter->second;
      }
    }

    NodeSelectionInfo& node_info = *node_info_ptr;
    std::list<CranedId> craned_ids;
    absl::Time expected_start_time;
    std::unordered_map<PartitionId, std::list<CranedId>> involved_part_craned;

    {
      auto all_partitions_meta_map =
          g_meta_container->GetAllPartitionsMetaMapConstPtr();
      auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();

      const util::Synchronized<PartitionMeta>& part_meta =
          all_partitions_meta_map->at(part_id);

      // Note! ok should always be true.
      bool ok = CalculateRunningNodesAndStartTime_(
          node_info, part_meta, *craned_meta_map, task.get(), now, &craned_ids,
          &expected_start_time);
      if (!ok) {
        task->pending_reason = "Resource";
        continue;
      }

      // For pending tasks, the `start time` field in TaskInCtld means
      // expected start time and the `end time` is expected end time. For
      // running tasks, the `start time` means the time when it starts and the
      // `end time` means the latest finishing time.
      task->SetStartTime(expected_start_time);
      task->SetEndTime(expected_start_time + task->time_limit);

      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "\t task #{} ExpectedStartTime=now+{}s, EndTime=now+{}s",
            task->TaskId(), absl::ToInt64Seconds(expected_start_time - now),
            absl::ToInt64Seconds(expected_start_time + task->time_limit - now));
      }

      if (task->reservation == "") {
        // The start time and craned ids have been determined.
        // Modify the corresponding NodeSelectionInfo now.
        // Note: Since a craned node may belong to multiple partition,
        //       the NodeSelectionInfo of all the partitions the craned node
        //       belongs to should be modified!

        for (CranedId const& craned_id : craned_ids) {
          auto craned_meta = craned_meta_map->at(craned_id).GetExclusivePtr();
          for (PartitionId const& partition_id :
               craned_meta->static_meta.partition_ids) {
            involved_part_craned[partition_id].emplace_back(craned_id);
          }
        }

        for (const auto& [partition_id, part_craned_ids] :
             involved_part_craned) {
          SubtractTaskResourceNodeSelectionInfo_(
              expected_start_time, task->time_limit, task->AllocatedRes(),
              part_craned_ids, &part_id_node_info_map.at(partition_id));
        }
      } else {
        SubtractTaskResourceNodeSelectionInfo_(
            expected_start_time, task->time_limit, task->AllocatedRes(),
            craned_ids, &node_info);
      }
    }

    if (expected_start_time == now) {
      // The task can be started now.

      // We leave the change in running_tasks to the scheduling thread
      // caller for the simplicity of selection algorithm. However, the
      // resource used by the task MUST be subtracted now because a craned
      // node may belong to multiple partitions. The resource usage MUST
      // takes effect right now. Otherwise, during the scheduling for the
      // next partition, the algorithm may use the resource which is already
      // allocated.
      for (CranedId const& craned_id : craned_ids)
        g_meta_container->MallocResourceFromNode(craned_id, task->TaskId(),
                                                 task->AllocatedRes());
      if (task->reservation != "") {
        g_meta_container->MallocResourceFromResv(
            task->reservation, task->TaskId(),
            {task->EndTime(), task->AllocatedRes()});
      }
      std::unique_ptr<TaskInCtld> moved_task;

      // Move task out of pending_task_map and insert it to the
      // scheduling_result_list.
      moved_task.swap(pending_task_it->second);

      selection_result_list->emplace_back(std::move(moved_task),
                                          std::move(craned_ids));

      // Erase the task ready to run from temporary
      // partition_pending_task_map and move to the next element
      pending_task_map->erase(pending_task_it);
    } else {
      // The task can't be started now. Set pending reason and move to the
      // next pending task.
      for (auto& craned_id : craned_ids) {
        if (node_info.GetFirstResvTime(craned_id) < now + task->time_limit) {
          task->pending_reason = "Resource Reserved";
          break;
        }
        auto& res_avail = node_info.GetTimeAvailResMap(craned_id).at(now);
        if (!(task->AllocatedRes().EachNodeResMap().at(craned_id) <=
              res_avail)) {
          task->pending_reason = "Resource";
          break;
        }
      }
      if (task->pending_reason == "") {
        task->pending_reason = "Priority";
      }
      continue;
    }
  }
}

void MinLoadFirst::SubtractTaskResourceNodeSelectionInfo_(
    const absl::Time& expected_start_time, const absl::Duration& duration,
    const ResourceV2& resources, std::list<CranedId> const& craned_ids,
    MinLoadFirst::NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_info = *node_selection_info;
  bool ok;

  absl::Time task_end_time = expected_start_time + duration;

  // Increase the running task num in Craned `crane_id`.
  for (CranedId const& craned_id : craned_ids) {
    ResourceInNode const& task_res_in_node = resources.at(craned_id);
    node_info.UpdateCost(craned_id, expected_start_time, task_end_time,
                         task_res_in_node);
    TimeAvailResMap& time_avail_res_map =
        node_info.GetTimeAvailResMap(craned_id);

    auto task_duration_begin_it =
        time_avail_res_map.upper_bound(expected_start_time);
    if (task_duration_begin_it == time_avail_res_map.end()) {
      --task_duration_begin_it;
      // Situation #1
      //                     task duration
      //                   |<-------------->|
      // *-----------------*----------------------> inf
      //                   ^
      //        task_duration_begin_it
      //
      // *-----------------*----------------|-----> inf
      //                           ^        ^
      //                           |      insert here
      //                      subtract resource here
      //
      // OR Situation #2
      //                        task duration
      //                      |<-------------->|
      // *-----------------*----------------------> inf
      //                   ^
      //        task_duration_begin_it
      //
      // *-----------------*--|----------------|--> inf
      //                      ^      ^         ^
      //                insert here  |     insert here
      //                      subtract resource here

      TimeAvailResMap::iterator inserted_it;
      std::tie(inserted_it, ok) = time_avail_res_map.emplace(
          task_end_time, task_duration_begin_it->second);
      CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

      if (task_duration_begin_it->first == expected_start_time) {
        // Situation #1
        CRANE_ASSERT(task_res_in_node <= task_duration_begin_it->second);
        task_duration_begin_it->second -= task_res_in_node;
      } else {
        // Situation #2
        std::tie(inserted_it, ok) = time_avail_res_map.emplace(
            expected_start_time, task_duration_begin_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        CRANE_ASSERT(task_res_in_node <= inserted_it->second);
        inserted_it->second -= task_res_in_node;
      }
    } else {
      --task_duration_begin_it;
      // Situation #3
      //                    task duration
      //                |<-------------->|
      // *-------*----------*---------*------------
      //         ^                    ^
      //  task_duration_begin_it     task_duration_end_it
      // *-------*------|---*---------*--|---------
      //                ^  ^     ^     ^ ^
      //       insert here |     |     | insert here
      //                 subtract at these points
      //
      // Or Situation #4
      //               task duration
      //         |<----------------->|
      // *-------*----------*--------*------------
      //         ^                   ^
      //  task_duration_begin_it   task_duration_end_it

      // std::prev can be used without any check here.
      // There will always be one time point (now) before task_end_time.

      if (task_duration_begin_it->first != expected_start_time) {
        // Situation #3 (begin)
        TimeAvailResMap::iterator inserted_it;
        std::tie(inserted_it, ok) = time_avail_res_map.emplace(
            expected_start_time, task_duration_begin_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        task_duration_begin_it = inserted_it;
      }

      auto task_duration_end_it =
          std::prev(time_avail_res_map.upper_bound(task_end_time));

      // Subtract the required resources within the interval.
      for (auto in_duration_it = task_duration_begin_it;
           in_duration_it != task_duration_end_it; in_duration_it++) {
        CRANE_ASSERT(task_res_in_node <= in_duration_it->second);
        in_duration_it->second -= task_res_in_node;
      }

      // Check if we need to insert a time point at
      // `task_end_time_plus_1s` Detailed version of why: Assume one task
      // end at time x-2, If "x+2" lies in the interval [x, y-1) in
      // time__avail_res__map,
      //  for example, x+2 in [x, y-1) with the available resources amount
      //  `a`, we need to divide this interval into to two intervals: [x,
      //  x+2]: a-k, where k is the resource amount that task requires,
      //  [x+3, y-1]: a
      // Therefore, we need to insert a key-value at x+3 to preserve this.
      // However, if the length of [x+3, y-1] is 0, or more simply, the
      // point x+3 exists, there's no need to save the interval [x+3,
      // y-1].
      if (task_duration_end_it->first != task_end_time) {
        // Situation #3 (end)
        TimeAvailResMap::iterator inserted_it;
        std::tie(inserted_it, ok) = time_avail_res_map.emplace(
            task_end_time, task_duration_end_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        CRANE_ASSERT(task_res_in_node <= task_duration_end_it->second);
        task_duration_end_it->second -= task_res_in_node;
      }
    }
  }
}

void TaskScheduler::ProcessFinalSteps_(std::vector<StepInCtld*> const& steps) {
  PersistAndTransferStepsToMongodb_(steps);
  // CallPluginHookForFinalTasks_(tasks);
}

void TaskScheduler::PersistAndTransferStepsToMongodb_(
    std::vector<StepInCtld*> const& steps) {
  if (steps.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginStepVarDbTransaction(&txn_id);
  for (StepInCtld* step : steps) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfStep(txn_id, step->StepDbId(),
                                                       step->RuntimeAttr()))
      CRANE_ERROR("Failed to call UpdateRuntimeAttrOfStep() for step #{}.{}",
                  step->job_id, step->StepId());
  }

  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  // TODO: Step finish before job, persist step into mongodb

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
        "Failed to call g_embedded_db_client->PurgeEndedTasks() "
        "for final tasks");
  }
}

void TaskScheduler::ProcessFinalTasks_(const std::vector<TaskInCtld*>& tasks) {
  PersistAndTransferTasksToMongodb_(tasks);
  CallPluginHookForFinalTasks_(tasks);
}

void TaskScheduler::CallPluginHookForFinalTasks_(
    std::vector<TaskInCtld*> const& tasks) {
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
    std::vector<TaskInCtld*> const& tasks) {
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
  std::vector<task_db_id_t> db_ids;
  for (TaskInCtld* task : tasks) db_ids.emplace_back(task->TaskDbId());

  if (!g_embedded_db_client->PurgeEndedTasks(db_ids)) {
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedTasks() "
        "for final tasks");
  }
}

CraneExpected<void> TaskScheduler::HandleUnsetOptionalInTaskToCtld(
    TaskInCtld* task) {
  if (task->type == crane::grpc::Batch) {
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
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVAILD_NODE_LIST);

    for (auto&& node : nodes) task->included_nodes.emplace(std::move(node));
  }

  if (!task->TaskToCtld().excludes().empty() && task->excluded_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(task->TaskToCtld().excludes(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVAILD_EX_NODE_LIST);

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
        auto reserved_craned_id_list = resv_meta->logical_part.craned_ids;
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
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVAILD_NODE_LIST);

    for (auto&& node : nodes) step->included_nodes.emplace(std::move(node));
  }

  if (!step->StepToCtld().excludes().empty() && step->excluded_nodes.empty()) {
    std::list<std::string> nodes;
    bool ok = util::ParseHostList(step->StepToCtld().excludes(), &nodes);
    if (!ok) return std::unexpected(CraneErrCode::ERR_INVAILD_EX_NODE_LIST);

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
                            "Terminated");
  } else {
    CRANE_TRACE("No task is executed by craned {}. Ignore cleaning step...",
                craned_id);
  }
}

std::vector<task_id_t> MultiFactorPriority::GetOrderedTaskIdList(
    const OrderedTaskMap& pending_task_map,
    const UnorderedTaskMap& running_task_map, size_t limit_num,
    absl::Time now) {
  CalculateFactorBound_(pending_task_map, running_task_map, now);

  std::vector<std::pair<TaskInCtld*, double>> task_priority_vec;
  for (const auto& [task_id, task] : pending_task_map) {
    if (task->Held()) {
      task->pending_reason = "Held";
      continue;
    }
    // Admin may manually specify the priority of a task.
    // In this case, MultiFactorPriority will not calculate the priority.
    double priority = (task->mandated_priority == 0.0)
                          ? CalculatePriority_(task.get(), now)
                          : task->mandated_priority;
    task->SetCachedPriority(priority);
    task->pending_reason = "";
    task_priority_vec.emplace_back(task.get(), priority);
  }

  std::sort(task_priority_vec.begin(), task_priority_vec.end(),
            [](const std::pair<TaskInCtld*, double>& a,
               const std::pair<TaskInCtld*, double>& b) {
              return a.second > b.second;
            });

  size_t id_vec_len = std::min(limit_num, task_priority_vec.size());

  std::vector<task_id_t> task_id_vec;
  task_id_vec.reserve(id_vec_len);

  for (int i = 0; i < id_vec_len; i++)
    task_id_vec.emplace_back(task_priority_vec[i].first->TaskId());

  return task_id_vec;
}

void MultiFactorPriority::CalculateFactorBound_(
    const OrderedTaskMap& pending_task_map,
    const UnorderedTaskMap& running_task_map, absl::Time now) {
  FactorBound& bound = m_factor_bound_;

  // Initialize the values of each max and min
  bound.age_max = 0;
  bound.age_min = std::numeric_limits<uint64_t>::max();

  bound.qos_priority_max = 0;
  bound.qos_priority_min = std::numeric_limits<uint32_t>::max();

  bound.part_priority_max = 0;
  bound.part_priority_min = std::numeric_limits<uint32_t>::max();

  bound.nodes_alloc_max = 0;
  bound.nodes_alloc_min = std::numeric_limits<uint32_t>::max();

  bound.mem_alloc_max = 0;
  bound.mem_alloc_min = std::numeric_limits<uint64_t>::max();

  bound.cpus_alloc_max = 0;
  bound.cpus_alloc_min = std::numeric_limits<double>::max();

  bound.service_val_max = 0;
  bound.service_val_min = std::numeric_limits<uint32_t>::max();

  bound.acc_service_val_map.clear();

  for (const auto& [task_id, task] : pending_task_map) {
    uint64_t age = ToInt64Seconds(now - task->SubmitTime());
    age = std::min(age, g_config.PriorityConfig.MaxAge);

    bound.acc_service_val_map[task->account] = 0.0;

    bound.age_min = std::min(age, bound.age_min);
    bound.age_max = std::max(age, bound.age_max);

    uint32_t nodes_alloc = task->node_num;
    bound.nodes_alloc_min = std::min(nodes_alloc, bound.nodes_alloc_min);
    bound.nodes_alloc_max = std::max(nodes_alloc, bound.nodes_alloc_max);

    uint64_t mem_alloc = task->allocated_res_view.MemoryBytes();
    bound.mem_alloc_min = std::min(mem_alloc, bound.mem_alloc_min);
    bound.mem_alloc_max = std::max(mem_alloc, bound.mem_alloc_max);

    double cpus_alloc = task->allocated_res_view.CpuCount();
    bound.cpus_alloc_min = std::min(cpus_alloc, bound.cpus_alloc_min);
    bound.cpus_alloc_max = std::max(cpus_alloc, bound.cpus_alloc_max);

    uint32_t qos_priority = task->qos_priority;
    bound.qos_priority_min = std::min(qos_priority, bound.qos_priority_min);
    bound.qos_priority_max = std::max(qos_priority, bound.qos_priority_max);

    uint32_t part_priority = task->partition_priority;
    bound.part_priority_min = std::min(part_priority, bound.part_priority_min);
    bound.part_priority_max = std::max(part_priority, bound.part_priority_max);
  }

  for (const auto& [task_id, task] : running_task_map) {
    double service_val = 0;
    if (bound.cpus_alloc_max != bound.cpus_alloc_min)
      service_val +=
          1.0 * (task->allocated_res_view.CpuCount() - bound.cpus_alloc_min) /
          (bound.cpus_alloc_max - bound.cpus_alloc_min);
    else
      // += 1.0 here rather than 0.0 in case that the final service_val is 0.
      // If the final service_val is 0, the running time of the task will not
      // be ruled out in calculation. We must avoid that.
      service_val += 1.0;

    if (bound.nodes_alloc_max != bound.nodes_alloc_min)
      service_val += 1.0 * (task->node_num - bound.nodes_alloc_min) /
                     (bound.nodes_alloc_max - bound.nodes_alloc_min);
    else
      service_val += 1.0;

    if (bound.mem_alloc_max != bound.mem_alloc_min)
      service_val +=
          1.0 *
          static_cast<double>(task->allocated_res_view.MemoryBytes() -
                              bound.mem_alloc_min) /
          static_cast<double>(bound.mem_alloc_max - bound.mem_alloc_min);
    else
      service_val += 1.0;

    uint64_t run_time = ToInt64Seconds(now - task->StartTime());
    bound.acc_service_val_map[task->account] +=
        service_val * static_cast<double>(run_time);
  }

  for (const auto& [acc_name, ser_val] : bound.acc_service_val_map) {
    bound.service_val_min = std::min(ser_val, bound.service_val_min);
    bound.service_val_max = std::max(ser_val, bound.service_val_max);
  }
}

double MultiFactorPriority::CalculatePriority_(Ctld::TaskInCtld* task,
                                               absl::Time now) const {
  FactorBound const& bound = m_factor_bound_;

  uint64_t task_age = ToUnixSeconds(now) - task->SubmitTimeInUnixSecond();
  task_age = std::min(task_age, g_config.PriorityConfig.MaxAge);

  uint32_t task_qos_priority = task->qos_priority;
  uint32_t task_part_priority = task->partition_priority;
  uint32_t task_nodes_alloc = task->node_num;
  uint64_t task_mem_alloc = task->requested_node_res_view.MemoryBytes();
  double task_cpus_alloc =
      static_cast<double>(task->requested_node_res_view.CpuCount());
  double task_service_val = bound.acc_service_val_map.at(task->account);

  double qos_factor{0};
  double age_factor{0};
  double partition_factor{0};
  double job_size_factor{0};
  double fair_share_factor{0};

  // age_factor
  if (bound.age_max != bound.age_min)
    age_factor = 1.0 * static_cast<double>(task_age - bound.age_min) /
                 static_cast<double>(bound.age_max - bound.age_min);

  // qos_factor
  if (bound.qos_priority_min != bound.qos_priority_max)
    qos_factor = 1.0 * (task_qos_priority - bound.qos_priority_min) /
                 (bound.qos_priority_max - bound.qos_priority_min);

  // partition_factor
  if (bound.part_priority_max != bound.part_priority_min)
    partition_factor = 1.0 * (task_part_priority - bound.part_priority_min) /
                       (bound.part_priority_max - bound.part_priority_min);

  // job_size_factor
  if (bound.cpus_alloc_max != bound.cpus_alloc_min)
    job_size_factor += 1.0 * (task_cpus_alloc - bound.cpus_alloc_min) /
                       (bound.cpus_alloc_max - bound.cpus_alloc_min);
  if (bound.nodes_alloc_max != bound.nodes_alloc_min)
    job_size_factor += 1.0 * (task_nodes_alloc - bound.nodes_alloc_min) /
                       (bound.nodes_alloc_max - bound.nodes_alloc_min);
  if (bound.mem_alloc_max != bound.mem_alloc_min)
    job_size_factor +=
        1.0 * static_cast<double>(task_mem_alloc - bound.mem_alloc_min) /
        static_cast<double>(bound.mem_alloc_max - bound.mem_alloc_min);
  if (g_config.PriorityConfig.FavorSmall)
    job_size_factor = 1.0 - job_size_factor / 3;
  else
    job_size_factor /= 3.0;

  // fair_share_factor
  if (bound.service_val_max != bound.service_val_min)
    fair_share_factor =
        1.0 - (task_service_val - bound.service_val_min) /
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
