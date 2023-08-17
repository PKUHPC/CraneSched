/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "TaskScheduler.h"

#include <google/protobuf/util/time_util.h>

#include "AccountManager.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "EmbeddedDbClient.h"

namespace Ctld {

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
}

bool TaskScheduler::Init() {
  using crane::grpc::TaskInEmbeddedDb;

  bool ok;
  CraneErr err;

  std::list<TaskInEmbeddedDb> running_list;
  ok = g_embedded_db_client->GetRunningQueueCopy(&running_list);
  if (!ok) {
    CRANE_ERROR("Failed to call g_embedded_db_client->GetRunningQueueCopy()");
    return false;
  }

  if (!running_list.empty()) {
    CRANE_INFO("{} running task(s) recovered.", running_list.size());

    for (auto&& task_in_embedded_db : running_list) {
      auto task = std::make_unique<TaskInCtld>();
      task->SetFieldsByTaskToCtld(task_in_embedded_db.task_to_ctld());
      task->SetFieldsByPersistedPart(task_in_embedded_db.persisted_part());
      task_id_t task_id = task->TaskId();
      task_db_id_t task_db_id = task->TaskDbId();

      CRANE_TRACE("Restore task #{} from embedded running queue.",
                  task->TaskId());

      err = AcquireTaskAttributes(task.get());
      if (err != CraneErr::kOk) {
        CRANE_INFO(
            "Failed to acquire task attributes for restored running task #{}. "
            "Mark it as FAILED and move it to the ended queue.",
            task_id);
        task->SetStatus(crane::grpc::Failed);
        ok = g_embedded_db_client->UpdatePersistedPartOfTask(
            task_db_id, task->PersistedPart());
        if (!ok) {
          CRANE_ERROR(
              "UpdatePersistedPartOfTask failed for task #{} when "
              "mark the task as FAILED.",
              task_id);
        }

        ok = g_embedded_db_client->MovePendingOrRunningTaskToEnded(task_db_id);
        if (!ok) {
          CRANE_ERROR(
              "MovePendingOrRunningTaskToEnded failed for task #{} when "
              "this running task to ended queue..",
              task_id);
        }

        // Move this problematic task into ended queue and
        // process next task.
        continue;
      }

      auto* stub = g_craned_keeper->GetCranedStub(task->executing_craned_id);
      if (stub == nullptr || stub->Invalid()) {
        CRANE_INFO(
            "The execution node of the restore task #{} is down. "
            "Clean all its allocated craned nodes, and "
            "move it to pending queue and re-run it.",
            task_id);

        for (const CranedId& craned_id : task->CranedIds()) {
          stub = g_craned_keeper->GetCranedStub(craned_id);
          if (stub != nullptr && !stub->Invalid())
            stub->ReleaseCgroupForTask(task->TaskId(), task->uid);
        }

        task->SetStatus(crane::grpc::Pending);

        task->nodes_alloc = 0;
        task->allocated_craneds_regex.clear();
        task->CranedIdsClear();

        ok = g_embedded_db_client->UpdatePersistedPartOfTask(
            task->TaskDbId(), task->PersistedPart());
        if (!ok) {
          CRANE_ERROR(
              "Failed to call "
              "g_embedded_db_client->UpdatePersistedPartOfTask()");
        }

        ok = g_embedded_db_client->MoveTaskFromRunningToPending(
            task->TaskDbId());
        if (!ok) {
          CRANE_ERROR(
              "Failed to call "
              "g_embedded_db_client->MoveTaskFromRunningToPending()");
        }

        // Now the task is moved to the embedded pending queue.
        // The embedded pending queue will be processed in the following code.
      } else {
        crane::grpc::TaskStatus status;
        err = stub->CheckTaskStatus(task->TaskId(), &status);
        if (err == CraneErr::kOk) {
          task->SetStatus(status);
          if (status == crane::grpc::Running) {
            // Exec node is up and the task is running.
            // Just allocate resource from allocated nodes and
            // put it back into the running queue.
            PutRecoveredTaskIntoRunningQueueLock_(std::move(task));

            CRANE_INFO(
                "Task #{} is still RUNNING. Put it into memory running queue.",
                task_id);
          } else {
            // Exec node is up and the task ended.

            CRANE_INFO(
                "Task #{} has ended with status {}. Put it into embedded ended "
                "queue.",
                task_id, crane::grpc::TaskStatus_Name(status));

            if (status != crane::grpc::Completed) {
              // Check whether the task is orphaned on the allocated nodes
              // in case that when processing TaskStatusChange CraneCtld
              // crashed, only part of Craned nodes executed TerminateTask gRPC.
              // Not needed for succeeded tasks.
              for (const CranedId& craned_id : task->CranedIds()) {
                stub = g_craned_keeper->GetCranedStub(craned_id);
                if (stub != nullptr && !stub->Invalid())
                  stub->TerminateOrphanedTask(task->TaskId());
              }
            }

            // For both succeeded and failed tasks, cgroup for them should be
            // released. Though some craned nodes might have released the
            // cgroup, just resend the gRPC again to guarantee that the cgroup
            // is always released.
            for (const CranedId& craned_id : task->CranedIds()) {
              stub = g_craned_keeper->GetCranedStub(craned_id);
              if (stub != nullptr && !stub->Invalid())
                stub->ReleaseCgroupForTask(task->TaskId(), task->uid);
            }

            ok = g_embedded_db_client->MovePendingOrRunningTaskToEnded(
                task->TaskDbId());
            if (!ok) {
              CRANE_ERROR(
                  "MovePendingOrRunningTaskToEnded failed for task #{} when "
                  "recovering running queue.",
                  task->TaskId());
            }

            ok = g_db_client->InsertJob(task.get());
            if (!ok) {
              CRANE_ERROR(
                  "InsertJob failed for task #{} when recovering running "
                  "queue.",
                  task->TaskId());
            }

            ok = g_embedded_db_client->PurgeTaskFromEnded(task->TaskDbId());
            if (!ok) {
              CRANE_ERROR(
                  "PurgeTaskFromEnded failed for task #{} when recovering "
                  "running queue.",
                  task->TaskId());
            }
          }
        } else {
          // Exec node is up but task id does not exist.
          // It may come up due to the craned has restarted during CraneCtld
          // was down. Thus, we should clean the Craned nodes on which the task
          // was possibly running.
          // The result of task is lost in such a scenario, and we should
          // requeue the task.
          CRANE_TRACE(
              "Task id #{} can't be found cannot be found in its exec craned. "
              "Clean all its allocated craned nodes, and "
              "move it to pending queue and re-run it.",
              task_id);

          for (const CranedId& craned_id : task->CranedIds()) {
            stub = g_craned_keeper->GetCranedStub(craned_id);
            if (stub != nullptr && !stub->Invalid())
              stub->TerminateOrphanedTask(task->TaskId());
          }

          for (const CranedId& craned_id : task->CranedIds()) {
            stub = g_craned_keeper->GetCranedStub(craned_id);
            if (stub != nullptr && !stub->Invalid())
              stub->ReleaseCgroupForTask(task->TaskId(), task->uid);
          }

          ok = g_embedded_db_client->MoveTaskFromRunningToPending(
              task->TaskDbId());
          if (!ok) {
            CRANE_ERROR(
                "Failed to call "
                "g_embedded_db_client->MoveTaskFromRunningToPending()");
          }

          // Now the task is moved to the embedded pending queue.
          // The embedded pending queue will be processed in the following code.
        }
      }
    }
  }

  // Process the pending tasks in the embedded pending queue. The task may
  //  come from the embedded running queue due to requeue failure.

  std::list<TaskInEmbeddedDb> pending_list;
  ok = g_embedded_db_client->GetPendingQueueCopy(&pending_list);
  if (!ok) {
    CRANE_ERROR("Failed to call g_embedded_db_client->GetPendingQueueCopy()");
    return false;
  }

  if (!pending_list.empty()) {
    CRANE_INFO("{} pending task(s) recovered.", pending_list.size());

    for (auto&& task_in_embedded_db : pending_list) {
      auto task = std::make_unique<TaskInCtld>();
      task->SetFieldsByTaskToCtld(task_in_embedded_db.task_to_ctld());
      task->SetFieldsByPersistedPart(task_in_embedded_db.persisted_part());

      task_id_t task_id = task->TaskId();
      task_db_id_t task_db_id = task->TaskDbId();

      CRANE_TRACE("Restore task #{} from embedded pending queue.",
                  task->TaskId());

      bool mark_task_as_failed = false;

      if (task->type != crane::grpc::Batch) {
        CRANE_INFO("Mark interactive task #{} as FAILED", task_id);
        mark_task_as_failed = true;
      }

      if (!mark_task_as_failed &&
          AcquireTaskAttributes(task.get()) != CraneErr::kOk) {
        CRANE_ERROR("AcquireTaskAttributes failed for task #{}", task_id);
        mark_task_as_failed = true;
      }

      if (!mark_task_as_failed &&
          CheckTaskValidity(task.get()) != CraneErr::kOk) {
        CRANE_ERROR("CheckTaskValidity failed for task #{}", task_id);
        mark_task_as_failed = true;
      }

      if (!mark_task_as_failed) {
        RequeueRecoveredTaskIntoPendingQueueLock_(std::move(task));
      } else {
        // If a batch task failed to requeue the task into pending queue due to
        // insufficient resource or other reasons or the task is an interactive
        // task, Mark it as FAILED and move it to the ended queue.
        CRANE_INFO(
            "Failed to requeue task #{}. Mark it as FAILED and "
            "move it to the ended queue.",
            task_id);
        task->SetStatus(crane::grpc::Failed);
        ok = g_embedded_db_client->UpdatePersistedPartOfTask(
            task_db_id, task->PersistedPart());
        if (!ok) {
          CRANE_ERROR(
              "UpdatePersistedPartOfTask failed for task #{} when "
              "mark the task as FAILED.",
              task_id);
        }

        ok = g_embedded_db_client->MovePendingOrRunningTaskToEnded(task_db_id);
        if (!ok) {
          CRANE_ERROR(
              "MovePendingOrRunningTaskToEnded failed for task #{} when "
              "requeue-ing this pending task in the embedded queue..",
              task_id);
        }
      }
    }
  }

  std::list<TaskInEmbeddedDb> ended_list;
  ok = g_embedded_db_client->GetEndedQueueCopy(&ended_list);
  if (!ok) {
    CRANE_ERROR("Failed to call g_embedded_db_client->GetEndedQueueCopy()");
    return false;
  }

  if (!ended_list.empty()) {
    CRANE_INFO("{} ended task(s) might not have been put to mongodb: ",
               ended_list.size());

    for (auto& task_in_embedded_db : ended_list) {
      task_id_t task_id = task_in_embedded_db.persisted_part().task_id();
      task_db_id_t db_id = task_in_embedded_db.persisted_part().task_db_id();
      ok = g_db_client->CheckTaskDbIdExisted(db_id);
      if (!ok) {
        if (!g_db_client->InsertRecoveredJob(task_in_embedded_db)) {
          CRANE_ERROR(
              "Failed to call g_db_client->InsertRecoveredJob() "
              "for task #{}",
              task_id);
        }
      }

      ok = g_embedded_db_client->PurgeTaskFromEnded(
          task_in_embedded_db.persisted_part().task_db_id());
      if (!ok) {
        CRANE_ERROR(
            "Failed to call g_embedded_db_client->PurgeTaskFromEnded()");
      }
    }
  }

  // Start schedule thread first.
  m_schedule_thread_ = std::thread([this] { ScheduleThread_(); });

  return true;
}

void TaskScheduler::RequeueRecoveredTaskIntoPendingQueueLock_(
    std::unique_ptr<TaskInCtld> task) {
  // The order of LockGuards matters.
  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  m_partition_to_tasks_map_[task->partition_id].emplace(task->TaskId());
  m_pending_task_map_.emplace(task->TaskId(), std::move(task));
}

void TaskScheduler::PutRecoveredTaskIntoRunningQueueLock_(
    std::unique_ptr<TaskInCtld> task) {
  for (const CranedId& craned_id : task->CranedIds())
    g_meta_container->MallocResourceFromNode(craned_id, task->TaskId(),
                                             task->resources);

  // The order of LockGuards matters.
  LockGuard running_guard(&m_running_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  for (const CranedId& craned_id : task->CranedIds())
    m_node_to_tasks_map_[craned_id].emplace(task->TaskId());

  m_partition_to_tasks_map_[task->partition_id].emplace(task->TaskId());
  m_running_task_map_.emplace(task->TaskId(), std::move(task));
}

void TaskScheduler::ScheduleThread_() {
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
      num_tasks_single_schedule = m_pending_task_map_.size();

      begin = std::chrono::steady_clock::now();

      std::list<INodeSelectionAlgo::NodeSelectionResult> selection_result_list;
      m_node_selection_algo_->NodeSelect(
          m_running_task_map_, &m_pending_task_map_, &selection_result_list);

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

        task->SetStatus(crane::grpc::TaskStatus::Running);
        task->SetCranedIds(std::move(it.second));
        task->nodes_alloc = task->CranedIds().size();

        // CRANE_DEBUG(
        // "Task #{} is allocated to partition {} and craned nodes: {}",
        // task->TaskId(), partition_id, fmt::join(task->CranedIds(), ", "));

        task->allocated_craneds_regex =
            util::HostNameListToStr(task->CranedIds());

        // For the task whose --node > 1, only execute the command at the first
        // allocated node.
        task->executing_craned_id = task->CranedIds().front();
      }
      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Set task fields costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      // RPC is time-consuming. Clustering rpc to one craned for performance.

      absl::flat_hash_map<CranedId, std::vector<std::pair<task_id_t, uid_t>>>
          craned_cgroup_map;
      absl::flat_hash_map<CranedId, std::vector<TaskInCtld const*>>
          craned_tasks_map;

      for (auto& it : selection_result_list) {
        auto& task = it.first;

        for (CranedId const& craned_id : task->CranedIds()) {
          craned_cgroup_map[craned_id].emplace_back(task->TaskId(), task->uid);
        }

        craned_tasks_map[task->executing_craned_id].emplace_back(task.get());
      }

      absl::BlockingCounter bl(craned_cgroup_map.size());
      for (auto const& iter : craned_cgroup_map) {
        CranedId const& craned_id = iter.first;
        auto const& task_uid_pairs = iter.second;
        g_thread_pool->push_task([=, &bl]() {
          CranedStub* stub = g_craned_keeper->GetCranedStub(craned_id);
          CRANE_TRACE("Send CreateCgroupForTasks for {} tasks to {}",
                      task_uid_pairs.size(), craned_id);
          stub->CreateCgroupForTasks(task_uid_pairs);

          bl.DecrementCount();
        });
      }
      bl.Wait();

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "CreateCgroupForTasks costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      // Move tasks into running queue.
      bool ok = g_embedded_db_client->BeginManualTransaction();
      if (!ok) {
        CRANE_ERROR("Embedded database failed to start manual transaction.");
      }

      for (auto& it : selection_result_list) {
        auto& task = it.first;
        if (task->type == crane::grpc::Interactive) {
          std::get<InteractiveMetaInTask>(task->meta)
              .cb_task_res_allocated(task->TaskId(),
                                     task->allocated_craneds_regex);
        }

        auto* task_ptr = task.get();

        // IMPORTANT: task must be put into running_task_map before any
        //  time-consuming operation, otherwise TaskStatusChange RPC will come
        //  earlier before task is put into running_task_map.
        m_running_task_map_mtx_.Lock();
        m_task_indexes_mtx_.Lock();

        for (const CranedId& craned_id : task->CranedIds()) {
          m_node_to_tasks_map_[craned_id].emplace(task_ptr->TaskId());
        }
        m_running_task_map_.emplace(task->TaskId(), std::move(task));

        m_running_task_map_mtx_.Unlock();
        m_task_indexes_mtx_.Unlock();

        g_embedded_db_client->UpdatePersistedPartOfTask(
            task_ptr->TaskDbId(), task_ptr->PersistedPart());

        ok = g_embedded_db_client->MoveTaskFromPendingToRunning(
            task_ptr->TaskDbId());
        if (!ok) {
          CRANE_ERROR(
              "Failed to call "
              "g_embedded_db_client->MoveTaskFromPendingToRunning({})",
              task_ptr->TaskDbId());
        }
      }

      ok = g_embedded_db_client->CommitManualTransaction();
      if (!ok) {
        CRANE_ERROR("Embedded database failed to commit manual transaction.");
      }

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "Move tasks into running queue costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      begin = std::chrono::steady_clock::now();

      for (auto const& [craned_id, tasks] : craned_tasks_map) {
        CranedStub* stub = g_craned_keeper->GetCranedStub(craned_id);
        CRANE_TRACE("Send ExecuteTasks for {} tasks to {}", tasks.size(),
                    craned_id);
        stub->ExecuteTasks(tasks);
      }

      end = std::chrono::steady_clock::now();
      CRANE_TRACE(
          "ExecuteTasks costed {} ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
              .count());

      schedule_end = end;
      CRANE_TRACE(
          "Scheduling {} pending tasks. {} get scheduled. Time elapsed: {}ms",
          num_tasks_single_schedule, num_tasks_single_execution,
          std::chrono::duration_cast<std::chrono::milliseconds>(schedule_end -
                                                                schedule_begin)
              .count());
    } else {
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

CraneErr TaskScheduler::SubmitTask(std::unique_ptr<TaskInCtld> task,
                                   uint32_t* task_id) {
  CraneErr err;

  err = AcquireTaskAttributes(task.get());
  if (err != CraneErr::kOk) return err;

  err = CheckTaskValidity(task.get());
  if (err != CraneErr::kOk) return err;

  // Add the task to the pending task queue.
  task->SetStatus(crane::grpc::Pending);
  task->SetSubmitTime(absl::Now());

  bool ok;
  ok = g_embedded_db_client->AppendTaskToPendingAndAdvanceTaskIds(task.get());
  if (!ok) {
    CRANE_ERROR("Failed to append the task to embedded db queue.");
    return CraneErr::kSystemErr;
  }

  *task_id = task->TaskId();

  m_task_indexes_mtx_.Lock();
  m_partition_to_tasks_map_[task->partition_id].emplace(task->TaskId());
  m_task_indexes_mtx_.Unlock();

  m_pending_task_map_mtx_.Lock();
  m_pending_task_map_.emplace(task->TaskId(), std::move(task));
  m_pending_task_map_mtx_.Unlock();

  return CraneErr::kOk;
}

CraneErr TaskScheduler::ChangeTaskTimeLimit(uint32_t task_id, int64_t secs) {
  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard running_guard(&m_running_task_map_mtx_);

  auto pd_iter = m_pending_task_map_.find(task_id);
  if (pd_iter != m_pending_task_map_.end()) {
    pd_iter->second->time_limit = absl::Seconds(secs);
    crane::grpc::TaskToCtld* task_to_ctld =
        pd_iter->second->MutableTaskToCtld();
    task_to_ctld->mutable_time_limit()->set_seconds(secs);
    g_embedded_db_client->UpdateTaskToCtld(pd_iter->second->TaskDbId(),
                                           *task_to_ctld);
    return CraneErr::kOk;
  }

  auto rn_iter = m_running_task_map_.find(task_id);
  if (rn_iter == m_running_task_map_.end()) {
    CRANE_ERROR("Task #{} was not found in running or pending queue", task_id);
    return CraneErr::kNonExistent;
  }

  rn_iter->second->time_limit = absl::Seconds(secs);
  crane::grpc::TaskToCtld* task_to_ctld = rn_iter->second->MutableTaskToCtld();
  task_to_ctld->mutable_time_limit()->set_seconds(secs);
  g_embedded_db_client->UpdateTaskToCtld(rn_iter->second->TaskDbId(),
                                         *task_to_ctld);

  // only send request to the first node
  CranedId const& craned_id = rn_iter->second->executing_craned_id;
  auto* stub = g_craned_keeper->GetCranedStub(craned_id);
  if (!stub->Invalid()) {
    CraneErr err = stub->ChangeTaskTimeLimit(task_id, secs);
    if (err != CraneErr::kOk) {
      CRANE_ERROR("Failed to change time limit of task #{} on Node {}", task_id,
                  craned_id);
      return err;
    }
  }

  return CraneErr::kOk;
}

void TaskScheduler::TaskStatusChangeNoLock_(uint32_t task_id,
                                            const CranedId& craned_index,
                                            crane::grpc::TaskStatus new_status,
                                            uint32_t exit_code) {
  auto iter = m_running_task_map_.find(task_id);
  if (iter == m_running_task_map_.end()) {
    CRANE_WARN("Ignoring unknown task id {} in TaskStatusChange.", task_id);
    return;
  }

  const std::unique_ptr<TaskInCtld>& task = iter->second;

  if (task->type == crane::grpc::Interactive) {
    auto& meta = std::get<InteractiveMetaInTask>(task->meta);

    // TaskStatusChange may indicate the time limit has been reached and
    // the task has been terminated. No more TerminateTask RPC should be sent to
    // the craned node if any further CancelTask or TaskCompletionRequest RPC is
    // received.
    meta.has_been_terminated_on_craned = true;

    if (new_status == crane::grpc::ExceedTimeLimit) {
      meta.has_been_cancelled_on_front_end = true;
      meta.cb_task_cancel(task->TaskId());
      task->SetStatus(crane::grpc::ExceedTimeLimit);
    } else {
      task->SetStatus(crane::grpc::Completed);
    }

    meta.cb_task_completed(task->TaskId());
  } else {
    if (new_status == crane::grpc::Completed) {
      task->SetStatus(crane::grpc::Completed);
    } else if (new_status == crane::grpc::Cancelled) {
      task->SetStatus(crane::grpc::Cancelled);
    } else {
      task->SetStatus(crane::grpc::Failed);
    }
  }

  task->SetExitCode(exit_code);
  task->SetEndTime(absl::Now());

  g_embedded_db_client->UpdatePersistedPartOfTask(task->TaskDbId(),
                                                  task->PersistedPart());

  for (CranedId const& craned_id : task->CranedIds()) {
    auto* stub = g_craned_keeper->GetCranedStub(craned_id);
    if (!stub->Invalid()) {
      CraneErr err = stub->ReleaseCgroupForTask(task_id, task->uid);
      if (err != CraneErr::kOk) {
        CRANE_ERROR("Failed to Release cgroup RPC for task#{} on Node {}",
                    task_id, craned_id);
      }
    }

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

  // As for now, task status change includes only
  // Pending / Running -> Completed / Failed / Cancelled.
  // It means all task status changes will put the task into mongodb,
  // so we don't have any branch code here and just put it into mongodb.

  TransferTaskToMongodb_(task.get());

  m_running_task_map_.erase(iter);
}

CraneErr TaskScheduler::TerminateRunningTaskNoLock_(TaskInCtld* task) {
  task_id_t task_id = task->TaskId();

  bool need_to_be_terminated = false;
  if (task->type == crane::grpc::Interactive) {
    auto& meta = std::get<InteractiveMetaInTask>(task->meta);
    if (!meta.has_been_terminated_on_craned) {
      meta.has_been_terminated_on_craned = true;
      need_to_be_terminated = true;
    }
  } else
    need_to_be_terminated = true;

  if (need_to_be_terminated) {
    auto stub = g_craned_keeper->GetCranedStub(task->executing_craned_id);
    return stub->TerminateTask(task_id);
  }

  return CraneErr::kOk;
}

crane::grpc::CancelTaskReply TaskScheduler::CancelPendingOrRunningTask(
    const crane::grpc::CancelTaskRequest& request) {
  crane::grpc::CancelTaskReply reply;

  uint32_t operator_uid = request.operator_uid();

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

  std::unordered_set<uint32_t> filter_task_ids_set(
      request.filter_task_ids().begin(), request.filter_task_ids().end());
  auto rng_filer_task_ids = [&](auto& it) {
    std::unique_ptr<TaskInCtld>& task = it.second;
    return request.filter_task_ids().empty() ||
           filter_task_ids_set.contains(task->TaskId());
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

  auto fn_transform_pending_task_to_id = [&](auto& it) -> task_id_t {
    task_id_t task_id = it.first;
    return task_id;
  };

  auto fn_cancel_pending_task = [&](task_id_t task_id) {
    CRANE_TRACE("Cancelling pending task #{}", task_id);

    auto task_it = m_pending_task_map_.find(task_id);
    CRANE_ASSERT(task_it != m_pending_task_map_.end());

    auto& task = task_it->second;

    if (operator_uid != 0 && task->uid != operator_uid) {
      reply.add_not_cancelled_tasks(task_id);
      reply.add_not_cancelled_reasons("Permission Denied.");
    } else {
      task->SetStatus(crane::grpc::Cancelled);
      task->SetEndTime(absl::Now());
      g_embedded_db_client->UpdatePersistedPartOfTask(task->TaskDbId(),
                                                      task->PersistedPart());
      TransferTaskToMongodb_(task.get());

      m_pending_task_map_.erase(task_it);

      reply.add_cancelled_tasks(task_id);
    }
  };

  auto fn_cancel_running_task = [&](auto& it) {
    auto task_id = it.first;
    TaskInCtld* task = it.second.get();

    CRANE_TRACE("Cancelling running task #{}", task_id);

    if (operator_uid != 0 && task->uid != operator_uid) {
      reply.add_not_cancelled_tasks(task_id);
      reply.add_not_cancelled_reasons("Permission Denied.");
    } else {
      if (task->type == crane::grpc::Batch) {
        CraneErr err = TerminateRunningTaskNoLock_(task);
        if (err == CraneErr::kOk) {
          reply.add_cancelled_tasks(task_id);
        } else {
          reply.add_not_cancelled_tasks(task_id);
          reply.add_not_cancelled_reasons(CraneErrStr(err).data());
        }
      } else {
        auto& meta = std::get<InteractiveMetaInTask>(task->meta);

        if (!meta.has_been_cancelled_on_front_end) {
          meta.has_been_cancelled_on_front_end = true;
          meta.cb_task_cancel(task_id);
        }

        reply.add_cancelled_tasks(task_id);
      }
    }
  };

  auto joined_filters = ranges::views::filter(rng_filter_state) |
                        ranges::views::filter(rng_filter_partition) |
                        ranges::views::filter(rng_filter_account) |
                        ranges::views::filter(rng_filter_task_name) |
                        ranges::views::filter(rng_filer_task_ids) |
                        ranges::views::filter(rng_filter_nodes);

  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard running_guard(&m_running_task_map_mtx_);

  auto pending_task_id_rng =
      m_pending_task_map_ | joined_filters |
      ranges::views::transform(fn_transform_pending_task_to_id);
  // Evaluate immediately. fn_cancel_pending_task will change the contents
  // of m_pending_task_map_ and invalidate the end() of pending_task_id_rng.
  auto pending_task_id_vec = ranges::to_vector(pending_task_id_rng);
  ranges::for_each(pending_task_id_vec, fn_cancel_pending_task);

  auto running_task_rng = m_running_task_map_ | joined_filters;
  ranges::for_each(running_task_rng, fn_cancel_running_task);

  return reply;
}

void TaskScheduler::QueryTasksInRam(
    const crane::grpc::QueryTasksInfoRequest* request,
    crane::grpc::QueryTasksInfoReply* response) {
  auto* task_list = response->mutable_task_info_list();

  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard running_guard(&m_running_task_map_mtx_);

  auto append_fn = [&](auto& it) {
    TaskInCtld& task = *it.second;
    auto* task_it = task_list->Add();
    task_it->set_type(task.TaskToCtld().type());
    task_it->set_task_id(task.PersistedPart().task_id());
    task_it->set_name(task.TaskToCtld().name());
    task_it->set_partition(task.TaskToCtld().partition_name());
    task_it->set_uid(task.TaskToCtld().uid());

    task_it->set_gid(task.PersistedPart().gid());
    task_it->mutable_time_limit()->set_seconds(ToInt64Seconds(task.time_limit));
    task_it->mutable_submit_time()->CopyFrom(
        task.PersistedPart().submit_time());
    task_it->mutable_start_time()->CopyFrom(task.PersistedPart().start_time());
    task_it->mutable_end_time()->CopyFrom(task.PersistedPart().end_time());
    task_it->set_account(task.account);

    task_it->set_node_num(task.node_num);
    task_it->set_cmd_line(task.TaskToCtld().cmd_line());
    task_it->set_cwd(task.TaskToCtld().cwd());
    task_it->set_username(task.Username());
    task_it->set_qos(task.qos);

    task_it->set_alloc_cpu(task.resources.allocatable_resource.cpu_count *
                           task.node_num);
    task_it->set_exit_code(0);
    task_it->set_priority(task.schedule_priority);

    task_it->set_status(task.PersistedPart().status());
    task_it->set_craned_list(
        util::HostNameListToStr(task.PersistedPart().craned_ids()));
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
               task.PersistedPart().submit_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               task.PersistedPart().submit_time() <= interval.upper_bound();
    }

    if (has_start_time_interval) {
      const auto& interval = request->filter_start_time_interval();
      valid &= !interval.has_lower_bound() ||
               task.PersistedPart().start_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               task.PersistedPart().start_time() <= interval.upper_bound();
    }

    if (has_end_time_interval) {
      const auto& interval = request->filter_end_time_interval();
      valid &= !interval.has_lower_bound() ||
               task.PersistedPart().end_time() >= interval.lower_bound();
      valid &= !interval.has_upper_bound() ||
               task.PersistedPart().end_time() <= interval.upper_bound();
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
           req_partitions.contains(task.TaskToCtld().partition_name());
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
           req_task_states.contains(task.PersistedPart().status());
  };

  auto pending_rng = m_pending_task_map_ | ranges::views::all;
  auto running_rng = m_running_task_map_ | ranges::views::all;
  auto pd_r_rng = ranges::views::concat(pending_rng, running_rng);

  int num_limit = request->num_limit() <= 0 ? kDefaultQueryTaskNumLimit
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

  ranges::for_each(filtered_rng, append_fn);
}

void MinLoadFirst::CalculateNodeSelectionInfoOfPartition_(
    const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::Time now, const PartitionId& partition_id,
    const PartitionMeta& partition_metas,
    const CranedMetaContainerInterface::CranedMetaMap& craned_meta_map,
    NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_selection_info_ref = *node_selection_info;

  for (const auto& craned_id : partition_metas.craned_ids) {
    CranedMeta const& craned_meta = craned_meta_map.at(craned_id);

    // An offline craned shouldn't be scheduled.
    if (!craned_meta.alive) continue;

    // Sort all running task in this node by ending time.
    std::vector<std::pair<absl::Time, uint32_t>> end_time_task_id_vec;

    node_selection_info_ref.task_num_node_id_map.emplace(
        craned_meta.running_task_resource_map.size(), craned_id);

    std::vector<std::string> running_task_ids_str;
    for (const auto& [task_id, res] : craned_meta.running_task_resource_map) {
      const auto& task = running_tasks.at(task_id);
      end_time_task_id_vec.emplace_back(task->StartTime() + task->time_limit,
                                        task_id);

      running_task_ids_str.emplace_back(std::to_string(task_id));
    }

    if constexpr (kAlgoTraceOutput) {
      CRANE_TRACE("Craned node {} has running tasks: {}", craned_id,
                  absl::StrJoin(running_task_ids_str, ", "));
    }

    std::sort(
        end_time_task_id_vec.begin(), end_time_task_id_vec.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    if constexpr (kAlgoTraceOutput) {
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

    // Calculate how many resources are available at [now, first task end,
    //  second task end, ...] in this node.
    auto& time_avail_res_map =
        node_selection_info_ref.node_time_avail_res_map[craned_id];

    // Insert [now, inf) interval and thus guarantee time_avail_res_map is not
    // null.
    time_avail_res_map[now] = craned_meta.res_avail;

    if constexpr (kAlgoTraceOutput) {
      CRANE_TRACE("Craned {} initial res_avail now: cpu: {}, mem: {}",
                  craned_id,
                  craned_meta.res_avail.allocatable_resource.cpu_count,
                  craned_meta.res_avail.allocatable_resource.memory_bytes);
    }

    {  // Limit the scope of `iter`
      auto cur_time_iter = time_avail_res_map.find(now);
      bool ok;
      for (auto& [end_time, task_id] : end_time_task_id_vec) {
        const auto& running_task = running_tasks.at(task_id);
        if (!time_avail_res_map.contains(end_time)) {
          /**
           * If there isn't any task that ends at the `end_time`,
           * insert an interval [end_time, inf) with the resource of
           * the previous interval for the following addition of
           * freed resources.
           * Note: Such two intervals [5,6), [6,inf) do not overlap with
           *       each other.
           */
          std::tie(cur_time_iter, ok) =
              time_avail_res_map.emplace(end_time, cur_time_iter->second);

          if constexpr (kAlgoTraceOutput) {
            CRANE_TRACE(
                "Insert duration [now+{}s, inf) with resource: "
                "cpu: {}, mem: {}",
                absl::ToInt64Seconds(end_time - now),
                craned_meta.res_avail.allocatable_resource.cpu_count,
                craned_meta.res_avail.allocatable_resource.memory_bytes);
          }
        }

        /**
         * For the situation in which multiple tasks may end at the same
         * time:
         * end_time__task_id_vec: [{now+1, 1}, {now+1, 2}, ...]
         * But we want only 1 time point in time__avail_res__map:
         * {{now+1+1: available_res(now) + available_res(1) +
         *  available_res(2)}, ...}
         */
        cur_time_iter->second += running_task->resources;

        if constexpr (kAlgoTraceOutput) {
          CRANE_TRACE("Craned {} res_avail at now + {}s: cpu: {}, mem: {}; ",
                      craned_id,
                      absl::ToInt64Seconds(cur_time_iter->first - now),
                      cur_time_iter->second.allocatable_resource.cpu_count,
                      cur_time_iter->second.allocatable_resource.memory_bytes);
        }
      }

      if constexpr (kAlgoTraceOutput) {
        std::string str;
        str.append(fmt::format("Node ({}, {}): ", partition_id, craned_id));
        auto prev_iter = time_avail_res_map.begin();
        auto iter = std::next(prev_iter);
        for (; iter != time_avail_res_map.end(); prev_iter++, iter++) {
          str.append(
              fmt::format("[ now+{}s , now+{}s ) Available allocatable "
                          "res: cpu core {}, mem {}",
                          absl::ToInt64Seconds(prev_iter->first - now),
                          absl::ToInt64Seconds(iter->first - now),
                          prev_iter->second.allocatable_resource.cpu_count,
                          prev_iter->second.allocatable_resource.memory_bytes));
        }
        str.append(
            fmt::format("[ now+{}s , inf ) Available allocatable "
                        "res: cpu core {}, mem {}",
                        absl::ToInt64Seconds(prev_iter->first - now),
                        prev_iter->second.allocatable_resource.cpu_count,
                        prev_iter->second.allocatable_resource.memory_bytes));
        CRANE_TRACE("{}", str);
      }
    }
  }
}

bool MinLoadFirst::CalculateRunningNodesAndStartTime_(
    const NodeSelectionInfo& node_selection_info,
    const PartitionMeta& partition_metas,
    const CranedMetaContainerInterface::CranedMetaMap& craned_meta_map,
    TaskInCtld* task, absl::Time now, std::list<CranedId>* craned_ids,
    absl::Time* start_time) {
  uint32_t selected_node_cnt = 0;
  std::vector<TimeSegment> intersected_time_segments;
  bool first_pass{true};

  std::list<CranedId> craned_indexes_;

  std::unordered_set<std::string> nodelist_set(
      task->TaskToCtld().nodelist().begin(),
      task->TaskToCtld().nodelist().end());
  std::unordered_set<std::string> excludes_set(
      task->TaskToCtld().excludes().begin(),
      task->TaskToCtld().excludes().end());

  auto task_num_node_id_it = node_selection_info.task_num_node_id_map.begin();
  while (selected_node_cnt < task->node_num &&
         task_num_node_id_it !=
             node_selection_info.task_num_node_id_map.end()) {
    auto craned_index = task_num_node_id_it->second;
    if (!partition_metas.craned_ids.contains(craned_index)) {
      // Todo: Performance issue! We can use cached available node set
      //  for the task when checking task validity in TaskScheduler.
      ++task_num_node_id_it;
      continue;
    }
    auto& time_avail_res_map =
        node_selection_info.node_time_avail_res_map.at(craned_index);
    auto& craned_meta = craned_meta_map.at(craned_index);

    // If any of the follow `if` is true, skip this node.
    if (!(task->resources <= craned_meta.res_total)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Task #{} needs more resource than that of craned {}. "
            "Skipping this craned.",
            task->TaskId(), craned_index);
      }
    } else if (!task->TaskToCtld().nodelist().empty() &&
               !nodelist_set.contains(craned_index)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE(
            "Craned {} is not in the nodelist of task #{}. "
            "Skipping this craned.",
            craned_index, task->TaskId());
      }
    } else if (!task->TaskToCtld().excludes().empty() &&
               excludes_set.contains(craned_index)) {
      if constexpr (kAlgoTraceOutput) {
        CRANE_TRACE("Task #{} excludes craned {}. Skipping this craned.",
                    task->TaskId(), craned_index);
      }
    } else {
      craned_indexes_.emplace_back(craned_index);
      ++selected_node_cnt;
    }
    ++task_num_node_id_it;
  }

  if (selected_node_cnt < task->node_num) return false;
  CRANE_ASSERT_MSG(selected_node_cnt == task->node_num,
                   "selected_node_cnt != task->node_num");

  for (CranedId craned_id : craned_indexes_) {
    if constexpr (kAlgoTraceOutput) {
      CRANE_TRACE("Find valid time segments for task #{} on craned {}",
                  task->TaskId(), craned_id);
    }

    auto& time_avail_res_map =
        node_selection_info.node_time_avail_res_map.at(craned_id);
    auto& node_meta = craned_meta_map.at(craned_id);

    // Find all valid time segments in this node for this task.
    // The expected start time must exist because all tasks in
    // pending_task_map can be run under the amount of all resources in this
    // node. At some future time point, all tasks will end and this pending
    // task can eventually be run because the total resource of all the nodes
    // in `craned_indexes` >= the resource required by the task.
    auto res_it = time_avail_res_map.begin();

    std::vector<TimeSegment> time_segments;
    absl::Duration valid_duration;
    absl::Time expected_start_time;

    // Figure out in this craned node, which time segments have sufficient
    // resource to run the task.
    // For example, if task needs 3 cpu cores, time_avail_res_map is:
    // [1, 3], [5, 6], [7, 2], [9, 6] | Format: [start_time, cores]
    // Then the valid time segments are:
    // [1,6], [9, inf] | Format: [start_time, duration]
    bool trying = true;
    while (true) {
      if (trying) {
        if (task->resources <= res_it->second) {
          trying = false;
          expected_start_time = res_it->first;

          if (std::next(res_it) == time_avail_res_map.end()) {
            valid_duration = absl::InfiniteDuration();
            time_segments.emplace_back(expected_start_time, valid_duration);
            break;
          } else {
            valid_duration = std::next(res_it)->first - res_it->first;
            res_it++;
            continue;
          }
        } else {
          if (++res_it == time_avail_res_map.end()) break;
          continue;
        }
      } else {
        if (task->resources <= res_it->second) {
          if (std::next(res_it) == time_avail_res_map.end()) {
            valid_duration = absl::InfiniteDuration();
            time_segments.emplace_back(expected_start_time, valid_duration);
            break;
          } else {
            valid_duration += std::next(res_it)->first - res_it->first;
            res_it++;
            continue;
          }
        } else {
          trying = true;
          time_segments.emplace_back(expected_start_time, valid_duration);
          res_it++;
          continue;
        }
      }
    }

    // Now we have the valid time segments for this node. Find the
    // intersection with the set in the previous pass.
    if (first_pass) {
      intersected_time_segments = std::move(time_segments);
      first_pass = false;

      if constexpr (kAlgoTraceOutput) {
        std::vector<std::string> valid_seg_str;
        for (auto& seg : intersected_time_segments) {
          valid_seg_str.emplace_back(
              fmt::format("[start: {}, duration: {}]",
                          absl::ToInt64Seconds(seg.start - now),
                          absl::ToInt64Seconds(seg.duration)));
        }
        CRANE_TRACE("After looping craned {}, valid time segments: {}",
                    craned_id, absl::StrJoin(valid_seg_str, ", "));
      }
    } else {
      std::vector<TimeSegment> new_intersected_time_segments;

      for (auto&& seg : time_segments) {
        absl::Time start = seg.start;
        absl::Time end = seg.start + seg.duration;
        // Segment: [start, end)
        // e.g. segment.start=5, segment.duration=1s => [5,6)

        // Find the first time point that >= seg.start + seg.duration
        auto it2 = std::lower_bound(intersected_time_segments.begin(),
                                    intersected_time_segments.end(), end);

        // If it2 == intersected_time_segments.begin(), this time segment has
        // no overlap with any time segment in intersected_time_segments. Just
        // skip it.
        //               it2
        //                V
        //  seg           *------*    *----* .... intersected_time_segments
        // *----------*
        //            ^
        //           end
        //
        // Do nothing under such situation.

        if (it2 == intersected_time_segments.begin()) {
          continue;
        } else {
          it2 = std::prev(it2);
          // it2 now looks like
          //   *-----------* seg
          // (.......)   *--------* *---* intersected_time_segments
          //            ^
          //           it2 (the last time segment to do intersection)

          // Find the first segment in `intersected_time_segments`
          // whose end >= seg.start.
          // Note: If end == seg.start, there's no intersection.
          //
          // We first find a segment (it1) in `intersected_time_segments`
          // whose start < seg.start and is closet to seg.start...
          // There may be 2 situation:
          // 1.
          //     start
          //       V
          //       *-------* seg
          //  *--------* *----*
          //             ^
          //            it1 ( != intersected_time_segment.end()  )
          //
          // 2.
          //   *-------* seg         *-------* seg
          //   *--*             or     *---*
          //   ^                       ^
          //  it1                     it1 ( ==
          //  intersected_time_segment.begin())
          auto it1 = std::upper_bound(intersected_time_segments.begin(),
                                      intersected_time_segments.end(), start);
          if (it1 != intersected_time_segments.begin())
          // If it1 == intersected_time_segments.begin(), there is no time
          // segment that starts previous to seg.start but there are some
          // segments immediately after seg.start. The first one of them is
          // the beginning of intersected_time_segments.
          //   begin()        it2
          //                   V
          //   *---*           *----*
          // *-------------------*
          // ^
          // start
          {
            // If there is an overlap between first segment and `seg`, take
            // the intersection. std::prev(it1)                 it1 V V
            // *----------------------------*  *--------*
            //         *-----------------------------*
            //         |<-intersected part->|
            //         ^
            //      start
            it1 = std::prev(it1);
            if (it1->start + it1->duration > start) {
              // Note: If start == seg.start, there's no intersection.
              new_intersected_time_segments.emplace_back(
                  start, it1->start + it1->duration - start);
            }
          }

          //           |<--    range  -->|
          // it1   it should start here     it2
          // v          v                    v
          // *~~~~~~~*  *----*  *--------*   *~~~~~~~~~~~~*
          //       *--------------------------------*
          // If std::distance(it1, it2) >= 2, there are other segments
          //  between it1 and it2.
          if (std::distance(it1, it2) >= 2) {
            auto it = std::next(it1);
            do {
              new_intersected_time_segments.emplace_back(it->start,
                                                         it->duration);
              ++it;
            } while (it != it2);
          }

          // the last insertion handles the following 2 situations.
          //                                        it2
          // *~~~~~~~*  *~~~~~~~*  *~~~~~~~~*   *------------*
          //       *--------------------------------*
          // OR
          //                                      it2
          // *~~~~~~~*  *~~~~~~~*  *~~~~~~~~*   *------*
          //       *-------------------------------------*
          //
          // The following situation will NOT happen.
          //                                                 it2
          // *~~~~~~~*  *~~~~~~~*  *~~~~~~~~*              *------*
          //       *-------------------------------------*
          if (std::distance(it1, it2) >= 1)
            new_intersected_time_segments.emplace_back(
                it2->start, std::min(it2->duration, end - it2->start));
        }
      }

      intersected_time_segments = std::move(new_intersected_time_segments);

      if constexpr (kAlgoTraceOutput) {
        std::vector<std::string> valid_seg_str;
        for (auto& seg : intersected_time_segments) {
          valid_seg_str.emplace_back(
              fmt::format("[start: {}, duration: {}]",
                          absl::ToInt64Seconds(seg.start - now),
                          absl::ToInt64Seconds(seg.duration)));
        }
        CRANE_TRACE("After looping craned {}, valid time segments: {}",
                    craned_id, absl::StrJoin(valid_seg_str, ", "));
      }
    }
  }

  *craned_ids = std::move(craned_indexes_);

  // Calculate the earliest start time
  for (auto&& seg : intersected_time_segments) {
    if (task->time_limit <= seg.duration) {
      *start_time = seg.start;
      return true;
    }
  }

  return false;
}

void MinLoadFirst::NodeSelect(
    const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  auto all_partitions_meta_map = g_meta_container->GetAllPartitionsMetaMapPtr();
  auto craned_meta_map = g_meta_container->GetCranedMetaMapPtr();

  std::unordered_map<PartitionId, NodeSelectionInfo> part_id_node_info_map;

  // Truncated by 1s.
  // We use the time now as the base time across the whole algorithm.
  absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

  // Calculate NodeSelectionInfo for all partitions
  for (auto& [partition_id, partition_metas] : *all_partitions_meta_map) {
    NodeSelectionInfo& node_info_in_a_partition =
        part_id_node_info_map[partition_id];
    CalculateNodeSelectionInfoOfPartition_(running_tasks, now, partition_id,
                                           partition_metas, *craned_meta_map,
                                           &node_info_in_a_partition);
  }

  std::vector<task_id_t> task_id_vec;
  task_id_vec = m_priority_sorter_->GetOrderedTaskIdList(*pending_task_map,
                                                         running_tasks);
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

    NodeSelectionInfo& node_info = part_id_node_info_map[part_id];
    auto& part_meta = all_partitions_meta_map->at(part_id);

    std::list<CranedId> craned_ids;
    absl::Time expected_start_time;

    // Note! ok should always be true.
    bool ok = CalculateRunningNodesAndStartTime_(
        node_info, part_meta, *craned_meta_map, task.get(), now, &craned_ids,
        &expected_start_time);
    if (!ok) {
      continue;
    }

    if constexpr (kAlgoTraceOutput) {
      CRANE_TRACE(
          "\t task #{} ExpectedStartTime=now+{}s, EndTime=now+{}s",
          task->TaskId(), absl::ToInt64Seconds(expected_start_time - now),
          absl::ToInt64Seconds(expected_start_time + task->time_limit - now));
    }

    // The start time and craned ids have been determined.
    // Modify the corresponding NodeSelectionInfo now.
    // Note: Since a craned node may belong to multiple partition,
    //       the NodeSelectionInfo of all the partitions the craned node
    //       belongs to should be modified!

    std::unordered_map<PartitionId, std::list<CranedId>> involved_part_craned;
    for (CranedId const& craned_id : craned_ids) {
      CranedMeta const& craned_meta = craned_meta_map->at(craned_id);
      for (PartitionId const& partition_id :
           craned_meta.static_meta.partition_ids) {
        involved_part_craned[partition_id].emplace_back(craned_id);
      }
    }

    for (const auto& [partition_id, part_craned_ids] : involved_part_craned) {
      SubtractTaskResourceNodeSelectionInfo_(
          expected_start_time, task->time_limit, task->resources,
          part_craned_ids, &part_id_node_info_map.at(partition_id));
    }

    if (expected_start_time == now) {
      // The task can be started now.
      task->SetStartTime(expected_start_time);

      // We leave the change in running_tasks to the scheduling thread
      // caller for the simplicity of selection algorithm. However, the
      // resource used by the task MUST be subtracted now because a craned
      // node may belong to multiple partitions. The resource usage MUST
      // takes effect right now. Otherwise, during the scheduling for the
      // next partition, the algorithm may use the resource which is already
      // allocated.
      for (CranedId const& craned_id : craned_ids)
        g_meta_container->MallocResourceFromNode(craned_id, task->TaskId(),
                                                 task->resources);

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
      // The task can't be started now. Move to the next pending task.
      continue;
    }
  }
}

void MinLoadFirst::SubtractTaskResourceNodeSelectionInfo_(
    const absl::Time& expected_start_time, const absl::Duration& duration,
    const Resources& resources, std::list<CranedId> const& craned_ids,
    MinLoadFirst::NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_info = *node_selection_info;
  bool ok;

  for (CranedId const& craned_id : craned_ids) {
    // Increase the running task num in Craned `crane_id`.
    for (auto it = node_info.task_num_node_id_map.begin();
         it != node_info.task_num_node_id_map.end(); ++it) {
      if (it->second == craned_id) {
        uint32_t num_task = it->first + 1;
        node_info.task_num_node_id_map.erase(it);
        node_info.task_num_node_id_map.emplace(num_task, craned_id);
      }
    }

    TimeAvailResMap& time_avail_res_map =
        node_info.node_time_avail_res_map[craned_id];

    absl::Time task_end_time = expected_start_time + duration;

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
        CRANE_ASSERT(resources <= task_duration_begin_it->second);
        task_duration_begin_it->second -= resources;
      } else {
        // Situation #2
        std::tie(inserted_it, ok) = time_avail_res_map.emplace(
            expected_start_time, task_duration_begin_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        CRANE_ASSERT(resources <= inserted_it->second);
        inserted_it->second -= resources;
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
      auto task_duration_end_it =
          std::prev(time_avail_res_map.upper_bound(task_end_time));

      if (task_duration_begin_it->first != expected_start_time) {
        // Situation #3 (begin)
        TimeAvailResMap::iterator inserted_it;
        std::tie(inserted_it, ok) = time_avail_res_map.emplace(
            expected_start_time, task_duration_begin_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        CRANE_ASSERT(resources <= inserted_it->second);
        inserted_it->second -= resources;
        task_duration_begin_it = std::next(inserted_it);
      }

      // Subtract the required resources within the interval.
      for (auto in_duration_it = task_duration_begin_it;
           in_duration_it != task_duration_end_it; in_duration_it++) {
        CRANE_ASSERT(resources <= in_duration_it->second);
        in_duration_it->second -= resources;
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

        CRANE_ASSERT(resources <= task_duration_end_it->second);
        task_duration_end_it->second -= resources;
      }
    }
  }
}

void TaskScheduler::TransferTaskToMongodb_(TaskInCtld* task) {
  bool ok;
  ok = g_embedded_db_client->MovePendingOrRunningTaskToEnded(task->TaskDbId());
  if (!ok) {
    CRANE_ERROR(
        "Failed to call "
        "g_embedded_db_client->MovePendingOrRunningTaskToEnded() for task "
        "#{}",
        task->TaskId());
  }

  if (!g_db_client->InsertJob(task)) {
    CRANE_ERROR("Failed to call g_db_client->InsertJob() for task #{}",
                task->TaskId());
  }

  ok = g_embedded_db_client->PurgeTaskFromEnded(task->TaskDbId());
  if (!ok) {
    CRANE_ERROR(
        "Failed to call "
        "g_embedded_db_client->PurgeTaskFromEnded() for task #{}",
        task->TaskId());
  }
}

CraneErr TaskScheduler::AcquireTaskAttributes(TaskInCtld* task) {
  {
    auto qos_ptr = g_account_manager->GetExistedQosInfo(task->qos);
    if (!qos_ptr) return CraneErr::kInvalidParam;

    task->qos_priority = qos_ptr->priority;
  }

  auto part_it = g_config.Partitions.find(task->partition_id);
  if (part_it == g_config.Partitions.end()) return CraneErr::kInvalidParam;

  task->partition_priority = part_it->second.priority;

  return CraneErr::kOk;
}

CraneErr TaskScheduler::CheckTaskValidity(TaskInCtld* task) {
  // Check whether the selected partition is able to run this task.
  auto metas_ptr = g_meta_container->GetPartitionMetasPtr(task->partition_id);

  // Check whether the selected partition is able to run this task.
  if (!(task->resources <=
        metas_ptr->partition_global_meta.m_resource_total_)) {
    CRANE_TRACE(
        "Resource not enough for task #{}. "
        "Partition total: cpu {}, mem: {}, mem+sw: {}",
        task->TaskId(),
        metas_ptr->partition_global_meta.m_resource_total_.allocatable_resource
            .cpu_count,
        util::ReadableMemory(metas_ptr->partition_global_meta.m_resource_total_
                                 .allocatable_resource.memory_bytes),
        util::ReadableMemory(metas_ptr->partition_global_meta.m_resource_total_
                                 .allocatable_resource.memory_sw_bytes));
    return CraneErr::kNoResource;
  }

  std::unordered_set<std::string> excluded_nodes(
      task->TaskToCtld().excludes().begin(),
      task->TaskToCtld().excludes().end());

  std::unordered_set<std::string> included_nodes(
      task->TaskToCtld().nodelist().begin(),
      task->TaskToCtld().nodelist().end());

  std::unordered_set<std::string> avail_nodes;
  {
    auto craned_meta_map = g_meta_container->GetCranedMetaMapPtr();

    for (auto craned_id : metas_ptr->craned_ids) {
      auto craned_meta = craned_meta_map->at(craned_id);
      if (craned_meta.alive && task->resources <= craned_meta.res_total &&
          (included_nodes.empty() || included_nodes.contains(craned_id)) &&
          (excluded_nodes.empty() || !excluded_nodes.contains(craned_id)))
        avail_nodes.emplace(craned_meta.static_meta.hostname);

      if (avail_nodes.size() >= task->node_num) break;
    }
  }

  if (task->node_num > avail_nodes.size()) {
    CRANE_TRACE(
        "Resource not enough. Task #{} needs {} nodes, while only {} "
        "nodes satisfy its requirement.",
        task->TaskId(), task->node_num, avail_nodes.size());
    return CraneErr::kInvalidNodeNum;
  }

  return CraneErr::kOk;
}

void TaskScheduler::TerminateTasksOnCraned(const CranedId& craned_id) {
  CRANE_TRACE("Terminate tasks on craned {}", craned_id);

  // The order of LockGuards matters.
  LockGuard running_guard(&m_running_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  auto it = m_node_to_tasks_map_.find(craned_id);
  if (it != m_node_to_tasks_map_.end()) {
    // m_node_to_tasks_map_[craned_id] will be cleaned in
    // TaskStatusChangeNoLock_. Do not clean it here and make a copy of
    // it->second.
    std::vector<task_id_t> task_ids(it->second.begin(), it->second.end());

    for (task_id_t task_id : task_ids)
      TaskStatusChangeNoLock_(task_id, craned_id,
                              crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeTerminal);
  } else {
    CRANE_TRACE("No task is executed by craned {}. Ignore cleaning step...",
                craned_id);
  }
}

std::vector<task_id_t> MultiFactorPriority::GetOrderedTaskIdList(
    const OrderedTaskMap& pending_task_map,
    const UnorderedTaskMap& running_task_map) {
  std::vector<task_id_t> task_id_vec;

  CalculateFactorBound_(pending_task_map, running_task_map);

  std::vector<std::pair<task_id_t, uint32_t>> task_priority_vec;
  for (const auto& [task_id, task] : pending_task_map) {
    uint32_t priority = CalculatePriority_(task.get());
    task_priority_vec.emplace_back(task->TaskId(), priority);
  }

  std::sort(task_priority_vec.begin(), task_priority_vec.end(),
            [](const std::pair<task_id_t, uint32_t>& a,
               const std::pair<task_id_t, uint32_t>& b) {
              return a.second > b.second;
            });

  for (auto& pair : task_priority_vec) task_id_vec.emplace_back(pair.first);

  return task_id_vec;
}

void MultiFactorPriority::CalculateFactorBound_(
    const OrderedTaskMap& pending_task_map,
    const UnorderedTaskMap& running_task_map) {
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
    uint64_t age = ToInt64Seconds((absl::Now() - task->SubmitTime()));
    age = std::min(age, g_config.PriorityConfig.MaxAge);

    bound.age_min = std::min(age, bound.age_min);
    bound.age_max = std::max(age, bound.age_max);

    uint32_t nodes_alloc = task->node_num;
    bound.nodes_alloc_min = std::min(nodes_alloc, bound.nodes_alloc_min);
    bound.nodes_alloc_max = std::max(nodes_alloc, bound.nodes_alloc_max);

    uint64_t mem_alloc = task->resources.allocatable_resource.memory_bytes;
    bound.mem_alloc_min = std::min(mem_alloc, bound.mem_alloc_min);
    bound.mem_alloc_max = std::max(mem_alloc, bound.mem_alloc_max);

    double cpus_alloc = task->resources.allocatable_resource.cpu_count;
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
    uint32_t service_val = 0;
    if (bound.cpus_alloc_max != bound.cpus_alloc_min)
      service_val += 1.0 *
                     (task->resources.allocatable_resource.cpu_count -
                      bound.cpus_alloc_min) /
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
      service_val += 1.0 *
                     (task->resources.allocatable_resource.memory_bytes -
                      bound.mem_alloc_min) /
                     (bound.mem_alloc_max - bound.mem_alloc_min);
    else
      service_val += 1.0;

    uint64_t run_time = ToInt64Seconds(absl::Now() - task->StartTime());
    bound.acc_service_val_map[task->account] += service_val * run_time;
  }

  for (const auto& [acc_name, ser_val] : bound.acc_service_val_map) {
    bound.service_val_min = std::min(ser_val, bound.service_val_min);
    bound.service_val_max = std::max(ser_val, bound.service_val_max);
  }
}

uint32_t MultiFactorPriority::CalculatePriority_(Ctld::TaskInCtld* task) {
  FactorBound& bound = m_factor_bound_;

  uint64_t task_age =
      ToUnixSeconds(absl::Now()) - task->SubmitTimeInUnixSecond();
  task_age = std::min(task_age, g_config.PriorityConfig.MaxAge);

  uint32_t task_qos_priority = task->qos_priority;
  uint32_t task_part_priority = task->partition_priority;
  uint32_t task_nodes_alloc = task->node_num;
  uint64_t task_mem_alloc = task->resources.allocatable_resource.memory_bytes;
  double task_cpus_alloc = task->resources.allocatable_resource.cpu_count;
  uint32_t task_service_val = bound.acc_service_val_map.at(task->account);

  double qos_factor{0};
  double age_factor{0};
  double partition_factor{0};
  double job_size_factor{0};
  double fair_share_factor{0};

  // age_factor
  if (bound.age_max != bound.age_min)
    age_factor =
        1.0 * (task_age - bound.age_min) / (bound.age_max - bound.age_min);

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
    job_size_factor += 1.0 * (task_mem_alloc - bound.mem_alloc_min) /
                       (bound.mem_alloc_max - bound.mem_alloc_min);
  if (g_config.PriorityConfig.FavorSmall)
    job_size_factor = 1 - job_size_factor / 3;
  else
    job_size_factor /= 3;

  // fair_share_factor
  if (bound.service_val_max != bound.service_val_min)
    fair_share_factor =
        1.0 - (task_service_val - bound.service_val_min) /
                  (bound.service_val_max - bound.service_val_min);

  uint32_t priority =
      g_config.PriorityConfig.WeightAge * age_factor +
      g_config.PriorityConfig.WeightPartition * partition_factor +
      g_config.PriorityConfig.WeightJobSize * job_size_factor +
      g_config.PriorityConfig.WeightFairShare * fair_share_factor +
      g_config.PriorityConfig.WeightQOS * qos_factor;

  task->schedule_priority = priority;

  return priority;
}

}  // namespace Ctld
