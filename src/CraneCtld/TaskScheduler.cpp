#include "TaskScheduler.h"

#include <google/protobuf/util/time_util.h>

#include "CranedKeeper.h"
#include "CtldGrpcServer.h"
#include "EmbeddedDbClient.h"

namespace Ctld {

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

      CRANE_TRACE("Restore task #{} from embedded running queue.",
                  task->TaskId());

      auto* stub = g_craned_keeper->GetCranedStub(task->executing_craned_id);
      if (stub == nullptr || stub->Invalid()) {
        CRANE_INFO(
            "The execution node of the restore task #{} is down. "
            "Requeue it to the pending queue.",
            task_id);
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

            if (status != crane::grpc::Finished) {
              // Check whether the task is orphaned on the allocated nodes
              // in case that when processing TaskStatusChange CraneCtld
              // crashed, only part of Craned nodes executed TerminateTask gRPC.
              // Not needed for succeeded tasks.
              for (CranedId craned_id : task->CranedIds()) {
                stub = g_craned_keeper->GetCranedStub(craned_id);
                if (stub != nullptr && !stub->Invalid())
                  stub->TerminateOrphanedTask(task->TaskId());
              }
            }

            // For both succeeded and failed tasks, cgroup for them should be
            // released. Though some craned nodes might have released the
            // cgroup, just resend the gRPC again to guarantee that the cgroup
            // is always released.
            for (CranedId craned_id : task->CranedIds()) {
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
          // It means that the result of task is lost.
          // Requeue the task.
          CRANE_TRACE(
              "Task id #{} can't be found cannot be found in its exec craned. "
              "Move it to pending queue and re-run it.",
              task_id);
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

      err = TryRequeueRecoveredTaskIntoPendingQueueLock_(std::move(task));
      if (err != CraneErr::kOk) {
        // Failed to requeue the task into pending queue due to
        // insufficient resource or other reasons. Mark it as FAILED and move
        // it to
        CRANE_INFO(
            "Failed to requeue task #{}. Mark it as FAILED and "
            "move it to the ended queue.",
            task_id);
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

CraneErr TaskScheduler::TryRequeueRecoveredTaskIntoPendingQueueLock_(
    std::unique_ptr<TaskInCtld> task) {
  // The order of LockGuards matters.
  LockGuard pending_guard(&m_pending_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  CraneErr err;

  // task->partition_id will be set in this call.
  err = CheckTaskValidityAndAcquireAttrs_(task.get());
  if (err != CraneErr::kOk) return err;

  m_partition_to_tasks_map_[task->partition_id].emplace(task->TaskId());
  m_pending_task_map_.emplace(task->TaskId(), std::move(task));

  return CraneErr::kOk;
}

void TaskScheduler::PutRecoveredTaskIntoRunningQueueLock_(
    std::unique_ptr<TaskInCtld> task) {
  // The order of LockGuards matters.
  LockGuard running_guard(&m_running_task_map_mtx_);
  LockGuard indexes_guard(&m_task_indexes_mtx_);

  for (CranedId craned_id : task->CranedIds()) {
    g_meta_container->MallocResourceFromNode(craned_id, task->TaskId(),
                                             task->resources);
    m_node_to_tasks_map_[craned_id].emplace(task->TaskId());
  }

  m_partition_to_tasks_map_[task->partition_id].emplace(task->TaskId());
  m_running_task_map_.emplace(task->TaskId(), std::move(task));
}

void TaskScheduler::ScheduleThread_() {
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

      auto all_part_metas = g_meta_container->GetAllPartitionsMetaMapPtr();
      auto craned_meta_map = g_meta_container->GetCranedMetaMapPtr();
      std::list<INodeSelectionAlgo::NodeSelectionResult> selection_result_list;

      m_node_selection_algo_->NodeSelect(
          *all_part_metas, *craned_meta_map, m_running_task_map_,
          &m_pending_task_map_, &selection_result_list);
      m_running_task_map_mtx_.Unlock();
      m_pending_task_map_mtx_.Unlock();

      for (auto& it : selection_result_list) {
        auto& task = it.first;
        PartitionId const& partition_id = task->partition_id;

        task->SetStatus(crane::grpc::TaskStatus::Running);
        task->SetCranedIds(std::move(it.second));
        task->nodes_alloc = task->CranedIds().size();

        for (CranedId craned_id : task->CranedIds()) {
          auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);

          g_meta_container->MallocResourceFromNode(craned_id, task->TaskId(),
                                                   task->resources);

          if (task->type == crane::grpc::Interactive) {
            InteractiveTaskAllocationDetail detail{
                .craned_id = craned_id,
                .ipv4_addr = craned_meta->static_meta.hostname,
                .port = craned_meta->static_meta.port,
                .resource_uuid = m_uuid_gen_(),
            };

            std::get<InteractiveMetaInTask>(task->meta).resource_uuid =
                detail.resource_uuid;

            g_ctld_server->AddAllocDetailToIaTask(task->TaskId(),
                                                  std::move(detail));
          }

          m_task_indexes_mtx_.Lock();
          m_node_to_tasks_map_[craned_id].emplace(task->TaskId());
          m_task_indexes_mtx_.Unlock();
        }

        task->allocated_craneds_regex =
            util::HostNameListToStr(task->CranedIds());

        // For the task whose --node > 1, only execute the command at the first
        // allocated node.
        CranedId const& first_node_id = task->CranedIds().front();
        task->executing_craned_id = first_node_id;

        for (CranedId const& craned_id : task->CranedIds()) {
          CranedStub* stub = g_craned_keeper->GetCranedStub(craned_id);
          CRANE_TRACE("Send CreateCgroupForTask for task #{} to {}",
                      task->TaskId(), craned_id);
          stub->CreateCgroupForTask(task->TaskId(), task->uid);
        }

        auto* task_ptr = task.get();

        // IMPORTANT: task must be put into running_task_map before any
        //  time-consuming operation, otherwise TaskStatusChange RPC will come
        //  earlier before task is put into running_task_map.
        m_running_task_map_mtx_.Lock();
        m_running_task_map_.emplace(task->TaskId(), std::move(task));
        m_running_task_map_mtx_.Unlock();

        g_embedded_db_client->UpdatePersistedPartOfTask(
            task_ptr->TaskDbId(), task_ptr->PersistedPart());

        // RPC is time-consuming.
        CranedStub* node_stub = g_craned_keeper->GetCranedStub(first_node_id);
        node_stub->ExecuteTask(task_ptr);

        bool ok = g_embedded_db_client->MoveTaskFromPendingToRunning(
            task_ptr->TaskDbId());
        if (!ok) {
          CRANE_ERROR(
              "Failed to call "
              "g_embedded_db_client->MoveTaskFromPendingToRunning({})",
              task_ptr->TaskDbId());
        }
      }
      // all_part_metas is unlocked here.
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

  // task->partition_id will be set in this call.
  err = CheckTaskValidityAndAcquireAttrs_(task.get());
  if (err != CraneErr::kOk) return err;

  // Add the task to the pending task queue.
  task->SetStatus(crane::grpc::Pending);

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
    g_ctld_server->RemoveAllocDetailOfIaTask(task_id);
  }

  CRANE_DEBUG("TaskStatusChange: Task #{} {}->{} In Node {}", task_id,
              task->Status(), new_status, craned_index);

  if (new_status == crane::grpc::Finished) {
    task->SetStatus(crane::grpc::Finished);
  } else if (new_status == crane::grpc::Cancelled) {
    task->SetStatus(crane::grpc::Cancelled);
  } else {
    task->SetStatus(crane::grpc::Failed);
  }

  task->SetExitCode(exit_code);
  task->SetEndTime(absl::Now());

  g_embedded_db_client->UpdatePersistedPartOfTask(task->TaskDbId(),
                                                  task->PersistedPart());

  for (CranedId const& craned_id : task->CranedIds()) {
    g_meta_container->FreeResourceFromNode(craned_id, task_id);

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

  // As for now, task status change includes only
  // Pending / Running -> Finished / Failed / Cancelled.
  // It means all task status changes will put the task into mongodb,
  // so we don't have any branch code here and just put it into mongodb.

  TransferTaskToMongodb_(task.get());

  m_running_task_map_.erase(iter);
}

bool TaskScheduler::QueryCranedIdOfRunningTaskNoLock_(uint32_t task_id,
                                                      CranedId* node_id) {
  auto iter = m_running_task_map_.find(task_id);
  if (iter == m_running_task_map_.end()) return false;

  *node_id = iter->second->executing_craned_id;

  return true;
}

CraneErr TaskScheduler::TerminateRunningTaskNoLock_(uint32_t task_id) {
  CranedId node_id;
  bool ok = QueryCranedIdOfRunningTaskNoLock_(task_id, &node_id);

  if (ok) {
    auto stub = g_craned_keeper->GetCranedStub(node_id);
    return stub->TerminateTask(task_id);
  } else
    return CraneErr::kNonExistent;
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
           task->Account() == request.filter_account();
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
    std::unique_ptr<TaskInCtld>& task = it.second;

    CRANE_TRACE("Cancelling running task #{}", task_id);

    if (operator_uid != 0 && task->uid != operator_uid) {
      reply.add_not_cancelled_tasks(task_id);
      reply.add_not_cancelled_reasons("Permission Denied.");
    } else {
      CraneErr err = TerminateRunningTaskNoLock_(task_id);
      if (err == CraneErr::kOk) {
        reply.add_cancelled_tasks(task_id);
      } else {
        reply.add_not_cancelled_tasks(task_id);
        if (err == CraneErr::kNonExistent)
          reply.add_not_cancelled_reasons("Task id doesn't exist!");
        else
          reply.add_not_cancelled_reasons(CraneErrStr(err).data());
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
    task_it->mutable_time_limit()->CopyFrom(task.TaskToCtld().time_limit());
    task_it->mutable_start_time()->CopyFrom(task.PersistedPart().start_time());
    task_it->mutable_end_time()->CopyFrom(task.PersistedPart().end_time());
    task_it->set_account(task.PersistedPart().account());

    task_it->set_node_num(task.TaskToCtld().node_num());
    task_it->set_cmd_line(task.TaskToCtld().cmd_line());
    task_it->set_cwd(task.TaskToCtld().cwd());

    task_it->set_alloc_cpus(task.resources.allocatable_resource.cpu_count);
    task_it->set_exit_code(0);

    task_it->set_status(task.PersistedPart().status());
    task_it->set_craned_list(
        util::HostNameListToStr(task.PersistedPart().craned_ids()));
  };

  auto pending_rng = m_pending_task_map_ | ranges::views::all;
  auto running_rng = m_running_task_map_ | ranges::views::all;
  auto pd_r_rng = ranges::views::concat(pending_rng, running_rng);

  if (!request->partition().empty() || request->task_id() != -1) {
    auto pd_r_filtered_rng =
        pd_r_rng | ranges::views::filter([&](auto& it) -> bool {
          std::unique_ptr<TaskInCtld>& task = it.second;
          bool res = true;
          if (!request->partition().empty()) {
            res &= task->partition_id == request->partition();
          }
          if (request->task_id() != -1) {
            res &= task->TaskId() == request->task_id();
          }
          return res;
        });
    ranges::for_each(pd_r_filtered_rng, append_fn);
  } else {
    ranges::for_each(pd_r_rng, append_fn);
  }
}

void MinLoadFirst::CalculateNodeSelectionInfo_(
    const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::Time now, const PartitionId& partition_id,
    const PartitionMeta& partition_metas,
    const CranedMetaContainerInterface::CranedMetaMap& craned_meta_map,
    const CranedId& craned_id, const CranedMeta& node_meta,
    NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_selection_info_ref = *node_selection_info;

  node_selection_info_ref.task_num_node_id_map.emplace(
      node_meta.running_task_resource_map.size(), craned_id);

  // Sort all running task in this node by ending time.
  std::vector<std::pair<absl::Time, uint32_t>> end_time_task_id_vec;
  for (const auto& [task_id, running_task] : running_tasks) {
    if (running_task->partition_id == partition_id &&
        std::count(running_task->CranedIds().begin(),
                   running_task->CranedIds().end(), craned_id) > 0)
      end_time_task_id_vec.emplace_back(
          running_task->StartTime() + running_task->time_limit, task_id);
  }
  std::sort(
      end_time_task_id_vec.begin(), end_time_task_id_vec.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

#ifndef NDEBUG
  {
    if (!end_time_task_id_vec.empty()) {
      std::string str;
      str.append(fmt::format("Node ({}, {}): ", partition_id, craned_id));
      for (auto [end_time, task_id] : end_time_task_id_vec) {
        str.append(fmt::format("Task #{} ends after {}s ", task_id,
                               absl::ToInt64Seconds(end_time - now)));
      }
      CRANE_TRACE("{}", str);
    }
  }
#endif

  // Calculate how many resources are available at [now, first task end,
  //  second task end, ...] in this node.
  auto& time_avail_res_map =
      node_selection_info_ref.node_time_avail_res_map[craned_id];

  // Insert [now, inf) interval and thus guarantee time_avail_res_map is not
  // null.
  time_avail_res_map[now] = node_meta.res_avail;

  {  // Limit the scope of `iter`
    auto prev_time_iter = time_avail_res_map.find(now);
    bool ok;
    for (auto& [end_time, task_id] : end_time_task_id_vec) {
      const auto& running_task = running_tasks.at(task_id);
      if (time_avail_res_map.count(end_time + absl::Seconds(1)) == 0) {
        /**
         * For the situation in which multiple tasks may end at the same
         * time:
         * end_time__task_id_vec: [{now+1, 1}, {now+1, 2}, ...]
         * But we want only 1 time point in time__avail_res__map:
         * {{now+1+1: available_res(now) + available_res(1) +
         *  available_res(2)}, ...}
         */
        std::tie(prev_time_iter, ok) = time_avail_res_map.emplace(
            end_time + absl::Seconds(1), prev_time_iter->second);
      }

      time_avail_res_map[end_time + absl::Seconds(1)] +=
          running_task->resources;
    }
#ifndef NDEBUG
    {
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
#endif
  }
}

bool MinLoadFirst::CalculateRunningNodesAndStartTime_(
    const NodeSelectionInfo& node_selection_info,
    const PartitionMeta& partition_metas,
    const CranedMetaContainerInterface::CranedMetaMap& craned_meta_map,
    const TaskInCtld* task, absl::Time now, std::list<CranedId>* craned_ids,
    absl::Time* start_time) {
  uint32_t selected_node_cnt = 0;
  auto task_num_node_id_it = node_selection_info.task_num_node_id_map.begin();
  std::vector<TimeSegment> intersected_time_segments;
  bool first_pass{true};

  std::list<CranedId> craned_indexes_;
  while (selected_node_cnt < task->node_num &&
         task_num_node_id_it !=
             node_selection_info.task_num_node_id_map.end()) {
    auto craned_index = task_num_node_id_it->second;
    auto& time_avail_res_map =
        node_selection_info.node_time_avail_res_map.at(craned_index);
    auto& craned_meta = craned_meta_map.at(craned_index);

    if (!(task->resources <= craned_meta.res_total)) {
      CRANE_TRACE(
          "Task #{} needs more resource than that of craned {}. "
          "Skipping this craned.",
          task->TaskId(), craned_index);
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
    auto& time_avail_res_map =
        node_selection_info.node_time_avail_res_map.at(craned_id);
    auto& node_meta = craned_meta_map.at(craned_id);

    // Find all valid time segments in this node for this task.
    // The expected start time must exist because all tasks in pending_task_map
    // can be run under the amount of all resources in this node. At some future
    // time point, all tasks will end and this pending task can eventually be
    // run.
    auto task_duration_it = time_avail_res_map.begin();

    absl::Time end_time = task_duration_it->first + task->time_limit;

    [[maybe_unused]] TimeAvailResMap::const_iterator task_duration_end_it;
    task_duration_end_it = time_avail_res_map.upper_bound(end_time);

    std::vector<TimeSegment> time_segments;
    absl::Duration valid_duration;
    bool trying = false;
    absl::Time expected_start_time;
    while (true) {
      auto next_it = std::next(task_duration_it);
      if (task->resources <= task_duration_it->second) {
        if (!trying) {
          trying = true;
          expected_start_time = task_duration_it->first;

          if (next_it == time_avail_res_map.end()) {
            CRANE_TRACE(
                "Duration [ now+{}s , inf ) for task #{} is "
                "valid and set as the start of a time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                task->TaskId());
            valid_duration = absl::InfiniteDuration();
            time_segments.emplace_back(expected_start_time, valid_duration);
            break;
          } else {
            CRANE_ASSERT_MSG(task_duration_it->first >= now,
                             "The start of a duration must be greater than or "
                             "equal to the end of it");
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "valid and set as the start of a time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->TaskId());
            valid_duration = next_it->first - task_duration_it->first;
          }
        } else {
          if (next_it == time_avail_res_map.end()) {
            CRANE_TRACE(
                "Duration [ now+{}s , inf ) for task #{} is "
                "valid and appended to the previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                task->TaskId());
            valid_duration += absl::InfiniteDuration();
            time_segments.emplace_back(expected_start_time, valid_duration);
            break;
          } else {
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "valid and appended to the previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->TaskId());
            valid_duration += next_it->first - task_duration_it->first;
          }
        }
      } else {
        if (trying) {
          if (next_it == time_avail_res_map.end()) {
            CRANE_TRACE(
                "Duration [ now+{}s , inf ) for task #{} is "
                "NOT valid. Save previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                task->TaskId());
          } else {
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "NOT valid. Save previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->TaskId());
          }
          trying = false;
          time_segments.emplace_back(expected_start_time, valid_duration);
        } else {
          if (next_it == time_avail_res_map.end()) {
            CRANE_TRACE(
                "Duration [ now+{}s , inf ) for task #{} is "
                "NOT valid. Skipping it...",
                absl::ToInt64Seconds(task_duration_it->first - now),
                task->TaskId());
          } else {
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "NOT valid. Skipping it...",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->TaskId());
          }
        }
      }
      task_duration_it = next_it;
    }

    // Now we have the valid time segments for this node. Find the intersection
    // with the set in the previous pass.
    if (first_pass) {
      intersected_time_segments = std::move(time_segments);
      first_pass = false;
    } else {
      std::vector<TimeSegment> new_intersected_time_segments;

      for (auto&& seg : time_segments) {
        absl::Time start = seg.start;
        absl::Time end = seg.start + seg.duration;

        // Find the first time point that <= seg.start + seg.duration
        auto it2 = std::upper_bound(intersected_time_segments.begin(),
                                    intersected_time_segments.end(), end);

        // If it2 == intersected_time_segments.begin(), this time segment has no
        // overlap with any time segment in intersected_time_segment. Just skip
        // it.
        //                begin()
        //               *------*    *----* ....
        // *----------*
        if (it2 != intersected_time_segments.begin()) {
          it2 = std::prev(it2);
          // Find the first time point that <= seg.start
          auto it1 = std::upper_bound(intersected_time_segments.begin(),
                                      intersected_time_segments.end(), start);
          if (it1 != intersected_time_segments.begin())
          // If it1 == intersected_time_segments.begin(), there is no time
          // segment that starts previous to seg.start but there are some
          // segments immediately after seg.start. The first one of them is
          // the beginning of intersected_time_segments.
          //   begin()
          //   *---*           *----*
          // *-------------------*
          {
            // If there is an overlap between first segment and `seg`, take the
            // intersection.
            // std::prev(it1)
            // *---------*
            //         *-----------*
            it1 = std::prev(it1);
            if (it1->start + it1->duration >= start) {
              new_intersected_time_segments.emplace_back(
                  it1->start, it1->start + it1->duration - start);
            }
          }

          //           |<--    range  -->|
          // it1   it should start here     it2
          // v          v                    v
          // *~~~~~~~*  *----*  *--------*   *~~~~~~~~~~~~*
          //       *--------------------------------*
          if (std::distance(it1, it2) >= 2) {
            auto it = it1;
            do {
              ++it;
              new_intersected_time_segments.emplace_back(it->start,
                                                         it->duration);
            } while (std::next(it) != it2);
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
    const CranedMetaContainerInterface::AllPartitionsMetaMap&
        all_partitions_meta_map,
    const CranedMetaContainerInterface::CranedMetaMap& craned_meta_map,
    const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  std::unordered_map<PartitionId, NodeSelectionInfo> part_id_node_info_map;

  // Truncated by 1s.
  // We use
  absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

  // Calculate NodeSelectionInfo for all partitions
  for (auto& [partition_id, partition_metas] : all_partitions_meta_map) {
    for (auto& craned_id : partition_metas.craned_ids) {
      CranedMeta const& craned_meta = craned_meta_map.at(craned_id);
      if (craned_meta.alive) {
        NodeSelectionInfo& node_info_in_a_partition =
            part_id_node_info_map[partition_id];
        CalculateNodeSelectionInfo_(running_tasks, now, partition_id,
                                    partition_metas, craned_meta_map, craned_id,
                                    craned_meta, &node_info_in_a_partition);
      }
    }
  }

  // Now we know, on each node in all partitions, the # of running tasks (which
  //  doesn't include those we select as the incoming running tasks in the
  //  following code) and how many resources are available at the end of each
  //  task.
  // Iterate over all the pending tasks and select the available node for the
  //  task to run in its partition.
  for (auto pending_task_it = pending_task_map->begin();
       pending_task_it != pending_task_map->end();) {
    PartitionId const& part_id = pending_task_it->second->partition_id;
    auto& task = pending_task_it->second;

    NodeSelectionInfo& node_info = part_id_node_info_map[part_id];
    auto& part_meta = all_partitions_meta_map.at(part_id);

    std::list<CranedId> node_ids;
    absl::Time expected_start_time;
    bool ok = CalculateRunningNodesAndStartTime_(
        node_info, part_meta, craned_meta_map, task.get(), now, &node_ids,
        &expected_start_time);
    if (!ok) {
      ++pending_task_it;
      continue;
    }

    // The start time and node ids have been determined.
    // Modify the corresponding NodeSelectionInfo now.

    for (CranedId const& node_id : node_ids) {
      // Increase the running task num in the local variable.

      for (auto it = node_info.task_num_node_id_map.begin();
           it != node_info.task_num_node_id_map.end(); ++it) {
        if (it->second == node_id) {
          uint32_t num_task = it->first + 1;
          node_info.task_num_node_id_map.erase(it);
          node_info.task_num_node_id_map.emplace(num_task, node_id);
        }
      }

      TimeAvailResMap& time_avail_res_map =
          node_info.node_time_avail_res_map[node_id];

      absl::Time task_end_time_plus_1s =
          expected_start_time + task->time_limit + absl::Seconds(1);
      // #ifndef NDEBUG
      //       CRANE_TRACE("\t task #{} end time + 1s = {}, ", task_id,
      //                    absl::ToInt64Seconds(task_end_time_plus_1s - now));
      // #endif
      // #ifndef NDEBUG
      //         if (task_duration_end_it == time_avail_res_map.end())
      //           CRANE_TRACE(
      //               "\t Insert duration [ now+{}s , inf ) : cpu: {} for task
      //               #{}", absl::ToInt64Seconds(task_end_time_plus_1s - now),
      //               task_duration_end_it->second.allocatable_resource.cpu_count,
      //               task_id);
      //         else
      //           CRANE_TRACE(
      //               "\t Insert duration [ now+{}s , now+{}s ) : cpu: {} for
      //               task #{}", absl::ToInt64Seconds(task_end_time_plus_1s -
      //               now), absl::ToInt64Seconds(task_duration_end_it->first -
      //               now),
      //               task_duration_end_it->second.allocatable_resource.cpu_count,
      //               task_id);
      // #endif
      //
      //         task_duration_end_it
      //                         v
      //  *-----------*----------*-------------- time_avail_res_map (Time Point)
      //                   ^^
      //    task_end_time--||-----time_end_time_plus_1s
      //   time_avail_res_map.count(task_end_time_plus_1s) == 0 holds.

      auto task_duration_begin_it =
          time_avail_res_map.upper_bound(expected_start_time);
      if (task_duration_begin_it == time_avail_res_map.end()) {
        --task_duration_begin_it;
        // Situation #1
        //                     task duration
        //                   |<-------------->|
        // *-----------------*--------------------
        // OR Situation #2
        //                      |<-------------->|
        // *-----------------*----------------------
        //                   ^
        //        task_duration_begin_it

        TimeAvailResMap::iterator inserted_it;
        std::tie(inserted_it, ok) = time_avail_res_map.emplace(
            task_end_time_plus_1s, task_duration_begin_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        if (task_duration_begin_it->first == expected_start_time) {
          // Situation #1
          task_duration_begin_it->second -= task->resources;
        } else {
          // Situation #2
          std::tie(inserted_it, ok) = time_avail_res_map.emplace(
              expected_start_time, task_duration_begin_it->second);
          CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

          inserted_it->second -= task->resources;
        }
      } else {
        // Situation #3
        //                    task duration
        //                |<-------------->|
        // *-------*----------*---------*------------
        //         ^                    ^
        //  task_duration_begin_it     end_it
        // OR
        // Situation #4
        //               task duration
        //         |<----------------->|
        // *-------*----------*--------*------------
        //         ^
        //  task_duration_begin_it
        --task_duration_begin_it;

        // std::prev can be used without any check here.
        // There will always be one time point (now) before
        // task_end_time_plus_1s.
        auto task_duration_end_it =
            std::prev(time_avail_res_map.upper_bound(task_end_time_plus_1s));

        if (task_duration_begin_it->first != expected_start_time) {
          // Situation #3 (begin)
          TimeAvailResMap::iterator inserted_it;
          std::tie(inserted_it, ok) = time_avail_res_map.emplace(
              expected_start_time, task_duration_begin_it->second);
          CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

          inserted_it->second -= task->resources;
          task_duration_begin_it = std::next(inserted_it);
        }

        // Subtract the required resources within the interval.
        for (auto in_duration_it = task_duration_begin_it;
             in_duration_it != task_duration_end_it; in_duration_it++) {
          in_duration_it->second -= task->resources;
        }

        // Assume one task end at time x-2,
        // If "x-2" lies in the interval [x, y-1) in time__avail_res__map,
        //  for example, x+2 in [x, y-1) with the available resources amount a,
        //  we need to divide this interval into to two intervals:
        //  [x, x+2]: a-k, where k is the resource amount that task requires,
        //  [x+3, y-1]: a
        // Therefore, we need to insert a key-value at x+3 to preserve this.
        // However, if the length of [x+3, y-1] is 0, or more simply, the point
        //  x+3 exists, there's no need to save the interval [x+3, y-1].
        if (task_duration_end_it->first != task_end_time_plus_1s) {
          // Situation #3 (end)
          TimeAvailResMap::iterator inserted_it;
          std::tie(inserted_it, ok) = time_avail_res_map.emplace(
              task_end_time_plus_1s, task_duration_end_it->second);
          CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

          task_duration_end_it->second -= task->resources;
        }
      }

      // #ifndef NDEBUG
      //       {
      //         std::string str{"\n"};
      //         str.append(fmt::format("Node ({}, {}): \n", task->partition_id,
      //                                task->node_index));
      //         auto prev_iter = time_avail_res_map.begin();
      //         auto iter = std::next(prev_iter);
      //         for (; iter != time_avail_res_map.end(); prev_iter++, iter++) {
      //           str.append(
      //               fmt::format("\t[ now+{}s , now+{}s ) Available
      //               allocatable
      //               "
      //                           "res: cpu core {}, mem {}\n",
      //                           absl::ToInt64Seconds(prev_iter->first - now),
      //                           absl::ToInt64Seconds(iter->first - now),
      //                           prev_iter->second.allocatable_resource.cpu_count,
      //                           prev_iter->second.allocatable_resource.memory_bytes));
      //         }
      //         str.append(
      //             fmt::format("\t[ now+{}s , inf ) Available allocatable "
      //                         "res: cpu core {}, mem {}\n",
      //                         absl::ToInt64Seconds(prev_iter->first - now),
      //                         prev_iter->second.allocatable_resource.cpu_count,
      //                         prev_iter->second.allocatable_resource.memory_bytes));
      //         CRANE_TRACE("{}", str);
      //       }
      // #endif
    }

    if (expected_start_time == now) {
      // The task can be started now.
      task->SetStartTime(expected_start_time);

      std::unique_ptr<TaskInCtld> moved_task;

      // Move task out of pending_task_map and insert it to the
      // scheduling_result_list.
      moved_task.swap(task);
      selection_result_list->emplace_back(std::move(moved_task),
                                          std::move(node_ids));

      // We leave the change in all_partitions_meta_map and running_tasks to
      // the scheduling thread caller to avoid lock maintenance and deadlock.

      // Erase the empty task unique_ptr from map and move to the next element
      pending_task_it = pending_task_map->erase(pending_task_it);
    } else {
      // The task can't be started now. Move to the next pending task.
      pending_task_it++;
    }
  }
}

void TaskScheduler::TransferTaskToMongodb_(TaskInCtld* task) {
  bool ok;
  ok = g_embedded_db_client->MovePendingOrRunningTaskToEnded(task->TaskDbId());
  if (!ok) {
    CRANE_ERROR(
        "Failed to call "
        "g_embedded_db_client->MovePendingOrRunningTaskToEnded() for task #{}",
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

CraneErr TaskScheduler::CheckTaskValidityAndAcquireAttrs_(TaskInCtld* task) {
  // Check whether the selected partition is able to run this task.
  auto metas_ptr = g_meta_container->GetPartitionMetasPtr(task->partition_id);
  if (task->node_num > metas_ptr->partition_global_meta.alive_craned_cnt) {
    CRANE_TRACE(
        "Task #{}'s node-num {} is greater than the number of "
        "alive nodes {} in its partition. Reject it.",
        task->TaskId(), task->node_num,
        metas_ptr->partition_global_meta.alive_craned_cnt);
    return CraneErr::kInvalidNodeNum;
  }

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

  auto craned_meta_map = g_meta_container->GetCranedMetaMapPtr();

  bool fit_in_a_node = false;
  for (CranedId const& craned_id : metas_ptr->craned_ids) {
    // Todo: Bug! Not applicable to a task with the num of required nodes > 0
    CranedMeta const& craned_meta = craned_meta_map->at(craned_id);

    if (!(task->resources <= craned_meta.res_total)) continue;
    fit_in_a_node = true;
    break;
  }
  if (!fit_in_a_node) {
    CRANE_TRACE("Resource not enough. Task cannot fit in any node.");
    return CraneErr::kNoResource;
  }

  return CraneErr::kOk;
}

void TaskScheduler::TerminateTasksOnCraned(CranedId craned_id) {
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

}  // namespace Ctld