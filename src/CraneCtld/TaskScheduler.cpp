#include "TaskScheduler.h"

#include <algorithm>
#include <map>

#include "CranedKeeper.h"
#include "CtldGrpcServer.h"
#include "crane/String.h"

namespace Ctld {

TaskScheduler::TaskScheduler(std::unique_ptr<INodeSelectionAlgo> algo)
    : m_node_selection_algo_(std::move(algo)) {
  uint64_t max_id;
  if (!g_db_client->GetMaxExistingJobId(&max_id)) {
    CRANE_ERROR("GetMaxExistingJobId failed.");
    std::exit(1);
  }
  CRANE_TRACE("During recovery set next task id to {}", max_id + 1);
  m_next_task_id_ = max_id + 1;

  std::list<TaskInCtld> task_list;
  if (!g_db_client->FetchJobRecordsWithStates(
          &task_list, {crane::grpc::Pending, crane::grpc::Running})) {
    CRANE_ERROR("FetchJobRecordsWithStates failed.");
    std::exit(1);
  }

  for (auto& task : task_list) {
    std::string field_name = "state";
    if (!g_db_client->UpdateJobRecordField(
            task.job_db_inx, field_name, std::to_string(crane::grpc::Failed))) {
      CRANE_ERROR("FetchJobRecordsWithStates failed.");
      std::exit(1);
    }
    CRANE_TRACE("During recovery set the status of task #{} to FAILED",
                task.task_id);
  }

  m_schedule_thread_ = std::thread([this] { ScheduleThread_(); });

  m_clean_ended_thread_ = std::thread([this] { CleanEndedTaskThread_(); });
}

TaskScheduler::~TaskScheduler() {
  m_thread_stop_ = true;
  m_schedule_thread_.join();
  m_clean_ended_thread_.join();
}

void TaskScheduler::ScheduleThread_() {
  while (!m_thread_stop_) {
    // Note: In other parts of code, we must avoid the happening of the
    // situation where m_running_task_map_mtx is acquired and then
    // m_pending_task_map_mtx_ needs to be acquired. Deadlock may happen under
    // such a situation.
    m_pending_task_map_mtx_.Lock();
    if (!m_pending_task_map_.empty()) {  // all_part_metas is locked here.
      auto all_part_metas = g_meta_container->GetAllPartitionsMetaMapPtr();
      std::list<INodeSelectionAlgo::NodeSelectionResult> selection_result_list;

      // Both read and write need a lock.
      m_running_task_map_mtx_.Lock();
      m_node_selection_algo_->NodeSelect(*all_part_metas, m_running_task_map_,
                                         &m_pending_task_map_,
                                         &selection_result_list);
      m_running_task_map_mtx_.Unlock();
      m_pending_task_map_mtx_.Unlock();

      for (auto& it : selection_result_list) {
        auto& task = it.first;
        uint32_t partition_id = task->partition_id;

        task->status = crane::grpc::TaskStatus::Running;
        task->node_indexes = std::move(it.second);
        task->nodes_alloc = task->node_indexes.size();

        for (uint32_t node_index : task->node_indexes) {
          CranedMeta node_meta =
              all_part_metas->at(partition_id).craned_meta_map.at(node_index);

          CranedId node_id{partition_id, node_index};
          g_meta_container->MallocResourceFromNode(node_id, task->task_id,
                                                   task->resources);

          task->nodes.emplace_back(node_meta.static_meta.hostname);

          if (task->type == crane::grpc::Interactive) {
            InteractiveTaskAllocationDetail detail{
                .craned_index = node_index,
                .ipv4_addr = node_meta.static_meta.hostname,
                .port = node_meta.static_meta.port,
                .resource_uuid = m_uuid_gen_(),
            };

            std::get<InteractiveMetaInTask>(task->meta).resource_uuid =
                detail.resource_uuid;

            g_ctld_server->AddAllocDetailToIaTask(task->task_id,
                                                  std::move(detail));
          }

          m_task_indexes_mtx_.Lock();
          m_node_to_tasks_map_[node_id.craned_index].emplace(task->task_id);
          m_task_indexes_mtx_.Unlock();
        }

        task->allocated_craneds_regex = util::HostNameListToStr(task->nodes);

        auto* task_ptr = task.get();
        auto job_db_inx = task->job_db_inx;
        auto nodes_alloc = task->nodes_alloc;
        auto allocated_nodes_regex = task->allocated_craneds_regex;

        // For the task whose --node > 1, only execute the command at the first
        // allocated node.
        CranedId first_node_id{partition_id, task->node_indexes.front()};
        task->executing_node_id = first_node_id;

        for (auto iter : task->node_indexes) {
          CranedId node_id{partition_id, iter};
          CranedStub* stub = g_craned_keeper->GetCranedStub(node_id);
          CRANE_TRACE("Send CreateCgroupForTask to {}: ", node_id);
          stub->CreateCgroupForTask(task->task_id, task->uid);
        }

        // IMPORTANT: task must be put into running_task_map before any
        //  time-consuming operation, otherwise TaskStatusChange RPC will come
        //  earlier before task is put into running_task_map.
        m_running_task_map_mtx_.Lock();
        m_running_task_map_.emplace(task->task_id, std::move(task));
        m_running_task_map_mtx_.Unlock();

        // RPC is time-consuming.
        CranedStub* node_stub = g_craned_keeper->GetCranedStub(first_node_id);
        node_stub->ExecuteTask(task_ptr);

        // Todo: Updating database is a extremely time-consuming operation.
        //  Consider an async way.
        uint64_t timestamp = ToUnixSeconds(absl::Now());
        g_db_client->UpdateJobRecordFields(
            job_db_inx, {"time_start", "nodes_alloc", "nodelist", "state"},
            {std::to_string(timestamp), std::to_string(nodes_alloc),
             allocated_nodes_regex, std::to_string(crane::grpc::Running)});
      }
      // all_part_metas is unlocked here.
    } else {
      m_pending_task_map_mtx_.Unlock();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kTaskScheduleIntervalMs));
  }
}

void TaskScheduler::CleanEndedTaskThread_() {
  while (!m_thread_stop_) {
    {
      LockGuard guard(m_ended_task_map_mtx_);
      for (auto iter = m_ended_task_map_.begin();
           iter != m_ended_task_map_.end();) {
        if (iter->second->end_time +
                absl::Seconds(kEndedTaskKeepingTimeSeconds) >
            absl::Now()) {
          auto copy_iter = iter++;

          TaskInCtld* task = copy_iter->second.get();
          {
            LockGuard indexes_guard(m_task_indexes_mtx_);
            m_partition_to_tasks_map_[task->partition_id].erase(task->task_id);
          }
          m_ended_task_map_.erase(copy_iter);
        } else
          iter++;
      }
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(kEndedTaskCleanIntervalSeconds));
  }
}

void TaskScheduler::SetNodeSelectionAlgo(
    std::unique_ptr<INodeSelectionAlgo> algo) {
  m_node_selection_algo_ = std::move(algo);
}

CraneErr TaskScheduler::SubmitTask(std::unique_ptr<TaskInCtld> task,
                                   bool resubmit, uint32_t* task_id) {
  uint32_t partition_id;
  if (!g_meta_container->GetPartitionId(task->partition_name, &partition_id))
    return CraneErr::kNonExistent;

  // Check whether the selected partition is able to run this task.
  auto metas_ptr = g_meta_container->GetPartitionMetasPtr(partition_id);
  if (task->node_num > metas_ptr->partition_global_meta.alive_craned_cnt) {
    CRANE_TRACE(
        "Task #{}'s node-num {} is greater than the number of "
        "alive nodes {} in its partition. Reject it.",
        task->task_id, task->node_num,
        metas_ptr->partition_global_meta.alive_craned_cnt);
    return CraneErr::kInvalidNodeNum;
  }

  if (!(task->resources <=
        metas_ptr->partition_global_meta.m_resource_total_)) {
    CRANE_TRACE(
        "Resource not enough for task #{}. "
        "Partition total: cpu {}, mem: {}, mem+sw: {}",
        task->task_id,
        metas_ptr->partition_global_meta.m_resource_total_.allocatable_resource
            .cpu_count,
        util::ReadableMemory(metas_ptr->partition_global_meta.m_resource_total_
                                 .allocatable_resource.memory_bytes),
        util::ReadableMemory(metas_ptr->partition_global_meta.m_resource_total_
                                 .allocatable_resource.memory_sw_bytes));
    return CraneErr::kNoResource;
  }

  bool fit_in_a_node = false;
  for (auto&& [node_index, node_meta] : metas_ptr->craned_meta_map) {
    if (!(task->resources <= node_meta.res_total)) continue;
    fit_in_a_node = true;
    break;
  }
  if (!fit_in_a_node) {
    CRANE_TRACE("Resource not enough. Task cannot fit in any node.");
    return CraneErr::kNoResource;
  }

  // Add the task to the pending task queue.
  task->partition_id = partition_id;
  task->status = crane::grpc::Pending;

  // If resubmit == true, task id has been allocated before.
  if (!resubmit) task->task_id = m_next_task_id_++;
  *task_id = task->task_id;

  std::string empty_string;
  uint32_t zero = 0;

  std::reference_wrapper<std::string> script{empty_string};
  if (task->type == crane::grpc::Batch)
    script = std::get<BatchMetaInTask>(task->meta).sh_script;

  if (!resubmit) {  // If task isn't re-submitted, it needs inserting into job
                    // table.
    uint64_t timestamp = ToUnixSeconds(absl::Now());
    if (!g_db_client->InsertJob(
            &task->job_db_inx, timestamp, task->account,
            task->resources.allocatable_resource.cpu_count,
            task->resources.allocatable_resource.memory_bytes, task->name,
            task->env, task->task_id, task->uid, task->gid, empty_string,
            task->node_num, empty_string, task->partition_name, zero, timestamp,
            script, task->status, ToInt64Seconds(task->time_limit), task->cwd,
            task->task_to_ctld)) {
      return CraneErr::kSystemErr;
    }
  }

  m_task_indexes_mtx_.Lock();
  m_partition_to_tasks_map_[task->partition_id].emplace(task->task_id);
  m_task_indexes_mtx_.Unlock();

  m_pending_task_map_mtx_.Lock();
  m_pending_task_map_.emplace(task->task_id, std::move(task));
  m_pending_task_map_mtx_.Unlock();

  return CraneErr::kOk;
}

void TaskScheduler::TaskStatusChange(uint32_t task_id, uint32_t craned_index,
                                     crane::grpc::TaskStatus new_status,
                                     std::optional<std::string> reason) {
  // The order of LockGuards matters.
  LockGuard running_guard(m_running_task_map_mtx_);
  LockGuard indexes_guard(m_task_indexes_mtx_);

  auto iter = m_running_task_map_.find(task_id);
  if (iter == m_running_task_map_.end()) {
    CRANE_ERROR("Unknown task id {} when change task status.", task_id);
    return;
  }

  const std::unique_ptr<TaskInCtld>& task = iter->second;

  if (task->type == crane::grpc::Interactive) {
    g_ctld_server->RemoveAllocDetailOfIaTask(task_id);
  }

  CRANE_DEBUG("TaskStatusChange: Task #{} {}->{} In Node #{}", task_id,
              task->status, new_status, craned_index);

  if (new_status == crane::grpc::Finished) {
    task->status = crane::grpc::Finished;
  } else if (new_status == crane::grpc::Cancelled) {
    task->status = crane::grpc::Cancelled;
  } else {
    task->status = crane::grpc::Failed;
  }

  for (auto&& task_node_index : task->node_indexes) {
    CranedId task_node_id{task->partition_id, task_node_index};
    g_meta_container->FreeResourceFromNode(task_node_id, task_id);
    auto* stub = g_craned_keeper->GetCranedStub(task_node_id);
    CraneErr err = stub->ReleaseCgroupForTask(task_id, task->uid);
    if (err != CraneErr::kOk) {
      CRANE_ERROR("Failed to Release cgroup RPC for task#{} on Node {}",
                  task_id, task_node_id);
    }

    m_node_to_tasks_map_[task_node_index].erase(task_id);
  }

  uint64_t timestamp = ToUnixSeconds(absl::Now());
  g_db_client->UpdateJobRecordFields(
      task->job_db_inx, {"time_end", "state"},
      {std::to_string(timestamp), std::to_string(task->status)});

  LockGuard ended_guard(m_ended_task_map_mtx_);
  m_ended_task_map_.emplace(task_id, std::move(iter->second));
  m_running_task_map_.erase(iter);
}

bool TaskScheduler::QueryCranedIdOfRunningTaskNoLock_(uint32_t task_id,
                                                      CranedId* node_id) {
  auto iter = m_running_task_map_.find(task_id);
  if (iter == m_running_task_map_.end()) return false;

  *node_id = iter->second->executing_node_id;

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

CraneErr TaskScheduler::CancelPendingOrRunningTask(uint32_t task_id) {
  LockGuard pending_guard(m_pending_task_map_mtx_);
  LockGuard running_guard(m_running_task_map_mtx_);
  LockGuard ended_guard(m_ended_task_map_mtx_);

  auto pending_iter = m_pending_task_map_.find(task_id);
  if (pending_iter != m_pending_task_map_.end()) {
    auto node = m_pending_task_map_.extract(task_id);
    auto task = std::move(node.mapped());

    task->status = crane::grpc::Cancelled;

    uint64_t timestamp = ToUnixSeconds(absl::Now());
    g_db_client->UpdateJobRecordFields(
        task->job_db_inx, {"time_end", "state"},
        {std::to_string(timestamp), std::to_string(task->status)});

    m_ended_task_map_.emplace(task_id, std::move(task));

    return CraneErr::kOk;
  }

  return TerminateRunningTaskNoLock_(task_id);
}

void TaskScheduler::QueryTaskBriefMetaInPartition(
    uint32_t partition_id, const QueryBriefTaskMetaFieldControl& field_control,
    crane::grpc::QueryJobsInPartitionReply* response) {
  auto* task_list = response->mutable_task_metas();
  auto* state_list = response->mutable_task_status();
  auto* allocated_craned_list = response->mutable_allocated_craneds();
  auto* id_list = response->mutable_task_ids();

  {
    LockGuard pending_guard(m_pending_task_map_mtx_);
    for (auto& [task_id, task] : m_pending_task_map_) {
      auto* task_it = task_list->Add();
      task_it->CopyFrom(task->task_to_ctld);

      auto* state_it = state_list->Add();
      *state_it = task->status;

      auto* node_list_it = allocated_craned_list->Add();
      *node_list_it = task->allocated_craneds_regex;

      auto* id_it = id_list->Add();
      *id_it = task->task_id;
    }
  }
  {
    LockGuard running_guard(m_running_task_map_mtx_);
    for (auto& [task_id, task] : m_running_task_map_) {
      auto* task_it = task_list->Add();
      task_it->CopyFrom(task->task_to_ctld);

      auto* state_it = state_list->Add();
      *state_it = task->status;

      auto* node_list_it = allocated_craned_list->Add();
      *node_list_it = task->allocated_craneds_regex;

      auto* id_it = id_list->Add();
      *id_it = task->task_id;
    }
  }
  {
    LockGuard ended_guard(m_ended_task_map_mtx_);
    for (auto& [task_id, task] : m_ended_task_map_) {
      auto* task_it = task_list->Add();
      task_it->CopyFrom(task->task_to_ctld);

      auto* state_it = state_list->Add();
      *state_it = task->status;

      auto* craned_list_it = allocated_craned_list->Add();
      *craned_list_it = task->allocated_craneds_regex;

      auto* id_it = id_list->Add();
      *id_it = task->task_id;
    }
  }
}

std::string TaskScheduler::QueryCranedListFromTaskId(uint32_t task_id) {
  LockGuard running_guard(m_running_task_map_mtx_);
#warning Check the correctness of the function

  auto iter = m_running_task_map_.find(task_id);
  std::string craned_list;
  if (iter != m_running_task_map_.end()) {
    int task_per_node = iter->second->ntasks_per_node;
    for (auto& node : iter->second->nodes) {
      craned_list += node + ":" + std::to_string(task_per_node) + "/n";
    }
  }
  return craned_list;
}

void MinLoadFirst::CalculateNodeSelectionInfo_(
    const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::Time now, uint32_t partition_id,
    const PartitionMetas& partition_metas, uint32_t node_id,
    const CranedMeta& node_meta, NodeSelectionInfo* node_selection_info) {
  NodeSelectionInfo& node_selection_info_ref = *node_selection_info;

  node_selection_info_ref.task_num_node_id_map.emplace(
      node_meta.running_task_resource_map.size(), node_id);

  // Sort all running task in this node by ending time.
  std::vector<std::pair<absl::Time, uint32_t>> end_time_task_id_vec;
  for (const auto& [task_id, running_task] : running_tasks) {
    if (running_task->partition_id == partition_id &&
        std::count(running_task->node_indexes.begin(),
                   running_task->node_indexes.end(), node_id) > 0)
      end_time_task_id_vec.emplace_back(
          running_task->start_time + running_task->time_limit, task_id);
  }
  std::sort(
      end_time_task_id_vec.begin(), end_time_task_id_vec.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

#ifndef NDEBUG
  {
    if (!end_time_task_id_vec.empty()) {
      std::string str;
      str.append(fmt::format("Node ({}, {}): ", partition_id, node_id));
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
      node_selection_info_ref.node_time_avail_res_map[node_id];

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
      str.append(fmt::format("Node ({}, {}): ", partition_id, node_id));
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
    const PartitionMetas& partition_metas, const TaskInCtld* task,
    absl::Time now, std::list<uint32_t>* node_ids, absl::Time* start_time) {
  uint32_t selected_node_cnt = 0;
  auto task_num_node_id_it = node_selection_info.task_num_node_id_map.begin();
  std::vector<TimeSegment> intersected_time_segments;
  bool first_pass{true};

  std::list<uint32_t> craned_indexes_;
  while (selected_node_cnt < task->node_num &&
         task_num_node_id_it !=
             node_selection_info.task_num_node_id_map.end()) {
    auto craned_index = task_num_node_id_it->second;
    auto& time_avail_res_map =
        node_selection_info.node_time_avail_res_map.at(craned_index);
    auto& craned_meta = partition_metas.craned_meta_map.at(craned_index);

    if (!(task->resources <= craned_meta.res_total)) {
      CRANE_TRACE(
          "Task #{} needs more resource than that of craned {}. "
          "Skipping this craned.",
          task->task_id, craned_index);
    } else {
      craned_indexes_.emplace_back(craned_index);
      ++selected_node_cnt;
    }
    ++task_num_node_id_it;
  }

  if (selected_node_cnt < task->node_num) return false;
  CRANE_ASSERT_MSG(selected_node_cnt == task->node_num,
                   "selected_node_cnt != task->node_num");

  for (uint32_t craned_id : craned_indexes_) {
    auto& time_avail_res_map =
        node_selection_info.node_time_avail_res_map.at(craned_id);
    auto& node_meta = partition_metas.craned_meta_map.at(craned_id);

    // Find all valid time segments in this node for this task.
    // The expected start time must exist because all tasks in pending_task_map
    // can be run under the amount of all resources in this node. At some future
    // time point, all tasks will end and this pending task can eventually be
    // run.
    auto task_duration_it = time_avail_res_map.begin();

    absl::Time end_time = task_duration_it->first + task->time_limit;

    TimeAvailResMap::const_iterator task_duration_end_it;
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
                task->task_id);
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
                absl::ToInt64Seconds(next_it->first - now), task->task_id);
            valid_duration = next_it->first - task_duration_it->first;
          }
        } else {
          if (next_it == time_avail_res_map.end()) {
            CRANE_TRACE(
                "Duration [ now+{}s , inf ) for task #{} is "
                "valid and appended to the previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                task->task_id);
            valid_duration += absl::InfiniteDuration();
            time_segments.emplace_back(expected_start_time, valid_duration);
            break;
          } else {
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "valid and appended to the previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->task_id);
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
                task->task_id);
          } else {
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "NOT valid. Save previous time segment.",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->task_id);
          }
          trying = false;
          time_segments.emplace_back(expected_start_time, valid_duration);
        } else {
          if (next_it == time_avail_res_map.end()) {
            CRANE_TRACE(
                "Duration [ now+{}s , inf ) for task #{} is "
                "NOT valid. Skipping it...",
                absl::ToInt64Seconds(task_duration_it->first - now),
                task->task_id);
          } else {
            CRANE_TRACE(
                "\t Trying duration [ now+{}s , now+{}s ) for task #{} is "
                "NOT valid. Skipping it...",
                absl::ToInt64Seconds(task_duration_it->first - now),
                absl::ToInt64Seconds(next_it->first - now), task->task_id);
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

  *node_ids = std::move(craned_indexes_);

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
    const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::btree_map<uint32_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  std::unordered_map<uint32_t /* Partition ID */, NodeSelectionInfo>
      part_id_node_info_map;

  // Truncated by 1s.
  // We use
  absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));

  // Calculate NodeSelectionInfo for all partitions
  for (auto& [partition_id, partition_metas] : all_partitions_meta_map) {
    for (auto& [node_index, node_meta] : partition_metas.craned_meta_map) {
      if (node_meta.alive) {
        NodeSelectionInfo& node_info_in_a_partition =
            part_id_node_info_map[partition_id];
        CalculateNodeSelectionInfo_(running_tasks, now, partition_id,
                                    partition_metas, node_index, node_meta,
                                    &node_info_in_a_partition);
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
    uint32_t task_id = pending_task_it->first;
    uint32_t part_id = pending_task_it->second->partition_id;
    auto& task = pending_task_it->second;

    NodeSelectionInfo& node_info = part_id_node_info_map[part_id];
    auto& part_meta = all_partitions_meta_map.at(part_id);

    std::list<uint32_t> node_ids;
    absl::Time expected_start_time;
    bool ok = CalculateRunningNodesAndStartTime_(
        node_info, part_meta, task.get(), now, &node_ids, &expected_start_time);
    if (!ok) {
      ++pending_task_it;
      continue;
    }

    // The start time and node ids have been determined.
    // Modify the corresponding NodeSelectionInfo now.

    for (uint32_t node_id : node_ids) {
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
      //#ifndef NDEBUG
      //      CRANE_TRACE("\t task #{} end time + 1s = {}, ", task_id,
      //                   absl::ToInt64Seconds(task_end_time_plus_1s - now));
      //#endif
      //#ifndef NDEBUG
      //        if (task_duration_end_it == time_avail_res_map.end())
      //          CRANE_TRACE(
      //              "\t Insert duration [ now+{}s , inf ) : cpu: {} for task
      //              #{}", absl::ToInt64Seconds(task_end_time_plus_1s - now),
      //              task_duration_end_it->second.allocatable_resource.cpu_count,
      //              task_id);
      //        else
      //          CRANE_TRACE(
      //              "\t Insert duration [ now+{}s , now+{}s ) : cpu: {} for
      //              task #{}", absl::ToInt64Seconds(task_end_time_plus_1s -
      //              now), absl::ToInt64Seconds(task_duration_end_it->first -
      //              now),
      //              task_duration_end_it->second.allocatable_resource.cpu_count,
      //              task_id);
      //#endif
      //
      //        task_duration_end_it
      //                        v
      // *-----------*----------*-------------- time_avail_res_map (Time Point)
      //                  ^^
      //   task_end_time--||-----time_end_time_plus_1s
      //  time_avail_res_map.count(task_end_time_plus_1s) == 0 holds.

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

      //#ifndef NDEBUG
      //      {
      //        std::string str{"\n"};
      //        str.append(fmt::format("Node ({}, {}): \n", task->partition_id,
      //                               task->node_index));
      //        auto prev_iter = time_avail_res_map.begin();
      //        auto iter = std::next(prev_iter);
      //        for (; iter != time_avail_res_map.end(); prev_iter++, iter++) {
      //          str.append(
      //              fmt::format("\t[ now+{}s , now+{}s ) Available allocatable
      //              "
      //                          "res: cpu core {}, mem {}\n",
      //                          absl::ToInt64Seconds(prev_iter->first - now),
      //                          absl::ToInt64Seconds(iter->first - now),
      //                          prev_iter->second.allocatable_resource.cpu_count,
      //                          prev_iter->second.allocatable_resource.memory_bytes));
      //        }
      //        str.append(
      //            fmt::format("\t[ now+{}s , inf ) Available allocatable "
      //                        "res: cpu core {}, mem {}\n",
      //                        absl::ToInt64Seconds(prev_iter->first - now),
      //                        prev_iter->second.allocatable_resource.cpu_count,
      //                        prev_iter->second.allocatable_resource.memory_bytes));
      //        CRANE_TRACE("{}", str);
      //      }
      //#endif
    }

    if (expected_start_time == now) {
      // The task can be started now.
      task->start_time = expected_start_time;

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

}  // namespace Ctld