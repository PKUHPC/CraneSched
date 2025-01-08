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

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "CranedMetaContainer.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using OrderedTaskMap = absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>;

using UnorderedTaskMap =
    absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>;

class IPrioritySorter {
 public:
  virtual std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map, size_t limit,
      absl::Time now) = 0;

  virtual ~IPrioritySorter() = default;
};

class BasicPriority : public IPrioritySorter {
 public:
  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map, size_t limit,
      absl::Time now) override {
    size_t len = std::min(pending_task_map.size(), limit);

    std::vector<task_id_t> task_id_vec;
    task_id_vec.reserve(len);

    int i = 0;
    for (auto it = pending_task_map.begin(); i < len; i++, it++) {
      TaskInCtld* task = it->second.get();
      if (!task->Held()) {
        task_id_vec.emplace_back(it->first);
        it->second->pending_reason = "Priority";
      } else {
        it->second->pending_reason = "Held";
      }
    }

    return task_id_vec;
  }
};

class MultiFactorPriority : public IPrioritySorter {
 public:
  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map, size_t limit_num,
      absl::Time now) override;

 private:
  struct FactorBound {
    uint64_t age_max, age_min;
    uint32_t qos_priority_max, qos_priority_min;
    uint32_t part_priority_max, part_priority_min;
    uint32_t nodes_alloc_max, nodes_alloc_min;
    uint64_t mem_alloc_max, mem_alloc_min;
    double cpus_alloc_max, cpus_alloc_min;
    double service_val_max, service_val_min;

    absl::flat_hash_map<std::string, double> acc_service_val_map;
  };

  void CalculateFactorBound_(const OrderedTaskMap& pending_task_map,
                             const UnorderedTaskMap& running_task_map,
                             absl::Time now);
  double CalculatePriority_(Ctld::TaskInCtld* task, absl::Time now) const;

  FactorBound m_factor_bound_;
};

class INodeSelectionAlgo {
 public:
  /**
   * This map stores how much resource is available
   * over time on each Craned node.
   *
   * In this map, the time is discretized by 1s and starts from absl::Now().
   * {x: a, y: b, z: c, ...} means that
   * In time interval [x, y-1], the amount of available resources is a.
   * In time interval [y, z-1], the amount of available resources is b.
   * In time interval [z, ...], the amount of available resources is c.
   */
  using TimeAvailResMap = std::map<absl::Time, ResourceInNode>;

  // Pair content: <The task which is going to be run,
  //                The craned id on which it will be run>
  // Partition information is not needed because scheduling is carried out in
  // one partition to which the task belongs.
  using NodeSelectionResult =
      std::pair<std::unique_ptr<TaskInCtld>, std::list<CranedId>>;

  virtual ~INodeSelectionAlgo() = default;

  /**
   * Do node selection for all pending tasks.
   * Note: During this function call, the global meta is locked. This function
   * should return as quick as possible.
   * @param[in,out] all_partitions_meta_map Callee should make necessary
   * modification in this structure to keep the consistency of global meta data.
   * e.g. When a task is added to \b selection_result_list, corresponding
   * resource should subtracted from the fields in all_partitions_meta.
   * @param[in,out] pending_task_map A list that contains all pending task. The
   * list is order by committing time. The later committed task is at the tail
   * of the list. When scheduling is done, scheduled tasks \b SHOULD be removed
   * from \b pending_task_list
   * @param[out] selected_tasks A list that contains the result of
   * scheduling. See the annotation of \b SchedulingResult
   */
  virtual void NodeSelect(
      const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) = 0;
};

class MinLoadFirst : public INodeSelectionAlgo {
 public:
  explicit MinLoadFirst(IPrioritySorter* priority_sorter)
      : m_priority_sorter_(priority_sorter) {}

  void NodeSelect(
      const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) override;

 private:
  static constexpr bool kAlgoTraceOutput = false;
  static constexpr bool kAlgoRedundantNode = false;
  static constexpr uint32_t kAlgoMaxTaskNumPerNode = 1000;
  static constexpr absl::Duration kAlgoMaxTimeWindow = absl::Hours(24 * 7);

  struct TimeAvailResTracker;

  struct TrackerList {
    struct Node {
      TimeAvailResTracker* tracker_ptr;
      absl::Time time;
      bool first_k;

      Node(TimeAvailResTracker* tracker_ptr, absl::Time time, bool first_k)
          : tracker_ptr(tracker_ptr), time(time), first_k(first_k) {}
    };

    using ListContainer = std::list<Node>;

    ListContainer m_tracker_list_;

    const int k_value;
    ListContainer::iterator kth_iter;

    explicit TrackerList(int k_value)
        : k_value(k_value), kth_iter(m_tracker_list_.end()) {}

    void try_push_back(TimeAvailResTracker* it, absl::Time time) {
      if (it->m_tracker_list_pos_ != m_tracker_list_.end()) return;

      m_tracker_list_.emplace_back(it, time, m_tracker_list_.size() < k_value);
      if (m_tracker_list_.size() == k_value) {
        CRANE_ASSERT(kth_iter == m_tracker_list_.end());
        kth_iter = std::prev(m_tracker_list_.end());
      }
      it->m_tracker_list_pos_ = std::prev(m_tracker_list_.end());
    }

    void try_erase(TimeAvailResTracker* it) {
      if (it->m_tracker_list_pos_ == m_tracker_list_.end()) return;

      auto elem = *it->m_tracker_list_pos_;
      it->m_tracker_list_pos_ = m_tracker_list_.end();

      if (kth_iter != m_tracker_list_.end() && elem.first_k) {
        kth_iter = std::next(kth_iter);
        if (kth_iter != m_tracker_list_.end()) {
          kth_iter->first_k = true;
        }
      }
      m_tracker_list_.erase(it->m_tracker_list_pos_);
    }

    absl::Time kth_time() const {
      return kth_iter == m_tracker_list_.end() ? kth_iter->time
                                               : absl::InfiniteFuture();
    }
  };

  struct TimeAvailResTracker {
    const CranedId craned_id;
    TimeAvailResMap::const_iterator it;
    const TimeAvailResMap::const_iterator end;
    const ResourceInNode* task_res;

    TrackerList* m_tracker_list_;
    TrackerList::ListContainer::iterator m_tracker_list_pos_;
    bool satisfied_flag;

    TimeAvailResTracker(const CranedId& craned_id,
                        const TimeAvailResMap::const_iterator& begin,
                        const TimeAvailResMap::const_iterator& end,
                        TrackerList* tracker_list,
                        const ResourceInNode* task_res)
        : craned_id(craned_id),
          it(begin),
          end(end),
          task_res(task_res),
          m_tracker_list_(tracker_list),
          m_tracker_list_pos_(m_tracker_list_->m_tracker_list_.end()) {
      satisfied_flag = satisfied();
    }

    bool satisfied() const { return *task_res <= it->second; }

    bool genNext() {
      satisfied_flag = !satisfied_flag;  // target state
      return satisfied_flag ? genNextSatisfied() : genNextUnsatisfied();
    }

    bool genNextUnsatisfied() {
      while (++it != end && satisfied());
      return it != end;
    }

    bool genNextSatisfied() {
      while (++it != end && !satisfied());
      return it != end;
    }
  };

  struct NodeSelectionInfo {
    // Craned_ids are sorted by cost.
    std::set<std::pair<uint64_t, CranedId>> cost_node_id_set;
    std::unordered_map<CranedId, uint64_t> node_cost_map;
    std::unordered_map<CranedId, TimeAvailResMap> node_time_avail_res_map;
    std::unordered_map<CranedId, ResourceInNode> node_res_total_map;

    void setCost(const CranedId& craned_id, uint64_t cost) {
      cost_node_id_set.erase({node_cost_map[craned_id], craned_id});
      node_cost_map[craned_id] = cost;
      cost_node_id_set.emplace(cost, craned_id);
    }
    void updateCost(const CranedId& craned_id, const absl::Time& start_time,
                    const absl::Time& end_time,
                    const ResourceInNode& resources) {
      auto& cost = node_cost_map[craned_id];
      cost_node_id_set.erase({cost, craned_id});
      auto& total_res = node_res_total_map[craned_id];
      double cpu_rate =
          static_cast<double>(resources.allocatable_res.cpu_count) /
          static_cast<double>(total_res.allocatable_res.cpu_count);
      cost += round((end_time - start_time) / absl::Seconds(1) * cpu_rate);
      cost_node_id_set.emplace(cost, craned_id);
    }
  };

  static void CalculateNodeSelectionInfoOfPartition_(
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::Time now, const PartitionId& partition_id,
      const std::unordered_set<CranedId> craned_ids,
      const CranedMetaContainer::CranedMetaRawMap& craned_meta_map,
      NodeSelectionInfo* node_selection_info);

  static bool CalculateRunningNodesAndStartTime_(
      const NodeSelectionInfo& node_selection_info,
      const util::Synchronized<PartitionMeta>& partition_meta_ptr,
      const CranedMetaContainer::CranedMetaRawMap& craned_meta_map,
      TaskInCtld* task, absl::Time now, std::list<CranedId>* craned_ids,
      absl::Time* start_time);

  static void SubtractTaskResourceNodeSelectionInfo_(
      absl::Time const& expected_start_time, absl::Duration const& duration,
      ResourceV2 const& resources, std::list<CranedId> const& craned_ids,
      NodeSelectionInfo* node_selection_info);

  IPrioritySorter* m_priority_sorter_;
};

class TaskScheduler {
  using TaskInEmbeddedDb = crane::grpc::TaskInEmbeddedDb;

  using Mutex = absl::Mutex;
  using LockGuard = absl::MutexLock;

  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K, typename V>
  using TreeMap = absl::btree_map<K, V>;

  template <typename K>
  using HashSet = absl::flat_hash_set<K>;

 public:
  TaskScheduler();

  ~TaskScheduler();

  bool Init();

  void SetNodeSelectionAlgo(std::unique_ptr<INodeSelectionAlgo> algo);

  /// \return The future is set to 0 if task submission is failed.
  /// Otherwise, it is set to newly allocated task id.
  std::future<task_id_t> SubmitTaskAsync(std::unique_ptr<TaskInCtld> task);

  std::future<CraneErr> HoldReleaseTaskAsync(task_id_t task_id, int64_t secs);

  CraneErr ChangeTaskTimeLimit(task_id_t task_id, int64_t secs);

  CraneErr ChangeTaskPriority(task_id_t task_id, double priority);

  void TaskStatusChangeWithReasonAsync(uint32_t task_id,
                                       const CranedId& craned_index,
                                       crane::grpc::TaskStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string>&& reason) {
    // Todo: Add reason implementation here!
    TaskStatusChangeAsync(task_id, craned_index, new_status, exit_code);
  }

  void TaskStatusChangeAsync(uint32_t task_id, const CranedId& craned_index,
                             crane::grpc::TaskStatus new_status,
                             uint32_t exit_code);

  void TerminateTasksOnCraned(const CranedId& craned_id, uint32_t exit_code);

  // Temporary inconsistency may happen. If 'false' is returned, just ignore it.
  void QueryTasksInRam(const crane::grpc::QueryTasksInfoRequest* request,
                       crane::grpc::QueryTasksInfoReply* response);

  crane::grpc::CancelTaskReply CancelPendingOrRunningTask(
      const crane::grpc::CancelTaskRequest& request);

  CraneErr TerminatePendingOrRunningIaTask(uint32_t task_id) {
    LockGuard pending_guard(&m_pending_task_map_mtx_);
    LockGuard running_guard(&m_running_task_map_mtx_);

    auto pd_it = m_pending_task_map_.find(task_id);
    if (pd_it != m_pending_task_map_.end()) {
      auto& task = pd_it->second;
      if (task->type == crane::grpc::TaskType::Interactive) {
        auto& meta = std::get<InteractiveMetaInTask>(task->meta);
        meta.has_been_cancelled_on_front_end = true;
      }
      m_cancel_task_queue_.enqueue(
          CancelPendingTaskQueueElem{.task = std::move(task)});
      m_cancel_task_async_handle_->send();
      m_pending_task_map_.erase(pd_it);
      return CraneErr::kOk;
    }

    auto rn_it = m_running_task_map_.find(task_id);
    if (rn_it == m_running_task_map_.end())
      return CraneErr::kNonExistent;
    else {
      auto& task = rn_it->second;
      if (task->type == crane::grpc::TaskType::Interactive) {
        auto& meta = std::get<InteractiveMetaInTask>(task->meta);
        meta.has_been_cancelled_on_front_end = true;
      }
    }

    return TerminateRunningTaskNoLock_(rn_it->second.get());
  }

  CraneErr TerminateRunningTask(uint32_t task_id) {
    LockGuard running_guard(&m_running_task_map_mtx_);

    auto iter = m_running_task_map_.find(task_id);
    if (iter == m_running_task_map_.end()) return CraneErr::kNonExistent;

    return TerminateRunningTaskNoLock_(iter->second.get());
  }

  static CraneErr AcquireTaskAttributes(TaskInCtld* task);

  static CraneErr CheckTaskValidity(TaskInCtld* task);

 private:
  template <class... Ts>
  struct VariantVisitor : Ts... {
    using Ts::operator()...;
  };

  template <class... Ts>
  VariantVisitor(Ts...) -> VariantVisitor<Ts...>;

  void RequeueRecoveredTaskIntoPendingQueueLock_(
      std::unique_ptr<TaskInCtld> task);

  void PutRecoveredTaskIntoRunningQueueLock_(std::unique_ptr<TaskInCtld> task);

  static void ProcessFinalTasks_(std::vector<TaskInCtld*> const& tasks);

  static void CallPluginHookForFinalTasks_(
      std::vector<TaskInCtld*> const& tasks);

  static void PersistAndTransferTasksToMongodb_(
      std::vector<TaskInCtld*> const& tasks);

  CraneErr TerminateRunningTaskNoLock_(TaskInCtld* task);

  CraneErr SetHoldForTaskInRamAndDb_(task_id_t task_id, bool hold);

  std::unique_ptr<INodeSelectionAlgo> m_node_selection_algo_;

  // Ordered by task id. Those who comes earlier are in the head,
  // Because they have smaller task id.
  TreeMap<task_id_t, std::unique_ptr<TaskInCtld>> m_pending_task_map_
      ABSL_GUARDED_BY(m_pending_task_map_mtx_);
  Mutex m_pending_task_map_mtx_;

  std::atomic_uint32_t m_pending_map_cached_size_;

  HashMap<task_id_t, std::unique_ptr<TaskInCtld>> m_running_task_map_
      ABSL_GUARDED_BY(m_running_task_map_mtx_);
  Mutex m_running_task_map_mtx_;

  // Task Indexes
  HashMap<CranedId, HashSet<uint32_t /* Task ID*/>> m_node_to_tasks_map_
      ABSL_GUARDED_BY(m_task_indexes_mtx_);
  Mutex m_task_indexes_mtx_;

  std::unique_ptr<IPrioritySorter> m_priority_sorter_;

  // If this variable is set to true, all threads must stop in a certain time.
  std::atomic_bool m_thread_stop_{};

  std::thread m_schedule_thread_;
  void ScheduleThread_();

  std::thread m_task_release_thread_;
  void ReleaseTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_cancel_thread_;
  void CancelTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_submit_thread_;
  void SubmitTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_status_change_thread_;
  void TaskStatusChangeThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  // Working as channels in golang.
  std::shared_ptr<uvw::timer_handle> m_task_timer_handle_;
  void CleanTaskTimerCb_();

  std::unordered_map<task_id_t, std::shared_ptr<uvw::timer_handle>>
      m_task_timer_handles_;

  std::shared_ptr<uvw::async_handle> m_task_timeout_async_handle_;

  using TaskTimerQueueElem =
      std::pair<std::pair<task_id_t, int64_t>, std::promise<CraneErr>>;
  ConcurrentQueue<TaskTimerQueueElem> m_task_timer_queue_;

  void TaskTimerAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_task_timer_queue_handle_;
  void CleanTaskTimerQueueCb_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::shared_ptr<uvw::timer_handle> m_cancel_task_timer_handle_;
  void CancelTaskTimerCb_();

  struct CancelPendingTaskQueueElem {
    std::unique_ptr<TaskInCtld> task;
  };

  struct CancelRunningTaskQueueElem {
    task_id_t task_id;
    CranedId craned_id;
  };

  using CancelTaskQueueElem =
      std::variant<CancelPendingTaskQueueElem, CancelRunningTaskQueueElem>;

  std::shared_ptr<uvw::async_handle> m_cancel_task_async_handle_;
  ConcurrentQueue<CancelTaskQueueElem> m_cancel_task_queue_;
  void CancelTaskAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_cancel_queue_handle_;
  void CleanCancelQueueCb_();

  std::shared_ptr<uvw::timer_handle> m_submit_task_timer_handle_;
  void SubmitTaskTimerCb_();

  std::shared_ptr<uvw::async_handle> m_submit_task_async_handle_;
  ConcurrentQueue<
      std::pair<std::unique_ptr<TaskInCtld>, std::promise<task_id_t>>>
      m_submit_task_queue_;
  void SubmitTaskAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_submit_queue_handle_;
  void CleanSubmitQueueCb_();

  std::shared_ptr<uvw::timer_handle> m_task_status_change_timer_handle_;
  void TaskStatusChangeTimerCb_();

  std::shared_ptr<uvw::async_handle> m_task_status_change_async_handle_;

  struct TaskStatusChangeArg {
    uint32_t task_id;
    uint32_t exit_code;
    crane::grpc::TaskStatus new_status;
    CranedId craned_index;
  };

  ConcurrentQueue<TaskStatusChangeArg> m_task_status_change_queue_;
  void TaskStatusChangeAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_task_status_change_handle_;
  void CleanTaskStatusChangeQueueCb_();
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::TaskScheduler> g_task_scheduler;