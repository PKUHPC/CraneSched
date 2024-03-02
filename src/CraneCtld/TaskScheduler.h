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

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "CranedMetaContainer.h"
#include "DbClient.h"
#include "crane/Lock.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using OrderedTaskMap = absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>;

using UnorderedTaskMap =
    absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>;

class IPrioritySorter {
 public:
  virtual std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map) = 0;

  virtual ~IPrioritySorter() = default;
};

class BasicPriority : public IPrioritySorter {
 public:
  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map) override {
    std::vector<task_id_t> task_id_vec;
    for (const auto& pair : pending_task_map) task_id_vec.push_back(pair.first);
    return task_id_vec;
  }
};

class MultiFactorPriority : public IPrioritySorter {
 public:
  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map) override;

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
                             const UnorderedTaskMap& running_task_map);
  double CalculatePriority_(Ctld::TaskInCtld* task);

  FactorBound m_factor_bound_;
};

class INodeSelectionAlgo {
 public:
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
  using TimeAvailResMap = std::map<absl::Time, Resources>;

  struct TimeSegment {
    TimeSegment(absl::Time start, absl::Duration duration)
        : start(start), duration(duration) {}
    absl::Time start;
    absl::Duration duration;

    bool operator<(const absl::Time& rhs) const { return this->start < rhs; }

    friend bool operator<(const absl::Time& lhs, const TimeSegment& rhs) {
      return lhs < rhs.start;
    }
  };

  struct NodeSelectionInfo {
    std::multimap<uint32_t /* # of running tasks */, CranedId>
        task_num_node_id_map;
    std::unordered_map<CranedId, TimeAvailResMap> node_time_avail_res_map;
  };

  static void CalculateNodeSelectionInfoOfPartition_(
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::Time now, const PartitionId& partition_id,
      const util::Synchronized<PartitionMeta>& partition_meta_ptr,
      const CranedMetaContainerInterface::CranedMetaRawMap& craned_meta_map,
      NodeSelectionInfo* node_selection_info);

  // Input should guarantee that provided nodes in `node_selection_info` has
  // enough nodes whose resource is >= task->resource.
  static bool CalculateRunningNodesAndStartTime_(
      const NodeSelectionInfo& node_selection_info,
      const util::Synchronized<PartitionMeta>& partition_meta_ptr,
      const CranedMetaContainerInterface::CranedMetaRawMap& craned_meta_map,
      TaskInCtld* task, absl::Time now, std::list<CranedId>* craned_ids,
      absl::Time* start_time);

  static void SubtractTaskResourceNodeSelectionInfo_(
      absl::Time const& expected_start_time, absl::Duration const& duration,
      Resources const& resources, std::list<CranedId> const& craned_ids,
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

  CraneErr ChangeTaskTimeLimit(uint32_t task_id, int64_t secs);

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

  CraneErr TerminateRunningTask(uint32_t task_id) {
    LockGuard running_guard(&m_running_task_map_mtx_);

    auto iter = m_running_task_map_.find(task_id);
    if (iter == m_running_task_map_.end()) return CraneErr::kNonExistent;

    return TerminateRunningTaskNoLock_(iter->second.get());
  }

  static CraneErr AcquireTaskAttributes(TaskInCtld* task);

  static CraneErr CheckTaskValidity(TaskInCtld* task);

 private:
  void RequeueRecoveredTaskIntoPendingQueueLock_(
      std::unique_ptr<TaskInCtld> task);

  void PutRecoveredTaskIntoRunningQueueLock_(std::unique_ptr<TaskInCtld> task);

  static void TransferTasksToMongodb_(std::vector<TaskInCtld*> const& tasks);

  CraneErr TerminateRunningTaskNoLock_(TaskInCtld* task);

  std::unique_ptr<INodeSelectionAlgo> m_node_selection_algo_;

  // Ordered by task id. Those who comes earlier are in the head,
  // Because they have smaller task id.
  TreeMap<task_id_t, std::unique_ptr<TaskInCtld>> m_pending_task_map_
      GUARDED_BY(m_pending_task_map_mtx_);
  Mutex m_pending_task_map_mtx_;

  std::atomic_uint32_t m_pending_map_cached_size_;

  HashMap<task_id_t, std::unique_ptr<TaskInCtld>> m_running_task_map_
      GUARDED_BY(m_running_task_map_mtx_);
  Mutex m_running_task_map_mtx_;

  HashMap<uint32_t /*Task Db Id*/, std::unique_ptr<TaskInEmbeddedDb>>
      m_persisted_task_map_ GUARDED_BY(m_persisted_task_map_mtx_);
  Mutex m_persisted_task_map_mtx_;

  // Task Indexes
  HashMap<CranedId, HashSet<uint32_t /* Task ID*/>> m_node_to_tasks_map_
      GUARDED_BY(m_task_indexes_mtx_);
  Mutex m_task_indexes_mtx_;

  std::unique_ptr<IPrioritySorter> m_priority_sorter_;

  // If this variable is set to true, all threads must stop in a certain time.
  std::atomic_bool m_thread_stop_{};

  std::thread m_schedule_thread_;
  void ScheduleThread_();

  std::thread m_task_cancel_thread_;
  void CancelTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_submit_thread_;
  void SubmitTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_status_change_thread_;
  void TaskStatusChangeThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  // Working as channels in golang.
  std::shared_ptr<uvw::timer_handle> m_cancel_task_timer_handle_;
  void CancelTaskTimerCb_();

  std::shared_ptr<uvw::async_handle> m_cancel_task_async_handle_;
  ConcurrentQueue<std::pair<task_id_t, CranedId>> m_cancel_task_queue_;
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