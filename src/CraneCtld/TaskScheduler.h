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
      if (task->Held()) {
        it->second->pending_reason = "Held";
        continue;
      }
      it->second->pending_reason = "";
      task_id_vec.emplace_back(it->first);
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
   * resource should be subtracted from the fields in all_partitions_meta.
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

  class TimeAvailResMapIter;

  class ResMapIterList {
   public:
    friend TimeAvailResMapIter;

    struct Node {
      TimeAvailResMapIter* res_map_it;
      absl::Time time;
      bool first_k;

      Node(TimeAvailResMapIter* it, absl::Time time, bool first_k)
          : res_map_it(it), time(time), first_k(first_k) {}
    };

    using ListContainer = std::list<Node>;
    using iterator = ListContainer::iterator;

    explicit ResMapIterList(size_t k_value)
        : m_k_value_(k_value), m_kth_it_(m_tracker_list_.end()) {}

    void emplace_back(TimeAvailResMapIter* it, absl::Time time) {
      if (it->m_tracker_list_it_ != m_tracker_list_.end()) return;

      m_tracker_list_.emplace_back(it, time,
                                   m_tracker_list_.size() < m_k_value_);
      if (m_tracker_list_.size() == m_k_value_) {
        CRANE_ASSERT(m_kth_it_ == m_tracker_list_.end());
        m_kth_it_ = std::prev(m_tracker_list_.end());
      }
      it->m_tracker_list_it_ = std::prev(m_tracker_list_.end());
    }

    void erase(TimeAvailResMapIter* it) {
      if (it->m_tracker_list_it_ == m_tracker_list_.end()) return;

      const Node& node = *it->m_tracker_list_it_;

      if (m_kth_it_ != m_tracker_list_.end() && node.first_k) {
        m_kth_it_ = std::next(m_kth_it_);
        if (m_kth_it_ != m_tracker_list_.end()) m_kth_it_->first_k = true;
      }
      m_tracker_list_.erase(it->m_tracker_list_it_);
      it->m_tracker_list_it_ = m_tracker_list_.end();
    }

    [[nodiscard]] absl::Time KthTime() const {
      if (m_kth_it_ == m_tracker_list_.end()) return absl::InfiniteFuture();
      return m_kth_it_->time;
    }

    iterator Begin() { return m_tracker_list_.begin(); }
    iterator KthIterator() const { return m_kth_it_; }
    iterator End() { return m_tracker_list_.end(); }

   private:
    ListContainer m_tracker_list_;
    const size_t m_k_value_;
    ListContainer::iterator m_kth_it_;
  };

  class TimeAvailResMapIter {
   public:
    TimeAvailResMapIter(const CranedId& craned_id,
                        const TimeAvailResMap::const_iterator& it,
                        const TimeAvailResMap::const_iterator& end,
                        ResMapIterList* tracker_list,
                        const ResourceInNode* task_res)
        : m_craned_id_(craned_id),
          m_it_(it),
          m_end_(end),
          task_res(task_res),
          m_tracker_list_it_(tracker_list->m_tracker_list_.end()) {
      m_satisfied_flag_ = Satisfied();
    }

    bool IsCurrentPosSatisfied() const { return m_satisfied_flag_; }
    bool ReachEnd() const { return m_it_ == m_end_; }

    TimeAvailResMapIter& operator++(int) {
      MoveToNext();
      return *this;
    }

    const TimeAvailResMap::value_type& operator*() const { return *m_it_; }
    const TimeAvailResMap::value_type* operator->() const { return &*m_it_; }

    const std::string& GetCranedId() const { return m_craned_id_; }

   private:
    friend ResMapIterList;

    bool Satisfied() const { return *task_res <= m_it_->second; }

    void MoveToNext() {
      m_satisfied_flag_ = !m_satisfied_flag_;  // target state
      if (m_satisfied_flag_)
        MoveToNextSatisfied();
      else
        MoveToNextUnsatisfied();
    }

    void MoveToNextUnsatisfied() { while (++m_it_ != m_end_ && Satisfied()); }
    void MoveToNextSatisfied() { while (++m_it_ != m_end_ && !Satisfied()); }

    const ResourceInNode* task_res;

    ResMapIterList::ListContainer::iterator m_tracker_list_it_;

    TimeAvailResMap::const_iterator m_it_;
    const TimeAvailResMap::const_iterator m_end_;

    const CranedId m_craned_id_;
    bool m_satisfied_flag_;
  };

  class NodeSelectionInfo {
   public:
    void InitCostAndTimeAvailResMap(const CranedId& craned_id,
                                    const ResourceInNode& res_total) {
      m_cost_node_id_set_.erase({m_node_cost_map_[craned_id], craned_id});
      m_node_cost_map_[craned_id] = 0;
      m_cost_node_id_set_.emplace(0, craned_id);
      m_node_res_total_map_[craned_id] = res_total;
      m_node_time_avail_res_map_[craned_id].clear();
    }

    void UpdateCost(const CranedId& craned_id, const absl::Time& start_time,
                    const absl::Time& end_time,
                    const ResourceInNode& resources) {
      uint64_t& cost = m_node_cost_map_.at(craned_id);
      m_cost_node_id_set_.erase({cost, craned_id});
      ResourceInNode& total_res = m_node_res_total_map_.at(craned_id);
      double cpu_ratio =
          static_cast<double>(resources.allocatable_res.cpu_count) /
          static_cast<double>(total_res.allocatable_res.cpu_count);
      cost += std::round((end_time - start_time) / absl::Seconds(1) *
                         cpu_ratio * 256);
      m_cost_node_id_set_.emplace(cost, craned_id);
    }

    TimeAvailResMap& GetTimeAvailResMap(const CranedId& craned_id) {
      return m_node_time_avail_res_map_.at(craned_id);
    }

    const TimeAvailResMap& GetTimeAvailResMap(const CranedId& craned_id) const {
      return m_node_time_avail_res_map_.at(craned_id);
    }

    const std::set<std::pair<uint64_t, CranedId>>& GetCostNodeIdSet() const {
      return m_cost_node_id_set_;
    }

    // Todo: Move to Reservation Mini-Scheduler.
    absl::Time GetFirstResvTime(const CranedId& craned_id) {
      auto iter = m_first_resv_time_map_.find(craned_id);
      if (iter == m_first_resv_time_map_.end()) return absl::InfiniteFuture();
      return iter->second;
    }

    void SetFirstResvTime(const CranedId& craned_id, absl::Time time) {
      m_first_resv_time_map_[craned_id] = time;
    }

   private:
    // Craned_ids are sorted by cost.
    std::set<std::pair<uint64_t, CranedId>> m_cost_node_id_set_;
    std::unordered_map<CranedId, uint64_t> m_node_cost_map_;
    std::unordered_map<CranedId, TimeAvailResMap> m_node_time_avail_res_map_;
    std::unordered_map<CranedId, absl::Time> m_first_resv_time_map_;

    // TODO: High copy cost, consider using pointer.
    std::unordered_map<CranedId, ResourceInNode> m_node_res_total_map_;
  };

  // Select a subset of craned nodes that can start the task as early as
  // possible. Not necessary if the number of craned nodes equals
  // task->node_num.
  class EarliestStartSubsetSelector {
   public:
    EarliestStartSubsetSelector(TaskInCtld* task,
                                const NodeSelectionInfo& node_selection_info,
                                const std::vector<CranedId>& craned_indexes)
        : m_satisfied_iters_(task->node_num),
          m_time_priority_queue_([](const TimeAvailResMapIter* lhs,
                                    const TimeAvailResMapIter* rhs) {
            return (*lhs)->first > (*rhs)->first;
          }) {
      m_res_map_iters_.reserve(craned_indexes.size());
      for (const CranedId& craned_id : craned_indexes) {
        const auto& time_avail_res_map =
            node_selection_info.GetTimeAvailResMap(craned_id);
        m_res_map_iters_.emplace_back(
            craned_id, time_avail_res_map.begin(), time_avail_res_map.end(),
            &m_satisfied_iters_, &task->Resources().at(craned_id));
        m_time_priority_queue_.emplace(&m_res_map_iters_.back());
      }
    }

    bool CalcEarliestStartTime(TaskInCtld* task, absl::Time now,
                               absl::Time* start_time,
                               std::list<CranedId>* craned_ids) {
      while (!m_time_priority_queue_.empty()) {
        absl::Time current_time = (*m_time_priority_queue_.top())->first;
        if (current_time - now > kAlgoMaxTimeWindow) return false;

        // Iterate over all iterators that have the same time.
        while (true) {
          if (m_time_priority_queue_.empty()) break;

          TimeAvailResMapIter* it = m_time_priority_queue_.top();
          if ((*it)->first != current_time) break;

          m_time_priority_queue_.pop();
          if (it->IsCurrentPosSatisfied())
            m_satisfied_iters_.emplace_back(it, current_time);
          else
            m_satisfied_iters_.erase(it);

          (*it)++;
          if (!it->ReachEnd()) m_time_priority_queue_.emplace(it);
        }

        absl::Time kth_time = m_satisfied_iters_.KthTime();
        if (kth_time == absl::InfiniteFuture()) continue;

        if (m_time_priority_queue_.empty() ||
            kth_time + task->time_limit <=
                (*m_time_priority_queue_.top())->first) {
          *start_time = kth_time;

          craned_ids->clear();
          auto it = m_satisfied_iters_.Begin();
          while (true) {
            craned_ids->emplace_back(it->res_map_it->GetCranedId());
            if (it++ == m_satisfied_iters_.KthIterator()) break;
          }

          CRANE_ASSERT(*start_time != absl::InfiniteFuture());
          CRANE_ASSERT(craned_ids->size() == task->node_num);
          return true;
        }
      }
      return false;
    }

   private:
    ResMapIterList m_satisfied_iters_;

    std::vector<TimeAvailResMapIter> m_res_map_iters_;
    std::priority_queue<TimeAvailResMapIter*, std::vector<TimeAvailResMapIter*>,
                        std::function<bool(const TimeAvailResMapIter*,
                                           const TimeAvailResMapIter*)>>
        m_time_priority_queue_;
  };

  static void CalculateNodeSelectionInfoOfPartition_(
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::Time now, const PartitionId& partition_id,
      const std::unordered_set<CranedId>& craned_ids,
      const CranedMetaContainer::CranedMetaRawMap& craned_meta_map,
      NodeSelectionInfo* node_selection_info);

  // TODO: Move to Reservation Mini-Scheduler.
  static void CalculateNodeSelectionInfoOfReservation_(
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::Time now, const ResvMeta* resv_meta,
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

  std::future<CraneErrCode> HoldReleaseTaskAsync(task_id_t task_id,
                                                 int64_t secs);

  CraneErrCode ChangeTaskTimeLimit(task_id_t task_id, int64_t secs);

  CraneErrCode ChangeTaskPriority(task_id_t task_id, double priority);

  void TaskStatusChangeWithReasonAsync(uint32_t task_id,
                                       const CranedId& craned_index,
                                       crane::grpc::TaskStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string>&& reason) {
    // TODO: Add reason implementation here!
    TaskStatusChangeAsync(task_id, craned_index, new_status, exit_code);
  }

  void TaskStatusChangeAsync(uint32_t task_id, const CranedId& craned_index,
                             crane::grpc::TaskStatus new_status,
                             uint32_t exit_code);

  void TerminateTasksOnCraned(const CranedId& craned_id, uint32_t exit_code);

  // Temporary inconsistency may happen. If 'false' is returned, just ignore it.
  void QueryTasksInRam(const crane::grpc::QueryTasksInfoRequest* request,
                       crane::grpc::QueryTasksInfoReply* response);

  void QueryJobOfNode(const CranedId& craned_id,
                      crane::grpc::ConfigureCranedRequest* req);

  void TerminateJobs(const std::vector<task_id_t>& jobs);

  crane::grpc::CancelTaskReply CancelPendingOrRunningTask(
      const crane::grpc::CancelTaskRequest& request);

  CraneErrCode TerminatePendingOrRunningIaTask(uint32_t task_id) {
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
      return CraneErrCode::SUCCESS;
    }

    auto rn_it = m_running_task_map_.find(task_id);
    if (rn_it == m_running_task_map_.end())
      return CraneErrCode::ERR_NON_EXISTENT;
    else {
      auto& task = rn_it->second;
      if (task->type == crane::grpc::TaskType::Interactive) {
        auto& meta = std::get<InteractiveMetaInTask>(task->meta);
        meta.has_been_cancelled_on_front_end = true;
      }
    }

    return TerminateRunningTaskNoLock_(rn_it->second.get());
  }

  CraneErrCode TerminateRunningTask(uint32_t task_id) {
    LockGuard running_guard(&m_running_task_map_mtx_);

    auto iter = m_running_task_map_.find(task_id);
    if (iter == m_running_task_map_.end())
      return CraneErrCode::ERR_NON_EXISTENT;

    return TerminateRunningTaskNoLock_(iter->second.get());
  }

  static CraneExpected<void> HandleUnsetOptionalInTaskToCtld(TaskInCtld* task);
  static CraneExpected<void> AcquireTaskAttributes(TaskInCtld* task);
  static CraneExpected<void> CheckTaskValidity(TaskInCtld* task);

  // TODO: Move to Reservation Mini-Scheduler.
  crane::grpc::CreateReservationReply CreateResv(
      const crane::grpc::CreateReservationRequest& request);

  crane::grpc::DeleteReservationReply DeleteResv(
      const crane::grpc::DeleteReservationRequest& request);

 private:
  std::expected<void, std::string> CreateResv_(
      const crane::grpc::CreateReservationRequest& request);

  std::expected<void, std::string> DeleteResvMeta_(
      CranedMetaContainer::ResvMetaMapPtr& resv_meta_map,
      const ResvId& resv_id);

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

  CraneErrCode TerminateRunningTaskNoLock_(TaskInCtld* task);

  CraneErrCode SetHoldForTaskInRamAndDb_(task_id_t task_id, bool hold);

  std::unique_ptr<INodeSelectionAlgo> m_node_selection_algo_;

  // Ordered by task id. Those who comes earlier are in the head,
  // Because they have smaller task id.
  TreeMap<task_id_t, std::unique_ptr<TaskInCtld>> m_pending_task_map_
      ABSL_GUARDED_BY(m_pending_task_map_mtx_);
  Mutex m_pending_task_map_mtx_;

  std::atomic_uint32_t m_pending_map_cached_size_;

  HashMap<task_id_t, std::unique_ptr<TaskInCtld>> m_running_task_map_
      ABSL_GUARDED_BY(m_running_task_map_mtx_);
  Mutex m_running_task_map_mtx_ ABSL_ACQUIRED_AFTER(m_pending_task_map_mtx_);

  // Task Indexes
  HashMap<CranedId, HashSet<uint32_t /* Task ID*/>> m_node_to_tasks_map_
      ABSL_GUARDED_BY(m_task_indexes_mtx_);
  Mutex m_task_indexes_mtx_ ABSL_ACQUIRED_AFTER(m_running_task_map_mtx_);

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
      std::pair<std::pair<task_id_t, int64_t>, std::promise<CraneErrCode>>;
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

  // TODO: Move to Reservation Mini-Scheduler.
  std::thread m_resv_clean_thread_;
  void CleanResvThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::unordered_map<ResvId, std::shared_ptr<uvw::timer_handle>>
      m_resv_timer_handles_;

  using ResvTimerQueueElem = std::pair<ResvId, absl::Time>;
  ConcurrentQueue<ResvTimerQueueElem> m_resv_timer_queue_;

  std::shared_ptr<uvw::async_handle> m_clean_resv_timer_queue_handle_;
  void CleanResvTimerQueueCb_(const std::shared_ptr<uvw::loop>& uvw_loop);
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::TaskScheduler> g_task_scheduler;