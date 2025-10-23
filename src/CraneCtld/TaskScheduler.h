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
#include "LicensesManager.h"
#include "protos/Crane.pb.h"

namespace Ctld {

class IUpdateNodeCostPolicy {
 public:
  virtual ~IUpdateNodeCostPolicy() = default;
  virtual void UpdateCost(double& cost, const absl::Time& start_time,
                          const absl::Time& end_time,
                          const ResourceInNode& resources,
                          const ResourceInNode& total_res) const = 0;
};

class MinCpuTimeRatioFirst : public IUpdateNodeCostPolicy {
 public:
  void UpdateCost(double& cost, const absl::Time& start_time,
                  const absl::Time& end_time, const ResourceInNode& resources,
                  const ResourceInNode& total_res) const override {
    cost += absl::ToInt64Seconds(end_time - start_time) *
            (static_cast<double>(resources.allocatable_res.cpu_count) /
             static_cast<double>(total_res.allocatable_res.cpu_count));
  }
};

struct RnJobInScheduler {
  task_id_t job_id;
  absl::Duration time_limit;

  PartitionId partition_id;
  std::string reservation;

  absl::Time submit_time;
  uint32_t partition_priority;
  uint32_t qos_priority;
  std::string account;

  uint32_t node_num;
  absl::Time start_time;
  absl::Time end_time;
  ResourceV2 allocated_res;
  ResourceView allocated_res_view;

  RnJobInScheduler(TaskInCtld* job)
      : job_id(job->TaskId()),
        time_limit(job->time_limit),
        partition_id(job->partition_id),
        reservation(job->reservation),
        submit_time(job->SubmitTime()),
        partition_priority(job->partition_priority),
        qos_priority(job->qos_priority),
        account(job->account),
        start_time(job->StartTime()),
        end_time(job->EndTime()),
        allocated_res(job->AllocatedRes()),
        allocated_res_view(job->allocated_res_view) {}
};

class IPrioritySorter {
 public:
  virtual void GetOrderedJobPtrVec(
      const absl::Time& now,
      const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs,
      const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
      size_t limit, std::vector<PdJobInScheduler*>& job_ptr_vec) = 0;

  virtual ~IPrioritySorter() = default;
};

class BasicPriority : public IPrioritySorter {
 public:
  void GetOrderedJobPtrVec(
      const absl::Time& now,
      const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs,
      const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
      size_t limit, std::vector<PdJobInScheduler*>& job_ptr_vec) override {
    size_t len = std::min(pending_jobs.size(), limit);

    job_ptr_vec.reserve(len);

    for (int i = 0; i < len; i++) {
      job_ptr_vec.emplace_back(pending_jobs[i].get());
    }
    for (int i = len; i < pending_jobs.size(); i++) {
      pending_jobs[i]->reason = "Priority";
    }
  }
};

class MultiFactorPriority : public IPrioritySorter {
 public:
  void GetOrderedJobPtrVec(
      const absl::Time& now,
      const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs,
      const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
      size_t limit, std::vector<PdJobInScheduler*>& job_ptr_vec) override;

 private:
  struct FactorBound {
    uint64_t age_max, age_min;
    uint32_t qos_priority_max, qos_priority_min;
    uint32_t part_priority_max, part_priority_min;
    uint32_t node_num_max, node_num_min;
    uint64_t mem_alloc_max, mem_alloc_min;
    double cpus_alloc_max, cpus_alloc_min;
    double service_val_max, service_val_min;

    absl::flat_hash_map<std::string, double> acc_service_val_map;
  };

  void CalculateFactorBound_(
      const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs,
      const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
      const absl::Time& now);
  double CalculatePriority_(PdJobInScheduler* job, const absl::Time& now) const;

  FactorBound m_factor_bound_;
};

class SchedulerAlgo {
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

  SchedulerAlgo(IPrioritySorter* priority_sorter)
      : m_priority_sorter_(priority_sorter) {}

  /**
   * Do node selection for all pending jobs.
   * Note: After this function call, events reduce resources should be
   * handled.
   * @param[in,out] running_jobs Const reference to the vector of running
   jobs.
   * @param[in,out] pending_jobs Const reference to the vector of pending
   jobs,
   * results of scheduling are stored in JobInScheduler.
   */
  void NodeSelect(
      const absl::Time& now,
      const std::vector<std::unique_ptr<RnJobInScheduler>>& running_jobs,
      const std::vector<std::unique_ptr<PdJobInScheduler>>& pending_jobs);

 private:
  static constexpr bool kAlgoTraceOutput = false;
  // TODO: move to config
  static constexpr bool kAlgoRedundantNode = false;
  static constexpr uint32_t kAlgoMaxTaskNumPerNode = 1000;
  static constexpr absl::Duration kAlgoMaxTimeWindow = absl::Hours(24 * 7);

  struct NodeState {
    // Running jobs and active reservations
    struct AllocatedRes {
      absl::Time end_time;
      ResourceInNode res;
    };
    // Pending reservations
    struct ReservedRes {
      absl::Time start_time;
      absl::Time end_time;
      ResourceInNode res;
    };

    const CranedId craned_id;
    const ResourceInNode res_total;
    ResourceInNode res_avail;
    std::vector<AllocatedRes> allocated_res;
    std::vector<ReservedRes> reserved_res;

    TimeAvailResMap time_avail_res_map;

    NodeState(const CranedId& craned_id, const ResourceInNode& res_total)
        : craned_id(craned_id), res_total(res_total), res_avail(res_total) {}

    void InitTimeAvailResMap(const absl::Time& now,
                             const absl::Time& end = absl::InfiniteFuture()) {
      std::vector<std::pair<absl::Time, std::pair<bool, const ResourceInNode*>>>
          resource_changes;
      for (const auto& [start_time, end_time, res] : reserved_res) {
        resource_changes.emplace_back(start_time, std::make_pair(true, &res));
        resource_changes.emplace_back(end_time, std::make_pair(false, &res));
      }

      for (const auto& [end_time, res] : allocated_res) {
        resource_changes.emplace_back(end_time, std::make_pair(false, &res));
        res_avail -= res;
      }

      std::ranges::sort(resource_changes, [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first ||
               (lhs.first == rhs.first &&
                lhs.second.first <
                    rhs.second.first);  // release before allocate
      });

      auto [cur_iter, ok] = time_avail_res_map.emplace(now, res_avail);

      for (const auto& change : resource_changes) {
        if (change.first != cur_iter->first) {
          std::tie(cur_iter, ok) =
              time_avail_res_map.emplace(change.first, cur_iter->second);
        }
        if (change.second.first) {
          cur_iter->second -= *(change.second.second);
        } else {
          cur_iter->second += *(change.second.second);
        }
      }

      time_avail_res_map[end].SetToZero();
    }

    void SubtractResourceInNode(const absl::Time& start_time,
                                const absl::Time& end_time,
                                const ResourceInNode& res) {
      bool ok;
      auto task_duration_begin_it = time_avail_res_map.upper_bound(start_time);
      if (task_duration_begin_it == time_avail_res_map.end()) {
        --task_duration_begin_it;  // task_duration_begin_it->first <=
                                   // start_time < end_time
        // Case #1
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
        // OR Case #2
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
            end_time, task_duration_begin_it->second);
        CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

        if (task_duration_begin_it->first == start_time) {
          // Case #1, subtract resource at start_time
          CRANE_ASSERT(res <= task_duration_begin_it->second);
          task_duration_begin_it->second -= res;
        } else {
          // Case #2, insert subtracted resource at start_time
          std::tie(inserted_it, ok) = time_avail_res_map.emplace(
              start_time, task_duration_begin_it->second);
          CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

          CRANE_ASSERT(res <= inserted_it->second);
          inserted_it->second -= res;
        }
      } else {
        --task_duration_begin_it;  // task_duration_begin_it->first <=
                                   // start_time < end_time
        // Case #3
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
        // Or Case #4
        //               task duration
        //         |<----------------->|
        // *-------*----------*--------*------------
        //         ^                   ^
        //  task_duration_begin_it   task_duration_end_it

        // std::prev can be used without any check here.
        // There will always be one time point (now) before end_time.

        if (task_duration_begin_it->first != start_time) {
          // Case #3, copy resource before start_time to start_time
          TimeAvailResMap::iterator inserted_it;
          std::tie(inserted_it, ok) = time_avail_res_map.emplace(
              start_time, task_duration_begin_it->second);
          CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

          task_duration_begin_it = inserted_it;
        }
        auto task_duration_end_it = std::prev(time_avail_res_map.upper_bound(
            end_time));  // task_duration_end_it->first <= end_time

        // Subtract the required resources within the interval.
        for (auto in_duration_it = task_duration_begin_it;
             in_duration_it != task_duration_end_it; in_duration_it++) {
          CRANE_ASSERT(res <= in_duration_it->second);
          in_duration_it->second -= res;
        }

        if (task_duration_end_it->first != end_time) {
          TimeAvailResMap::iterator inserted_it;
          std::tie(inserted_it, ok) = time_avail_res_map.emplace(
              end_time, task_duration_end_it->second);
          CRANE_ASSERT_MSG(ok == true, "Insertion must be successful.");

          CRANE_ASSERT(res <= task_duration_end_it->second);
          task_duration_end_it->second -= res;
        }
      }
    }
  };

  class INodeSelector {
   public:
    virtual ~INodeSelector() = default;

    virtual TimeAvailResMap& GetTimeAvailResMap(const CranedId& craned_id) = 0;
    virtual const TimeAvailResMap& GetTimeAvailResMap(
        const CranedId& craned_id) const = 0;

    virtual void AddNode(const absl::Time& now, NodeState* node_state,
                         double cost = 0.0) = 0;

    virtual void UpdateCost(const CranedId& craned_id,
                            const absl::Time& start_time,
                            const absl::Time& end_time,
                            const ResourceInNode& resources) = 0;

    virtual NodeState* GetNodeState(const CranedId& craned_id) const = 0;
    virtual const std::set<std::pair<double, NodeState*>>& GetOrderedNodesSet()
        const = 0;

    virtual void SubtractResource(const absl::Time& start_time,
                                  const absl::Time& end_time,
                                  const ResourceV2& res) = 0;
  };

  class NodeSelector : public INodeSelector {
   public:
    explicit NodeSelector(
        std::unique_ptr<const IUpdateNodeCostPolicy> update_cost_policy)
        : m_update_cost_policy_(std::move(update_cost_policy)) {}

    struct NodeRater {
      NodeRater(const IUpdateNodeCostPolicy* policy, const absl::Time& now,
                NodeState* node_state, double initial_cost = 0.0)
          : node_state(node_state), cost(initial_cost) {
        for (const auto& [start_time, end_time, res] :
             node_state->reserved_res) {
          policy->UpdateCost(cost, start_time, end_time, res,
                             node_state->res_total);
        }

        for (const auto& [end_time, res] : node_state->allocated_res) {
          policy->UpdateCost(cost, now, end_time, res, node_state->res_total);
        }
      }

      NodeState* node_state;
      double cost;
      std::set<std::pair<double, NodeState*>>::iterator cost_node_info_set_it;
    };

    TimeAvailResMap& GetTimeAvailResMap(const CranedId& craned_id) override {
      return m_node_info_map_.at(craned_id).node_state->time_avail_res_map;
    }
    const TimeAvailResMap& GetTimeAvailResMap(
        const CranedId& craned_id) const override {
      return m_node_info_map_.at(craned_id).node_state->time_avail_res_map;
    }

    void UpdateCost(const CranedId& craned_id, const absl::Time& start_time,
                    const absl::Time& end_time,
                    const ResourceInNode& resources) override {
      NodeRater& node_info = m_node_info_map_.at(craned_id);
      m_cost_node_info_set_.erase(node_info.cost_node_info_set_it);
      m_update_cost_policy_->UpdateCost(node_info.cost, start_time, end_time,
                                        resources,
                                        node_info.node_state->res_total);
      node_info.cost_node_info_set_it =
          m_cost_node_info_set_.emplace(node_info.cost, node_info.node_state)
              .first;
    }

    void AddNode(const absl::Time& now, NodeState* node_state,
                 double cost = 0.0) override {
      NodeRater& node_info = m_node_info_map_
                                 .emplace(node_state->craned_id,
                                          NodeRater(m_update_cost_policy_.get(),
                                                    now, node_state, cost))
                                 .first->second;
      node_info.cost_node_info_set_it =
          m_cost_node_info_set_.emplace(node_info.cost, node_info.node_state)
              .first;
    }

    NodeState* GetNodeState(const CranedId& craned_id) const override {
      return m_node_info_map_.at(craned_id).node_state;
    }

    const std::set<std::pair<double, NodeState*>>& GetOrderedNodesSet()
        const override {
      return m_cost_node_info_set_;
    }

    void SubtractResource(const absl::Time& start_time,
                          const absl::Time& end_time,
                          const ResourceV2& res) override {
      for (const auto& [craned_id, res_in_node] : res.EachNodeResMap()) {
        m_node_info_map_.at(craned_id).node_state->SubtractResourceInNode(
            start_time, end_time, res_in_node);
        UpdateCost(craned_id, start_time, end_time, res_in_node);
      }
    }

   private:
    std::unique_ptr<const IUpdateNodeCostPolicy> m_update_cost_policy_;
    absl::flat_hash_map<CranedId, NodeRater> m_node_info_map_;
    // Nodes are sorted by cost, store NodeState* rather than CranedId to reduce
    // memory usage and improve performance.
    std::set<std::pair<double, NodeState*>> m_cost_node_info_set_;
  };

  class LocalScheduler {
   public:
    using Mutex = absl::Mutex;
    using LockGuard = absl::MutexLock;
    using TimeAvailResMap = std::map<absl::Time, ResourceInNode>;

    template <typename Policy, typename... Args>
    void InitializeNodeSelector(const absl::Time& now,
                                const std::vector<NodeState*>& node_info_vec,
                                Args&&... args) {
      m_node_selector_ = std::make_unique<NodeSelector>(
          std::make_unique<const Policy>(std::forward<Args>(args)...));
      for (const auto& node_state : node_info_vec) {
        m_node_selector_->AddNode(now, node_state);
      }
    }

    bool CalculateRunningNodesAndStartTime_(const absl::Time& now,
                                            PdJobInScheduler* job);

    void UpdateNodeSelector(PdJobInScheduler* job) {
      m_node_selector_->SubtractResource(job->start_time,
                                         job->start_time + job->time_limit,
                                         job->allocated_res);
    }

   private:
    std::unique_ptr<INodeSelector> m_node_selector_;
  };

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

  class EarliestStartSubsetSelector {
   public:
    EarliestStartSubsetSelector(PdJobInScheduler* job,
                                const std::vector<NodeState*> nodes)
        : m_satisfied_iters_(job->node_num),
          m_time_priority_queue_([](const TimeAvailResMapIter* lhs,
                                    const TimeAvailResMapIter* rhs) {
            return (*lhs)->first > (*rhs)->first;
          }) {
      m_res_map_iters_.reserve(nodes.size());
      for (const NodeState* node : nodes) {
        const auto& time_avail_res_map = node->time_avail_res_map;
        m_res_map_iters_.emplace_back(
            node->craned_id, time_avail_res_map.begin(),
            time_avail_res_map.end(), &m_satisfied_iters_,
            &job->allocated_res.at(node->craned_id));
        m_time_priority_queue_.emplace(&m_res_map_iters_.back());
      }
    }

    bool CalcEarliestStartTime(absl::Time now, PdJobInScheduler* job) {
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
            kth_time + job->time_limit <=
                (*m_time_priority_queue_.top())->first) {
          job->start_time = kth_time;

          job->craned_ids.clear();
          auto it = m_satisfied_iters_.Begin();
          while (true) {
            job->craned_ids.emplace_back(it->res_map_it->GetCranedId());
            if (it++ == m_satisfied_iters_.KthIterator()) break;
          }

          CRANE_ASSERT(job->start_time != absl::InfiniteFuture());
          CRANE_ASSERT(job->craned_ids.size() == job->node_num);
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

  /// \return The future is set to an error code if task submission failed.
  /// Otherwise, it is set to newly allocated task id.
  std::future<CraneExpected<task_id_t>> SubmitTaskAsync(
      std::unique_ptr<TaskInCtld> task);

  std::future<CraneExpected<step_id_t>> SubmitStepAsync(
      std::unique_ptr<CommonStepInCtld> step);

  std::future<CraneErrCode> HoldReleaseTaskAsync(task_id_t task_id,
                                                 int64_t secs);

  CraneErrCode ChangeTaskTimeLimit(task_id_t task_id, int64_t secs);

  CraneErrCode ChangeTaskPriority(task_id_t task_id, double priority);

  CraneErrCode ChangeTaskExtraAttrs(task_id_t task_id,
                                    const std::string& new_extra_attr);

  std::optional<std::future<CraneRichError>> JobSubmitLuaCheck(
      TaskInCtld* task);

  void JobModifyLuaCheck(const crane::grpc::ModifyTaskRequest& request,
                         crane::grpc::ModifyTaskReply* response,
                         std::list<task_id_t>* task_ids);

  CraneExpected<std::future<CraneExpected<task_id_t>>> SubmitTaskToScheduler(
      std::unique_ptr<TaskInCtld> task);

  void StepStatusChangeWithReasonAsync(uint32_t task_id, step_id_t step_id,
                                       const CranedId& craned_index,
                                       crane::grpc::TaskStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string>&& reason,
                                       google::protobuf::Timestamp timestamp) {
    // TODO: Add reason implementation here!
    StepStatusChangeAsync(task_id, step_id, craned_index, new_status, exit_code,
                          reason.value_or(""), std::move(timestamp));
  }

  void StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                             const CranedId& craned_index,
                             crane::grpc::TaskStatus new_status,
                             uint32_t exit_code, std::string reason,
                             google::protobuf::Timestamp timestamp);

  void TerminateTasksOnCraned(const CranedId& craned_id, uint32_t exit_code);

  void QueryTasksInRam(
      const crane::grpc::QueryTasksInfoRequest* request,
      std::unordered_map<job_id_t, crane::grpc::TaskInfo>* job_info_map);

  void QueryRnJobOnCtldForNodeConfig(const CranedId& craned_id,
                                     crane::grpc::ConfigureCranedRequest* req);

  void TerminateOrphanedSteps(
      const std::unordered_map<job_id_t, std::set<step_id_t>>& steps,
      const CranedId& excluded_node);

  crane::grpc::CancelTaskReply CancelPendingOrRunningTask(
      const crane::grpc::CancelTaskRequest& request);

  crane::grpc::AttachContainerStepReply AttachContainerStep(
      const crane::grpc::AttachContainerStepRequest& request);

  crane::grpc::ExecInContainerStepReply ExecInContainerStep(
      const crane::grpc::ExecInContainerStepRequest& request);

  CraneErrCode TerminatePendingOrRunningIaStep(job_id_t job_id,
                                               step_id_t step_id) {
    LockGuard pending_guard(&m_pending_task_map_mtx_);
    LockGuard running_guard(&m_running_task_map_mtx_);

    auto pd_it = m_pending_task_map_.find(job_id);
    if (pd_it != m_pending_task_map_.end()) {
      auto& task = pd_it->second;
      if (task->type != crane::grpc::Interactive) {
        return CraneErrCode::ERR_INVALID_PARAM;
      }
      auto& meta = std::get<InteractiveMeta>(task->meta);
      meta.has_been_cancelled_on_front_end = true;

      m_cancel_task_queue_.enqueue(
          CancelPendingTaskQueueElem{.task = std::move(task)});
      m_cancel_task_async_handle_->send();
      m_pending_task_map_.erase(pd_it);
      return CraneErrCode::SUCCESS;
    }

    auto rn_it = m_running_task_map_.find(job_id);
    if (rn_it == m_running_task_map_.end())
      return CraneErrCode::ERR_NON_EXISTENT;

    CommonStepInCtld* step;
    auto& task = rn_it->second;
    step = task->GetStep(step_id);
    if (step) {
      if (step->type == crane::grpc::Interactive) {
        auto& meta = step->ia_meta.value();
        meta.has_been_cancelled_on_front_end = true;
        if (step->Status() == crane::grpc::TaskStatus::Pending) {
          m_cancel_task_queue_.enqueue(
              CancelPendingStepQueueElem{.step = task->EraseStep(step_id)});
          m_cancel_task_async_handle_->send();
          return CraneErrCode::SUCCESS;
        }
      } else {
        return CraneErrCode::ERR_INVALID_PARAM;
      }
    } else {
      return CraneErrCode::ERR_NON_EXISTENT;
    }

    return TerminateRunningStepNoLock_(step);
  }

  CraneErrCode TerminateRunningStep(
      std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps) {
    LockGuard running_guard(&m_running_task_map_mtx_);

    std::vector<CommonStepInCtld*> steps;
    for (auto& [job_id, step_ids] : job_steps) {
      auto iter = m_running_task_map_.find(job_id);
      if (iter == m_running_task_map_.end())
        return CraneErrCode::ERR_NON_EXISTENT;
      auto* job = iter->second.get();
      for (auto step_id : step_ids) {
        auto* step = job->GetStep(step_id);
        if (!step) return CraneErrCode::ERR_NON_EXISTENT;
        steps.push_back(step);
      }
    }

    for (auto* step : steps) {
      auto err = TerminateRunningStepNoLock_(step);
      if (err != CraneErrCode::SUCCESS) return err;
    }
    return CraneErrCode::SUCCESS;
  }

  static CraneExpected<void> HandleUnsetOptionalInTaskToCtld(TaskInCtld* task);
  static CraneExpected<void> AcquireTaskAttributes(TaskInCtld* task);
  static CraneExpected<void> CheckTaskValidity(TaskInCtld* task);

  static CraneExpected<void> AcquireStepAttributes(StepInCtld* step);
  static CraneExpected<void> CheckStepValidity(StepInCtld* step);

  // TODO: Move to Reservation Mini-Scheduler.
  crane::grpc::CreateReservationReply CreateResv(
      const crane::grpc::CreateReservationRequest& request);

  crane::grpc::DeleteReservationReply DeleteResv(
      const crane::grpc::DeleteReservationRequest& request);

  // For failed dependency, timestamp should be absl::InfiniteFuture()
  void AddDependencyEvent(task_id_t dependent, task_id_t dependee,
                          absl::Time timestamp) {
    m_dependency_event_queue_.enqueue(
        DependencyEvent{.dependent_job_id = dependent,
                        .dependee_job_id = dependee,
                        .event_time = timestamp});
  }

 private:
  void RequeueRecoveredTaskIntoPendingQueueLock_(
      std::unique_ptr<TaskInCtld> task);

  void PutRecoveredTaskIntoRunningQueueLock_(std::unique_ptr<TaskInCtld> task);
  void HandleFailToRecoverRngJob_(task_id_t task_id);

  static void ProcessFinalSteps_(std::unordered_set<StepInCtld*> const& steps);
  static void PersistAndTransferStepsToMongodb_(
      std::unordered_set<StepInCtld*> const& steps);

  static void ProcessFinalTasks_(const std::unordered_set<TaskInCtld*>& tasks);

  static void CallPluginHookForFinalTasks_(
      std::unordered_set<TaskInCtld*> const& tasks);

  static void PersistAndTransferTasksToMongodb_(
      std::unordered_set<TaskInCtld*> const& tasks);

  CraneErrCode TerminateRunningStepNoLock_(CommonStepInCtld* step);

  CraneErrCode SetHoldForTaskInRamAndDb_(task_id_t task_id, bool hold);

  std::expected<void, std::string> CreateResv_(
      const crane::grpc::CreateReservationRequest& request);

  std::expected<void, std::string> DeleteResvMeta_(const ResvId& resv_id);

  std::unique_ptr<SchedulerAlgo> m_node_selection_algo_;

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

  HashMap<job_id_t, std::uint32_t> m_job_pending_step_num_map_
      ABSL_GUARDED_BY(m_step_num_mutex_);
  Mutex m_step_num_mutex_ ABSL_ACQUIRED_BEFORE(m_task_indexes_mtx_);

  std::unique_ptr<IPrioritySorter> m_priority_sorter_;

  // If this variable is set to true, all threads must stop in a certain time.
  std::atomic_bool m_thread_stop_{};

  // RPC worker pool for RPC calls with mutex `m_pending_task_map_mtx_` or
  // `m_running_task_map_mtx_` or `m_task_indexes_mtx_` held. Put there RPCs in
  // global thread pool can be dangerous is some cases, e.g., deadlock may
  // happen. So we create a separate thread pool for those RPCs. Consider a
  // scenario:
  // There is task `A` (Like CraneStub::ConfigureCraned) in thread pool: Try to
  // acquire `mutex` and send something via RPC.
  //
  // And another thread (like StepStatusChangeThread): Acquired `mutex` and
  // create task `B` to send something to craned, and a latch is used to wait
  // for RPCs completion in the thread.
  //
  // If too many tasks like `A` are in the thread pool, all threads
  // may be occupied, then task `B` can never be scheduled, and therefore
  // the latch will cause a deadlock.
  std::unique_ptr<BS::thread_pool> m_rpc_worker_pool_;

  std::thread m_schedule_thread_;
  void ScheduleThread_();

  std::thread m_step_schedule_thread_;
  void StepScheduleThread_();

  std::thread m_task_release_thread_;
  void ReleaseTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_cancel_thread_;
  void CancelTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_task_submit_thread_;
  void SubmitTaskThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::thread m_step_submit_thread_;
  void StepSubmitThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

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

  struct CancelPendingStepQueueElem {
    std::unique_ptr<CommonStepInCtld> step;
  };

  struct CancelRunningTaskQueueElem {
    job_id_t job_id;
    step_id_t step_id;
    CranedId craned_id;
  };

  using CancelTaskQueueElem =
      std::variant<CancelPendingTaskQueueElem, CancelPendingStepQueueElem,
                   CancelRunningTaskQueueElem>;

  std::shared_ptr<uvw::async_handle> m_cancel_task_async_handle_;
  ConcurrentQueue<CancelTaskQueueElem> m_cancel_task_queue_;
  void CancelTaskAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_cancel_queue_handle_;
  void CleanCancelQueueCb_();

  std::shared_ptr<uvw::timer_handle> m_submit_task_timer_handle_;
  void SubmitTaskTimerCb_();

  std::shared_ptr<uvw::async_handle> m_submit_task_async_handle_;
  ConcurrentQueue<std::pair<std::unique_ptr<TaskInCtld>,
                            std::promise<CraneExpected<task_id_t>>>>
      m_submit_task_queue_;
  void SubmitTaskAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_submit_queue_handle_;
  void CleanSubmitQueueCb_();

  std::shared_ptr<uvw::timer_handle> m_submit_step_timer_handle_;
  void SubmitStepTimerCb_();

  std::shared_ptr<uvw::async_handle> m_submit_step_async_handle_;
  ConcurrentQueue<std::pair<std::unique_ptr<CommonStepInCtld>,
                            std::promise<CraneExpected<step_id_t>>>>
      m_submit_step_queue_;
  void SubmitStepAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_step_submit_queue_handle_;
  void CleanStepSubmitQueueCb_();

  std::shared_ptr<uvw::timer_handle> m_task_status_change_timer_handle_;
  void TaskStatusChangeTimerCb_();

  std::shared_ptr<uvw::async_handle> m_task_status_change_async_handle_;

  struct TaskStatusChangeArg {
    job_id_t job_id;
    step_id_t step_id;
    uint32_t exit_code;
    crane::grpc::TaskStatus new_status;
    CranedId craned_index;
    std::string reason;
    google::protobuf::Timestamp timestamp;
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

  struct DependencyEvent {
    task_id_t dependent_job_id;
    task_id_t dependee_job_id;
    absl::Time event_time;
  };

  ConcurrentQueue<DependencyEvent> m_dependency_event_queue_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::TaskScheduler> g_task_scheduler;
