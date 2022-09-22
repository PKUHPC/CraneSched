#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <event2/event.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>

#include "CtldPublicDefs.h"
#include "DbClient.h"
#include "CranedMetaContainer.h"
#include "crane/Lock.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.pb.h"

namespace Ctld {

// Task ID is used for querying this structure.
// The node index has also been recorded in CranedMetaContainer, so there's no
// need to query it here.
struct QueryBriefTaskMetaFieldControl {
  bool type;
  bool status;
  bool start_time;
  bool node_index;
};

class INodeSelectionAlgo {
 public:
  // Pair content: <The task which is going to be run,
  //                The node index on which it will be run>
  // Partition information is not needed because scheduling is carried out in
  // one partition to which the task belongs.
  using NodeSelectionResult =
      std::pair<std::unique_ptr<TaskInCtld>, std::list<uint32_t>>;

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
      const CranedMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::btree_map<uint32_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) = 0;
};

class MinLoadFirst : public INodeSelectionAlgo {
  /**
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
  using ValidTimeSegmentsVec = std::vector<TimeSegment>;

  struct NodeSelectionInfo {
    std::multimap<uint32_t /* # of running tasks */, uint32_t /* node index */>
        task_num_node_id_map;
    std::unordered_map<uint32_t /* Node Index*/, TimeAvailResMap>
        node_time_avail_res_map;
  };

  static void CalculateNodeSelectionInfo_(
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::Time now, uint32_t partition_id,
      const PartitionMetas& partition_metas, uint32_t node_id,
      const CranedMeta& node_meta, NodeSelectionInfo* node_selection_info);

  // Input should guarantee that provided nodes in `node_selection_info` has
  // enough nodes whose resource is >= task->resource.
  static bool CalculateRunningNodesAndStartTime_(
      const NodeSelectionInfo& node_selection_info,
      const PartitionMetas& partition_metas, const TaskInCtld* task,
      absl::Time now, std::list<uint32_t>* node_ids, absl::Time* start_time);

 public:
  void NodeSelect(
      const CranedMetaContainerInterface::AllPartitionsMetaMap&
          all_partitions_meta_map,
      const absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::btree_map<uint32_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) override;
};

class TaskScheduler {
  using Mutex = absl::Mutex;
  using LockGuard = util::AbslMutexLockGuard;

  template <typename K, typename V>
  using HashMap = absl::flat_hash_map<K, V>;

  template <typename K>
  using HashSet = absl::flat_hash_set<K>;

 public:
  explicit TaskScheduler(std::unique_ptr<INodeSelectionAlgo> algo);

  ~TaskScheduler();

  void SetNodeSelectionAlgo(std::unique_ptr<INodeSelectionAlgo> algo);

  CraneErr SubmitTask(std::unique_ptr<TaskInCtld> task, bool resubmit,
                      uint32_t* task_id);

  void TaskStatusChange(uint32_t task_id, uint32_t craned_index,
                        crane::grpc::TaskStatus new_status,
                        std::optional<std::string> reason);

  // Temporary inconsistency may happen. If 'false' is returned, just ignore it.
  void QueryTaskBriefMetaInPartition(
      uint32_t partition_id,
      const QueryBriefTaskMetaFieldControl& field_control,
      crane::grpc::QueryJobsInPartitionReply* response);

  bool QueryCranedIdOfRunningTask(uint32_t task_id, CranedId* craned_id) {
    LockGuard running_guard(m_running_task_map_mtx_);
    return QueryCranedIdOfRunningTaskNoLock_(task_id, craned_id);
  }

  std::string QueryCranedListFromTaskId(uint32_t task_id);

  CraneErr CancelPendingOrRunningTask(uint32_t task_id);

  CraneErr TerminateRunningTask(uint32_t task_id) {
    LockGuard running_guard(m_running_task_map_mtx_);
    LockGuard ended_guard(m_ended_task_map_mtx_);
    return TerminateRunningTaskNoLock_(task_id);
  }

 private:
  void ScheduleThread_();

  void CleanEndedTaskThread_();

  bool QueryCranedIdOfRunningTaskNoLock_(uint32_t task_id, CranedId* node_id);

  CraneErr TerminateRunningTaskNoLock_(uint32_t task_id);

  std::unique_ptr<INodeSelectionAlgo> m_node_selection_algo_;

  boost::uuids::random_generator_mt19937 m_uuid_gen_;
  uint32_t m_next_task_id_;

  // Ordered by task id. Those who comes earlier are in the head,
  // Because they have smaller task id.
  absl::btree_map<uint32_t /*Task Id*/, std::unique_ptr<TaskInCtld>>
      m_pending_task_map_ GUARDED_BY(m_pending_task_map_mtx_);
  Mutex m_pending_task_map_mtx_;

  absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>> m_running_task_map_
      GUARDED_BY(m_running_task_map_mtx_);
  Mutex m_running_task_map_mtx_;

  absl::flat_hash_map<uint32_t, std::unique_ptr<TaskInCtld>> m_ended_task_map_
      GUARDED_BY(m_ended_task_map_mtx_);
  Mutex m_ended_task_map_mtx_;

  // Task Indexes
  HashMap<uint32_t /* Node Index */, HashSet<uint32_t /* Task ID*/>>
      m_node_to_tasks_map_ GUARDED_BY(m_task_indexes_mtx_);
  HashMap<uint32_t /* Partition ID */, HashSet<uint32_t /* Task ID */>>
      m_partition_to_tasks_map_ GUARDED_BY(m_task_indexes_mtx_);
  Mutex m_task_indexes_mtx_;

  std::thread m_schedule_thread_;
  std::thread m_clean_ended_thread_;
  std::atomic_bool m_thread_stop_{};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::TaskScheduler> g_task_scheduler;