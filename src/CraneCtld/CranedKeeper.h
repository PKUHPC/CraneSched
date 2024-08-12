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

#include "crane/Lock.h"
#include "crane/Network.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

class CranedKeeper;

/**
 * A class that encapsulate the detail of the underlying gRPC stub.
 */
class CranedStub {
 public:
  explicit CranedStub(CranedKeeper *craned_keeper);

  ~CranedStub();

  static crane::grpc::ExecuteTasksRequest NewExecuteTasksRequests(
      const CranedId &craned_id, const std::vector<TaskInCtld *> &tasks);

  std::vector<task_id_t> ExecuteTasks(
      const crane::grpc::ExecuteTasksRequest &request);

  CraneErr CreateCgroupForTasks(std::vector<CgroupSpec> const &cgroup_specs);

  CraneErr ReleaseCgroupForTasks(
      const std::vector<std::pair<task_id_t, uid_t>> &task_uid_pairs);

  CraneErr TerminateTasks(const std::vector<task_id_t> &task_ids);

  CraneErr TerminateOrphanedTask(task_id_t task_id);

  CraneErr CheckTaskStatus(task_id_t task_id, crane::grpc::TaskStatus *status);

  CraneErr ChangeTaskTimeLimit(uint32_t task_id, uint64_t seconds);

  CraneErr QueryActualDres(DedicatedResourceInNode *resource);

  bool Invalid() const { return m_invalid_; }

 private:
  CranedKeeper *m_craned_keeper_;

  grpc_connectivity_state m_prev_channel_state_;
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::Craned::Stub> m_stub_;

  // Set if underlying gRPC is down.
  bool m_invalid_;

  static constexpr uint32_t s_maximum_retry_times_ = 2;
  uint32_t m_failure_retry_times_;

  CranedId m_craned_id_;

  // void* parameter is m_data_. Used to free m_data_ when CranedStub is being
  // destructed.
  std::function<void(CranedStub *)> m_clean_up_cb_;

  friend class CranedKeeper;
};

class CranedKeeper {
 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using NodeHashMap = absl::node_hash_map<K, V, Hash>;

  using Mutex = absl::Mutex;
  using ReaderLock = absl::ReaderMutexLock;
  using WriterLock = absl::WriterMutexLock;

 public:
  explicit CranedKeeper(uint32_t node_num);

  ~CranedKeeper();

  void Shutdown();

  void InitAndRegisterCraneds(const std::list<CranedId> &craned_id_list);

  uint32_t AvailableCranedCount();

  /**
   * Get the pointer to CranedStub.
   * @param craned_id the index of CranedStub
   * @return nullptr if index points to an invalid slot, the pointer to
   * CranedStub otherwise.
   * @attention It's ok to return the pointer of CranedStub directly. The
   * CranedStub will not be freed before the CranedIsDown() callback returns.
   * The callback registerer should do necessary synchronization to clean up all
   * the usage of the CranedStub pointer before CranedIsDown() returns.
   */
  std::shared_ptr<CranedStub> GetCranedStub(const CranedId &craned_id);

  void SetCranedIsUpCb(std::function<void(CranedId)> cb);

  void SetCranedIsDownCb(std::function<void(CranedId)> cb);

  void PutNodeIntoUnavailList(const std::string &crane_id);

 private:
  struct CqTag {
    enum Type { kInitializingCraned, kEstablishedCraned };
    Type type;
    CranedStub *craned;
  };

  static void CranedChannelConnectFail_(CranedStub *stub);

  void ConnectCranedNode_(CranedId const &craned_id);

  CqTag *InitCranedStateMachine_(CranedStub *craned,
                                 grpc_connectivity_state new_state);
  CqTag *EstablishedCranedStateMachine_(CranedStub *craned,
                                        grpc_connectivity_state new_state);

  void StateMonitorThreadFunc_(int thread_id);

  void PeriodConnectCranedThreadFunc_();

  std::function<void(CranedId)> m_craned_is_up_cb_;

  // Guarantee that the Craned will not be freed before this callback is
  // called.
  std::function<void(CranedId)> m_craned_is_down_cb_;

  Mutex m_tag_pool_mtx_;

  // Must be declared previous to any grpc::CompletionQueue, so it can be
  // constructed before any CompletionQueue and be destructed after any
  // CompletionQueue.
  std::unique_ptr<std::pmr::synchronized_pool_resource> m_pmr_pool_res_;
  std::unique_ptr<std::pmr::polymorphic_allocator<CqTag>> m_tag_sync_allocator_;

  Mutex m_connected_craned_mtx_;
  NodeHashMap<CranedId, std::shared_ptr<CranedStub>>
      m_connected_craned_id_stub_map_ ABSL_GUARDED_BY(m_connected_craned_mtx_);

  Mutex m_unavail_craned_set_mtx_;
  std::unordered_set<CranedId> m_unavail_craned_set_
      ABSL_GUARDED_BY(m_unavail_craned_set_mtx_);
  std::unordered_set<CranedId> m_connecting_craned_set_
      ABSL_GUARDED_BY(m_unavail_craned_set_mtx_);

  std::vector<grpc::CompletionQueue> m_cq_vec_;
  std::vector<Mutex> m_cq_mtx_vec_;
  std::atomic_bool m_cq_closed_;

  std::vector<std::thread> m_cq_thread_vec_;

  std::thread m_period_connect_thread_;

  std::atomic_uint64_t m_channel_count_{0};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedKeeper> g_craned_keeper;