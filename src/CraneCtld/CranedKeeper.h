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
  CranedStub(const CranedStub &) = delete;

  CranedStub &operator=(const CranedStub &) = delete;

  ~CranedStub();

  void SetRegToken(const RegToken &token) {
    absl::MutexLock l(&m_lock_);
    m_token_ = token;
  }

  [[nodiscard]] bool CheckToken(const RegToken &token) {
    absl::MutexLock l(&m_lock_);
    bool ret = m_token_.has_value() && m_token_.value() == token;
    if (!ret) {
      CRANE_DEBUG("Token for {} mismatch, resetting token.", m_craned_id_);
      m_token_.reset();
    }
    return ret;
  }

  void ConfigureCraned(const CranedId &craned_id, const RegToken &token);

  void SetReady() {
    CRANE_TRACE("Craned {} stub ready.", m_craned_id_);
    m_registered_.store(true, std::memory_order_release);

    absl::MutexLock l(&m_lock_);
    m_token_.reset();
  }

  static crane::grpc::ExecuteTasksRequest NewExecuteTasksRequests(
      const CranedId &craned_id, const std::vector<TaskInCtld *> &tasks);

  std::vector<task_id_t> ExecuteTasks(
      const crane::grpc::ExecuteTasksRequest &request);

  CraneErrCode CreateCgroupForTasks(
      std::vector<CgroupSpec> const &cgroup_specs);

  CraneErrCode ReleaseCgroupForTasks(
      const std::vector<std::pair<task_id_t, uid_t>> &task_uid_pairs);

  CraneErrCode TerminateTasks(const std::vector<task_id_t> &task_ids);

  CraneErrCode TerminateOrphanedTask(task_id_t task_id);

  CraneErrCode ChangeTaskTimeLimit(uint32_t task_id, uint64_t seconds);

  bool Invalid() const {
    return m_disconnected_.load(std::memory_order_acquire) ||
           !m_registered_.load(std::memory_order_acquire);
  }

 private:
  void HandleGrpcErrorCode_(grpc::StatusCode code);

  CranedKeeper *m_craned_keeper_;

  grpc_connectivity_state m_prev_channel_state_;
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::Craned::Stub> m_stub_;

  // Set if underlying gRPC is down.
  std::atomic_bool m_disconnected_;
  std::atomic_bool m_registered_{false};

  static constexpr uint32_t s_maximum_retry_times_ = 2;
  uint32_t m_failure_retry_times_;

  CranedId m_craned_id_;

  absl::Mutex m_lock_;
  std::optional<RegToken> m_token_ ABSL_GUARDED_BY(m_lock_){std::nullopt};

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

  uint32_t AvailableCranedCount();

  bool IsCranedConnected(const CranedId &craned_id);

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

  void SetCranedConnectedCb(std::function<void(CranedId, const RegToken &)> cb);

  void SetCranedDisconnectedCb(std::function<void(CranedId)> cb);

  void PutNodeIntoUnavailSet(const std::string &crane_id,
                             const RegToken &token);

 private:
  struct CqTag {
    enum Type { kInitializingCraned, kEstablishedCraned };
    Type type;
    CranedStub *craned;
  };

  static void CranedChannelConnectFail_(CranedStub *stub);

  void ConnectCranedNode_(CranedId const &craned_id, const RegToken &token);

  CqTag *InitCranedStateMachine_(CranedStub *craned,
                                 grpc_connectivity_state new_state);
  CqTag *EstablishedCranedStateMachine_(CranedStub *craned,
                                        grpc_connectivity_state new_state);

  void StateMonitorThreadFunc_(int thread_id);

  void PeriodConnectCranedThreadFunc_();

  std::function<void(CranedId, const RegToken &)> m_craned_connected_cb_;

  // Guarantee that the Craned will not be freed before this callback is
  // called.
  std::function<void(CranedId)> m_craned_disconnected_cb_;

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
  std::unordered_map<CranedId, RegToken> m_unavail_craned_set_
      ABSL_GUARDED_BY(m_unavail_craned_set_mtx_);
  std::unordered_map<CranedId, RegToken> m_connecting_craned_set_
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