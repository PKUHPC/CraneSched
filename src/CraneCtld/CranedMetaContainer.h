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

#include "crane/AtomicHashMap.h"
#include "crane/Lock.h"
#include "crane/Pointer.h"

namespace Ctld {

class CranedMetaContainer final {
 public:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashSet = absl::flat_hash_set<K, Hash>;

  template <typename K, typename V>
  using TreeMap = absl::btree_map<K, V>;

  template <typename K>
  using TreeSet = absl::btree_set<K>;

  using AllPartitionsMetaAtomicMap =
      util::AtomicHashMap<HashMap, PartitionId, PartitionMeta>;
  using AllPartitionsMetaRawMap = AllPartitionsMetaAtomicMap::RawMap;

  /**
   * A map from CranedId to global craned information
   */
  using CranedMetaAtomicMap =
      util::AtomicHashMap<HashMap, CranedId, CranedMeta>;
  using CranedMetaRawMap = CranedMetaAtomicMap::RawMap;

  using AllPartitionsMetaMapConstPtr =
      util::ScopeConstSharedPtr<AllPartitionsMetaRawMap, util::rw_mutex>;
  using CranedMetaMapConstPtr =
      util::ScopeConstSharedPtr<CranedMetaRawMap, util::rw_mutex>;

  using PartitionMetaPtr =
      util::ManagedScopeExclusivePtr<PartitionMeta,
                                     AllPartitionsMetaAtomicMap::CombinedLock>;
  using CranedMetaPtr =
      util::ManagedScopeExclusivePtr<CranedMeta,
                                     CranedMetaAtomicMap::CombinedLock>;

  CranedMetaContainer() = default;
  ~CranedMetaContainer() = default;

  void InitFromConfig(const Config& config);

  void AddDedicatedResource(const CranedId& node_id,
                            const DedicatedResourceInNode& resource);

  crane::grpc::QueryCranedInfoReply QueryAllCranedInfo();

  crane::grpc::QueryCranedInfoReply QueryCranedInfo(const CranedId& node_name);

  crane::grpc::QueryPartitionInfoReply QueryAllPartitionInfo();

  crane::grpc::QueryPartitionInfoReply QueryPartitionInfo(
      const PartitionId& partition_name);

  crane::grpc::QueryClusterInfoReply QueryClusterInfo(
      const crane::grpc::QueryClusterInfoRequest& request);

  crane::grpc::ModifyCranedStateReply ChangeNodeState(
      const crane::grpc::ModifyCranedStateRequest& request);

  bool UpdateNodeDrainState(const std::string& craned_id, bool is_drain,
                            const std::string& reason);

  void UpdateNodeState(const CranedId& craned_id, bool health_check_result);

  CraneExpected<void> ModifyPartitionAcl(
      const std::string& partition_name, bool is_allowed_list,
      std::unordered_set<std::string>&& accounts);

  CraneExpected<void> CheckIfAccountIsAllowedInPartition(
      const std::string& partition_name, const std::string& account_name);

  void CranedUp(const CranedId& craned_id,
                const crane::grpc::CranedRemoteMeta& remote_meta);

  void CranedDown(const CranedId& craned_id);

  bool CheckCranedOnline(const CranedId& craned_id);

  int GetOnlineCranedCount();

  PartitionMetaPtr GetPartitionMetasPtr(const PartitionId& partition_id);

  CranedMetaPtr GetCranedMetaPtr(const CranedId& craned_id);

  AllPartitionsMetaMapConstPtr GetAllPartitionsMetaMapConstPtr();

  CranedMetaMapConstPtr GetCranedMetaMapConstPtr();

  bool CheckCranedAllowed(const std::string& hostname) {
    return craned_meta_map_.Contains(hostname);
  };

  void MallocResourceFromNode(CranedId node_id, task_id_t task_id,
                              const ResourceV2& resources);

  void FreeResourceFromNode(CranedId craned_id, uint32_t task_id);

  // TODO: Move to Reservation Mini-Scheduler. Craned only use LogicalPartition.
  using ResvMetaAtomicMap = util::AtomicHashMap<HashMap, std::string, ResvMeta>;
  using ResvMetaRawMap = ResvMetaAtomicMap::RawMap;

  using ResvMetaMapConstPtr =
      util::ScopeConstSharedPtr<ResvMetaRawMap, util::rw_mutex>;

  using ResvMetaMapPtr = ResvMetaAtomicMap::MapSharedPtr;

  using ResvMetaMapExclusivePtr = ResvMetaAtomicMap::MapExclusivePtr;

  using ResvMetaPtr =
      util::ManagedScopeExclusivePtr<ResvMeta, ResvMetaAtomicMap::CombinedLock>;

  crane::grpc::QueryReservationInfoReply QueryAllResvInfo();

  crane::grpc::QueryReservationInfoReply QueryResvInfo(const ResvId& resv_name);

  ResvMetaPtr GetResvMetaPtr(const ResvId& name);

  ResvMetaMapConstPtr GetResvMetaMapConstPtr();

  ResvMetaMapPtr GetResvMetaMapPtr();

  ResvMetaMapExclusivePtr GetResvMetaMapExclusivePtr();

  void MallocResourceFromResv(ResvId resv_id, task_id_t task_id,
                              const ResourceV2& res);

  void FreeResourceFromResv(ResvId resv_id, task_id_t task_id);

  // Store Resource reduction events happened during scheduling here.
  // Cases:
  // 1. Craned node down/drain: resources on nodes are reduced.
  // 2. Reservation created: resources on nodes are reduced.
  // 3. Reservation deleted: resources in reservation are reduced.
  // 4. Reservation modified (not implemented): resources in old reservation and
  // on nodes of new reservation are reduced.
  struct ResReduceEvent {
    using affected_resv_t = ResvId;
    using affected_nodes_t = std::pair<absl::Time, std::vector<CranedId>>;

    std::variant<affected_resv_t, affected_nodes_t> affected_resources;
  };

  void LockResReduceEvents() { m_res_reduce_events_mtx_.Lock(); }
  void UnlockResReduceEvents() { m_res_reduce_events_mtx_.Unlock(); }
  void AddResReduceEventsAndUnlock(ResReduceEvent&& event) {
    if (logging_enabled) {
      m_res_reduce_events_.emplace_back(std::move(event));
    }
    UnlockResReduceEvents();
  }
  const std::vector<ResReduceEvent>& LockAndGetResReduceEvents() {
    LockResReduceEvents();
    return m_res_reduce_events_;
  }
  void StopLoggingAndUnlock() {
    m_res_reduce_events_.clear();
    logging_enabled = false;
    UnlockResReduceEvents();
  }
  void StartLogging() {
    absl::MutexLock lk(&m_res_reduce_events_mtx_);
    logging_enabled = true;
  }

 private:
  // In this part of code, the following lock sequence MUST be held
  // to avoid deadlock:
  // 1. lock elements in partition_meta_map_
  // 2. lock elements in craned_meta_map_
  // 3. unlock elements in craned_meta_map_
  // 4. unlock elements in partition_meta_map_
  CranedMetaAtomicMap craned_meta_map_;
  AllPartitionsMetaAtomicMap partition_meta_map_;

  // TODO: Move to Reservation Logical Partition.
  ResvMetaAtomicMap resv_meta_map_;

  // A craned node may belong to multiple partitions.
  // Use this map as a READ-ONLY index, so multi-thread reading is ok.
  HashMap<CranedId /*craned hostname*/, std::list<PartitionId>>
      craned_id_part_ids_map_;

 private:  // Helper functions
  void SetGrpcCranedInfoByCranedMeta_(const CranedMeta& craned_meta,
                                      crane::grpc::CranedInfo* craned_info);

  std::vector<ResReduceEvent> m_res_reduce_events_
      ABSL_GUARDED_BY(m_res_reduce_events_mtx_);
  bool logging_enabled{false};
  absl::Mutex
      m_res_reduce_events_mtx_;  // lock before get resv_meta & craned_meta
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedMetaContainer> g_meta_container;