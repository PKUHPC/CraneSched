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

  using ResvMetaPtr =
      util::ManagedScopeExclusivePtr<ResvMeta, ResvMetaAtomicMap::CombinedLock>;

  crane::grpc::QueryReservationInfoReply QueryAllResvInfo();

  crane::grpc::QueryReservationInfoReply QueryResvInfo(const ResvId& resv_name);

  ResvMetaPtr GetResvMetaPtr(const ResvId& name);

  ResvMetaMapConstPtr GetResvMetaMapConstPtr();

  ResvMetaMapPtr GetResvMetaMapPtr();

  void MallocResourceFromResv(ResvId resv_id, task_id_t task_id,
                              const LogicalPartition::RnTaskRes& res);

  void FreeResourceFromResv(ResvId reservation_id, task_id_t task_id);

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
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedMetaContainer> g_meta_container;