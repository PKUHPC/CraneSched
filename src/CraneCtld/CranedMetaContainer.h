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

  crane::grpc::QueryCranedInfoReply QueryCranedInfo(
      const std::string& node_name);

  crane::grpc::QueryPartitionInfoReply QueryAllPartitionInfo();

  crane::grpc::QueryPartitionInfoReply QueryPartitionInfo(
      const std::string& partition_name);

  crane::grpc::QueryClusterInfoReply QueryClusterInfo(
      const crane::grpc::QueryClusterInfoRequest& request);

  crane::grpc::ModifyCranedStateReply ChangeNodeState(
      const crane::grpc::ModifyCranedStateRequest& request);

  void CranedUp(const CranedId& craned_id);

  void CranedDown(const CranedId& craned_id);

  bool CheckCranedOnline(const CranedId& craned_id);

  PartitionMetaPtr GetPartitionMetasPtr(PartitionId partition_id);

  CranedMetaPtr GetCranedMetaPtr(CranedId craned_id);

  AllPartitionsMetaMapConstPtr GetAllPartitionsMetaMapConstPtr();

  CranedMetaMapConstPtr GetCranedMetaMapConstPtr();

  bool CheckCranedAllowed(const std::string& hostname) {
    return craned_meta_map_.Contains(hostname);
  };

  void MallocResourceFromNode(CranedId node_id, task_id_t task_id,
                              const ResourceV2& resources);

  void FreeResourceFromNode(CranedId craned_id, uint32_t task_id);

 private:
  // In this part of code, the following lock sequence MUST be held
  // to avoid deadlock:
  // 1. lock elements in partition_metas_map_
  // 2. lock elements in craned_meta_map_
  // 3. unlock elements in craned_meta_map_
  // 4. unlock elements in partition_metas_map_
  CranedMetaAtomicMap craned_meta_map_;
  AllPartitionsMetaAtomicMap partition_metas_map_;

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