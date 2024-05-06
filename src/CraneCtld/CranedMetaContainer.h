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
#include "AccountManager.h"
#include "crane/AtomicHashMap.h"
#include "crane/Lock.h"
#include "crane/Pointer.h"

namespace Ctld {

/**
 * All public methods in this class is thread-safe.
 */
class CranedMetaContainerInterface {
 protected:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K, typename V>
  using TreeMap = absl::btree_map<K, V>;

  CranedMetaContainerInterface() = default;

 public:
  //  using AllPartitionsMetaMap = std::unordered_map<PartitionId,
  //  PartitionMeta>;
  using AllPartitionsMetaAtomicMap =
      util::AtomicHashMap<HashMap, PartitionId, PartitionMeta>;
  using AllPartitionsMetaRawMap = AllPartitionsMetaAtomicMap::RawMap;

  /**
   * A map from CranedId to global craned information
   */
  //  using CranedMetaMap = std::unordered_map<CranedId, CranedMeta>;
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

  virtual ~CranedMetaContainerInterface() = default;

  virtual void CranedUp(const CranedId& node_id) = 0;

  virtual void CranedDown(const CranedId& node_id) = 0;

  virtual bool CheckCranedOnline(const CranedId& node_id) = 0;

  virtual void InitFromConfig(const Config& config) = 0;

  virtual bool CheckCranedExisted(const CranedId& hostname) = 0;

  virtual bool CheckPartitionExisted(const PartitionId& name) = 0;

  virtual crane::grpc::QueryCranedInfoReply QueryAllCranedInfo() = 0;

  virtual crane::grpc::QueryCranedInfoReply QueryCranedInfo(
      const std::string& craned_name) = 0;

  virtual crane::grpc::QueryPartitionInfoReply QueryAllPartitionInfo() = 0;

  virtual crane::grpc::QueryPartitionInfoReply QueryPartitionInfo(
      const std::string& partition_name) = 0;

  virtual crane::grpc::QueryClusterInfoReply QueryClusterInfo(
      const crane::grpc::QueryClusterInfoRequest& request) = 0;

  virtual crane::grpc::ModifyCranedStateReply ChangeNodeState(
      const crane::grpc::ModifyCranedStateRequest& request) = 0;

  virtual void MallocResourceFromNode(CranedId node_id, uint32_t task_id,
                                      const Resources& resources) = 0;
  virtual void FreeResourceFromNode(CranedId node_id, uint32_t task_id) = 0;

  /**
   * Provide a thread-safe way to access NodeMeta.
   * @return a ScopeExclusivePointerType class. During the initialization of
   * this type, the unique ownership of data pointed by data is acquired. If the
   * partition does not exist, a nullptr is returned and no lock is held. Use
   * bool() to check it.
   */
  virtual PartitionMetaPtr GetPartitionMetasPtr(PartitionId partition_id) = 0;

  virtual CranedMetaPtr GetCranedMetaPtr(CranedId node_id) = 0;

  virtual AllPartitionsMetaMapConstPtr GetAllPartitionsMetaMapConstPtr() = 0;

  virtual CranedMetaMapConstPtr GetCranedMetaMapConstPtr() = 0;

  virtual result::result<void, std::string> AddPartition(
      PartitionMeta&& partition) = 0;

  virtual result::result<void, std::string> AddNode(CranedMeta&& node) = 0;

  virtual result::result<void, std::string> DeletePartition(
      PartitionId name) = 0;

  virtual result::result<void, std::string> DeleteNode(CranedId name) = 0;

  virtual result::result<void, std::string> UpdatePartition(
      PartitionMeta&& partition) = 0;

  virtual result::result<void, std::string> UpdateNode(CranedMeta&& node) = 0;

  virtual void RegisterPhysicalResource(const CranedId& name, double cpu,
                                        uint64_t memory_bytes) = 0;
};

class CranedMetaContainerSimpleImpl final
    : public CranedMetaContainerInterface {
 public:
  CranedMetaContainerSimpleImpl() = default;
  ~CranedMetaContainerSimpleImpl() override {
    m_thread_stop_ = true;
    if (m_delete_craned_thread_.joinable()) m_delete_craned_thread_.join();
  };

  void InitFromConfig(const Config& config) override;

  crane::grpc::QueryCranedInfoReply QueryAllCranedInfo() override;

  crane::grpc::QueryCranedInfoReply QueryCranedInfo(
      const std::string& node_name) override;

  crane::grpc::QueryPartitionInfoReply QueryAllPartitionInfo() override;

  crane::grpc::QueryPartitionInfoReply QueryPartitionInfo(
      const std::string& partition_name) override;

  crane::grpc::QueryClusterInfoReply QueryClusterInfo(
      const crane::grpc::QueryClusterInfoRequest& request) override;

  crane::grpc::ModifyCranedStateReply ChangeNodeState(
      const crane::grpc::ModifyCranedStateRequest& request) override;

  void CranedUp(const CranedId& craned_id) override;

  void CranedDown(const CranedId& craned_id) override;

  bool CheckCranedOnline(const CranedId& craned_id) override;

  PartitionMetaPtr GetPartitionMetasPtr(PartitionId partition_id) override;

  CranedMetaPtr GetCranedMetaPtr(CranedId craned_id) override;

  AllPartitionsMetaMapConstPtr GetAllPartitionsMetaMapConstPtr() override;

  CranedMetaMapConstPtr GetCranedMetaMapConstPtr() override;

  bool CheckCranedExisted(const CranedId& hostname) override {
    return craned_meta_map_.Contains(hostname);
  };

  bool CheckPartitionExisted(const PartitionId& name) override {
    return partition_metas_map_.Contains(name);
  }

  void MallocResourceFromNode(CranedId crane_id, task_id_t task_id,
                              const Resources& resources) override;

  void FreeResourceFromNode(CranedId craned_id, uint32_t task_id) override;

  result::result<void, std::string> AddPartition(
      PartitionMeta&& partition) override;

  result::result<void, std::string> AddNode(CranedMeta&& node) override;

  result::result<void, std::string> DeletePartition(PartitionId name) override;

  result::result<void, std::string> DeleteNode(CranedId name) override;

  result::result<void, std::string> UpdatePartition(
      PartitionMeta&& new_partition) override;

  result::result<void, std::string> UpdateNode(CranedMeta&& node) override;

  void RegisterPhysicalResource(const CranedId& name, double cpu,
                                uint64_t memory_bytes) override;

 private:
  void WaitJobsTerminatedCb_();

  void CleanDeletionQueueCb_();

  void DeleteCranedThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  // In this part of code, the following lock sequence MUST be held
  // to avoid deadlock:
  // 1. lock elements in partition_metas_map_
  // 2. lock elements in craned_meta_map_
  // 3. unlock elements in craned_meta_map_
  // 4. unlock elements in partition_metas_map_
  CranedMetaAtomicMap craned_meta_map_;
  AllPartitionsMetaAtomicMap partition_metas_map_;

  ConcurrentQueue<CranedId> m_delete_craned_queue_;
  ConcurrentQueue<CranedId> m_wait_jobs_terminated_queue_;

  std::shared_ptr<uvw::async_handle> m_clean_deletion_queue_handle_;
  std::shared_ptr<uvw::timer_handle> m_wait_jobs_terminated_handle_;

  std::atomic_bool m_thread_stop_{};
  std::thread m_delete_craned_thread_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedMetaContainerInterface> g_meta_container;