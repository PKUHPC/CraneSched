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
#include "crane/Pointer.h"

namespace Ctld {

/**
 * All public methods in this class is thread-safe.
 */
class CranedMetaContainerInterface {
 public:
  using Mutex = util::recursive_mutex;
  using LockGuard = util::recursive_lock_guard;

  using AllPartitionsMetaMap = absl::flat_hash_map<PartitionId, PartitionMeta>;

  /**
   * A map from CranedId to global craned information
   */
  using CranedMetaMap = std::unordered_map<CranedId, CranedMeta>;

  using AllPartitionsMetaMapPtr =
      util::ScopeExclusivePtr<AllPartitionsMetaMap, Mutex>;
  using CranedMetaMapPtr = util::ScopeExclusivePtr<CranedMetaMap, Mutex>;

  using PartitionMetasPtr = util::ScopeExclusivePtr<PartitionMeta, Mutex>;
  using CranedMetaPtr = util::ScopeExclusivePtr<CranedMeta, Mutex>;

  virtual ~CranedMetaContainerInterface() = default;

  virtual void CranedUp(const CranedId& node_id) = 0;

  virtual void CranedDown(CranedId node_id) = 0;

  virtual void InitFromConfig(const Config& config) = 0;

  virtual bool CheckCranedAllowed(const std::string& hostname) = 0;

  virtual crane::grpc::QueryCranedInfoReply QueryAllCranedInfo() = 0;

  virtual crane::grpc::QueryCranedInfoReply QueryCranedInfo(
      const std::string& craned_name) = 0;

  virtual crane::grpc::QueryPartitionInfoReply QueryAllPartitionInfo() = 0;

  virtual crane::grpc::QueryPartitionInfoReply QueryPartitionInfo(
      const std::string& partition_name) = 0;

  virtual crane::grpc::QueryClusterInfoReply QueryClusterInfo(
      const crane::grpc::QueryClusterInfoRequest& request) = 0;

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
  virtual PartitionMetasPtr GetPartitionMetasPtr(PartitionId partition_id) = 0;

  virtual CranedMetaPtr GetCranedMetaPtr(CranedId node_id) = 0;

  virtual AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() = 0;

  virtual CranedMetaMapPtr GetCranedMetaMapPtr() = 0;

 protected:
  CranedMetaContainerInterface() = default;
};

class CranedMetaContainerSimpleImpl final
    : public CranedMetaContainerInterface {
 public:
  CranedMetaContainerSimpleImpl() = default;
  ~CranedMetaContainerSimpleImpl() override = default;

  void InitFromConfig(const Config& config) override;

  crane::grpc::QueryCranedInfoReply QueryAllCranedInfo() override;

  crane::grpc::QueryCranedInfoReply QueryCranedInfo(
      const std::string& node_name) override;

  crane::grpc::QueryPartitionInfoReply QueryAllPartitionInfo() override;

  crane::grpc::QueryPartitionInfoReply QueryPartitionInfo(
      const std::string& partition_name) override;

  crane::grpc::QueryClusterInfoReply QueryClusterInfo(
      const crane::grpc::QueryClusterInfoRequest& request) override;

  void CranedUp(const CranedId& craned_id) override;

  void CranedDown(CranedId craned_id) override;

  PartitionMetasPtr GetPartitionMetasPtr(PartitionId partition_id) override;

  CranedMetaPtr GetCranedMetaPtr(CranedId craned_id) override;

  AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() override;

  CranedMetaMapPtr GetCranedMetaMapPtr() override;

  bool CheckCranedAllowed(const std::string& hostname) override;

  void MallocResourceFromNode(CranedId node_id, task_id_t task_id,
                              const Resources& resources) override;

  void FreeResourceFromNode(CranedId craned_id, uint32_t task_id) override;

 private:
  AllPartitionsMetaMap partition_metas_map_;
  CranedMetaMap craned_meta_map_;

  // A craned node may belong to multiple partitions.
  absl::flat_hash_map<CranedId /*craned hostname*/, std::list<PartitionId>>
      craned_id_part_ids_map_;

  Mutex mtx_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedMetaContainerInterface> g_meta_container;