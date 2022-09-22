#pragma once

#include <absl/container/flat_hash_map.h>

#include <unordered_map>

#include "CtldPublicDefs.h"
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

  using AllPartitionsMetaMap =
      absl::flat_hash_map<uint32_t /*partition id*/, PartitionMetas>;

  using AllPartitionsMetaMapPtr =
      util::ScopeExclusivePtr<AllPartitionsMetaMap, Mutex>;
  using PartitionMetasPtr = util::ScopeExclusivePtr<PartitionMetas, Mutex>;
  using CranedMetaPtr = util::ScopeExclusivePtr<CranedMeta, Mutex>;

  virtual ~CranedMetaContainerInterface() = default;

  virtual void CranedUp(const CranedId& node_id) = 0;

  virtual void CranedDown(CranedId node_id) = 0;

  virtual void InitFromConfig(const Config& config) = 0;

  /**
   * Check whether partition exists.
   */
  virtual bool PartitionExists(const std::string& partition_name) = 0;

  virtual bool CheckCranedAllowed(const std::string& hostname) = 0;

  virtual crane::grpc::QueryCranedInfoReply* QueryAllCranedInfo() = 0;

  virtual crane::grpc::QueryCranedInfoReply* QueryCranedInfo(
      const std::string& craned_name) = 0;

  virtual crane::grpc::QueryPartitionInfoReply* QueryAllPartitionInfo() = 0;

  virtual crane::grpc::QueryPartitionInfoReply* QueryPartitionInfo(
      const std::string& partition_name) = 0;

  virtual crane::grpc::QueryClusterInfoReply* QueryClusterInfo() = 0;

  virtual bool GetCraneId(const std::string& hostname, CranedId* node_id) = 0;

  /**
   * @return The partition id. If partition name doesn't exist, a new partition
   * id will be allocated to this partition name.
   */
  virtual bool GetPartitionId(const std::string& partition_name,
                              uint32_t* partition_id) = 0;

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
  virtual PartitionMetasPtr GetPartitionMetasPtr(uint32_t partition_id) = 0;

  virtual CranedMetaPtr GetNodeMetaPtr(CranedId node_id) = 0;

  virtual AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() = 0;

 protected:
  CranedMetaContainerInterface() = default;
};

class CranedMetaContainerSimpleImpl final
    : public CranedMetaContainerInterface {
 public:
  CranedMetaContainerSimpleImpl() = default;
  ~CranedMetaContainerSimpleImpl() override = default;

  void InitFromConfig(const Config& config) override;

  crane::grpc::QueryCranedInfoReply* QueryAllCranedInfo() override;

  crane::grpc::QueryCranedInfoReply* QueryCranedInfo(
      const std::string& node_name) override;

  crane::grpc::QueryPartitionInfoReply* QueryAllPartitionInfo() override;

  crane::grpc::QueryPartitionInfoReply* QueryPartitionInfo(
      const std::string& partition_name) override;

  crane::grpc::QueryClusterInfoReply* QueryClusterInfo() override;

  bool GetCraneId(const std::string& hostname, CranedId* craned_id) override;

  void CranedUp(const CranedId& craned_id) override;

  void CranedDown(CranedId craned_id) override;

  PartitionMetasPtr GetPartitionMetasPtr(uint32_t partition_id) override;

  CranedMetaPtr GetNodeMetaPtr(CranedId node_id) override;

  AllPartitionsMetaMapPtr GetAllPartitionsMetaMapPtr() override;

  bool PartitionExists(const std::string& partition_name) override;

  bool CheckCranedAllowed(const std::string& hostname) override;

  bool GetPartitionId(const std::string& partition_name,
                      uint32_t* partition_id) override;

  void MallocResourceFromNode(CranedId node_id, uint32_t task_id,
                              const Resources& resources) override;

  void FreeResourceFromNode(CranedId craned_id, uint32_t task_id) override;

 private:
  AllPartitionsMetaMap partition_metas_map_;

  absl::flat_hash_map<std::string /*partition name*/, uint32_t /*partition id*/>
      partition_name_id_map_;

  absl::flat_hash_map<std::string /*node hostname*/, uint32_t /*partition id*/>
      node_hostname_part_id_map_;

  absl::flat_hash_map<std::pair<uint32_t, std::string /*hostname*/>,
                      uint32_t /*node index in a partition*/>
      part_id_host_index_map_;

  Mutex mtx_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedMetaContainerInterface> g_meta_container;