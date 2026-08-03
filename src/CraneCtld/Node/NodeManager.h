/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "protos/Crane.pb.h"

namespace Ctld {

class NodeManager final {
 public:
  using DynamicNodeRecord = crane::grpc::DynamicNodeRecord;
  using DynamicNodeSpec = crane::grpc::DynamicNodeSpec;

  struct ScaleUpResult {
    std::vector<CranedId> nodes_to_power_on;
    std::vector<CranedId> nodes_to_wake;
    std::vector<CranedId> reserved_nodes;
    bool in_progress{false};
  };

  struct RegistrationStartResult {
    bool connected;
    std::string connection_hostname;
  };

  bool Init();
  void RestoreDynamicNodes();
  void ReconcilePluginState();

  crane::grpc::CreateNodesReply CreateNodes(
      const crane::grpc::CreateNodesRequest& request);
  crane::grpc::DeleteNodesReply DeleteNodes(
      const crane::grpc::DeleteNodesRequest& request);

  crane::grpc::PrepareCranedRegistrationReply PrepareCranedRegistration(
      const crane::grpc::PrepareCranedRegistrationRequest& request);

  std::expected<RegistrationStartResult, std::string> BeginRegistration(
      const CranedId& node_id, uint64_t generation, const RegToken& token,
      std::string_view registration_token = {});
  std::expected<void, std::string> ValidateRegistration(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token = {});
  std::expected<void, std::string> MarkRegistered(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token = {});
  std::expected<void, std::string> MarkDisconnected(const CranedId& node_id,
                                                    uint64_t generation = 0);
  std::expected<void, std::string> UpdatePowerState(
      const CranedId& node_id, crane::grpc::CranedPowerState power_state);
  std::expected<ScaleUpResult, std::string> RequestScaleUp(
      const PartitionId& partition, const ResourceView& node_resource,
      const ResourceView& task_resource, uint32_t min_tasks_per_node,
      uint32_t max_tasks_per_node, uint32_t node_count, uint32_t task_count,
      const std::vector<uint32_t>& available_node_task_counts,
      const std::unordered_set<std::string>& included_nodes,
      const std::unordered_set<std::string>& excluded_nodes);

 private:
  std::expected<void, std::string> ValidateRegistrationNoLock_(
      const CranedId& node_id, uint64_t generation) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::expected<void, std::string> ValidatePresentRecord_(
      const DynamicNodeRecord& record) const;
  std::expected<void, std::string> ReleaseExpiredRegistrationLeasesNoLock_()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupExpiredTombstonesNoLock_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  std::expected<void, std::string> ValidateReportedSpecStructure_(
      const DynamicNodeSpec& reported) const;
  std::expected<void, std::string> ValidateReportedSpec_(
      const DynamicNodeSpec& expected, const DynamicNodeSpec& reported) const;
  std::expected<void, std::string> ValidateRegistrationToken_(
      const DynamicNodeRecord& record,
      std::string_view registration_token) const;
  std::expected<DynamicNodeRecord, std::string> BuildRegisteredRecord_(
      const DynamicNodeRecord& record,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token) const;
  std::optional<CranedId> FindNodeByPhysicalHostnameNoLock_(
      std::string_view physical_hostname,
      std::string_view excluded_node = {}) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static bool RegistrationLeaseExpired_(const DynamicNodeRecord& record);
  static bool FeaturesMatch_(const DynamicNodeSpec& expected,
                             const DynamicNodeSpec& reported);
  static bool GresAllocationMatch_(const DynamicNodeSpec& expected,
                                   const DynamicNodeSpec& allocated);
  static bool GresMatch_(const DynamicNodeSpec& expected,
                         const DynamicNodeSpec& reported);
  static std::string GenerateRegistrationToken_();
  static void FillPreparationReply_(
      const DynamicNodeRecord& record,
      crane::grpc::PrepareCranedRegistrationReply* reply);

  absl::Mutex mutex_;
  std::unordered_map<CranedId, DynamicNodeRecord> records_
      ABSL_GUARDED_BY(mutex_);
  uint64_t catalog_revision_ ABSL_GUARDED_BY(mutex_){0};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::NodeManager> g_node_manager;
