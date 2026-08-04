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

#include <absl/synchronization/notification.h>

#include <thread>

#include "protos/Crane.pb.h"

namespace Ctld {

class NodeManager final {
 public:
  using DynamicNodeRecord = crane::grpc::DynamicNodeRecord;
  using DynamicNodeSpec = crane::grpc::DynamicNodeSpec;

  struct PowerTarget {
    CranedId node_id;
    uint64_t generation;
  };

  struct ScaleUpResult {
    std::vector<PowerTarget> nodes_to_power_on;
    std::vector<PowerTarget> nodes_to_wake;
    std::vector<CranedId> reserved_nodes;
    bool in_progress{false};
  };

  bool Init();
  ~NodeManager();
  void RestoreDynamicNodes();
  void StartReconcileThread();
  void ReconcilePluginState();

  crane::grpc::CreateNodesReply CreateNodes(
      const crane::grpc::CreateNodesRequest& request);
  crane::grpc::DeleteNodesReply DeleteNodes(
      const crane::grpc::DeleteNodesRequest& request);

  crane::grpc::PrepareCranedRegistrationReply PrepareCranedRegistration(
      const crane::grpc::PrepareCranedRegistrationRequest& request);

  CraneExpectedRich<void> BeginRegistration(
      const CranedId& node_id, uint64_t generation, const RegToken& token,
      std::string_view registration_token = {});
  CraneExpectedRich<void> ValidateRegistration(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token = {});
  CraneExpectedRich<void> MarkRegistered(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token = {});
  CraneExpectedRich<void> MarkRegistrationFailed(
      const CranedId& node_id, uint64_t generation,
      std::string_view registration_token);
  CraneExpectedRich<void> MarkDisconnectedIfUntracked(const CranedId& node_id);
  // A true value means the report belongs to a stale dynamic incarnation and
  // must not reach the runtime state/event path.
  CraneExpectedRich<bool> UpdatePowerState(
      const CranedId& node_id, uint64_t generation,
      crane::grpc::CranedPowerState power_state);
  // Cheap prefilter for the scheduler: whether the partition has any dynamic
  // node that RequestScaleUp could act on at all.
  bool HasScalableNodes(const PartitionId& partition);
  CraneExpectedRich<ScaleUpResult> RequestScaleUp(
      const PartitionId& partition, const ResourceView& node_resource,
      const ResourceView& task_resource, uint32_t min_tasks_per_node,
      uint32_t max_tasks_per_node, uint32_t node_count, uint32_t task_count,
      const std::vector<uint32_t>& available_node_task_counts,
      const std::unordered_set<std::string>& included_nodes,
      const std::unordered_set<std::string>& excluded_nodes);

 private:
  void ReconcileThreadFunc_();

  CraneExpectedRich<void> ValidateRegistrationNoLock_(const CranedId& node_id,
                                                      uint64_t generation) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  CraneExpectedRich<void> MarkDisconnectedNoLock_(const CranedId& node_id,
                                                  uint64_t generation)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  CraneExpectedRich<void> ValidatePresentRecord_(
      const DynamicNodeRecord& record) const;
  CraneExpectedRich<void> ReleaseExpiredRegistrationLeasesNoLock_()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupExpiredTombstonesNoLock_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  CraneExpectedRich<uint64_t> NextGenerationNoLock_(
      const CranedId& node_id) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  CraneExpectedRich<void> ValidateReportedSpecStructure_(
      const DynamicNodeSpec& reported) const;
  CraneExpectedRich<void> ValidateReportedSpec_(
      const DynamicNodeSpec& expected, const DynamicNodeSpec& reported) const;
  CraneExpectedRich<void> ValidateRegistrationToken_(
      const DynamicNodeRecord& record,
      std::string_view registration_token) const;
  CraneExpectedRich<DynamicNodeRecord> BuildRegisteredRecord_(
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
  // Exact per-name totals; typed counts must cover the expected ones.
  static bool GresCountsMatch_(const crane::grpc::GresMap& expected,
                               const crane::grpc::GresMap& allocated);
  // Reported counts must be at least the expected ones.
  static bool GresMatch_(const crane::grpc::GresMap& expected,
                         const crane::grpc::GresMap& reported);
  static std::string GenerateRegistrationToken_();
  static void FillPreparationReply_(
      const DynamicNodeRecord& record,
      crane::grpc::PrepareCranedRegistrationReply* reply);

  // Lock order: mutex_ may be held while taking CranedMetaContainer's
  // topology lock (Create/Delete/Prepare paths). Never call into NodeManager
  // while holding the topology lock; the scheduler must release its topology
  // read lock before calling RequestScaleUp.
  absl::Mutex mutex_;
  std::unordered_map<CranedId, DynamicNodeRecord> records_
      ABSL_GUARDED_BY(mutex_);
  std::unordered_map<CranedId, uint64_t> generation_high_watermarks_
      ABSL_GUARDED_BY(mutex_);
  // Present records whose partitions no longer exist in the static
  // configuration. They are excluded from the runtime topology and cannot
  // register or be leased; only deletion is allowed.
  std::unordered_set<CranedId> quarantined_nodes_ ABSL_GUARDED_BY(mutex_);
  uint64_t catalog_revision_ ABSL_GUARDED_BY(mutex_){0};

  absl::Notification reconcile_stop_notification_;
  std::thread reconcile_thread_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::NodeManager> g_node_manager;
