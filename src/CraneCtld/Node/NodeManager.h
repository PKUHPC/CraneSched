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

  struct ScaleUpRequest {
    PartitionId partition;
    ResourceView node_resource;
    ResourceView task_resource;
    uint32_t min_tasks_per_node;
    uint32_t max_tasks_per_node;
    uint32_t node_count;
    uint32_t task_count;
    std::vector<uint32_t> available_node_task_counts;
    std::unordered_set<std::string> included_nodes;
    std::unordered_set<std::string> excluded_nodes;
  };

  struct ScaleUpResult {
    std::vector<PowerTarget> nodes_to_power_on;
    std::vector<PowerTarget> nodes_to_wake;
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

  // observed_peer_address is the connection source seen by the controller;
  // it is recorded as the node's connect-back address because the claimed
  // physical hostname may not resolve to it (NAT, multi-homing).
  crane::grpc::PrepareCranedRegistrationReply PrepareCranedRegistration(
      const crane::grpc::PrepareCranedRegistrationRequest& request,
      const std::string& observed_peer_address);

  CraneExpectedRich<void> BeginRegistration(
      const CranedId& node_id, uint64_t generation, const RegToken& token,
      std::string_view registration_token = {});
  CraneExpectedRich<void> ValidateRegistration(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token = {});
  // On success returns the session token of this registration; uplink RPCs
  // from the craned must echo it. Empty for static nodes.
  CraneExpectedRich<std::string> MarkRegistered(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta,
      std::string_view registration_token = {});
  CraneExpectedRich<void> MarkRegistrationFailed(
      const CranedId& node_id, uint64_t generation,
      std::string_view registration_token);
  // Post-commit rollback of MarkRegistered, fenced by the session token it
  // returned: a no-op if a newer registration has taken over the node.
  CraneExpectedRich<void> RevokeRegistration(const CranedId& node_id,
                                             uint64_t generation,
                                             std::string_view session_token);
  // Whether an uplink RPC belongs to the node's current registration.
  // Static nodes carry no session and always pass.
  bool ValidateUplinkSession(const CranedId& node_id,
                             std::string_view session_token) const;
  CraneExpectedRich<void> MarkDisconnectedIfUntracked(const CranedId& node_id);
  // Single write entry for reported power states: persists the dynamic
  // record and applies the runtime state under one lock. A true value means
  // the report was ignored (stale incarnation or out-of-order sequence) and
  // must not reach the event path.
  CraneExpectedRich<bool> UpdatePowerState(
      const CranedId& node_id, uint64_t generation,
      crane::grpc::CranedPowerState power_state, uint64_t report_sequence);
  // Cheap prefilter for the scheduler: whether the partition has any dynamic
  // node that RequestScaleUp could act on at all.
  bool HasScalableNodes(const PartitionId& partition);
  // Processes one scheduling round's scale-up demands under a single lock
  // and one persistence batch, in the given (priority) order: a node
  // claimed for an earlier request is not offered to later ones. Returns
  // one result per request.
  CraneExpectedRich<std::vector<ScaleUpResult>> RequestScaleUp(
      const std::vector<ScaleUpRequest>& requests);

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
  void ReconcileDisconnectedNodesNoLock_()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupExpiredTombstonesNoLock_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  CraneExpectedRich<uint64_t> NextGenerationNoLock_(
      const CranedId& node_id) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  size_t CountPresentNodesNoLock_() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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
  // Last accepted (generation, sequence) per node; reports whose sequence
  // does not advance within the same generation arrived out of order.
  std::unordered_map<CranedId, std::pair<uint64_t, uint64_t>>
      power_report_progress_ ABSL_GUARDED_BY(mutex_);
  // Present records whose partitions no longer exist in the static
  // configuration. They are excluded from the runtime topology and cannot
  // register or be leased; only deletion is allowed.
  std::unordered_set<CranedId> quarantined_nodes_ ABSL_GUARDED_BY(mutex_);
  uint64_t catalog_revision_ ABSL_GUARDED_BY(mutex_){0};

  // Sessions of the current registrations, in-memory only: after a restart
  // every uplink RPC is rejected until the craned re-registers. Guarded by
  // a dedicated mutex so the per-ping/per-step-status validation does not
  // contend on mutex_.
  mutable absl::Mutex session_mtx_;
  std::unordered_map<CranedId, std::string> sessions_
      ABSL_GUARDED_BY(session_mtx_);

  absl::Notification reconcile_stop_notification_;
  std::thread reconcile_thread_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::NodeManager> g_node_manager;
