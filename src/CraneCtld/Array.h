/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/time/time.h>

#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "protos/Crane.pb.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

// Array lifecycle invariants:
//   1. The array parent is the only control entity. It stays in the scheduler
//      pending map and owns deadline/dependency/summary/final state.
//   2. A materialized array child is a normal real job with its own
//      job_id/job_db_id/status and lives in the running map.
//   3. Unmaterialized tasks exist only in ArraySpec. They are not queried,
//      cancelled, modified, or persisted as virtual jobs.
//   4. ArrayManager exposes lifecycle events. Scheduler code should not drive
//      task iteration or parent finalization in multiple steps.
//   5. ArrayMeta owns the state and behavior of one array parent.
class ArrayMeta {
 public:
  ~ArrayMeta() = default;

 private:
  friend class ArrayManager;

  explicit ArrayMeta(JobInCtld* parent_job);

  const JobInCtld& Parent() const { return *parent_job_; }
  JobInCtld& MutableParent() { return *parent_job_; }
  JobInCtld* ParentPtr() const { return parent_job_; }

  // Materialization bookkeeping (task_id -> child job id).
  std::optional<job_id_t> ChildJobIdOfTask(array_task_id_t task_id) const;

  std::optional<array_task_id_t> NextMaterializableTaskId() const;
  bool WillCompleteMaterializationAfter(array_task_id_t task_id) const;
  std::unique_ptr<JobInCtld> BuildChild(array_task_id_t task_id) const;

  size_t RunningChildCount() const { return running_child_job_ids_.size(); }
  const absl::flat_hash_set<job_id_t>& RunningChildJobIds() const {
    return running_child_job_ids_;
  }
  bool ParentStartEventTriggered() const {
    return parent_start_event_triggered_;
  }
  std::optional<std::pair<crane::grpc::JobStatus, uint32_t>>
  TerminalStatusOverride() const {
    return terminal_status_override_;
  }

  bool TrackMaterialized(JobInCtld* child);
  bool TrackMaterialized(array_task_id_t task_id, job_id_t child_job_id);
  void OnChildRunning(job_id_t child_job_id);
  void OnChildTerminal(job_id_t child_job_id);
  bool MarkParentStarted(absl::Time start_time);
  void FinishForTerminalStatus(crane::grpc::JobStatus status,
                               uint32_t exit_code, absl::Time end_time);
  bool CanSpawnMore(absl::Time now, std::string* reason) const;

  JobInCtld* parent_job_{nullptr};
  // Greatest task id that has ever been materialized (in task-id space, not
  // index space). Zero-initialized is fine because task ids start at
  // array_spec.start() >= 0; we use std::optional semantics via
  // has_any_materialized_.
  array_task_id_t max_materialized_task_id_{0};
  bool has_any_materialized_{false};
  absl::flat_hash_map<array_task_id_t, job_id_t> child_job_id_by_task_id_;
  absl::flat_hash_set<job_id_t> running_child_job_ids_;
  bool parent_start_event_triggered_{false};
  std::optional<std::pair<crane::grpc::JobStatus, uint32_t>>
      terminal_status_override_;
};

namespace ArrayUtil {
uint32_t Stride(const crane::grpc::ArraySpec& array_spec);
uint32_t TaskCount(const crane::grpc::ArraySpec& array_spec);
uint32_t EffectiveRunLimit(const crane::grpc::ArraySpec& array_spec);
bool ContainsTaskId(const crane::grpc::ArraySpec& array_spec,
                    array_task_id_t task_id);
uint32_t TaskIdByIndex(const crane::grpc::ArraySpec& array_spec,
                       uint32_t index);
}  // namespace ArrayUtil

// ArrayManager owns array parent metas and exposes event-style lifecycle
// operations to the scheduler. All instance methods require the scheduler's
// pending-job-map mutex held by the caller, unless noted otherwise.
class ArrayManager {
 public:
  struct FinalizedArrayParent {
    // Scheduler fills `parent_job` by moving the parent unique_ptr out of the
    // pending map before ProcessFinalParents runs.
    std::unique_ptr<JobInCtld> parent_job;
    job_id_t array_job_id{0};
  };

  struct ResolvedJobIdSelectors {
    // Only array-task selectors can fail to resolve; plain job_id selectors
    // pass through unconditionally. So an unresolved entry always carries the
    // array_task_id the user asked for.
    struct UnresolvedSelector {
      job_id_t job_id{0};
      array_task_id_t array_task_id{0};
      std::string reason;
    };

    std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps;
    // Reverse lookup: child_job_id -> (array_parent_id, array_task_id).
    // Lets downstream code surface a failure on a materialized child back to
    // the user as the array task they originally asked for.
    std::unordered_map<job_id_t, std::pair<job_id_t, array_task_id_t>>
        array_selector_by_child_job_id;
    std::vector<UnresolvedSelector> unresolved_selectors;
  };

  struct ParentMaterializationDecision {
    bool can_materialize{false};
    std::string pending_reason;
  };

  struct FinishedParentResult {
    bool parent_found{false};
    std::vector<job_id_t> running_child_job_ids;
    std::vector<FinalizedArrayParent> final_parents;
  };

  ArrayManager() = default;

  void RegisterParent(JobInCtld* parent);
  CraneExpected<void> BindRecoveredChildrenForParent(
      job_id_t array_job_id, const std::vector<JobInCtld*>& children);
  CraneExpected<void> TrackRecoveredTerminalChild(JobInCtld* child);
  CraneExpected<void> TrackRecoveredTerminalChild(job_id_t array_job_id,
                                                  array_task_id_t task_id,
                                                  job_id_t child_job_id);
  void RecoverAccountingForRegisteredParents();

  bool IsRegisteredParent(job_id_t array_job_id) const;

  // Sweeps all registered parents and finalizes those whose materialization
  // and child execution have both completed. Used at recovery end to surface
  // parents whose children all reached terminal state in the previous run
  // (recovery uses TrackRecoveredTerminalChild, which does not fire
  // OnChildTerminal events).
  std::vector<FinalizedArrayParent> FinalizeAllCompletedParents();

  // Returns whether the parent can enter normal node selection to materialize
  // one child in this tick.
  ParentMaterializationDecision PrepareParentForMaterialization(
      const JobInCtld& parent, absl::Time now);

  // Builds and persists the next real child for an already allocated parent.
  std::unique_ptr<JobInCtld> MaterializeChildForAllocation(JobInCtld* parent,
                                                           absl::Time now);

  // Called after a materialized child has successfully started.
  bool OnChildStarted(JobInCtld* child);

  // Called when a running child reaches a terminal status.
  std::vector<FinalizedArrayParent> OnChildTerminal(
      JobInCtld* child, const std::unordered_set<JobInCtld*>& final_jobs);

  // Stops further materialization and finalizes immediately if no child is
  // running; otherwise finalization happens when the last running child exits.
  FinishedParentResult FinishParentWithStatus(job_id_t parent_job_id,
                                              crane::grpc::JobStatus status,
                                              uint32_t exit_code,
                                              absl::Time end_time);
  std::vector<FinalizedArrayParent> TryFinalizeParentIfComplete(
      job_id_t parent_job_id);

  std::vector<job_id_t> RunningChildJobIdsForParent(
      job_id_t parent_job_id) const;

  // Resolves a single JobIdSelector. Requires the scheduler's
  // pending-job-map mutex held by the caller (reads m_metas_).
  ResolvedJobIdSelectors ResolveJobIdSelector(
      const crane::grpc::JobIdSelector& selector,
      bool expand_array_parents) const;

  // Batch wrapper: resolves every selector and merges into one result.
  // Same locking contract as ResolveJobIdSelector.
  ResolvedJobIdSelectors ResolveJobIdSelectors(
      const google::protobuf::RepeatedPtrField<crane::grpc::JobIdSelector>&
          selectors,
      bool expand_array_parents) const;

  // Persistence / plugin hooks for final array parents.
  static void ProcessFinalParents(
      const std::vector<FinalizedArrayParent>& parents);

 private:
  ArrayMeta* FindMeta_(job_id_t array_job_id);
  const ArrayMeta* FindMeta_(job_id_t array_job_id) const;

  static bool ArrayMaterializationComplete_(const JobInCtld& parent);
  static bool WasFinalizedBeforeRecovery_(const JobInCtld& parent);
  static void InheritChildAttributesFromParent_(JobInCtld& child);
  static const absl::flat_hash_set<job_id_t>& RunningChildJobIds_(
      const ArrayMeta& meta);

  // Merges `src` into `dst`, preserving the "empty step set = all steps"
  // sentinel semantics of job_steps.
  static void MergeResolved_(ResolvedJobIdSelectors& dst,
                             ResolvedJobIdSelectors&& src);

  CraneExpected<void> AdmitChildNoLock_(ArrayMeta& meta, JobInCtld& child);

  void TryFinalizeParentNoLock_(
      ArrayMeta* meta, std::vector<FinalizedArrayParent>* final_parents,
      const std::unordered_set<JobInCtld*>& final_jobs = {});

  static std::pair<crane::grpc::JobStatus, uint32_t> BuildAggregateResult_(
      const ArrayMeta& meta, const std::unordered_set<JobInCtld*>& final_jobs);

  static crane::grpc::JobInEmbeddedDb BuildFinalParentRecord_(
      const JobInCtld& array_parent);
  static void CallPluginHookForFinalParents_(
      const std::vector<FinalizedArrayParent>& parents);
  static void PersistAndTransferParentsToMongodb_(
      const std::vector<FinalizedArrayParent>& parents);

  absl::flat_hash_map<job_id_t, std::unique_ptr<ArrayMeta>> m_metas_;
};

}  // namespace Ctld
