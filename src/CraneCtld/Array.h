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

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <google/protobuf/repeated_field.h>

#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

#include "protos/Crane.pb.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

class ArrayManager;  // forward declared for friendship

// ArrayMeta tracks scheduler-side materialization + lifecycle state of array
// children. The scheduler owns the array root JobInCtld via
// m_pending_job_map_; ArrayMeta keeps a non-owning pointer to it. The running
// set is maintained by the manager via scheduler-driven callbacks; ArrayMeta
// never inspects the scheduler's own pending/running maps.
class ArrayMeta {
 public:
  explicit ArrayMeta(JobInCtld* root_job);

  const JobInCtld& Root() const { return *root_job_; }
  JobInCtld& MutableRoot() { return *root_job_; }
  JobInCtld* RootPtr() const { return root_job_; }

  // Materialization bookkeeping (task_id -> child job id).
  bool IsTaskMaterialized(array_task_id_t task_id) const;
  size_t MaterializedTaskCount() const;
  std::optional<job_id_t> ChildJobIdOfTask(array_task_id_t task_id) const;

  size_t RunningChildCount() const { return running_child_job_ids_.size(); }
  size_t ActiveChildCount() const { return running_child_job_ids_.size(); }

  bool IsTaskCancelledBeforeMaterialized(array_task_id_t task_id) const {
    return cancelled_before_materialized_task_ids_.contains(task_id);
  }

 private:
  friend class ArrayManager;

  void TrackMaterialized(JobInCtld* child);
  void MarkChildRunning(job_id_t child_job_id);
  void MarkChildInactive(job_id_t child_job_id);
  void MarkTaskCancelledBeforeMaterialized(array_task_id_t task_id);

  JobInCtld* root_job_{nullptr};
  // Greatest task id that has ever been materialized (in task-id space, not
  // index space). Zero-initialized is fine because task ids start at
  // array_spec.start() >= 0; we use std::optional semantics via
  // has_any_materialized_.
  array_task_id_t max_materialized_task_id_{0};
  bool has_any_materialized_{false};
  absl::flat_hash_map<array_task_id_t, job_id_t> child_job_id_by_task_id_;
  absl::flat_hash_set<job_id_t> running_child_job_ids_;
  absl::flat_hash_set<array_task_id_t> cancelled_before_materialized_task_ids_;
};

struct ArrayTaskResolveResult {
  std::vector<job_id_t> child_job_ids;
  std::vector<crane::grpc::JobInfo> virtual_jobs;
};

enum class ArrayMutationKind {
  kCancel,
  kModify,
};

struct ArrayTaskResolveMutationResult {
  // Existing materialized children that the caller can apply the mutation to.
  std::vector<job_id_t> child_job_ids;
  // Task ids that were just recorded as cancelled-before-materialized. The
  // caller persists a finished DB record for each so cqueue/cacct show them.
  // Only populated for mutation_kind == kCancel.
  std::vector<array_task_id_t> virtual_task_ids_cancelled;
  // Task ids for which the mutation is not allowed on an unmaterialized task
  // (e.g. modify). Caller surfaces an explicit error per task id.
  std::vector<array_task_id_t> virtual_task_ids_rejected;
};

namespace ArrayUtil {
uint32_t Stride(const crane::grpc::ArraySpec& array_spec);
uint32_t TaskCount(const crane::grpc::ArraySpec& array_spec);
bool ContainsTaskId(const crane::grpc::ArraySpec& array_spec,
                    array_task_id_t task_id);
uint32_t TaskIdByIndex(const crane::grpc::ArraySpec& array_spec,
                       uint32_t index);
crane::grpc::ArrayTaskMeta BuildTaskMeta(
    const crane::grpc::ArraySpec& array_spec);

// Build a virtual pending JobInfo entry for a task that hasn't been
// materialized yet. This is a query-only synthesized view and is not inserted
// into the scheduler pending map. Returns false if the task id is out of range.
bool BuildVirtualArrayChildJobInfo(const JobInCtld& array_root,
                                   uint32_t task_id,
                                   crane::grpc::JobInfo* job_info);
}  // namespace ArrayUtil

// ArrayManager owns array parent metas and drives array child
// materialization, lifecycle tracking, and finalization.
//
// Ownership / encapsulation contract:
//   * The scheduler's m_pending_job_map_ owns every array parent JobInCtld
//     via unique_ptr. ArrayMeta::root_job_ is a non-owning raw pointer into
//     that map. ArrayManager never owns parent JobInCtlds.
//   * ArrayManager NEVER reads or mutates the scheduler's pending/running
//     maps. All per-meta active-child state is updated through
//     scheduler-driven callbacks: ExpandChildWithAllocation and
//     OnChildTerminal.
//   * Children are never in the scheduler's pending map. ExpandChildWith-
//     Allocation builds the next child, persists it to DB, and hands it
//     back already-materialized; the scheduler applies an allocation and
//     inserts it directly into m_running_job_map_.
//   * All instance methods require the scheduler's pending-job-map mutex
//     held by the caller, unless noted otherwise.
class ArrayManager {
 public:
  struct FinalizedArrayRoot {
    // Scheduler fills `root_job` by moving the parent unique_ptr out of
    // m_pending_job_map_ after ExtractFinalRoots{,ById} returns.
    std::unique_ptr<JobInCtld> root_job;
    std::shared_ptr<ArrayMeta> meta;
    job_id_t array_job_id{0};
  };

  ArrayManager() = default;

  // Lookup -------------------------------------------------------------------
  ArrayMeta* FindMeta(job_id_t array_job_id);
  const ArrayMeta* FindMeta(job_id_t array_job_id) const;

  template <typename Fn>
  void ForEachMeta(Fn&& fn) {
    for (auto& [root_id, meta] : m_metas_) fn(root_id, *meta);
  }
  template <typename Fn>
  void ForEachMetaConst(Fn&& fn) const {
    for (const auto& [root_id, meta] : m_metas_) fn(root_id, *meta);
  }

  // Copy of the per-root running child id set. Used by modify/cancel paths to
  // propagate a root-level mutation to child jobs.
  std::vector<job_id_t> RunningChildJobIds(const ArrayMeta& meta) const;

  // True iff every task is materialized-or-cancelled, the parent has
  // `array_children_expanded` set, and no active children remain. Callers
  // that just finished a bulk mutation can use this to trigger an
  // immediate finalize instead of waiting for a child terminal event.
  bool IsRootReadyToFinalizeNoLock(const ArrayMeta& meta) const;

  // Enumerate every task id belonging to an array parent, in ascending
  // order. Caller must hold m_pending_job_map_mtx_. Returns empty when the
  // meta is missing or the parent is not an array.
  std::vector<array_task_id_t> AllTaskIdsNoLock(job_id_t array_job_id) const;

  // Registration -------------------------------------------------------------
  // Register a freshly submitted array parent. Caller retains ownership of
  // the unique_ptr in m_pending_job_map_; we only store a raw pointer.
  std::shared_ptr<ArrayMeta> RegisterParent(JobInCtld* parent);

  // Scheduler loop -----------------------------------------------------------
  // True iff array-local state allows materializing another child this tick:
  // the parent is not fully expanded, max-concurrent headroom remains, and a
  // next task id exists. Generic pending-job gates (held/begin_time/
  // dependencies) belong to ScheduleThread_.
  bool CanSpawnAnotherChild(const JobInCtld& parent) const;

  // Returns nullptr when array-local state allows spawning another child.
  // Otherwise returns the pending_reason owned by array scheduling. An empty
  // string means the parent is blocked without a useful user-facing reason
  // (for example, no next task id remains).
  const char* GetSpawnBlockedReason(const JobInCtld& parent) const;

  // Convenience: list raw pointers of array parents with array-local capacity.
  // The caller already owns each parent via its pending map, so we do not
  // extend lifetime.
  std::vector<JobInCtld*> GetSchedulableParents() const;

  // Route a dependency event to a pending plain job OR an array root. Returns
  // true if the event was consumed by the array side.
  bool ApplyDependencyEvent(job_id_t dependent_id, job_id_t dependee_id,
                            absl::Time event_time);

  // Phase 2 hook. Builds the next child for a parent that NodeSelect just
  // produced an allocation for, persists it to DB (consuming a new job_id),
  // marks it running on the meta, and refreshes the root summary. Returns
  // nullptr if no materializable task remains (e.g., all remaining tasks
  // were cancelled between tick start and here) or if DB persistence fails.
  std::unique_ptr<JobInCtld> ExpandChildWithAllocation(job_id_t parent_job_id);

  // Lifecycle callbacks ------------------------------------------------------
  // Called when a running child reaches a terminal status.
  ArrayMeta* OnChildTerminal(JobInCtld* child);

  // Finalization -------------------------------------------------------------
  // The returned bundles have `root_job == nullptr`; the scheduler must move
  // the parent unique_ptr out of m_pending_job_map_ via
  // TakeFinalizedParentFromPendingMap() or an equivalent helper before
  // calling ProcessFinalRoots.
  std::vector<FinalizedArrayRoot> ExtractFinalRootsById(
      const std::unordered_set<job_id_t>& candidate_root_ids,
      const std::unordered_set<JobInCtld*>& final_jobs);

  std::vector<FinalizedArrayRoot> ExtractFinalRoots(
      const std::unordered_set<ArrayMeta*>& candidate_metas,
      const std::unordered_set<JobInCtld*>& final_jobs);

  // Resolve ------------------------------------------------------------------
  // Query path: pure read. Returns ids for materialized tasks and synthesized
  // JobInfo entries for unmaterialized valid tasks. Never mutates state.
  ArrayTaskResolveResult ResolveForQueryNoLock(
      job_id_t array_job_id,
      const std::unordered_set<uint32_t>& task_ids) const;

  // Mutation path: returns ids for existing materialized tasks; for
  // unmaterialized tasks records a cancellation (Cancel) or rejects
  // (Modify). Does not build new children.
  ArrayTaskResolveMutationResult ResolveForMutationNoLock(
      job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids,
      ArrayMutationKind kind);

  // Build a JobInCtld for a task the user cancelled before it was ever
  // materialized. Status is Cancelled and start/end times are pinned to
  // `now`. The returned job carries no job_id yet — the caller is
  // responsible for allocating an id via the embedded DB and persisting
  // to MongoDB so cqueue/cacct see the cancelled task.
  std::unique_ptr<JobInCtld> BuildCancelledVirtualChildNoLock(
      job_id_t array_job_id, array_task_id_t task_id, absl::Time now) const;

  // Persistence / plugin hooks for final array roots.
  static void ProcessFinalRoots(const std::vector<FinalizedArrayRoot>& roots);
  static crane::grpc::JobInEmbeddedDb BuildFinalRootRecord(
      const JobInCtld& array_root);
  static void CallPluginHookForFinalRoots(
      const std::vector<FinalizedArrayRoot>& roots);
  static void PersistAndTransferRootsToMongodb(
      const std::vector<FinalizedArrayRoot>& roots);

 private:
  std::unique_ptr<JobInCtld> BuildChildJob_(const JobInCtld& array_parent,
                                            uint32_t task_id) const;
  std::unique_ptr<JobInCtld> BuildNextChildNoLock_(ArrayMeta* meta) const;

  // Next task id to materialize, honoring stride and the cancelled-before-
  // materialized set. Returns nullopt if there is nothing left to materialize.
  std::optional<array_task_id_t> NextTaskIdNoLock_(const ArrayMeta& meta) const;

  static void InheritChildAttributesFromParent_(JobInCtld& child);

  CraneExpected<void> AdmitChildNoLock_(ArrayMeta& meta, JobInCtld& child);

  void RefreshRootSummaryStateNoLock_(ArrayMeta* meta);
  void TryFinalizeRootNoLock_(ArrayMeta* meta,
                              const std::unordered_set<JobInCtld*>& final_jobs,
                              std::vector<FinalizedArrayRoot>* final_roots);

  static uint32_t EffectiveArrayRunLimit_(const JobInCtld& root);
  static bool ArrayChildrenExpanded_(const JobInCtld& root);

  // Flip array_children_expanded in-memory iff materialized + cancelled-
  // before-materialized covers every task in the spec. Returns true on a
  // false→true transition. The flag is persisted only when the next
  // finalize / admit call writes the parent's runtime_attr to the DB —
  // callers that need durable persistence must follow up with a
  // finalize pass.
  bool FlipExpandedIfFullyAccountedNoLock_(ArrayMeta* meta) const;

  static std::pair<crane::grpc::JobStatus, uint32_t> BuildAggregateResult_(
      const ArrayMeta& meta, const std::unordered_set<JobInCtld*>& final_jobs);

  absl::flat_hash_map<job_id_t, std::shared_ptr<ArrayMeta>> m_metas_;
};

}  // namespace Ctld
