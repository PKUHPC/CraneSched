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

// ArrayMeta tracks scheduler-side materialization state of array children.
// ArrayManager owns the array root JobInCtld; ArrayMeta keeps a non-owning
// pointer to it.
//
// All mutating methods must be called while m_pending_job_map_mtx_ is held
// by the caller. Active (pending/running) partitioning is NOT stored on
// ArrayMeta; it is derived by the ArrayManager from the scheduler's pending
// and running maps.
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

  // Active (not-yet-terminal) child bookkeeping.
  // A child is "active" from pending admission until it terminates.
  // Pending-vs-running state lives in the scheduler maps, not here.
  size_t ActiveChildCount() const { return active_child_job_ids_.size(); }
  const absl::flat_hash_set<job_id_t>& ActiveChildJobIds() const {
    return active_child_job_ids_;
  }

  array_task_id_t NextTaskIndex() const { return next_array_task_index_; }
  void SetNextTaskIndex(array_task_id_t idx) { next_array_task_index_ = idx; }

 private:
  // Bookkeeping mutations are driven by the manager.
  friend class ArrayManager;

  void TrackMaterialized(JobInCtld* child);
  void UntrackMaterialized(JobInCtld* child);
  void MarkChildActive(job_id_t child_job_id);
  void MarkChildInactive(job_id_t child_job_id);

  JobInCtld* root_job_{nullptr};
  array_task_id_t next_array_task_index_{0};
  absl::flat_hash_map<array_task_id_t, job_id_t> child_job_id_by_task_id_;
  absl::flat_hash_set<job_id_t> active_child_job_ids_;
};

enum class ArrayTaskResolveMode {
  kQueryOnly,
  kCreateIfMissing,
};

struct ArrayTaskResolveResult {
  std::vector<job_id_t> child_job_ids;
  std::vector<crane::grpc::JobInfo> virtual_jobs;
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
// materialized yet. Returns false if the task id is out of range.
bool BuildVirtualArrayChildJobInfo(const JobInCtld& array_root,
                                   uint32_t task_id,
                                   crane::grpc::JobInfo* job_info);
}  // namespace ArrayUtil

// ArrayManager owns all array root jobs and parent metas, and drives array
// child materialization, lifecycle tracking, and finalization.
//
// Locking contract:
//   * All instance methods require m_pending_job_map_mtx_ held by the caller
//     unless explicitly noted. OnChildRunning additionally requires the
//     running-map lock (caller already holds it in the scheduler loop).
//   * ResolveArrayTaskIds is the only public method that acquires the two
//     scheduler mutexes on its own; it is invoked from RPC paths that do
//     not hold either lock.
class ArrayManager {
 public:
  using PendingJobMap = absl::btree_map<job_id_t, std::unique_ptr<JobInCtld>>;
  using RunningJobMap =
      absl::flat_hash_map<job_id_t, std::unique_ptr<JobInCtld>>;

  // Bundle of references the manager uses to inspect/update the scheduler
  // maps. The references must outlive the ArrayManager (they point at
  // JobScheduler's own fields).
  struct ActiveJobMaps {
    PendingJobMap& pending_jobs;
    RunningJobMap& running_jobs;
    std::atomic_uint32_t& pending_cached_size;
  };

  struct FinalizedArrayRoot {
    // Keeps the non-owning ArrayMeta root pointer valid while finalization
    // runs outside scheduler locks.
    std::unique_ptr<JobInCtld> root_job;
    std::shared_ptr<ArrayMeta> meta;
  };

  ArrayManager(ActiveJobMaps maps, absl::Mutex* pending_mtx,
               absl::Mutex* running_mtx);

  // Lookup -------------------------------------------------------------------
  ArrayMeta* FindMeta(job_id_t array_job_id);
  const ArrayMeta* FindMeta(job_id_t array_job_id) const;
  bool Empty() const { return m_metas_.empty(); }
  size_t Size() const { return m_metas_.size(); }

  // Iteration helpers (for query, modify, cancel, hold paths).
  template <typename Fn>
  void ForEachMeta(Fn&& fn) {
    for (auto& [root_id, meta] : m_metas_) fn(root_id, *meta);
  }
  template <typename Fn>
  void ForEachMetaConst(Fn&& fn) const {
    for (const auto& [root_id, meta] : m_metas_) fn(root_id, *meta);
  }

  // Produce the list of active (pending and/or running) child job ids for a
  // given root, partitioned against the scheduler maps. Useful for modify
  // operations that need to propagate changes to a subset of children.
  std::vector<job_id_t> PendingChildJobIds(const ArrayMeta& meta) const;
  std::vector<job_id_t> RunningChildJobIds(const ArrayMeta& meta) const;
  size_t PendingChildCount(const ArrayMeta& meta) const;
  size_t RunningChildCount(const ArrayMeta& meta) const;

  // Registration -------------------------------------------------------------
  // Register a freshly submitted array parent. The parent's JobId must be
  // assigned already. Returns a shared pointer to the meta.
  std::shared_ptr<ArrayMeta> RegisterParent(std::unique_ptr<JobInCtld> parent);

  // Recovery binding: mark a child that was recovered from the embedded DB
  // and already sits inside either the pending or running scheduler map.
  void BindRecoveredChild(ArrayMeta* meta, JobInCtld* child);

  // Recovery refresh: recompute summary state after all children bound.
  void RefreshRecoveredSummary(ArrayMeta* meta);

  // Scheduler loop -----------------------------------------------------------
  // True if any root has headroom to materialize another child in this tick;
  // used by the scheduler's quick "should we run the loop?" check.
  bool HasReadyChildToMaterialize() const;

  // Route a dependency event to either a pending plain job or to an array
  // root (via its ArrayMeta). Returns true if the event was consumed.
  bool ApplyDependencyEvent(job_id_t dependent_id, job_id_t dependee_id,
                            absl::Time event_time);

  // Per-tick materialization pass. Walks all metas, materializing next
  // children for roots that have headroom and satisfy gating conditions.
  void TryEnqueueReadyChildren(absl::Time now);

  // Lifecycle callbacks ------------------------------------------------------
  // Called right after a child is inserted into the pending map via
  // RegisterParent / admit paths. (In practice, the internal admit flow
  // already calls this — the public entry is exposed for submit paths that
  // build the child outside of the manager.)
  void OnChildPending(JobInCtld* child);

  // Called when a pending child is moved to running in the scheduler loop.
  // Requires both pending and running locks to be held by the caller
  // (derivation of running state relies on the running map being stable).
  void OnChildRunning(JobInCtld* child);

  // Called when a child reaches a terminal status. Removes it from active
  // bookkeeping. Returns the owning ArrayMeta (non-owning pointer) so the
  // caller can funnel it into a "roots pending finalize" set.
  ArrayMeta* OnChildTerminal(JobInCtld* child);

  // Finalization -------------------------------------------------------------
  // For each candidate root id (typically the set of array roots whose
  // children just finalized), if the root is expanded and has zero active
  // children, aggregate the final status against `final_jobs`, mutate the
  // root's runtime attrs, run the dependency triggers, and extract ArrayMeta
  // plus root ownership from the manager. Returns finalized root carriers that
  // keep root jobs alive while DB/plugin work runs outside scheduler locks.
  std::vector<FinalizedArrayRoot> ExtractFinalRootsById(
      const std::unordered_set<job_id_t>& candidate_root_ids,
      const std::unordered_set<JobInCtld*>& final_jobs);

  // Same as above but takes raw ArrayMeta pointers (cheap path for callers
  // that already resolved the meta pointer during the child-terminal loop).
  std::vector<FinalizedArrayRoot> ExtractFinalRoots(
      const std::unordered_set<ArrayMeta*>& candidate_metas,
      const std::unordered_set<JobInCtld*>& final_jobs);

  // Resolve ------------------------------------------------------------------
  // Public entry used by RPC paths. Acquires both scheduler locks.
  ArrayTaskResolveResult ResolveArrayTaskIds(
      job_id_t array_job_id,
      const google::protobuf::RepeatedField<uint32_t>& array_task_ids,
      ArrayTaskResolveMode mode);

  // No-lock variant for callers already holding both scheduler locks.
  ArrayTaskResolveResult ResolveArrayTaskIdsNoLock(
      job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids,
      ArrayTaskResolveMode mode);

  // Persistence / plugin hooks for final array roots.
  // These are thread-safe against the manager (they do not touch m_metas_).
  static void ProcessFinalRoots(const std::vector<FinalizedArrayRoot>& roots);
  static crane::grpc::JobInEmbeddedDb BuildFinalRootRecord(
      const JobInCtld& array_root);
  static void CallPluginHookForFinalRoots(
      const std::vector<FinalizedArrayRoot>& roots);
  static void PersistAndTransferRootsToMongodb(
      const std::vector<FinalizedArrayRoot>& roots);

 private:
  // --- helpers (all must run while m_pending_job_map_mtx_ is held) ---
  JobInCtld* FindJobInPendingOrRunningNoLock_(job_id_t job_id);
  bool IsChildInRunningNoLock_(job_id_t child_job_id) const;
  bool IsChildInPendingNoLock_(job_id_t child_job_id) const;
  size_t RunningChildCountNoLock_(const ArrayMeta& meta) const;
  size_t PendingChildCountNoLock_(const ArrayMeta& meta) const;

  std::unique_ptr<JobInCtld> BuildChildJob_(const JobInCtld& array_parent,
                                            uint32_t task_id) const;
  std::unique_ptr<JobInCtld> BuildNextChildNoLock_(ArrayMeta* meta) const;
  void SyncNextTaskIndexNoLock_(ArrayMeta* meta) const;

  bool CanMaterializeRootNoLock_(ArrayMeta* meta, absl::Time now) const;

  static void InheritChildAttributesFromParent_(JobInCtld& child);

  CraneExpected<void> AdmitChildPtrsNoLock_(
      ArrayMeta* meta, const std::vector<JobInCtld*>& children);
  CraneExpected<void> AdmitChildNoLock_(ArrayMeta* meta, JobInCtld* child);
  CraneExpected<void> AdmitChildrenNoLock_(
      ArrayMeta* meta, std::vector<std::unique_ptr<JobInCtld>>& children);

  void EnqueuePendingJobNoLock_(std::unique_ptr<JobInCtld> job);
  bool TryEnqueueNextChildNoLock_(ArrayMeta* meta);
  std::vector<job_id_t> MaterializeSpecificTasksNoLock_(
      job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids);

  void RefreshRootSummaryStateNoLock_(ArrayMeta* meta);
  void TryFinalizeRootNoLock_(ArrayMeta* meta,
                              const std::unordered_set<JobInCtld*>& final_jobs,
                              std::vector<FinalizedArrayRoot>* final_roots);

  static uint32_t EffectiveArrayRunLimit_(const JobInCtld& root);
  static bool ArrayChildrenExpanded_(const JobInCtld& root);
  static void SetArrayChildrenExpanded_(JobInCtld* root, bool expanded);
  static void TriggerTerminalDependencyEvents_(JobInCtld* job,
                                               absl::Time end_time);
  static void TriggerTerminalDependencyEvents_(ArrayMeta* root,
                                               absl::Time end_time);

  static std::pair<crane::grpc::JobStatus, uint32_t> BuildAggregateResult_(
      job_id_t array_job_id, const std::unordered_set<JobInCtld*>& final_jobs);

  PendingJobMap& m_pending_jobs_;
  RunningJobMap& m_running_jobs_;
  std::atomic_uint32_t& m_pending_cached_size_;
  absl::Mutex* m_pending_mtx_;
  absl::Mutex* m_running_mtx_;

  absl::flat_hash_map<job_id_t, std::unique_ptr<JobInCtld>> m_root_jobs_;
  absl::flat_hash_map<job_id_t, std::shared_ptr<ArrayMeta>> m_metas_;
};

}  // namespace Ctld
