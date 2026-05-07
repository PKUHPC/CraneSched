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

#include "Array.h"

#include <google/protobuf/util/time_util.h>

#include "Accounting/AccountMetaContainer.h"
#include "Database/DbClient.h"
#include "Database/EmbeddedDbClient.h"
#include "crane/PluginClient.h"

namespace Ctld {

namespace ArrayUtil {

uint32_t Stride(const crane::grpc::ArraySpec& array_spec) {
  uint32_t stride = array_spec.has_stride() ? array_spec.stride() : 1;
  return stride == 0 ? 1 : stride;
}

uint32_t TaskCount(const crane::grpc::ArraySpec& array_spec) {
  if (array_spec.end() < array_spec.start()) {
    return 0;
  }
  return (array_spec.end() - array_spec.start()) / Stride(array_spec) + 1;
}

bool ContainsTaskId(const crane::grpc::ArraySpec& array_spec,
                    array_task_id_t task_id) {
  if (task_id < array_spec.start() || task_id > array_spec.end()) {
    return false;
  }
  return (task_id - array_spec.start()) % Stride(array_spec) == 0;
}

uint32_t TaskIdByIndex(const crane::grpc::ArraySpec& array_spec,
                       uint32_t index) {
  return array_spec.start() + index * Stride(array_spec);
}

crane::grpc::ArrayTaskMeta BuildTaskMeta(
    const crane::grpc::ArraySpec& array_spec) {
  crane::grpc::ArrayTaskMeta task_meta;
  task_meta.set_task_count(TaskCount(array_spec));
  task_meta.set_task_min(array_spec.start());
  task_meta.set_task_max(array_spec.end());
  task_meta.set_task_step(Stride(array_spec));
  return task_meta;
}

bool BuildVirtualArrayChildJobInfo(const JobInCtld& array_root,
                                   uint32_t task_id,
                                   crane::grpc::JobInfo* job_info) {
  const auto* array_spec = array_root.GetArraySpec();
  if (array_spec == nullptr || !ContainsTaskId(*array_spec, task_id)) {
    return false;
  }

  array_root.SetFieldsOfJobInfo(job_info);
  job_info->set_job_id(0);
  auto* array_task = job_info->mutable_array_task();
  array_task->set_array_job_id(array_root.JobId());
  array_task->set_task_id(task_id);
  job_info->set_status(crane::grpc::Pending);
  job_info->mutable_start_time()->Clear();
  job_info->mutable_end_time()->Clear();
  job_info->mutable_elapsed_time()->Clear();
  job_info->set_craned_list("");
  job_info->mutable_execution_node()->Clear();
  job_info->set_exit_code(0);
  *job_info->mutable_allocated_res_view() = crane::grpc::ResourceView{};
  job_info->clear_step_info_list();
  return true;
}

}  // namespace ArrayUtil

// ----------------------------------------------------------------------------
// ArrayMeta
// ----------------------------------------------------------------------------

ArrayMeta::ArrayMeta(JobInCtld* root_job) : root_job_(root_job) {}

bool ArrayMeta::IsTaskMaterialized(array_task_id_t task_id) const {
  return child_job_id_by_task_id_.contains(task_id);
}

size_t ArrayMeta::MaterializedTaskCount() const {
  return child_job_id_by_task_id_.size();
}

std::optional<job_id_t> ArrayMeta::ChildJobIdOfTask(
    array_task_id_t task_id) const {
  auto it = child_job_id_by_task_id_.find(task_id);
  if (it == child_job_id_by_task_id_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void ArrayMeta::TrackMaterialized(JobInCtld* child) {
  CRANE_ASSERT(child != nullptr && child->IsArrayChild());
  auto task_id = child->ArrayTaskId();
  CRANE_ASSERT(task_id.has_value());
  child_job_id_by_task_id_[*task_id] = child->JobId();
  if (!has_any_materialized_ || *task_id > max_materialized_task_id_) {
    max_materialized_task_id_ = *task_id;
    has_any_materialized_ = true;
  }
}

void ArrayMeta::MarkChildRunning(job_id_t child_job_id) {
  running_child_job_ids_.insert(child_job_id);
}

void ArrayMeta::MarkChildInactive(job_id_t child_job_id) {
  running_child_job_ids_.erase(child_job_id);
}

void ArrayMeta::MarkTaskCancelledBeforeMaterialized(array_task_id_t task_id) {
  cancelled_before_materialized_task_ids_.insert(task_id);
}

// ----------------------------------------------------------------------------
// ArrayManager: anonymous-namespace helpers
// ----------------------------------------------------------------------------

namespace {

void MergeArrayTaskTerminalStatus(crane::grpc::JobStatus status,
                                  uint32_t exit_code, bool* any_cancelled,
                                  bool* any_exceed_time_limit,
                                  bool* any_out_of_memory, bool* any_failed,
                                  uint32_t* max_exit_code) {
  *max_exit_code = std::max(*max_exit_code, exit_code);
  if (status == crane::grpc::JobStatus::Completed && exit_code == 0) {
    return;
  }

  switch (status) {
  case crane::grpc::JobStatus::Cancelled:
    *any_cancelled = true;
    break;
  case crane::grpc::JobStatus::ExceedTimeLimit:
    *any_exceed_time_limit = true;
    break;
  case crane::grpc::JobStatus::OutOfMemory:
    *any_out_of_memory = true;
    break;
  default:
    *any_failed = true;
    break;
  }
}

crane::grpc::JobStatus BuildArrayAggregateStatus(bool any_cancelled,
                                                 bool any_exceed_time_limit,
                                                 bool any_out_of_memory,
                                                 bool any_failed) {
  if (any_out_of_memory) {
    return crane::grpc::JobStatus::OutOfMemory;
  }
  if (any_exceed_time_limit) {
    return crane::grpc::JobStatus::ExceedTimeLimit;
  }
  if (any_cancelled) {
    return crane::grpc::JobStatus::Cancelled;
  }
  if (any_failed) {
    return crane::grpc::JobStatus::Failed;
  }
  return crane::grpc::JobStatus::Completed;
}

}  // namespace

// ----------------------------------------------------------------------------
// ArrayManager: static helpers
// ----------------------------------------------------------------------------

uint32_t ArrayManager::EffectiveArrayRunLimit_(const JobInCtld& root) {
  const auto* array_spec = root.GetArraySpec();
  if (array_spec == nullptr) {
    return 0;
  }
  uint32_t task_count = ArrayUtil::TaskCount(*array_spec);
  if (!array_spec->has_max_concurrent() || array_spec->max_concurrent() == 0) {
    return task_count;
  }
  return std::min(array_spec->max_concurrent(), task_count);
}

bool ArrayManager::ArrayChildrenExpanded_(const JobInCtld& root) {
  return root.RuntimeAttr().has_array_children_expanded() &&
         root.RuntimeAttr().array_children_expanded();
}

std::pair<crane::grpc::JobStatus, uint32_t> ArrayManager::BuildAggregateResult_(
    const ArrayMeta& meta, const std::unordered_set<JobInCtld*>& final_jobs) {
  job_id_t array_job_id = meta.Root().JobId();
  // Cancelled-before-materialized tasks are tracked in-memory and their
  // MongoDB inserts may be pending on a background worker when finalize
  // runs. Treat any presence in that set as a cancellation signal so the
  // aggregate status is correct regardless of insert ordering.
  bool any_cancelled = !meta.cancelled_before_materialized_task_ids_.empty();
  bool any_exceed_time_limit = false;
  bool any_out_of_memory = false;
  bool any_failed = false;
  uint32_t max_exit_code = 0;

  for (JobInCtld* job : final_jobs) {
    if (job == nullptr || !job->ArrayJobId().has_value() ||
        job->ArrayJobId().value() != array_job_id) {
      continue;
    }
    MergeArrayTaskTerminalStatus(job->Status(), job->ExitCode(), &any_cancelled,
                                 &any_exceed_time_limit, &any_out_of_memory,
                                 &any_failed, &max_exit_code);
  }

  auto aggregate_info = g_db_client->FetchArrayTaskAggregateInfo(array_job_id);
  if (!aggregate_info.has_value()) {
    return {BuildArrayAggregateStatus(any_cancelled, any_exceed_time_limit,
                                      any_out_of_memory, any_failed),
            max_exit_code};
  }

  max_exit_code = std::max(max_exit_code, aggregate_info->max_exit_code);
  any_cancelled = any_cancelled || aggregate_info->any_cancelled;
  any_exceed_time_limit =
      any_exceed_time_limit || aggregate_info->any_exceed_time_limit;
  any_out_of_memory = any_out_of_memory || aggregate_info->any_out_of_memory;
  any_failed = any_failed || aggregate_info->any_failed;

  return {BuildArrayAggregateStatus(any_cancelled, any_exceed_time_limit,
                                    any_out_of_memory, any_failed),
          max_exit_code};
}

// Slurm-parity: the array parent already reserved a submit-slot per task at
// submission time. Child materialization owes no independent QoS submit
// check — it only inherits the parent's qos/time_limit into its own proto.
void ArrayManager::InheritChildAttributesFromParent_(JobInCtld& child) {
  child.MutableJobToCtld()->set_qos(child.qos);
  child.MutableJobToCtld()->mutable_time_limit()->set_seconds(
      ToInt64Seconds(child.time_limit));
}

// ----------------------------------------------------------------------------
// ArrayManager: lookup + per-meta projections
// ----------------------------------------------------------------------------

ArrayMeta* ArrayManager::FindMeta(job_id_t array_job_id) {
  auto it = m_metas_.find(array_job_id);
  return it == m_metas_.end() ? nullptr : it->second.get();
}

const ArrayMeta* ArrayManager::FindMeta(job_id_t array_job_id) const {
  auto it = m_metas_.find(array_job_id);
  return it == m_metas_.end() ? nullptr : it->second.get();
}

std::vector<job_id_t> ArrayManager::RunningChildJobIds(
    const ArrayMeta& meta) const {
  return {meta.running_child_job_ids_.begin(),
          meta.running_child_job_ids_.end()};
}

bool ArrayManager::IsRootReadyToFinalizeNoLock(const ArrayMeta& meta) const {
  if (meta.root_job_ == nullptr) return false;
  return ArrayChildrenExpanded_(meta.Root()) && meta.ActiveChildCount() == 0;
}

std::vector<array_task_id_t> ArrayManager::AllTaskIdsNoLock(
    job_id_t array_job_id) const {
  std::vector<array_task_id_t> out;
  const auto* meta = FindMeta(array_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr) return out;
  const auto& spec = meta->Root().JobToCtld().array_spec();
  uint32_t stride = ArrayUtil::Stride(spec);
  if (spec.end() < spec.start()) return out;
  out.reserve(ArrayUtil::TaskCount(spec));
  for (array_task_id_t t = spec.start(); t <= spec.end(); t += stride) {
    out.push_back(t);
  }
  return out;
}

bool ArrayManager::FlipExpandedIfFullyAccountedNoLock_(ArrayMeta* meta) const {
  if (meta == nullptr || meta->root_job_ == nullptr) return false;
  auto& root = meta->MutableRoot();
  if (ArrayChildrenExpanded_(root)) return false;
  uint32_t task_count = ArrayUtil::TaskCount(root.JobToCtld().array_spec());
  size_t accounted = meta->MaterializedTaskCount() +
                     meta->cancelled_before_materialized_task_ids_.size();
  if (accounted < task_count) return false;
  root.SetArrayChildrenExpanded(true);
  return true;
}

// ----------------------------------------------------------------------------
// ArrayManager: registration + recovery
// ----------------------------------------------------------------------------

std::shared_ptr<ArrayMeta> ArrayManager::RegisterParent(JobInCtld* parent) {
  CRANE_ASSERT(parent != nullptr && parent->IsArrayParent());

  if (parent->qos.empty()) {
    parent->qos = parent->JobToCtld().qos();
  }

  job_id_t root_id = parent->JobId();
  auto meta = std::make_shared<ArrayMeta>(parent);
  m_metas_[root_id] = meta;
  return meta;
}

// ----------------------------------------------------------------------------
// ArrayManager: materialization
// ----------------------------------------------------------------------------

std::unique_ptr<JobInCtld> ArrayManager::BuildChildJob_(
    const JobInCtld& array_parent, uint32_t task_id) const {
  const auto* array_spec = array_parent.GetArraySpec();
  if (array_spec == nullptr ||
      !ArrayUtil::ContainsTaskId(*array_spec, task_id)) {
    return nullptr;
  }

  auto child = std::make_unique<JobInCtld>();
  child->SetFieldsByJobToCtld(array_parent.JobToCtld());
  child->SetStatus(crane::grpc::Pending);
  child->SetArrayTaskIdentity(array_parent.JobId(), task_id);
  child->SetUsername(array_parent.Username());
  child->qos = array_parent.qos;
  child->account_chain = array_parent.account_chain;
  child->req_total_res_view = array_parent.req_total_res_view;

  {
    std::list<std::string> included_list;
    util::ParseHostList(array_parent.JobToCtld().nodelist(), &included_list);
    for (auto&& node : included_list)
      child->included_nodes.emplace(std::move(node));
  }
  {
    std::list<std::string> excluded_list;
    util::ParseHostList(array_parent.JobToCtld().excludes(), &excluded_list);
    for (auto&& node : excluded_list)
      child->excluded_nodes.emplace(std::move(node));
  }

  child->ntasks_per_node_min = array_parent.ntasks_per_node_min;
  child->ntasks_per_node_max = array_parent.ntasks_per_node_max;
  child->partition_priority = array_parent.partition_priority;
  child->qos_priority = array_parent.qos_priority;
  child->mandated_priority = array_parent.mandated_priority;
  child->SetCachedPriority(array_parent.CachedPriority());
  child->using_default_wckey = array_parent.using_default_wckey;
  child->wckey = array_parent.wckey;
  child->SetSubmitTime(array_parent.SubmitTime());
  return child;
}

std::optional<array_task_id_t> ArrayManager::NextTaskIdNoLock_(
    const ArrayMeta& meta) const {
  const auto& root = meta.Root();
  const auto& array_spec = root.JobToCtld().array_spec();
  uint32_t stride = ArrayUtil::Stride(array_spec);
  array_task_id_t end = array_spec.end();

  array_task_id_t candidate =
      meta.has_any_materialized_
          ? static_cast<array_task_id_t>(meta.max_materialized_task_id_ +
                                         stride)
          : static_cast<array_task_id_t>(array_spec.start());
  // Skip any task ids the user cancelled before materialization. The skipped
  // tasks remain counted in the "fully expanded" task count so the root can
  // eventually finalize.
  while (candidate <= end &&
         meta.IsTaskCancelledBeforeMaterialized(candidate)) {
    candidate += stride;
  }
  if (candidate > end) return std::nullopt;
  return candidate;
}

std::unique_ptr<JobInCtld> ArrayManager::BuildNextChildNoLock_(
    ArrayMeta* meta) const {
  if (meta == nullptr || meta->root_job_ == nullptr ||
      ArrayChildrenExpanded_(meta->Root())) {
    return nullptr;
  }
  auto next = NextTaskIdNoLock_(*meta);
  if (!next.has_value()) return nullptr;
  return BuildChildJob_(meta->Root(), *next);
}

std::unique_ptr<JobInCtld> ArrayManager::BuildCancelledVirtualChildNoLock(
    job_id_t array_job_id, array_task_id_t task_id, absl::Time now) const {
  const auto* meta = FindMeta(array_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr) return nullptr;
  auto child = BuildChildJob_(meta->Root(), task_id);
  if (child == nullptr) return nullptr;
  InheritChildAttributesFromParent_(*child);
  child->SetStatus(crane::grpc::Cancelled);
  child->SetStartTime(now);
  child->SetEndTime(now);
  child->SetExitCode(0);
  return child;
}

CraneExpected<void> ArrayManager::AdmitChildNoLock_(ArrayMeta& meta,
                                                    JobInCtld& child) {
  CRANE_ASSERT(meta.root_job_ != nullptr);
  CRANE_ASSERT(child.IsArrayChild() && child.ArrayJobId().has_value() &&
               child.ArrayJobId().value() == meta.Root().JobId());
  auto& root = meta.MutableRoot();
  InheritChildAttributesFromParent_(child);

  uint32_t task_count = ArrayUtil::TaskCount(root.JobToCtld().array_spec());
  size_t accounted = meta.MaterializedTaskCount() + 1 +
                     meta.cancelled_before_materialized_task_ids_.size();
  bool mark_expanded = accounted == task_count;

  std::vector<EmbeddedDbClient::ExtraVariableWrite> extra_writes;
  if (mark_expanded) {
    root.SetArrayChildrenExpanded(true);
    extra_writes.push_back({root.JobDbId(), &root.RuntimeAttr()});
  }

  std::vector<JobInCtld*> one{&child};
  // This embedded-DB API is used here only to allocate/persist
  // job_id/job_db_id for the materialized child. The child is never inserted
  // into the scheduler pending map; ScheduleThread_ immediately applies the
  // allocation and moves it into the running map.
  if (!g_embedded_db_client->AppendJobsToPendingAndAdvanceJobIds(
          one, extra_writes)) {
    CRANE_ERROR("Failed to persist array child for parent job #{}.",
                root.JobId());
    if (mark_expanded) {
      root.SetArrayChildrenExpanded(false);
    }
    return std::unexpected(CraneErrCode::ERR_DB_INSERT_FAILED);
  }

  meta.TrackMaterialized(&child);
  return {};
}

// ----------------------------------------------------------------------------
// ArrayManager: summary + finalize
// ----------------------------------------------------------------------------

void ArrayManager::RefreshRootSummaryStateNoLock_(ArrayMeta* meta) {
  if (meta == nullptr || meta->root_job_ == nullptr) return;
  auto& root = meta->MutableRoot();

  // Aggregate terminal statuses derived from children are authoritative.
  if (IsFinishedStepStatus(root.Status())) return;

  // Once a root has ever entered Running, keep it Running until finalize. An
  // intermediate "all prior batch done, next batch not yet scheduled" window
  // must not flip the status back to Pending.
  if (root.Status() == crane::grpc::Running) return;

  if (meta->RunningChildCount() > 0) {
    root.SetStatus(crane::grpc::Running);
    return;
  }

  // No running children; only leave as Pending. Completed is assigned by
  // TryFinalizeRootNoLock_ based on aggregated child results.
  root.SetStatus(crane::grpc::Pending);
}

void ArrayManager::TryFinalizeRootNoLock_(
    ArrayMeta* meta, const std::unordered_set<JobInCtld*>& final_jobs,
    std::vector<FinalizedArrayRoot>* final_roots) {
  if (meta == nullptr || meta->root_job_ == nullptr) return;

  if (!ArrayChildrenExpanded_(meta->Root()) || meta->ActiveChildCount() != 0) {
    RefreshRootSummaryStateNoLock_(meta);
    return;
  }

  auto& root = meta->MutableRoot();
  job_id_t array_job_id = root.JobId();
  auto [final_status, final_exit_code] =
      BuildAggregateResult_(*meta, final_jobs);
  root.SetStatus(final_status);
  root.SetExitCode(final_exit_code);
  absl::Time end_time = absl::Now();
  root.SetEndTime(end_time);

  // Terminal dependency events, previously wrapped in a template helper.
  root.TriggerDependencyEvents(crane::grpc::DependencyType::AFTER_ANY,
                               end_time);
  root.TriggerDependencyEvents(
      crane::grpc::DependencyType::AFTER_OK,
      root.ExitCode() == 0 ? end_time : absl::InfiniteFuture());
  root.TriggerDependencyEvents(
      crane::grpc::DependencyType::AFTER_NOT_OK,
      root.ExitCode() != 0 ? end_time : absl::InfiniteFuture());

  // Slurm-parity: the parent pre-reserved task_count submit slots at submit
  // time; each materialized child's terminal FreeQosResource releases one.
  // Release any leftover slots for tasks that were never materialized.
  uint32_t task_count = ArrayUtil::TaskCount(root.JobToCtld().array_spec());
  auto materialized = static_cast<uint32_t>(meta->MaterializedTaskCount());
  if (task_count > materialized) {
    uint32_t leftover = task_count - materialized;
    g_account_meta_container->FreeQosSubmitResource(root, leftover);
  }

  auto meta_it = m_metas_.find(array_job_id);
  if (meta_it == m_metas_.end()) return;

  if (final_roots != nullptr) {
    // The scheduler owns the parent unique_ptr in m_pending_job_map_ and
    // will splice it in once this bundle is returned. We only surface the
    // id + meta here.
    final_roots->push_back(FinalizedArrayRoot{/*root_job=*/nullptr,
                                              meta_it->second, array_job_id});
  }
  m_metas_.erase(meta_it);
}

std::vector<ArrayManager::FinalizedArrayRoot>
ArrayManager::ExtractFinalRootsById(
    const std::unordered_set<job_id_t>& candidate_root_ids,
    const std::unordered_set<JobInCtld*>& final_jobs) {
  std::vector<FinalizedArrayRoot> out;
  out.reserve(candidate_root_ids.size());
  for (job_id_t root_id : candidate_root_ids) {
    auto* meta = FindMeta(root_id);
    TryFinalizeRootNoLock_(meta, final_jobs, &out);
  }
  return out;
}

std::vector<ArrayManager::FinalizedArrayRoot> ArrayManager::ExtractFinalRoots(
    const std::unordered_set<ArrayMeta*>& candidate_metas,
    const std::unordered_set<JobInCtld*>& final_jobs) {
  std::vector<FinalizedArrayRoot> out;
  out.reserve(candidate_metas.size());
  for (ArrayMeta* meta : candidate_metas) {
    TryFinalizeRootNoLock_(meta, final_jobs, &out);
  }
  return out;
}

// ----------------------------------------------------------------------------
// ArrayManager: lifecycle
// ----------------------------------------------------------------------------

ArrayMeta* ArrayManager::OnChildTerminal(JobInCtld* child) {
  CRANE_ASSERT(child != nullptr && child->IsArrayChild() &&
               child->ArrayJobId().has_value());
  auto* meta = FindMeta(child->ArrayJobId().value());
  if (meta == nullptr) return nullptr;
  meta->MarkChildInactive(child->JobId());
  return meta;
}

// ----------------------------------------------------------------------------
// ArrayManager: scheduler-loop entry points
// ----------------------------------------------------------------------------

bool ArrayManager::ApplyDependencyEvent(job_id_t dependent_id,
                                        job_id_t dependee_id,
                                        absl::Time event_time) {
  auto* meta = FindMeta(dependent_id);
  if (meta == nullptr) return false;
  meta->MutableRoot().UpdateDependency(dependee_id, event_time);
  return true;
}

bool ArrayManager::CanSpawnAnotherChild(const JobInCtld& parent) const {
  return GetSpawnBlockedReason(parent) == nullptr;
}

const char* ArrayManager::GetSpawnBlockedReason(const JobInCtld& parent) const {
  const auto* meta = FindMeta(parent.JobId());
  if (meta == nullptr) return "";
  if (ArrayChildrenExpanded_(parent)) return "ArrayExpanded";
  if (!NextTaskIdNoLock_(*meta).has_value()) return "";

  size_t limit = EffectiveArrayRunLimit_(parent);
  if (meta->RunningChildCount() >= limit) return "ArrayTaskLimit";
  return nullptr;
}

std::vector<JobInCtld*> ArrayManager::GetSchedulableParents() const {
  std::vector<JobInCtld*> out;
  out.reserve(m_metas_.size());
  for (const auto& [_, meta] : m_metas_) {
    if (meta == nullptr || meta->root_job_ == nullptr) continue;
    if (CanSpawnAnotherChild(meta->Root())) {
      out.push_back(meta->root_job_);
    }
  }
  return out;
}

std::unique_ptr<JobInCtld> ArrayManager::ExpandChildWithAllocation(
    job_id_t parent_job_id) {
  auto* meta = FindMeta(parent_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr) return nullptr;

  auto child = BuildNextChildNoLock_(meta);
  if (child == nullptr) return nullptr;

  auto admit = AdmitChildNoLock_(*meta, *child);
  if (!admit.has_value()) {
    CRANE_ERROR("Failed to admit array child task {} for parent job #{}: {}.",
                child->ArrayTaskId().value_or(0), parent_job_id,
                CraneErrStr(admit.error()));
    return nullptr;
  }

  // Child skips the pending phase: ExpandChildWithAllocation is only called
  // when NodeSelect has already given the parent an allocation.
  meta->MarkChildRunning(child->JobId());
  RefreshRootSummaryStateNoLock_(meta);
  return child;
}

// ----------------------------------------------------------------------------
// ArrayManager: resolve
// ----------------------------------------------------------------------------

ArrayTaskResolveResult ArrayManager::ResolveForQueryNoLock(
    job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids) const {
  ArrayTaskResolveResult result;

  const auto* meta = FindMeta(array_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr || task_ids.empty()) {
    return result;
  }
  const auto& root = meta->Root();
  const auto& array_spec = root.JobToCtld().array_spec();

  for (uint32_t task_id : task_ids) {
    if (!ArrayUtil::ContainsTaskId(array_spec, task_id)) continue;

    if (auto child_job_id = meta->ChildJobIdOfTask(task_id);
        child_job_id.has_value()) {
      // Caller guarantees conversion from child_job_id -> JobInfo via the
      // pending/running/finished-job paths it already walks.
      result.child_job_ids.push_back(*child_job_id);
      continue;
    }

    crane::grpc::JobInfo virtual_info;
    if (ArrayUtil::BuildVirtualArrayChildJobInfo(root, task_id,
                                                 &virtual_info)) {
      result.virtual_jobs.emplace_back(std::move(virtual_info));
    }
  }
  return result;
}

ArrayTaskResolveMutationResult ArrayManager::ResolveForMutationNoLock(
    job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids,
    ArrayMutationKind kind) {
  ArrayTaskResolveMutationResult result;

  auto* meta = FindMeta(array_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr || task_ids.empty()) {
    return result;
  }
  const auto& array_spec = meta->Root().JobToCtld().array_spec();

  for (uint32_t task_id : task_ids) {
    if (!ArrayUtil::ContainsTaskId(array_spec, task_id)) continue;

    if (auto child_job_id = meta->ChildJobIdOfTask(task_id);
        child_job_id.has_value()) {
      result.child_job_ids.push_back(*child_job_id);
      continue;
    }

    if (kind == ArrayMutationKind::kCancel) {
      meta->MarkTaskCancelledBeforeMaterialized(task_id);
      result.virtual_task_ids_cancelled.push_back(task_id);
    } else {
      result.virtual_task_ids_rejected.push_back(task_id);
    }
  }

  // If this cancel batch just closed out every task in the spec, flip the
  // parent's expanded flag in memory so TryFinalizeRootNoLock_ will succeed
  // the next time the caller runs it — otherwise a pure-virtual cancel
  // (no surviving running children to drive OnChildTerminal) would leave
  // the parent wedged in m_pending_job_map_ forever.
  if (kind == ArrayMutationKind::kCancel &&
      !result.virtual_task_ids_cancelled.empty()) {
    FlipExpandedIfFullyAccountedNoLock_(meta);
  }
  return result;
}

// ----------------------------------------------------------------------------
// ArrayManager: persistence / plugin static helpers
// ----------------------------------------------------------------------------

crane::grpc::JobInEmbeddedDb ArrayManager::BuildFinalRootRecord(
    const JobInCtld& array_root) {
  crane::grpc::JobInEmbeddedDb record;
  *record.mutable_job_to_ctld() = array_root.JobToCtld();
  *record.mutable_runtime_attr() = array_root.RuntimeAttr();
  record.mutable_runtime_attr()->set_job_id(array_root.JobId());
  record.mutable_runtime_attr()->set_job_db_id(array_root.JobDbId());
  return record;
}

void ArrayManager::CallPluginHookForFinalRoots(
    const std::vector<FinalizedArrayRoot>& roots) {
  if (!g_config.Plugin.Enabled || roots.empty()) return;

  std::vector<crane::grpc::JobInfo> jobs_post_comp;
  jobs_post_comp.reserve(roots.size());
  for (const auto& root : roots) {
    if (root.root_job == nullptr) continue;
    crane::grpc::JobInfo t;
    root.root_job->SetFieldsOfJobInfo(&t);
    jobs_post_comp.emplace_back(std::move(t));
  }
  g_plugin_client->EndHookAsync(std::move(jobs_post_comp));
}

void ArrayManager::PersistAndTransferRootsToMongodb(
    const std::vector<FinalizedArrayRoot>& roots) {
  if (roots.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  for (const auto& root : roots) {
    if (root.root_job == nullptr) continue;
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJob(
            txn_id, root.root_job->JobDbId(), root.root_job->RuntimeAttr()))
      CRANE_ERROR("Failed to call UpdateRuntimeAttrOfJob() for array root #{}",
                  root.root_job->JobId());
  }
  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  for (const auto& root : roots) {
    if (root.root_job == nullptr) continue;
    auto record = BuildFinalRootRecord(*root.root_job);
    if (!g_db_client->InsertRecoveredJob(record))
      CRANE_ERROR("Failed to insert array root #{} into MongoDB",
                  root.root_job->JobId());
  }

  std::unordered_map<job_id_t, job_db_id_t> db_ids;
  for (const auto& root : roots) {
    if (root.root_job == nullptr) continue;
    db_ids[root.root_job->JobId()] = root.root_job->JobDbId();
  }
  if (!g_embedded_db_client->PurgeEndedJobs(db_ids))
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedJobs() "
        "for final array roots");
}

void ArrayManager::ProcessFinalRoots(
    const std::vector<FinalizedArrayRoot>& roots) {
  PersistAndTransferRootsToMongodb(roots);
  CallPluginHookForFinalRoots(roots);
}

}  // namespace Ctld
