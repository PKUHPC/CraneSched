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

#include "Array.h"

#include <google/protobuf/util/time_util.h>

#include "AccountMetaContainer.h"
#include "DbClient.h"
#include "EmbeddedDbClient.h"
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

crane::grpc::ArrayTaskSummary BuildSummary(
    const crane::grpc::ArraySpec& array_spec) {
  crane::grpc::ArrayTaskSummary summary;
  summary.set_task_count(TaskCount(array_spec));
  summary.set_task_min(array_spec.start());
  summary.set_task_max(array_spec.end());
  summary.set_task_step(Stride(array_spec));
  return summary;
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

ArrayMeta::ArrayMeta(std::unique_ptr<JobInCtld> root_job)
    : root_job_(std::move(root_job)) {}

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
  if (child == nullptr || !child->IsArrayChild()) {
    return;
  }
  auto task_id = child->ArrayTaskId();
  CRANE_ASSERT(task_id.has_value());
  child_job_id_by_task_id_[*task_id] = child->JobId();
}

void ArrayMeta::UntrackMaterialized(JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild()) {
    return;
  }
  auto task_id = child->ArrayTaskId();
  if (!task_id.has_value()) return;

  auto it = child_job_id_by_task_id_.find(*task_id);
  if (it != child_job_id_by_task_id_.end() && it->second == child->JobId()) {
    child_job_id_by_task_id_.erase(it);
  }
}

void ArrayMeta::MarkChildActive(job_id_t child_job_id) {
  active_child_job_ids_.insert(child_job_id);
}

void ArrayMeta::MarkChildInactive(job_id_t child_job_id) {
  active_child_job_ids_.erase(child_job_id);
}

// ----------------------------------------------------------------------------
// ArrayManager: anonymous-namespace helpers (kept file-local)
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

template <typename T>
void TriggerTerminalDependencyEventsImpl(T* obj, uint32_t exit_code,
                                         absl::Time end_time) {
  obj->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER_ANY,
                               end_time);
  obj->TriggerDependencyEvents(
      crane::grpc::DependencyType::AFTER_OK,
      exit_code == 0 ? end_time : absl::InfiniteFuture());
  obj->TriggerDependencyEvents(
      crane::grpc::DependencyType::AFTER_NOT_OK,
      exit_code != 0 ? end_time : absl::InfiniteFuture());
}

}  // namespace

// ----------------------------------------------------------------------------
// ArrayManager: static helpers
// ----------------------------------------------------------------------------

bool ArrayManager::IsArrayRootTerminalStatus_(crane::grpc::JobStatus status) {
  switch (status) {
  case crane::grpc::Completed:
  case crane::grpc::Failed:
  case crane::grpc::Cancelled:
  case crane::grpc::ExceedTimeLimit:
  case crane::grpc::OutOfMemory:
  case crane::grpc::Deadline:
    return true;
  default:
    return false;
  }
}

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

void ArrayManager::SetArrayChildrenExpanded_(JobInCtld* root, bool expanded) {
  CRANE_ASSERT(root != nullptr);
  root->SetArrayChildrenExpanded(expanded);
}

void ArrayManager::TriggerTerminalDependencyEvents_(JobInCtld* job,
                                                    absl::Time end_time) {
  if (job == nullptr) return;
  TriggerTerminalDependencyEventsImpl(job, job->ExitCode(), end_time);
}

void ArrayManager::TriggerTerminalDependencyEvents_(ArrayMeta* meta,
                                                    absl::Time end_time) {
  if (meta == nullptr || meta->root_job_ == nullptr) return;
  TriggerTerminalDependencyEventsImpl(meta->root_job_.get(),
                                      meta->root_job_->ExitCode(), end_time);
}

std::pair<crane::grpc::JobStatus, uint32_t> ArrayManager::BuildAggregateResult_(
    job_id_t array_job_id, const std::unordered_set<JobInCtld*>& final_jobs) {
  bool any_cancelled = false;
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

// Slurm-parity: the array parent already reserved a submit-slot per task
// at submission time (JobScheduler::SubmitJobToScheduler with
// count=task_count). Child materialization therefore owes no independent
// QoS submit check — it only inherits the parent's qos/time_limit into its
// own proto so downstream consumers can read it per child.
void ArrayManager::InheritChildAttributesFromParent_(JobInCtld& child) {
  child.MutableJobToCtld()->set_qos(child.qos);
  child.MutableJobToCtld()->mutable_time_limit()->set_seconds(
      ToInt64Seconds(child.time_limit));
}

// ----------------------------------------------------------------------------
// ArrayManager: construction + lookup
// ----------------------------------------------------------------------------

ArrayManager::ArrayManager(ActiveJobMaps maps, absl::Mutex* pending_mtx,
                           absl::Mutex* running_mtx)
    : m_pending_jobs_(maps.pending_jobs),
      m_running_jobs_(maps.running_jobs),
      m_pending_cached_size_(maps.pending_cached_size),
      m_pending_mtx_(pending_mtx),
      m_running_mtx_(running_mtx) {}

ArrayMeta* ArrayManager::FindMeta(job_id_t array_job_id) {
  auto it = m_metas_.find(array_job_id);
  return it == m_metas_.end() ? nullptr : it->second.get();
}

const ArrayMeta* ArrayManager::FindMeta(job_id_t array_job_id) const {
  auto it = m_metas_.find(array_job_id);
  return it == m_metas_.end() ? nullptr : it->second.get();
}

JobInCtld* ArrayManager::FindJobInPendingOrRunningNoLock_(job_id_t job_id) {
  auto pd_it = m_pending_jobs_.find(job_id);
  if (pd_it != m_pending_jobs_.end()) {
    return pd_it->second.get();
  }
  auto rn_it = m_running_jobs_.find(job_id);
  if (rn_it != m_running_jobs_.end()) {
    return rn_it->second.get();
  }
  return nullptr;
}

bool ArrayManager::IsChildInRunningNoLock_(job_id_t child_job_id) const {
  return m_running_jobs_.contains(child_job_id);
}

bool ArrayManager::IsChildInPendingNoLock_(job_id_t child_job_id) const {
  return m_pending_jobs_.contains(child_job_id);
}

size_t ArrayManager::RunningChildCountNoLock_(const ArrayMeta& meta) const {
  size_t count = 0;
  for (job_id_t cid : meta.active_child_job_ids_) {
    if (IsChildInRunningNoLock_(cid)) ++count;
  }
  return count;
}

size_t ArrayManager::PendingChildCountNoLock_(const ArrayMeta& meta) const {
  size_t count = 0;
  for (job_id_t cid : meta.active_child_job_ids_) {
    if (IsChildInPendingNoLock_(cid)) ++count;
  }
  return count;
}

std::vector<job_id_t> ArrayManager::PendingChildJobIds(
    const ArrayMeta& meta) const {
  std::vector<job_id_t> out;
  out.reserve(meta.active_child_job_ids_.size());
  for (job_id_t cid : meta.active_child_job_ids_) {
    if (IsChildInPendingNoLock_(cid)) out.push_back(cid);
  }
  return out;
}

std::vector<job_id_t> ArrayManager::RunningChildJobIds(
    const ArrayMeta& meta) const {
  std::vector<job_id_t> out;
  out.reserve(meta.active_child_job_ids_.size());
  for (job_id_t cid : meta.active_child_job_ids_) {
    if (IsChildInRunningNoLock_(cid)) out.push_back(cid);
  }
  return out;
}

size_t ArrayManager::PendingChildCount(const ArrayMeta& meta) const {
  return PendingChildCountNoLock_(meta);
}

size_t ArrayManager::RunningChildCount(const ArrayMeta& meta) const {
  return RunningChildCountNoLock_(meta);
}

// ----------------------------------------------------------------------------
// ArrayManager: registration + recovery
// ----------------------------------------------------------------------------

std::shared_ptr<ArrayMeta> ArrayManager::RegisterParent(
    std::unique_ptr<JobInCtld> parent) {
  if (parent == nullptr) return nullptr;

  if (parent->qos.empty()) {
    parent->qos = parent->JobToCtld().qos();
  }

  bool expanded = ArrayChildrenExpanded_(*parent);
  array_task_id_t starting_index =
      expanded ? ArrayUtil::TaskCount(parent->JobToCtld().array_spec()) : 0;

  job_id_t root_id = parent->JobId();
  auto meta = std::make_shared<ArrayMeta>(std::move(parent));
  meta->SetNextTaskIndex(starting_index);
  m_metas_[root_id] = meta;
  return meta;
}

void ArrayManager::BindRecoveredChild(ArrayMeta* meta, JobInCtld* child) {
  if (meta == nullptr || child == nullptr || !child->IsArrayChild()) {
    return;
  }
  meta->TrackMaterialized(child);
  meta->MarkChildActive(child->JobId());
}

void ArrayManager::RefreshRecoveredSummary(ArrayMeta* meta) {
  if (meta == nullptr || meta->root_job_ == nullptr) return;
  if (!ArrayChildrenExpanded_(meta->Root())) {
    SyncNextTaskIndexNoLock_(meta);
  }
  RefreshRootSummaryStateNoLock_(meta);
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

void ArrayManager::SyncNextTaskIndexNoLock_(ArrayMeta* meta) const {
  if (meta == nullptr || meta->root_job_ == nullptr) return;

  const auto& array_spec = meta->Root().JobToCtld().array_spec();
  uint32_t task_count = ArrayUtil::TaskCount(array_spec);
  auto idx = meta->NextTaskIndex();
  while (idx < task_count &&
         meta->IsTaskMaterialized(ArrayUtil::TaskIdByIndex(array_spec, idx))) {
    ++idx;
  }
  meta->SetNextTaskIndex(idx);
}

std::unique_ptr<JobInCtld> ArrayManager::BuildNextChildNoLock_(
    ArrayMeta* meta) const {
  if (meta == nullptr || meta->root_job_ == nullptr ||
      ArrayChildrenExpanded_(meta->Root())) {
    return nullptr;
  }

  SyncNextTaskIndexNoLock_(meta);

  const auto& root = meta->Root();
  const auto& array_spec = root.JobToCtld().array_spec();
  uint32_t total = ArrayUtil::TaskCount(array_spec);
  if (meta->NextTaskIndex() >= total) {
    return nullptr;
  }

  uint32_t task_id =
      ArrayUtil::TaskIdByIndex(array_spec, meta->NextTaskIndex());
  return meta->IsTaskMaterialized(task_id) ? nullptr
                                           : BuildChildJob_(root, task_id);
}

bool ArrayManager::CanMaterializeRootNoLock_(ArrayMeta* meta,
                                             absl::Time now) const {
  if (meta == nullptr || meta->root_job_ == nullptr) return false;
  auto& root = meta->MutableRoot();

  if (root.Held()) {
    root.pending_reason = "Held";
    return false;
  }
  if (root.begin_time > now) {
    root.pending_reason = "BeginTime";
    return false;
  }
  if (!root.Dependencies().is_met(now)) {
    if (root.Dependencies().is_failed()) {
      root.pending_reason = "DependencyNeverSatisfied";
    } else {
      root.pending_reason = "Dependency";
    }
    return false;
  }
  if (root.deadline_time <= now) {
    root.pending_reason = "Deadline";
    return false;
  }
  root.pending_reason.clear();
  return true;
}

CraneExpected<void> ArrayManager::AdmitChildPtrsNoLock_(
    ArrayMeta* meta, const std::vector<JobInCtld*>& child_ptrs) {
  if (meta == nullptr || meta->root_job_ == nullptr || child_ptrs.empty()) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  auto& root = meta->MutableRoot();
  // Child submit-slots were pre-reserved on the parent at submission time;
  // here we only propagate inherited fields onto the child's proto.
  for (JobInCtld* child : child_ptrs) {
    InheritChildAttributesFromParent_(*child);
  }

  bool mark_expanded = meta->MaterializedTaskCount() + child_ptrs.size() ==
                       ArrayUtil::TaskCount(root.JobToCtld().array_spec());

  std::vector<EmbeddedDbClient::ExtraVariableWrite> extra_writes;
  if (mark_expanded) {
    SetArrayChildrenExpanded_(&root, true);
    extra_writes.push_back({root.JobDbId(), &root.RuntimeAttr()});
  }

  if (!g_embedded_db_client->AppendJobsToPendingAndAdvanceJobIds(
          child_ptrs, extra_writes)) {
    CRANE_ERROR("Failed to persist array children for parent job #{}.",
                root.JobId());
    if (mark_expanded) {
      SetArrayChildrenExpanded_(&root, false);
    }
    return std::unexpected(CraneErrCode::ERR_DB_INSERT_FAILED);
  }
  return {};
}

CraneExpected<void> ArrayManager::AdmitChildNoLock_(ArrayMeta* meta,
                                                    JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild() || !child->ArrayJobId()) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }
  if (meta == nullptr || meta->root_job_ == nullptr) {
    return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  }
  if (child->ArrayJobId().value() != meta->Root().JobId()) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }
  return AdmitChildPtrsNoLock_(meta, std::vector<JobInCtld*>{child});
}

CraneExpected<void> ArrayManager::AdmitChildrenNoLock_(
    ArrayMeta* meta, std::vector<std::unique_ptr<JobInCtld>>& children) {
  if (children.empty()) return {};
  if (meta == nullptr || meta->root_job_ == nullptr) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }
  std::vector<JobInCtld*> child_ptrs;
  child_ptrs.reserve(children.size());
  for (auto& child : children) child_ptrs.push_back(child.get());
  return AdmitChildPtrsNoLock_(meta, child_ptrs);
}

void ArrayManager::EnqueuePendingJobNoLock_(std::unique_ptr<JobInCtld> job) {
  if (job == nullptr) return;
  m_pending_jobs_.emplace(job->JobId(), std::move(job));
  m_pending_cached_size_.store(m_pending_jobs_.size(),
                               std::memory_order_release);
}

bool ArrayManager::TryEnqueueNextChildNoLock_(ArrayMeta* meta) {
  if (meta == nullptr || meta->root_job_ == nullptr) return false;
  auto& root = meta->MutableRoot();

  auto child = BuildNextChildNoLock_(meta);
  if (child == nullptr) {
    if (!ArrayChildrenExpanded_(root) &&
        meta->MaterializedTaskCount() ==
            ArrayUtil::TaskCount(root.JobToCtld().array_spec())) {
      SetArrayChildrenExpanded_(&root, true);
      if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
              0, root.JobDbId(), root.RuntimeAttr())) {
        CRANE_ERROR(
            "Failed to persist expanded state for array parent job #{}.",
            root.JobId());
        SetArrayChildrenExpanded_(&root, false);
      }
    }
    RefreshRootSummaryStateNoLock_(meta);
    return false;
  }

  JobInCtld* child_raw = child.get();
  auto admit = AdmitChildNoLock_(meta, child_raw);
  if (!admit.has_value()) {
    CRANE_ERROR("Failed to admit array child task {} for parent job #{}: {}.",
                child_raw->ArrayTaskId().value_or(0), root.JobId(),
                CraneErrStr(admit.error()));
    return false;
  }

  meta->TrackMaterialized(child_raw);
  meta->MarkChildActive(child_raw->JobId());
  SyncNextTaskIndexNoLock_(meta);
  EnqueuePendingJobNoLock_(std::move(child));
  RefreshRootSummaryStateNoLock_(meta);
  return true;
}

std::vector<job_id_t> ArrayManager::MaterializeSpecificTasksNoLock_(
    job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids) {
  std::vector<job_id_t> materialized_child_ids;

  auto* meta = FindMeta(array_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr || task_ids.empty()) {
    return materialized_child_ids;
  }
  const auto& root = meta->Root();

  std::vector<std::unique_ptr<JobInCtld>> children;
  for (uint32_t task_id : task_ids) {
    if (meta->IsTaskMaterialized(task_id)) continue;
    auto child = BuildChildJob_(root, task_id);
    if (child == nullptr) continue;
    children.push_back(std::move(child));
  }

  if (children.empty()) {
    if (!ArrayChildrenExpanded_(root) &&
        meta->MaterializedTaskCount() ==
            ArrayUtil::TaskCount(root.JobToCtld().array_spec())) {
      auto& mutable_root = meta->MutableRoot();
      SetArrayChildrenExpanded_(&mutable_root, true);
      if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
              0, mutable_root.JobDbId(), mutable_root.RuntimeAttr())) {
        CRANE_ERROR(
            "Failed to persist expanded state for array parent job #{}.",
            mutable_root.JobId());
        SetArrayChildrenExpanded_(&mutable_root, false);
      }
    }
    return materialized_child_ids;
  }

  CRANE_INFO(
      "Materializing {} specific array tasks for parent job #{} (range "
      "[{}-{}:{}]).",
      children.size(), root.JobId(), root.JobToCtld().array_spec().start(),
      root.JobToCtld().array_spec().end(),
      ArrayUtil::Stride(root.JobToCtld().array_spec()));

  auto admit = AdmitChildrenNoLock_(meta, children);
  if (!admit.has_value()) {
    CRANE_ERROR(
        "Failed to materialize specific array tasks for parent job #{}: {}.",
        root.JobId(), CraneErrStr(admit.error()));
    return {};
  }

  for (auto& child : children) {
    JobInCtld* child_raw = child.get();
    meta->TrackMaterialized(child_raw);
    meta->MarkChildActive(child_raw->JobId());
    EnqueuePendingJobNoLock_(std::move(child));
  }
  SyncNextTaskIndexNoLock_(meta);
  RefreshRootSummaryStateNoLock_(meta);

  for (uint32_t task_id : task_ids) {
    if (auto child_job_id = meta->ChildJobIdOfTask(task_id);
        child_job_id.has_value()) {
      materialized_child_ids.push_back(*child_job_id);
    }
  }
  return materialized_child_ids;
}

// ----------------------------------------------------------------------------
// ArrayManager: summary + finalize
// ----------------------------------------------------------------------------

void ArrayManager::RefreshRootSummaryStateNoLock_(ArrayMeta* meta) {
  if (meta == nullptr || meta->root_job_ == nullptr) return;
  auto& root = meta->MutableRoot();

  // Aggregate terminal statuses derived from children are authoritative.
  if (IsArrayRootTerminalStatus_(root.Status())) return;

  crane::grpc::JobStatus summary = crane::grpc::Pending;
  size_t running_count = RunningChildCountNoLock_(*meta);
  size_t pending_count = PendingChildCountNoLock_(*meta);

  if (running_count > 0) {
    summary = crane::grpc::Running;
  } else if (ArrayChildrenExpanded_(root) && pending_count == 0) {
    summary = crane::grpc::Completed;
  }
  root.SetStatus(summary);
}

void ArrayManager::TryFinalizeRootNoLock_(
    ArrayMeta* meta, const std::unordered_set<JobInCtld*>& final_jobs,
    std::vector<std::shared_ptr<ArrayMeta>>* final_roots) {
  if (meta == nullptr || meta->root_job_ == nullptr) return;

  RefreshRootSummaryStateNoLock_(meta);
  if (!ArrayChildrenExpanded_(meta->Root()) || meta->ActiveChildCount() != 0) {
    return;
  }

  auto& root = meta->MutableRoot();
  job_id_t array_job_id = root.JobId();
  auto [final_status, final_exit_code] =
      BuildAggregateResult_(array_job_id, final_jobs);
  root.SetStatus(final_status);
  root.SetExitCode(final_exit_code);
  root.SetEndTime(absl::Now());
  TriggerTerminalDependencyEvents_(meta, root.EndTime());

  // Slurm-parity: the parent pre-reserved task_count submit slots at submit
  // time; each materialized child's terminal FreeQosResource releases one.
  // Release any leftover slots for tasks that were never materialized.
  uint32_t task_count = ArrayUtil::TaskCount(root.JobToCtld().array_spec());
  auto materialized = static_cast<uint32_t>(meta->MaterializedTaskCount());
  if (task_count > materialized) {
    uint32_t leftover = task_count - materialized;
    g_account_meta_container->FreeQosSubmitResource(root, leftover);
  }

  auto it = m_metas_.find(array_job_id);
  if (it == m_metas_.end()) return;

  if (final_roots != nullptr) {
    final_roots->emplace_back(it->second);
  }
  m_metas_.erase(it);
}

std::vector<std::shared_ptr<ArrayMeta>> ArrayManager::ExtractFinalRootsById(
    const std::unordered_set<job_id_t>& candidate_root_ids,
    const std::unordered_set<JobInCtld*>& final_jobs) {
  std::vector<std::shared_ptr<ArrayMeta>> out;
  out.reserve(candidate_root_ids.size());
  for (job_id_t root_id : candidate_root_ids) {
    auto* meta = FindMeta(root_id);
    TryFinalizeRootNoLock_(meta, final_jobs, &out);
  }
  return out;
}

std::vector<std::shared_ptr<ArrayMeta>> ArrayManager::ExtractFinalRoots(
    const std::unordered_set<ArrayMeta*>& candidate_metas,
    const std::unordered_set<JobInCtld*>& final_jobs) {
  std::vector<std::shared_ptr<ArrayMeta>> out;
  out.reserve(candidate_metas.size());
  for (ArrayMeta* meta : candidate_metas) {
    TryFinalizeRootNoLock_(meta, final_jobs, &out);
  }
  return out;
}

// ----------------------------------------------------------------------------
// ArrayManager: lifecycle
// ----------------------------------------------------------------------------

void ArrayManager::OnChildPending(JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild() ||
      !child->ArrayJobId().has_value()) {
    return;
  }
  auto* meta = FindMeta(child->ArrayJobId().value());
  if (meta == nullptr) return;
  meta->TrackMaterialized(child);
  meta->MarkChildActive(child->JobId());
}

void ArrayManager::OnChildRunning(JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild() ||
      !child->ArrayJobId().has_value()) {
    return;
  }
  auto* meta = FindMeta(child->ArrayJobId().value());
  if (meta == nullptr) return;
  // Active bookkeeping already contains this child; the distinction between
  // pending and running is derived from the scheduler maps on demand.
  meta->TrackMaterialized(child);
  meta->MarkChildActive(child->JobId());
  RefreshRootSummaryStateNoLock_(meta);
}

ArrayMeta* ArrayManager::OnChildTerminal(JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild() ||
      !child->ArrayJobId().has_value()) {
    return nullptr;
  }
  auto* meta = FindMeta(child->ArrayJobId().value());
  if (meta == nullptr) return nullptr;
  meta->MarkChildInactive(child->JobId());
  return meta;
}

// ----------------------------------------------------------------------------
// ArrayManager: scheduler-loop entry points
// ----------------------------------------------------------------------------

bool ArrayManager::HasReadyChildToMaterialize() const {
  for (const auto& [_, meta] : m_metas_) {
    const auto& root = meta->Root();
    if (ArrayChildrenExpanded_(root)) continue;
    size_t limit = EffectiveArrayRunLimit_(root);
    size_t pending_count = PendingChildCountNoLock_(*meta);
    size_t running_count = RunningChildCountNoLock_(*meta);
    if (pending_count == 0 && running_count < limit) {
      return true;
    }
  }
  return false;
}

bool ArrayManager::ApplyDependencyEvent(job_id_t dependent_id,
                                        job_id_t dependee_id,
                                        absl::Time event_time) {
  auto pd_it = m_pending_jobs_.find(dependent_id);
  if (pd_it != m_pending_jobs_.end()) {
    pd_it->second->UpdateDependency(dependee_id, event_time);
    return true;
  }
  auto* meta = FindMeta(dependent_id);
  if (meta != nullptr) {
    meta->MutableRoot().UpdateDependency(dependee_id, event_time);
    return true;
  }
  return false;
}

void ArrayManager::TryEnqueueReadyChildren(absl::Time now) {
  for (auto& [_, meta_ptr] : m_metas_) {
    auto* meta = meta_ptr.get();
    auto& root = meta->MutableRoot();
    if (ArrayChildrenExpanded_(root) || !CanMaterializeRootNoLock_(meta, now)) {
      continue;
    }

    size_t limit = EffectiveArrayRunLimit_(root);
    if (RunningChildCountNoLock_(*meta) >= limit) {
      root.pending_reason = "ArrayTaskLimit";
      continue;
    }

    if (PendingChildCountNoLock_(*meta) != 0) {
      continue;
    }

    TryEnqueueNextChildNoLock_(meta);
  }
}

// ----------------------------------------------------------------------------
// ArrayManager: resolve
// ----------------------------------------------------------------------------

ArrayTaskResolveResult ArrayManager::ResolveArrayTaskIdsNoLock(
    job_id_t array_job_id, const std::unordered_set<uint32_t>& task_ids,
    ArrayTaskResolveMode mode) {
  ArrayTaskResolveResult result;

  auto* meta = FindMeta(array_job_id);
  if (meta == nullptr || meta->root_job_ == nullptr || task_ids.empty()) {
    return result;
  }

  const auto& root = meta->Root();
  const auto& array_spec = root.JobToCtld().array_spec();

  if (mode == ArrayTaskResolveMode::kCreateIfMissing) {
    MaterializeSpecificTasksNoLock_(array_job_id, task_ids);
  }

  for (uint32_t task_id : task_ids) {
    if (!ArrayUtil::ContainsTaskId(array_spec, task_id)) continue;

    if (auto child_job_id = meta->ChildJobIdOfTask(task_id);
        child_job_id.has_value()) {
      result.child_job_ids.push_back(*child_job_id);
      continue;
    }

    if (mode == ArrayTaskResolveMode::kQueryOnly &&
        !meta->IsTaskMaterialized(task_id)) {
      crane::grpc::JobInfo virtual_info;
      if (ArrayUtil::BuildVirtualArrayChildJobInfo(root, task_id,
                                                   &virtual_info)) {
        result.virtual_jobs.emplace_back(std::move(virtual_info));
      }
    }
  }
  return result;
}

ArrayTaskResolveResult ArrayManager::ResolveArrayTaskIds(
    job_id_t array_job_id,
    const google::protobuf::RepeatedField<uint32_t>& array_task_ids,
    ArrayTaskResolveMode mode) {
  std::unordered_set<uint32_t> requested_task_ids(array_task_ids.begin(),
                                                  array_task_ids.end());

  absl::MutexLock pending_guard(m_pending_mtx_);
  absl::MutexLock running_guard(m_running_mtx_);
  return ResolveArrayTaskIdsNoLock(array_job_id, requested_task_ids, mode);
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
    const std::vector<std::shared_ptr<ArrayMeta>>& roots) {
  if (!g_config.Plugin.Enabled || roots.empty()) return;

  std::vector<crane::grpc::JobInfo> jobs_post_comp;
  jobs_post_comp.reserve(roots.size());
  for (const auto& meta : roots) {
    crane::grpc::JobInfo t;
    meta->RootPtr()->SetFieldsOfJobInfo(&t);
    jobs_post_comp.emplace_back(std::move(t));
  }
  g_plugin_client->EndHookAsync(std::move(jobs_post_comp));
}

void ArrayManager::PersistAndTransferRootsToMongodb(
    const std::vector<std::shared_ptr<ArrayMeta>>& roots) {
  if (roots.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  for (const auto& meta : roots) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJob(
            txn_id, meta->RootPtr()->JobDbId(), meta->RootPtr()->RuntimeAttr()))
      CRANE_ERROR("Failed to call UpdateRuntimeAttrOfJob() for array root #{}",
                  meta->RootPtr()->JobId());
  }
  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  for (const auto& meta : roots) {
    auto record = BuildFinalRootRecord(*meta->RootPtr());
    if (!g_db_client->InsertRecoveredJob(record))
      CRANE_ERROR("Failed to insert array root #{} into MongoDB",
                  meta->RootPtr()->JobId());
  }

  std::unordered_map<job_id_t, job_db_id_t> db_ids;
  for (const auto& meta : roots)
    db_ids[meta->RootPtr()->JobId()] = meta->RootPtr()->JobDbId();
  if (!g_embedded_db_client->PurgeEndedJobs(db_ids))
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedJobs() "
        "for final array roots");
}

void ArrayManager::ProcessFinalRoots(
    const std::vector<std::shared_ptr<ArrayMeta>>& roots) {
  PersistAndTransferRootsToMongodb(roots);
  CallPluginHookForFinalRoots(roots);
}

}  // namespace Ctld
