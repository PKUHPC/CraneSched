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

uint32_t EffectiveRunLimit(const crane::grpc::ArraySpec& array_spec) {
  uint32_t task_count = TaskCount(array_spec);
  if (!array_spec.has_max_concurrent() || array_spec.max_concurrent() == 0) {
    return task_count;
  }
  return std::min(array_spec.max_concurrent(), task_count);
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

}  // namespace ArrayUtil

// ----------------------------------------------------------------------------
// ArrayMeta
// ----------------------------------------------------------------------------

ArrayMeta::ArrayMeta(JobInCtld* parent_job) : parent_job_(parent_job) {
  CRANE_ASSERT(parent_job_ != nullptr);
}

std::optional<job_id_t> ArrayMeta::ChildJobIdOfTask(
    array_task_id_t task_id) const {
  auto it = child_job_id_by_task_id_.find(task_id);
  if (it == child_job_id_by_task_id_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<array_task_id_t> ArrayMeta::NextMaterializableTaskId() const {
  if (parent_job_ == nullptr || !parent_job_->HasArraySpec()) {
    return std::nullopt;
  }

  const auto& array_spec = parent_job_->JobToCtld().array_spec();
  if (!has_any_materialized_) return array_spec.start();

  // Children are materialized strictly in task-id order. Do not scan backward
  // for gaps: after recovery, an earlier child may already have completed and
  // been purged from embedded DB, so rematerializing that task would duplicate
  // work.
  uint32_t stride = ArrayUtil::Stride(array_spec);
  if (max_materialized_task_id_ > array_spec.end() ||
      array_spec.end() - max_materialized_task_id_ < stride) {
    return std::nullopt;
  }

  return static_cast<array_task_id_t>(max_materialized_task_id_ + stride);
}

bool ArrayMeta::WillCompleteMaterializationAfter(
    array_task_id_t task_id) const {
  if (parent_job_ == nullptr || !parent_job_->HasArraySpec()) return false;
  if (parent_job_->ArrayMaterializationComplete()) return false;

  const auto& array_spec = parent_job_->JobToCtld().array_spec();
  uint32_t task_count = ArrayUtil::TaskCount(array_spec);
  if (task_count == 0) return false;

  return task_id == ArrayUtil::TaskIdByIndex(array_spec, task_count - 1);
}

std::unique_ptr<JobInCtld> ArrayMeta::BuildChild(
    array_task_id_t task_id) const {
  if (parent_job_ == nullptr) return nullptr;

  const auto* array_spec = parent_job_->GetArraySpec();
  if (array_spec == nullptr ||
      !ArrayUtil::ContainsTaskId(*array_spec, task_id)) {
    return nullptr;
  }

  auto child = std::make_unique<JobInCtld>();
  child->SetFieldsByJobToCtld(parent_job_->JobToCtld());
  child->MutableJobToCtld()->clear_array_spec();
  child->SetStatus(crane::grpc::Pending);
  child->SetArrayTaskIdentity(parent_job_->JobId(), task_id);
  child->SetUsername(parent_job_->Username());
  child->qos = parent_job_->qos;
  child->time_limit = parent_job_->time_limit;
  child->account_chain = parent_job_->account_chain;
  child->req_total_res_view = parent_job_->req_total_res_view;

  {
    std::list<std::string> included_list;
    util::ParseHostList(parent_job_->JobToCtld().nodelist(), &included_list);
    for (auto&& node : included_list)
      child->included_nodes.emplace(std::move(node));
  }
  {
    std::list<std::string> excluded_list;
    util::ParseHostList(parent_job_->JobToCtld().excludes(), &excluded_list);
    for (auto&& node : excluded_list)
      child->excluded_nodes.emplace(std::move(node));
  }

  child->ntasks_per_node_min = parent_job_->ntasks_per_node_min;
  child->ntasks_per_node_max = parent_job_->ntasks_per_node_max;
  child->partition_priority = parent_job_->partition_priority;
  child->qos_priority = parent_job_->qos_priority;
  child->mandated_priority = parent_job_->mandated_priority;
  child->SetCachedPriority(parent_job_->CachedPriority());
  child->using_default_wckey = parent_job_->using_default_wckey;
  child->wckey = parent_job_->wckey;
  child->SetSubmitTime(parent_job_->SubmitTime());
  child->deadline_time = parent_job_->deadline_time;
  child->MutableJobToCtld()->mutable_deadline_time()->set_seconds(
      ToUnixSeconds(parent_job_->deadline_time));
  return child;
}

bool ArrayMeta::TrackMaterialized(JobInCtld* child) {
  CRANE_ASSERT(child != nullptr && child->IsArrayChild());
  auto task_id = child->ArrayTaskId();
  CRANE_ASSERT(task_id.has_value());
  return TrackMaterialized(*task_id, child->JobId());
}

bool ArrayMeta::TrackMaterialized(array_task_id_t task_id,
                                  job_id_t child_job_id) {
  child_job_id_by_task_id_[task_id] = child_job_id;
  if (!has_any_materialized_ || task_id > max_materialized_task_id_) {
    max_materialized_task_id_ = task_id;
    has_any_materialized_ = true;
  }

  if (!WillCompleteMaterializationAfter(task_id)) return false;
  parent_job_->SetArrayMaterializationComplete(true);
  return true;
}

void ArrayMeta::OnChildRunning(job_id_t child_job_id) {
  running_child_job_ids_.insert(child_job_id);
}

void ArrayMeta::OnChildTerminal(job_id_t child_job_id) {
  running_child_job_ids_.erase(child_job_id);
}

void ArrayMeta::FinishForTerminalStatus(crane::grpc::JobStatus status,
                                        uint32_t exit_code,
                                        absl::Time end_time) {
  if (parent_job_ == nullptr) return;
  // Idempotent: a parent may be finished twice (e.g. deadline timer fires
  // concurrently with an explicit cancel). The first call wins.
  if (terminal_status_override_.has_value()) return;
  terminal_status_override_ = std::pair{status, exit_code};
  // Deadline/cancel may finish an array parent before any child starts. Emit
  // the parent's start event first so its timeline is start -> terminal.
  if (!parent_start_event_triggered_) {
    parent_job_->SetStartTime(end_time);
    parent_job_->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER,
                                         end_time);
    parent_start_event_triggered_ = true;
    parent_job_->SetArrayParentStarted(true);
  }
  parent_job_->SetArrayMaterializationComplete(true);
  parent_job_->SetStatus(status);
  parent_job_->SetExitCode(exit_code);
  parent_job_->SetEndTime(end_time);
}

bool ArrayMeta::MarkParentStarted(absl::Time start_time) {
  if (parent_job_ == nullptr || parent_start_event_triggered_) return false;
  parent_job_->SetStartTime(start_time);
  // Parent's stored Status stays Pending while running; the "Running"
  // representation is synthesized at query time from
  // parent_start_event_triggered_ (mirrored to JobInCtld::array_parent_started)
  // to keep the pending map free of Running-status entries.
  parent_job_->TriggerDependencyEvents(crane::grpc::DependencyType::AFTER,
                                       start_time);
  parent_start_event_triggered_ = true;
  parent_job_->SetArrayParentStarted(true);
  return true;
}

bool ArrayMeta::CanSpawnMore(absl::Time now, std::string* reason) const {
  if (parent_job_ == nullptr) {
    if (reason != nullptr) *reason = "";
    return false;
  }
  if (parent_job_->ArrayMaterializationComplete()) {
    if (reason != nullptr) *reason = "ArrayMaterializationComplete";
    return false;
  }
  if (parent_job_->CancelRequested()) {
    if (reason != nullptr) *reason = "Cancelled";
    return false;
  }
  if (parent_job_->deadline_time <= now) {
    if (reason != nullptr) *reason = "Deadline";
    return false;
  }
  if (!NextMaterializableTaskId().has_value()) {
    if (reason != nullptr) *reason = "";
    return false;
  }

  size_t limit =
      ArrayUtil::EffectiveRunLimit(parent_job_->JobToCtld().array_spec());
  if (RunningChildCount() >= limit) {
    if (reason != nullptr) *reason = "ArrayTaskLimit";
    return false;
  }
  return true;
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

bool ArrayManager::ArrayMaterializationComplete_(const JobInCtld& parent) {
  return parent.ArrayMaterializationComplete();
}

// True iff the parent's persisted state shows it was finalized via
// FinishForTerminalStatus before a previous crash. Used by RegisterParent
// to rebuild the transient terminal_status_override_ /
// parent_start_event_triggered_ cache on startup.
bool ArrayManager::WasFinalizedBeforeRecovery_(const JobInCtld& parent) {
  return parent.ArrayMaterializationComplete() &&
         IsFinishedStepStatus(parent.Status());
}

std::pair<crane::grpc::JobStatus, uint32_t> ArrayManager::BuildAggregateResult_(
    const ArrayMeta& meta, const std::unordered_set<JobInCtld*>& final_jobs) {
  if (auto override_status = meta.TerminalStatusOverride();
      override_status.has_value()) {
    return *override_status;
  }

  job_id_t array_job_id = meta.Parent().JobId();
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

// The array parent owns submit-slot accounting for the effective concurrent
// task limit. Child materialization owes no independent QoS submit check.
// BuildChild copies effective parent fields into the child, and this mirrors
// them into the child proto before persistence.
void ArrayManager::InheritChildAttributesFromParent_(JobInCtld& child) {
  child.MutableJobToCtld()->set_qos(child.qos);
  child.MutableJobToCtld()->mutable_time_limit()->set_seconds(
      ToInt64Seconds(child.time_limit));
}

const absl::flat_hash_set<job_id_t>& ArrayManager::RunningChildJobIds_(
    const ArrayMeta& meta) {
  return meta.RunningChildJobIds();
}

// ----------------------------------------------------------------------------
// ArrayManager: lookup + per-meta projections
// ----------------------------------------------------------------------------

ArrayMeta* ArrayManager::FindMeta_(job_id_t array_job_id) {
  auto it = m_metas_.find(array_job_id);
  return it == m_metas_.end() ? nullptr : it->second.get();
}

const ArrayMeta* ArrayManager::FindMeta_(job_id_t array_job_id) const {
  auto it = m_metas_.find(array_job_id);
  return it == m_metas_.end() ? nullptr : it->second.get();
}

// ----------------------------------------------------------------------------
// ArrayManager: registration + recovery
// ----------------------------------------------------------------------------

void ArrayManager::RegisterParent(JobInCtld* parent) {
  CRANE_ASSERT(parent != nullptr && parent->IsArrayParent());

  if (parent->qos.empty()) {
    parent->qos = parent->JobToCtld().qos();
  }

  job_id_t parent_id = parent->JobId();
  auto meta = std::unique_ptr<ArrayMeta>(new ArrayMeta(parent));

  // Recovery path: FinishForTerminalStatus persists Status/ExitCode/EndTime
  // and ArrayMaterializationComplete=true, but terminal_status_override_ and
  // parent_start_event_triggered_ are transient. Rebuild them so a later
  // BuildAggregateResult_ doesn't overwrite the persisted terminal status
  // with a child-aggregate.
  if (WasFinalizedBeforeRecovery_(*parent)) {
    meta->terminal_status_override_ =
        std::pair{parent->Status(), parent->ExitCode()};
    meta->parent_start_event_triggered_ = true;
    parent->SetArrayParentStarted(true);
  }

  m_metas_[parent_id] = std::move(meta);
}

CraneExpected<void> ArrayManager::BindRecoveredChildrenForParent(
    job_id_t array_job_id, const std::vector<JobInCtld*>& children) {
  auto* meta = FindMeta_(array_job_id);
  if (meta == nullptr || meta->ParentPtr() == nullptr) {
    return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  }

  for (JobInCtld* child : children) {
    if (child == nullptr || !child->IsArrayChild() ||
        !child->ArrayJobId().has_value() ||
        child->ArrayJobId().value() != array_job_id ||
        !child->ArrayTaskId().has_value() ||
        !ArrayUtil::ContainsTaskId(meta->Parent().JobToCtld().array_spec(),
                                   child->ArrayTaskId().value())) {
      return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
    }
  }

  bool materialization_complete = false;
  for (JobInCtld* child : children) {
    materialization_complete |= meta->TrackMaterialized(child);
    meta->OnChildRunning(child->JobId());
    meta->MarkParentStarted(child->StartTime());
  }

  if (materialization_complete) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
            0, meta->Parent().JobDbId(), meta->Parent().RuntimeAttr())) {
      CRANE_ERROR("Failed to persist recovered array parent #{} runtime attr.",
                  array_job_id);
    }
  }
  return {};
}

CraneExpected<void> ArrayManager::TrackRecoveredTerminalChild(
    JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild() ||
      !child->ArrayJobId().has_value()) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  auto task_id = child->ArrayTaskId();
  if (!task_id.has_value()) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  return TrackRecoveredTerminalChild(child->ArrayJobId().value(), *task_id,
                                     child->JobId());
}

CraneExpected<void> ArrayManager::TrackRecoveredTerminalChild(
    job_id_t array_job_id, array_task_id_t task_id, job_id_t child_job_id) {
  auto* meta = FindMeta_(array_job_id);
  if (meta == nullptr || meta->ParentPtr() == nullptr) {
    return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  }
  if (!ArrayUtil::ContainsTaskId(meta->Parent().JobToCtld().array_spec(),
                                 task_id)) {
    return std::unexpected(CraneErrCode::ERR_INVALID_PARAM);
  }

  if (meta->TrackMaterialized(task_id, child_job_id)) {
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
            0, meta->Parent().JobDbId(), meta->Parent().RuntimeAttr())) {
      CRANE_ERROR("Failed to persist recovered array parent #{} runtime attr.",
                  array_job_id);
    }
  }
  return {};
}

void ArrayManager::RecoverAccountingForRegisteredParents() {
  for (const auto& [array_job_id, meta] : m_metas_) {
    if (meta == nullptr || meta->ParentPtr() == nullptr) continue;
    JobInCtld& parent = meta->MutableParent();
    uint32_t reserve_count =
        ArrayUtil::EffectiveRunLimit(parent.JobToCtld().array_spec());
    g_account_meta_container->MallocQosSubmitResource(parent, reserve_count);
    g_account_meta_container->UserAddJob(parent.Username(), reserve_count);
  }
}

// ----------------------------------------------------------------------------
// ArrayManager: materialization
// ----------------------------------------------------------------------------

CraneExpected<void> ArrayManager::AdmitChildNoLock_(ArrayMeta& meta,
                                                    JobInCtld& child) {
  CRANE_ASSERT(meta.ParentPtr() != nullptr);
  CRANE_ASSERT(child.IsArrayChild() && child.ArrayJobId().has_value() &&
               child.ArrayJobId().value() == meta.Parent().JobId());
  auto& parent = meta.MutableParent();
  InheritChildAttributesFromParent_(child);

  auto child_task_id = child.ArrayTaskId();
  bool mark_materialization_complete =
      child_task_id.has_value() &&
      meta.WillCompleteMaterializationAfter(child_task_id.value());

  std::vector<EmbeddedDbClient::ExtraVariableWrite> extra_writes;
  if (mark_materialization_complete) {
    parent.SetArrayMaterializationComplete(true);
    extra_writes.push_back({parent.JobDbId(), &parent.RuntimeAttr()});
  }

  std::vector<JobInCtld*> one{&child};
  // This embedded-DB API is used here only to allocate/persist
  // job_id/job_db_id for the materialized child. The child is never inserted
  // into the scheduler pending map; ScheduleThread_ immediately applies the
  // allocation and moves it into the running map.
  if (!g_embedded_db_client->AppendJobsToPendingAndAdvanceJobIds(
          one, extra_writes)) {
    CRANE_ERROR("Failed to persist array child for parent job #{}.",
                parent.JobId());
    if (mark_materialization_complete) {
      parent.SetArrayMaterializationComplete(false);
    }
    return std::unexpected(CraneErrCode::ERR_DB_INSERT_FAILED);
  }

  meta.TrackMaterialized(&child);
  return {};
}

// ----------------------------------------------------------------------------
// ArrayManager: finalize
// ----------------------------------------------------------------------------

void ArrayManager::TryFinalizeParentNoLock_(
    ArrayMeta* meta, std::vector<FinalizedArrayParent>* final_parents,
    const std::unordered_set<JobInCtld*>& final_jobs) {
  if (meta == nullptr || meta->ParentPtr() == nullptr) return;

  if (!ArrayMaterializationComplete_(meta->Parent()) ||
      meta->RunningChildCount() != 0) {
    return;
  }

  auto& parent = meta->MutableParent();
  job_id_t array_job_id = parent.JobId();
  auto [final_status, final_exit_code] =
      BuildAggregateResult_(*meta, final_jobs);
  parent.SetStatus(final_status);
  parent.SetExitCode(final_exit_code);
  absl::Time end_time = absl::Now();
  if (!meta->ParentStartEventTriggered() &&
      final_status == crane::grpc::JobStatus::Cancelled) {
    parent.SetStartTime(end_time);
    parent.TriggerDependencyEvents(crane::grpc::DependencyType::AFTER,
                                   end_time);
  }
  parent.SetEndTime(end_time);

  parent.TriggerTerminalDependencyEvents(end_time);

  // TODO: Revisit array submit-slot accounting. Currently the parent reserves
  // EffectiveRunLimit(array_spec) submit slots and releases them at parent
  // finalization. If we switch to per-child submit accounting later, release
  // one slot when each child reaches a terminal state.
  g_account_meta_container->FreeQosSubmitResource(
      parent, ArrayUtil::EffectiveRunLimit(parent.JobToCtld().array_spec()));

  auto meta_it = m_metas_.find(array_job_id);
  if (meta_it == m_metas_.end()) return;

  if (final_parents != nullptr) {
    final_parents->push_back(FinalizedArrayParent{
        /*parent_job=*/nullptr, /*array_job_id=*/array_job_id});
  }
  m_metas_.erase(meta_it);
}

ArrayManager::FinishedParentResult ArrayManager::FinishParentWithStatus(
    job_id_t parent_job_id, crane::grpc::JobStatus status, uint32_t exit_code,
    absl::Time end_time) {
  FinishedParentResult result;
  auto meta_it = m_metas_.find(parent_job_id);
  if (meta_it == m_metas_.end()) return result;

  ArrayMeta* meta = meta_it->second.get();
  if (meta == nullptr || meta->ParentPtr() == nullptr) {
    return result;
  }
  result.parent_found = true;
  const auto& running_child_job_ids = RunningChildJobIds_(*meta);
  result.running_child_job_ids.assign(running_child_job_ids.begin(),
                                      running_child_job_ids.end());

  meta->FinishForTerminalStatus(status, exit_code, end_time);
  if (!g_embedded_db_client->UpdateRuntimeAttrOfJobIfExists(
          0, meta->Parent().JobDbId(), meta->Parent().RuntimeAttr())) {
    CRANE_ERROR("Failed to persist finished array parent #{} runtime attr.",
                parent_job_id);
  }

  if (meta->RunningChildCount() != 0) {
    return result;
  }

  auto& parent = meta->MutableParent();
  parent.TriggerTerminalDependencyEvents(end_time);

  g_account_meta_container->FreeQosSubmitResource(
      parent, ArrayUtil::EffectiveRunLimit(parent.JobToCtld().array_spec()));

  result.final_parents.push_back(FinalizedArrayParent{
      /*parent_job=*/nullptr, /*array_job_id=*/parent_job_id});
  m_metas_.erase(meta_it);
  return result;
}

std::vector<ArrayManager::FinalizedArrayParent>
ArrayManager::TryFinalizeParentIfComplete(job_id_t parent_job_id) {
  std::vector<FinalizedArrayParent> final_parents;
  auto* meta = FindMeta_(parent_job_id);
  if (meta == nullptr) return final_parents;
  TryFinalizeParentNoLock_(meta, &final_parents);
  return final_parents;
}

std::vector<job_id_t> ArrayManager::RunningChildJobIdsForParent(
    job_id_t parent_job_id) const {
  const auto* meta = FindMeta_(parent_job_id);
  if (meta == nullptr) return {};
  const auto& running_child_job_ids = RunningChildJobIds_(*meta);
  return {running_child_job_ids.begin(), running_child_job_ids.end()};
}

// ----------------------------------------------------------------------------
// ArrayManager: lifecycle
// ----------------------------------------------------------------------------

std::vector<ArrayManager::FinalizedArrayParent> ArrayManager::OnChildTerminal(
    JobInCtld* child, const std::unordered_set<JobInCtld*>& final_jobs) {
  std::vector<FinalizedArrayParent> final_parents;
  CRANE_ASSERT(child != nullptr && child->IsArrayChild() &&
               child->ArrayJobId().has_value());
  auto* meta = FindMeta_(child->ArrayJobId().value());
  if (meta == nullptr) return final_parents;
  meta->OnChildTerminal(child->JobId());
  TryFinalizeParentNoLock_(meta, &final_parents, final_jobs);
  return final_parents;
}

bool ArrayManager::IsRegisteredParent(job_id_t array_job_id) const {
  return FindMeta_(array_job_id) != nullptr;
}

// ----------------------------------------------------------------------------
// ArrayManager: scheduler-loop entry points
// ----------------------------------------------------------------------------

std::vector<ArrayManager::FinalizedArrayParent>
ArrayManager::FinalizeAllCompletedParents() {
  std::vector<FinalizedArrayParent> final_parents;

  // TryFinalizeParentNoLock_ erases from m_metas_, so snapshot the candidate
  // ids first to avoid mutating the map during iteration.
  std::vector<job_id_t> candidates;
  candidates.reserve(m_metas_.size());
  for (const auto& [array_job_id, meta] : m_metas_) {
    if (ArrayMaterializationComplete_(meta->Parent())) {
      candidates.push_back(array_job_id);
    }
  }
  for (job_id_t array_job_id : candidates) {
    TryFinalizeParentNoLock_(FindMeta_(array_job_id), &final_parents);
  }
  return final_parents;
}

ArrayManager::ParentMaterializationDecision
ArrayManager::PrepareParentForMaterialization(const JobInCtld& parent,
                                              absl::Time now) {
  ParentMaterializationDecision decision;
  const auto* meta = FindMeta_(parent.JobId());
  if (meta == nullptr) {
    decision.pending_reason = "";
    return decision;
  }

  std::string reason;
  if (meta->CanSpawnMore(now, &reason)) {
    decision.can_materialize = true;
  } else {
    decision.pending_reason = std::move(reason);
  }
  return decision;
}

std::unique_ptr<JobInCtld> ArrayManager::MaterializeChildForAllocation(
    JobInCtld* parent, absl::Time now) {
  if (parent == nullptr || !parent->IsArrayParent()) return nullptr;
  auto* meta = FindMeta_(parent->JobId());
  if (meta == nullptr || meta->ParentPtr() == nullptr) return nullptr;
  if (!meta->CanSpawnMore(now, nullptr)) return nullptr;

  auto next_task_id = meta->NextMaterializableTaskId();
  if (!next_task_id.has_value()) return nullptr;
  auto child = meta->BuildChild(*next_task_id);
  if (child == nullptr) return nullptr;

  auto admit = AdmitChildNoLock_(*meta, *child);
  if (!admit.has_value()) {
    CRANE_ERROR("Failed to admit array child task {} for parent job #{}: {}.",
                child->ArrayTaskId().value_or(0), parent->JobId(),
                CraneErrStr(admit.error()));
    return nullptr;
  }

  // Child skips the pending phase: materialization is only called
  // when NodeSelect has already given the parent an allocation.
  meta->OnChildRunning(child->JobId());
  return child;
}

bool ArrayManager::OnChildStarted(JobInCtld* child) {
  if (child == nullptr || !child->IsArrayChild() ||
      !child->ArrayJobId().has_value()) {
    return false;
  }
  auto* meta = FindMeta_(child->ArrayJobId().value());
  if (meta == nullptr || meta->ParentPtr() == nullptr) return false;
  return meta->MarkParentStarted(child->StartTime());
}

// ----------------------------------------------------------------------------
// ArrayManager: resolve
// ----------------------------------------------------------------------------

namespace {

// Empty `steps` is the "all steps" sentinel: once a key has been inserted
// with an empty set, later non-empty inputs must not narrow it.
void MergeJobSteps_(
    std::unordered_map<job_id_t, std::unordered_set<step_id_t>>& job_steps,
    job_id_t job_id, const std::unordered_set<step_id_t>& steps) {
  auto [it, inserted] = job_steps.try_emplace(job_id);
  if (steps.empty()) {
    it->second.clear();
  } else if (inserted || !it->second.empty()) {
    it->second.insert(steps.begin(), steps.end());
  }
}

}  // namespace

void ArrayManager::MergeResolved_(ResolvedJobIdSelectors& dst,
                                  ResolvedJobIdSelectors&& src) {
  for (auto& [job_id, steps] : src.job_steps) {
    MergeJobSteps_(dst.job_steps, job_id, steps);
  }
  for (auto& [child_id, selector] : src.array_selector_by_child_job_id) {
    dst.array_selector_by_child_job_id.try_emplace(child_id, selector);
  }
  for (auto& unresolved : src.unresolved_selectors) {
    dst.unresolved_selectors.push_back(std::move(unresolved));
  }
}

ArrayManager::ResolvedJobIdSelectors ArrayManager::ResolveJobIdSelectors(
    const google::protobuf::RepeatedPtrField<crane::grpc::JobIdSelector>&
        selectors,
    bool expand_array_parents) const {
  ResolvedJobIdSelectors result;
  for (const auto& selector : selectors) {
    MergeResolved_(result,
                   ResolveJobIdSelector(selector, expand_array_parents));
  }
  return result;
}

ArrayManager::ResolvedJobIdSelectors ArrayManager::ResolveJobIdSelector(
    const crane::grpc::JobIdSelector& selector,
    bool expand_array_parents) const {
  ResolvedJobIdSelectors result;
  std::unordered_set<step_id_t> steps(selector.steps().begin(),
                                      selector.steps().end());
  job_id_t job_id = selector.job_id();

  if (!selector.has_array_task_id()) {
    const auto* meta = FindMeta_(job_id);
    if (meta != nullptr && meta->ParentPtr() != nullptr) {
      // Targeting an array parent with explicit step ids is meaningless: array
      // parents have no steps of their own. Surface it as unresolved.
      if (!steps.empty()) {
        result.unresolved_selectors.push_back({
            .job_id = job_id,
            .array_task_id = 0,
            .reason = "Array parent has no steps",
        });
        return result;
      }
      if (expand_array_parents) {
        MergeJobSteps_(result.job_steps, job_id, steps);
        for (const auto& [_, child_id] : meta->child_job_id_by_task_id_) {
          MergeJobSteps_(result.job_steps, child_id, steps);
        }
        return result;
      }
    }

    MergeJobSteps_(result.job_steps, job_id, steps);
    return result;
  }

  const auto* meta = FindMeta_(job_id);
  if (meta == nullptr || meta->ParentPtr() == nullptr) {
    result.unresolved_selectors.push_back({
        .job_id = job_id,
        .array_task_id = selector.array_task_id(),
        .reason = "No such job",
    });
    return result;
  }

  array_task_id_t array_task_id = selector.array_task_id();
  const auto& array_spec = meta->Parent().JobToCtld().array_spec();
  if (!ArrayUtil::ContainsTaskId(array_spec, array_task_id)) {
    result.unresolved_selectors.push_back({
        .job_id = job_id,
        .array_task_id = array_task_id,
        .reason = "No such job",
    });
    return result;
  }

  auto child_job_id = meta->ChildJobIdOfTask(array_task_id);
  if (!child_job_id.has_value()) {
    result.unresolved_selectors.push_back({
        .job_id = job_id,
        .array_task_id = array_task_id,
        .reason = "No such job",
    });
    return result;
  }

  MergeJobSteps_(result.job_steps, *child_job_id, steps);
  result.array_selector_by_child_job_id.try_emplace(*child_job_id, job_id,
                                                    array_task_id);
  return result;
}

// ----------------------------------------------------------------------------
// ArrayManager: persistence / plugin static helpers
// ----------------------------------------------------------------------------

crane::grpc::JobInEmbeddedDb ArrayManager::BuildFinalParentRecord_(
    const JobInCtld& array_parent) {
  crane::grpc::JobInEmbeddedDb record;
  *record.mutable_job_to_ctld() = array_parent.JobToCtld();
  *record.mutable_runtime_attr() = array_parent.RuntimeAttr();
  record.mutable_runtime_attr()->set_job_id(array_parent.JobId());
  record.mutable_runtime_attr()->set_job_db_id(array_parent.JobDbId());
  return record;
}

void ArrayManager::CallPluginHookForFinalParents_(
    const std::vector<FinalizedArrayParent>& parents) {
  if (!g_config.Plugin.Enabled || parents.empty()) return;

  std::vector<crane::grpc::JobInfo> jobs_post_comp;
  jobs_post_comp.reserve(parents.size());
  for (const auto& parent : parents) {
    if (parent.parent_job == nullptr) continue;
    crane::grpc::JobInfo t;
    parent.parent_job->SetFieldsOfJobInfo(&t);
    jobs_post_comp.emplace_back(std::move(t));
  }
  g_plugin_client->EndHookAsync(std::move(jobs_post_comp));
}

void ArrayManager::PersistAndTransferParentsToMongodb_(
    const std::vector<FinalizedArrayParent>& parents) {
  if (parents.empty()) return;

  txn_id_t txn_id;
  g_embedded_db_client->BeginVariableDbTransaction(&txn_id);
  for (const auto& parent : parents) {
    if (parent.parent_job == nullptr) continue;
    if (!g_embedded_db_client->UpdateRuntimeAttrOfJob(
            txn_id, parent.parent_job->JobDbId(),
            parent.parent_job->RuntimeAttr()))
      CRANE_ERROR(
          "Failed to call UpdateRuntimeAttrOfJob() for array parent #{}",
          parent.parent_job->JobId());
  }
  g_embedded_db_client->CommitVariableDbTransaction(txn_id);

  for (const auto& parent : parents) {
    if (parent.parent_job == nullptr) continue;
    auto record = BuildFinalParentRecord_(*parent.parent_job);
    if (!g_db_client->InsertRecoveredJob(record))
      CRANE_ERROR("Failed to insert array parent #{} into MongoDB",
                  parent.parent_job->JobId());
  }

  std::unordered_map<job_id_t, job_db_id_t> db_ids;
  for (const auto& parent : parents) {
    if (parent.parent_job == nullptr) continue;
    db_ids[parent.parent_job->JobId()] = parent.parent_job->JobDbId();
  }
  if (!g_embedded_db_client->PurgeEndedJobs(db_ids))
    CRANE_ERROR(
        "Failed to call g_embedded_db_client->PurgeEndedJobs() "
        "for final array parents");
}

void ArrayManager::ProcessFinalParents(
    const std::vector<FinalizedArrayParent>& parents) {
  PersistAndTransferParentsToMongodb_(parents);
  CallPluginHookForFinalParents_(parents);
}

}  // namespace Ctld
