// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "JobQueryPrivacy.h"

namespace Ctld {
namespace {

// Copy known fields explicitly, including nested messages. New/unknown fields
// must not become public simply because they were added to a shared schema.
template <typename Time>
void CopyTime(const Time& source, Time* target) {
  target->set_seconds(source.seconds());
  target->set_nanos(source.nanos());
}

void CopyResource(const crane::grpc::ResourceView& source,
                  crane::grpc::ResourceView* target) {
  target->set_cpu_count(source.cpu_count());
  target->set_memory_bytes(source.memory_bytes());
  target->set_memory_sw_bytes(source.memory_sw_bytes());
  if (source.has_gres_map()) {
    auto* devices = target->mutable_gres_map()->mutable_name_gres_map();
    for (const auto& [name, count] : source.gres_map().name_gres_map()) {
      auto& device = (*devices)[name];
      device.set_total(count.total());
      *device.mutable_specified() = count.specified();
    }
  }
}

crane::grpc::StepInfo ProjectStep(const crane::grpc::StepInfo& source,
                                  bool details) {
  crane::grpc::StepInfo result;
  if (details) {
    result = source;
    result.set_is_container(source.has_container_meta());
    if (result.has_container_meta() && result.container_meta().has_image())
      result.mutable_container_meta()->mutable_image()->clear_password();
    result.DiscardUnknownFields();
    result.set_query_visibility(crane::grpc::QUERY_VISIBILITY_DETAILS);
    return result;
  }

  result.set_type(source.type());
  result.set_is_container(source.has_container_meta());
  result.set_step_type(source.step_type());
  result.set_job_id(source.job_id());
  result.set_step_id(source.step_id());
  result.set_name(source.name());
  result.set_uid(source.uid());
  *result.mutable_gid() = source.gid();
  if (source.has_time_limit())
    CopyTime(source.time_limit(), result.mutable_time_limit());
  if (source.has_start_time())
    CopyTime(source.start_time(), result.mutable_start_time());
  if (source.has_end_time())
    CopyTime(source.end_time(), result.mutable_end_time());
  if (source.has_submit_time())
    CopyTime(source.submit_time(), result.mutable_submit_time());
  if (source.has_elapsed_time())
    CopyTime(source.elapsed_time(), result.mutable_elapsed_time());
  if (source.has_deadline_time())
    CopyTime(source.deadline_time(), result.mutable_deadline_time());
  result.set_node_num(source.node_num());
  result.set_ntasks(source.ntasks());
  if (source.has_req_total_res_view())
    CopyResource(source.req_total_res_view(),
                 result.mutable_req_total_res_view());
  if (source.has_allocated_res_view())
    CopyResource(source.allocated_res_view(),
                 result.mutable_allocated_res_view());
  *result.mutable_req_nodes() = source.req_nodes();
  *result.mutable_exclude_nodes() = source.exclude_nodes();
  result.set_held(source.held());
  result.set_status(source.status());
  result.set_exit_code(source.exit_code());
  result.set_craned_list(source.craned_list());
  *result.mutable_execution_node() = source.execution_node();
  result.set_query_visibility(crane::grpc::QUERY_VISIBILITY_PUBLIC);
  return result;
}

}  // namespace

crane::grpc::JobInfo ProjectJobForQuery(
    const crane::grpc::JobInfo& source,
    std::optional<uint32_t> authenticated_uid, bool is_admin) {
  const bool details = authenticated_uid.has_value() &&
                       (is_admin || *authenticated_uid == source.uid());
  crane::grpc::JobInfo result;
  if (details) {
    result = source;
    result.clear_step_info_list();
    result.DiscardUnknownFields();
  } else {
    result.set_type(source.type());
    result.set_job_id(source.job_id());
    result.set_name(source.name());
    result.set_partition(source.partition());
    result.set_uid(source.uid());
    result.set_gid(source.gid());
    result.set_username(source.username());
    result.set_account(source.account());
    result.set_qos(source.qos());
    result.set_reservation(source.reservation());
    result.set_wckey(source.wckey());
    if (source.has_time_limit())
      CopyTime(source.time_limit(), result.mutable_time_limit());
    if (source.has_start_time())
      CopyTime(source.start_time(), result.mutable_start_time());
    if (source.has_end_time())
      CopyTime(source.end_time(), result.mutable_end_time());
    if (source.has_submit_time())
      CopyTime(source.submit_time(), result.mutable_submit_time());
    if (source.has_elapsed_time())
      CopyTime(source.elapsed_time(), result.mutable_elapsed_time());
    if (source.has_deadline_time())
      CopyTime(source.deadline_time(), result.mutable_deadline_time());
    result.set_held(source.held());
    result.set_status(source.status());
    result.set_exit_code(source.exit_code());
    result.set_priority(source.priority());
    result.set_requeue_count(source.requeue_count());
    result.set_requeue_if_failed(source.requeue_if_failed());
    result.set_node_num(source.node_num());
    result.set_ntasks(source.ntasks());
    if (source.has_req_total_res_view())
      CopyResource(source.req_total_res_view(),
                   result.mutable_req_total_res_view());
    if (source.has_allocated_res_view())
      CopyResource(source.allocated_res_view(),
                   result.mutable_allocated_res_view());
    *result.mutable_licenses_count() = source.licenses_count();
    *result.mutable_req_nodes() = source.req_nodes();
    *result.mutable_exclude_nodes() = source.exclude_nodes();
    if (source.has_pending_reason())
      result.set_pending_reason(source.pending_reason());
    else if (source.has_craned_list())
      result.set_craned_list(source.craned_list());
    *result.mutable_execution_node() = source.execution_node();
    result.set_exclusive(source.exclusive());
    result.set_submit_hostname(source.submit_hostname());
    if (source.has_array_spec()) {
      const auto& spec = source.array_spec();
      auto* output = result.mutable_array_spec();
      output->set_start(spec.start());
      output->set_end(spec.end());
      if (spec.has_stride()) output->set_stride(spec.stride());
      if (spec.has_max_concurrent())
        output->set_max_concurrent(spec.max_concurrent());
    }
    if (source.has_array_task()) {
      result.mutable_array_task()->set_array_job_id(
          source.array_task().array_job_id());
      result.mutable_array_task()->set_task_id(source.array_task().task_id());
    }
    if (source.has_dependency_status()) {
      const auto& dependency = source.dependency_status();
      auto* output = result.mutable_dependency_status();
      output->set_is_or(dependency.is_or());
      for (const auto& condition : dependency.pending()) {
        auto* pending = output->add_pending();
        pending->set_job_id(condition.job_id());
        pending->set_type(condition.type());
        pending->set_delay_seconds(condition.delay_seconds());
      }
      if (dependency.has_ready_time())
        CopyTime(dependency.ready_time(), output->mutable_ready_time());
      else if (dependency.has_infinite_future())
        output->set_infinite_future(dependency.infinite_future());
      else if (dependency.has_infinite_past())
        output->set_infinite_past(dependency.infinite_past());
    }
  }

  for (const auto& step : source.step_info_list()) {
    const bool step_details =
        details && (is_admin || *authenticated_uid == step.uid());
    *result.add_step_info_list() = ProjectStep(step, step_details);
  }
  result.set_query_visibility(details ? crane::grpc::QUERY_VISIBILITY_DETAILS
                                      : crane::grpc::QUERY_VISIBILITY_PUBLIC);
  return result;
}

}  // namespace Ctld
