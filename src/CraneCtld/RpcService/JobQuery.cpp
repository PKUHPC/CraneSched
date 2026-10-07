// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "JobQuery.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "JobQueryPrivacy.h"

namespace Ctld {

grpc::Status HandleJobQuery(const crane::grpc::QueryJobsInfoRequest& request,
                            crane::grpc::QueryJobsInfoReply* response,
                            const JobQueryDependencies& deps) {
  response->Clear();
  if (!deps.ready)
    return {grpc::StatusCode::UNAVAILABLE, "CraneCtld Server is not ready"};
  if (!request.has_uid())
    return {grpc::StatusCode::INVALID_ARGUMENT, "Caller UID is required"};

  std::optional<uint32_t> authenticated_uid;
  bool is_admin = false;
  // A non-TLS UID is only a claim, including an explicit claim to be root.
  if (deps.tls_enabled) {
    if (auto status = deps.authenticate(request.uid()); !status.ok())
      return status;
    if (auto status = deps.resolve_admin(request.uid(), &is_admin);
        !status.ok())
      return status;
    authenticated_uid = request.uid();
  }

  const size_t num_limit =
      request.num_limit() == 0 ? deps.default_limit : request.num_limit();
  const size_t probe_limit = num_limit + 1;

  auto normalized_request = request;
  for (auto& node : *normalized_request.mutable_filter_nodename_list())
    node = deps.resolve_node_alias(node);

  JobQueryResult job_info_map;
  deps.query_ram(&normalized_request, &job_info_map, probe_limit);

  auto finish = [&] {
    auto* jobs = response->mutable_job_info_list();
    jobs->Reserve(job_info_map.size());
    for (auto& [id, job] : job_info_map) *jobs->Add() = std::move(job);
    std::sort(jobs->begin(), jobs->end(),
              [](const crane::grpc::JobInfo& a, const crane::grpc::JobInfo& b) {
                return a.status() == b.status() ? a.priority() > b.priority()
                                                : a.status() < b.status();
              });
    const bool has_more = jobs->size() > num_limit;
    response->set_has_more(has_more);
    if (has_more) jobs->DeleteSubrange(num_limit, jobs->size() - num_limit);
    // One response exit for both RAM/history paths, after historical step
    // merge.
    for (auto& job : *jobs)
      job = ProjectJobForQuery(job, authenticated_uid, is_admin);
    response->set_ok(true);
    return grpc::Status::OK;
  };

  if (job_info_map.size() >= probe_limit ||
      !normalized_request.option_include_completed_jobs()) {
    if (normalized_request.option_include_completed_jobs() &&
        !deps.query_history_steps(&normalized_request, &job_info_map))
      return grpc::Status::OK;  // Existing business-failure contract: ok=false.
    return finish();
  }

  // Fetch a full probe window: a history record may also be present in RAM.
  if (!deps.query_history(&normalized_request, &job_info_map, probe_limit))
    return grpc::Status::OK;
  return finish();
}

}  // namespace Ctld
