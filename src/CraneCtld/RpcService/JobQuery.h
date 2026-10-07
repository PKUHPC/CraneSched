// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <grpcpp/support/status.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

#include "protos/Crane.pb.h"

namespace Ctld {

using JobQueryResult = std::unordered_map<uint32_t, crane::grpc::JobInfo>;

// Read-only dependencies of the external job query. Kept separate from the
// daemon globals so the production handler can be tested without a database.
struct JobQueryDependencies {
  bool ready;
  bool tls_enabled;
  size_t default_limit;
  std::function<grpc::Status(uint32_t)> authenticate;
  std::function<grpc::Status(uint32_t, bool*)> resolve_admin;
  std::function<std::string(const std::string&)> resolve_node_alias;
  std::function<void(const crane::grpc::QueryJobsInfoRequest*, JobQueryResult*,
                     size_t)>
      query_ram;
  std::function<bool(const crane::grpc::QueryJobsInfoRequest*, JobQueryResult*,
                     size_t)>
      query_history;
  std::function<bool(const crane::grpc::QueryJobsInfoRequest*, JobQueryResult*)>
      query_history_steps;
};

grpc::Status HandleJobQuery(const crane::grpc::QueryJobsInfoRequest& request,
                            crane::grpc::QueryJobsInfoReply* response,
                            const JobQueryDependencies& deps);

}  // namespace Ctld
