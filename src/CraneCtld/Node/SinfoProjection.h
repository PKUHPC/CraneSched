// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include "CtldPublicDefs.h"
#include "Node/NodeDefs.h"

namespace Ctld {

inline void RecordSinfoDownReason(CranedMeta& node, uint32_t uid,
                                 absl::Time when) {
  if (node.power_state != crane::grpc::CRANE_POWER_ACTIVE &&
      node.power_state != crane::grpc::CRANE_POWER_IDLE)
    return;
  std::string reason = node.sinfo_down_reason ? node.sinfo_down_reason->text
                                            : node.state_reason;
  if (reason.empty())
    reason = "Not responding";
  else if (reason.find(" : Not responding") == std::string::npos)
    reason += " : Not responding";
  node.sinfo_down_reason = SinfoDownReason{std::move(reason), uid, when};
}

// Crane configures logical CPUs and sockets. Its scheduling units have no
// separate SMT policy, so project them as single-thread cores. In particular,
// host hardware outside the configured CPU capacity is not part of this view.
inline std::optional<crane::grpc::NodeTopoInfo> SinfoSchedulingTopology(
    uint32_t cpus, uint32_t sockets) {
  if (!cpus || !sockets || cpus % sockets) return std::nullopt;
  crane::grpc::NodeTopoInfo topology;
  topology.set_boards(1);
  topology.set_sockets(sockets);
  topology.set_cores_per_socket(cpus / sockets);
  topology.set_threads_per_core(1);
  topology.set_total_cpus(cpus);
  return topology;
}

// A present entry means the allocation is ending. Its nodes are those that
// still own cleanup work; an empty set is different from an active allocation.
using SinfoCompletingJobs =
    std::unordered_map<job_id_t, std::unordered_set<CranedId>>;

inline std::optional<std::unordered_set<CranedId>> SinfoCleanupNodes(
    JobInCtld& job) {
  const auto completing = [](const StepInCtld* step) {
    return step && step->Status() == crane::grpc::Completing;
  };
  if (!completing(job.PrimaryStep()) && !completing(job.DaemonStep()) &&
      !IsFinishedStepStatus(job.PrimaryStepStatus()))
    return std::nullopt;

  std::unordered_set<CranedId> nodes;
  const auto append = [&nodes](const StepInCtld* step) {
    if (step && !IsFinishedStepStatus(step->Status()))
      nodes.insert(step->RunningNodes().begin(), step->RunningNodes().end());
  };
  append(job.DaemonStep());
  append(job.PrimaryStep());
  for (const auto& [id, step] : job.Steps()) append(step.get());
  return nodes;
}

struct SinfoAllocation {
  ResourceInNodeV3 active;
  bool completing{false};
};

inline SinfoAllocation ProjectSinfoAllocation(
    const CranedMeta& meta, const SinfoCompletingJobs& completing_jobs) {
  SinfoAllocation result;
  for (const auto& [id, resources] : meta.rn_job_res_map) {
    const auto cleanup = completing_jobs.find(id);
    if (cleanup == completing_jobs.end()) {
      result.active += resources;
    } else if (cleanup->second.contains(meta.static_meta.hostname)) {
      result.completing = true;
    }
  }
  return result;
}

}  // namespace Ctld
