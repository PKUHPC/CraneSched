#pragma once

#include <sys/types.h>

#include <cstddef>
#include <expected>
#include <string>
#include <vector>

#include "PublicDefs.pb.h"

namespace Craned {

struct GroupResolutionDiagnostics {
  size_t requested_supplementary_count{0};
  size_t accepted_supplementary_count{0};
  size_t dropped_supplementary_count{0};
  size_t backend_only_count{0};

  bool HasMismatch() const noexcept {
    return dropped_supplementary_count != 0 || backend_only_count != 0;
  }
};

struct ResolvedGroups {
  std::vector<gid_t> gids;
  GroupResolutionDiagnostics diagnostics;
};

class GroupResolver {
 public:
  // Resolve the requested ordered group list against the node's NSS database.
  // The caller must invoke this before fork; this function may call NSS.
  static std::expected<ResolvedGroups, std::string> Resolve(
      uid_t uid, const std::vector<uint32_t>& requested);

  // Resolve the execution identity carried by a step. Container steps use
  // the pod run-as UID; native steps use the submitting UID.
  static std::expected<ResolvedGroups, std::string> ResolveStep(
      const crane::grpc::StepToD& step);
  static std::expected<ResolvedGroups, std::string> ResolveStep(
      const crane::grpc::StepToD& step, const std::vector<uint32_t>& requested);
  static uid_t ExecutionUid(const crane::grpc::StepToD& step) noexcept;

  // Pure reconciliation used by Resolve and unit tests.
  static std::expected<ResolvedGroups, std::string> Reconcile(
      const std::vector<uint32_t>& requested, const std::vector<gid_t>& actual);
};

}  // namespace Craned
