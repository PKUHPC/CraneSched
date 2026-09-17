#include "GroupResolver.h"

#include <grp.h>
#include <pwd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <format>
#include <limits>
#include <ranges>
#include <unordered_set>

#include "crane/PasswordEntry.h"

namespace Craned {

namespace {

constexpr size_t kMaxGroups = 256;

std::expected<std::vector<gid_t>, std::string> ActualGroups_(uid_t uid) {
  PasswordEntry pwd(uid);
  if (!pwd.Valid()) {
    return std::unexpected("user lookup failed");
  }

  int group_count = 16;
  std::vector<gid_t> groups(static_cast<size_t>(group_count));
  for (;;) {
    errno = 0;
    int requested_count = group_count;
    int rc = getgrouplist(pwd.Username().c_str(), pwd.Gid(), groups.data(),
                          &requested_count);
    if (rc >= 0) {
      if (requested_count < 0 ||
          static_cast<size_t>(requested_count) > kMaxGroups) {
        return std::unexpected("user group list exceeds maximum size");
      }
      groups.resize(static_cast<size_t>(requested_count));
      return groups;
    }

    if (requested_count <= group_count ||
        static_cast<size_t>(requested_count) > kMaxGroups) {
      return std::unexpected(
          std::format("getgrouplist failed: {}", std::strerror(errno)));
    }
    group_count = requested_count;
    groups.resize(static_cast<size_t>(group_count));
  }
}

}  // namespace

std::expected<ResolvedGroups, std::string> GroupResolver::Resolve(
    uid_t uid, const std::vector<uint32_t>& requested) {
  auto actual_expt = ActualGroups_(uid);
  if (!actual_expt) return std::unexpected(actual_expt.error());
  return Reconcile(requested, *actual_expt);
}

std::expected<ResolvedGroups, std::string> GroupResolver::ResolveStep(
    const crane::grpc::StepToD& step) {
  return ResolveStep(
      step, std::vector<uint32_t>(step.gids().begin(), step.gids().end()));
}

std::expected<ResolvedGroups, std::string> GroupResolver::ResolveStep(
    const crane::grpc::StepToD& step, const std::vector<uint32_t>& requested) {
  return Resolve(ExecutionUid(step), requested);
}

uid_t GroupResolver::ExecutionUid(const crane::grpc::StepToD& step) noexcept {
  if (step.has_pod_meta()) return step.pod_meta().run_as_user();
  return step.uid();
}

std::expected<ResolvedGroups, std::string> GroupResolver::Reconcile(
    const std::vector<uint32_t>& requested,
    const std::vector<gid_t>& actual_groups) {
  if (requested.empty()) {
    return std::unexpected("requested group list is empty");
  }
  if (requested.size() > kMaxGroups) {
    return std::unexpected("requested group list exceeds maximum size");
  }

  std::unordered_set<gid_t> actual(actual_groups.begin(), actual_groups.end());
  const gid_t primary = static_cast<gid_t>(requested.front());
  if (!actual.contains(primary)) {
    return std::unexpected(std::format(
        "PRIMARY_GID_NOT_AUTHORIZED: gid {} is absent from node groups",
        primary));
  }

  ResolvedGroups result;
  result.gids.reserve(requested.size());
  std::unordered_set<gid_t> seen;
  seen.reserve(requested.size());
  result.gids.push_back(primary);
  seen.insert(primary);

  std::unordered_set<gid_t> requested_set;
  requested_set.reserve(requested.size());
  std::unordered_set<gid_t> requested_supplementary;
  requested_supplementary.reserve(requested.size());
  for (size_t index = 1; index < requested.size(); ++index) {
    requested_supplementary.insert(static_cast<gid_t>(requested[index]));
  }
  requested_supplementary.erase(primary);
  result.diagnostics.requested_supplementary_count =
      requested_supplementary.size();

  for (uint32_t raw_gid : requested) {
    gid_t gid = static_cast<gid_t>(raw_gid);
    requested_set.insert(gid);
    if (seen.contains(gid)) continue;
    if (actual.contains(gid)) {
      result.gids.push_back(gid);
      seen.insert(gid);
    }
  }

  result.diagnostics.accepted_supplementary_count = result.gids.size() - 1;
  result.diagnostics.dropped_supplementary_count =
      std::ranges::count_if(requested_supplementary,
                            [&](gid_t gid) { return !actual.contains(gid); });
  result.diagnostics.backend_only_count = std::ranges::count_if(
      actual, [&](gid_t gid) { return !requested_set.contains(gid); });
  return result;
}

}  // namespace Craned
