#pragma once

#include <sys/types.h>

#include <algorithm>
#include <expected>
#include <span>
#include <string>

namespace util {

// The first host GID is the effective GID. Container IDs never select a host
// NSS account; userns allows mapped container IDs without extra host groups.
inline std::expected<void, std::string> ValidateContainerIdentity(
    uid_t host_uid, std::span<const gid_t> host_gids, bool userns,
    uid_t container_uid, gid_t container_gid) {
  if (host_gids.empty())
    return std::unexpected("effective group list is empty");

  if (userns) {
    if (std::ranges::any_of(host_gids.subspan(1), [&](gid_t gid) {
          return gid != host_gids.front();
        }))
      return std::unexpected(
          "userns containers do not support supplementary groups; submit "
          "without extra groups or disable userns");
    // Mapping bounds are checked on the execution node after resolving SubIDs.
  } else if (container_uid != host_uid || container_gid != host_gids.front()) {
    return std::unexpected(
        "without userns, container UID/GID must match the submitter's "
        "UID/effective GID");
  }
  return {};
}

}  // namespace util
