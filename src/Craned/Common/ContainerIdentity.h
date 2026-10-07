#pragma once

#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <expected>
#include <span>
#include <string>

#include "PublicDefs.pb.h"
#include "crane/ContainerIdentity.h"
#include "cri/api.pb.h"

namespace Craned {

// Pod and container security contexts share the same identity rules. The
// caller supplies the resolved host identity and configures namespaces first.
template <typename SecurityContext>
std::expected<void, std::string> SetContainerIdentity(
    uid_t host_uid, std::span<const gid_t> host_gids,
    const crane::grpc::PodJobAdditionalMeta& pod_meta, SecurityContext* ctx) {
  auto valid = util::ValidateContainerIdentity(
      host_uid, host_gids, pod_meta.userns(), pod_meta.run_as_user(),
      pod_meta.run_as_group());
  if (!valid) return valid;

  if (pod_meta.userns()) {
    const auto& mappings = ctx->namespace_options().userns_options();
    auto mapped = [](uint32_t id, const auto& ranges) {
      return std::ranges::any_of(ranges, [id](const auto& range) {
        return id >= range.container_id() &&
               uint64_t{id} - range.container_id() < range.length();
      });
    };
    if (!mapped(pod_meta.run_as_user(), mappings.uids()))
      return std::unexpected(
          "container UID is outside the user namespace mapping");
    if (!mapped(pod_meta.run_as_group(), mappings.gids()))
      return std::unexpected(
          "container GID is outside the user namespace mapping");
  }

  ctx->mutable_run_as_user()->set_value(pod_meta.run_as_user());
  ctx->mutable_run_as_group()->set_value(pod_meta.run_as_group());
  ctx->clear_supplemental_groups();
  // Do not add image-defined groups to the validated submission identity.
  ctx->set_supplemental_groups_policy(runtime::v1::Strict);
  if (!pod_meta.userns()) {
    for (gid_t gid : host_gids.subspan(1)) {
      if (gid != host_gids.front()) ctx->add_supplemental_groups(gid);
    }
  }
  return {};
}

}  // namespace Craned
