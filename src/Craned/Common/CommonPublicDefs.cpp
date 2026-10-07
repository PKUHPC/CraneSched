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

#include "CommonPublicDefs.h"

#include <algorithm>
#include <ranges>

namespace Craned::Common {

namespace {

template <typename SecurityContext>
std::expected<void, std::string> SetContainerIdentity_(
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

}  // namespace

std::expected<std::vector<gid_t>, std::string> ResolveStepGroups(
    const crane::grpc::StepToD& step) {
  return ResolveStepGroups(
      step, std::vector<uint32_t>(step.gids().begin(), step.gids().end()));
}

std::expected<std::vector<gid_t>, std::string> ResolveStepGroups(
    const crane::grpc::StepToD& step, const std::vector<uint32_t>& requested) {
  if (step.has_pod_meta()) {
    const auto& pod = step.pod_meta();
    auto valid =
        util::ValidateContainerIdentity(step.uid(), requested, pod.userns(),
                                        pod.run_as_user(), pod.run_as_group());
    if (!valid) return std::unexpected(valid.error());
  }
  return util::os::ResolveGroups(step.uid(), requested);
}

std::expected<void, std::string> SetContainerIdentity(
    uid_t host_uid, std::span<const gid_t> host_gids,
    const crane::grpc::PodJobAdditionalMeta& pod_meta,
    runtime::v1::LinuxSandboxSecurityContext* ctx) {
  return SetContainerIdentity_(host_uid, host_gids, pod_meta, ctx);
}

std::expected<void, std::string> SetContainerIdentity(
    uid_t host_uid, std::span<const gid_t> host_gids,
    const crane::grpc::PodJobAdditionalMeta& pod_meta,
    runtime::v1::LinuxContainerSecurityContext* ctx) {
  return SetContainerIdentity_(host_uid, host_gids, pod_meta, ctx);
}

}  // namespace Craned::Common
