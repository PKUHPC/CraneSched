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

#pragma once

#include "PreCompiledHeader.h"
// Precompiled header comes first

#include "crane/Network.h"
#include "crane/OS.h"
#include "crane/PublicHeader.h"

namespace Craned::Common {
using EnvMap = std::unordered_map<std::string, std::string>;
constexpr uint32_t kStepRequestCheckIntervalMs = 500;

// Resolve the submitter's host groups before fork; this may call NSS. Pod
// run-as IDs remain independent and never select the host account.
std::expected<std::vector<gid_t>, std::string> ResolveStepGroups(
    const crane::grpc::StepToD& step);
std::expected<std::vector<gid_t>, std::string> ResolveStepGroups(
    const crane::grpc::StepToD& step, const std::vector<uint32_t>& requested);

// Configure namespaces before applying the resolved host and Pod identities.
std::expected<void, std::string> SetContainerIdentity(
    uid_t host_uid, std::span<const gid_t> host_gids,
    const crane::grpc::PodJobAdditionalMeta& pod_meta,
    runtime::v1::LinuxSandboxSecurityContext* ctx);
std::expected<void, std::string> SetContainerIdentity(
    uid_t host_uid, std::span<const gid_t> host_gids,
    const crane::grpc::PodJobAdditionalMeta& pod_meta,
    runtime::v1::LinuxContainerSecurityContext* ctx);

}  // namespace Craned::Common
