// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <cstdint>
#include <optional>

#include "protos/PublicDefs.pb.h"

namespace Ctld {

// Creates an external query view without modifying scheduler/persisted data.
// A UID may only be supplied after authenticating the caller.
crane::grpc::JobInfo ProjectJobForQuery(
    const crane::grpc::JobInfo& source,
    std::optional<uint32_t> authenticated_uid, bool is_admin);

}  // namespace Ctld
