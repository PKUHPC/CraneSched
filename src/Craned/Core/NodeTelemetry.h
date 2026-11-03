// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <sys/sysinfo.h>

#include <cstdint>
#include <optional>

namespace Craned {

struct NodeTelemetry {
  uint32_t cpu_load;
  uint64_t free_memory_mb;
};

inline std::optional<NodeTelemetry> ReadNodeTelemetry() {
  struct sysinfo sample{};
  if (sysinfo(&sample) != 0) return std::nullopt;
  // The sinfo contract uses the five-minute load in hundredths and Linux
  // MemFree, not available memory or unallocated scheduler capacity.
  const float load =
      static_cast<float>(sample.loads[1]) / (1U << SI_LOAD_SHIFT);
  return NodeTelemetry{.cpu_load = static_cast<uint32_t>(load * 100.0),
                       .free_memory_mb = uint64_t{sample.freeram} *
                                         sample.mem_unit / (1U << 20)};
}

}  // namespace Craned
