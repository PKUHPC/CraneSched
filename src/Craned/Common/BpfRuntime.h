/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <cstdint>
#include <expected>
#include <filesystem>
#include <map>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "../../Misc/BPF/DevicePolicy.h"

namespace Craned::Common {

// Only the device manager assigns indices. Supervisors receive a snapshot of
// these associations; device discovery is not repeated when applying a policy.
using BpfDeviceCatalog = std::map<std::string, std::vector<DeviceKey>>;
using BpfDeviceIndices = std::map<std::string, std::vector<uint32_t>>;
using BpfResult = std::expected<void, std::string>;

class BpfRuntimeInfo {
 public:
  struct Paths {
    std::filesystem::path object = "/usr/local/lib64/bpf/cgroup_dev_bpf.o";
    std::filesystem::path pins = "/sys/fs/bpf/crane_devices_v1";
    std::filesystem::path lock = "/run/lock/crane_bpf_devices.lock";
  };

  BpfRuntimeInfo();
  explicit BpfRuntimeInfo(Paths paths);
  ~BpfRuntimeInfo();
  BpfRuntimeInfo(const BpfRuntimeInfo&) = delete;
  BpfRuntimeInfo& operator=(const BpfRuntimeInfo&) = delete;

  // Craned bootstraps once or reuses the existing program/maps without writing
  // to them. A changed catalog requires an explicit reconfiguration.
  BpfResult Initialize(const BpfDeviceCatalog& catalog);
  // A supervisor only opens existing state and installs Craned's slot indices.
  BpfResult Connect(const BpfDeviceIndices& indices);
  BpfDeviceIndices DeviceIndices() const;
  BpfResult SetDeviceAccess(const std::filesystem::path& cgroup,
                            const std::unordered_set<std::string>& slots,
                            bool read, bool write, bool mknod);
  BpfResult RecoverPolicy(const std::filesystem::path& cgroup);
  // Reserved for a future coordinated device/policy reconfiguration. Ordinary
  // daemon restarts must never take this path or renumber existing devices.
  BpfResult Reconfigure(const BpfDeviceCatalog& catalog);

 private:
  BpfResult Open_();
  BpfResult Create_(const BpfDeviceCatalog& catalog);
  BpfResult Validate_();
  BpfResult ResolveIndices_(const BpfDeviceCatalog& catalog);
  std::expected<bool, std::string> Attached_(int cgroup_fd) const;
  void Close_();

  Paths m_paths_;
  mutable std::mutex m_mutex_;
  int m_program_fd_{-1};
  int m_devices_fd_{-1};
  int m_policies_fd_{-1};
  uint32_t m_program_id_{};
  std::unordered_set<uint32_t> m_indices_;
  BpfDeviceIndices m_device_indices_;
};

}  // namespace Craned::Common
