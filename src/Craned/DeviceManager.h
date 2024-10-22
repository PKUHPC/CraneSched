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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

namespace Craned {

struct BasicDevice {
  std::string dev_id;

  // e.g GPU
  std::string name;
  // Device type e.g. A100
  std::string type;

  std::string env_injector;

  struct DeviceMeta {
    std::string path;
    unsigned int major;
    unsigned int minor;
    char op_type;
  };
  std::vector<DeviceMeta> device_metas;

  BasicDevice(const std::string& device_name, const std::string& device_type,
              const std::vector<std::string>& device_path,
              const std::string& env_injector);

  BasicDevice(const BasicDevice& another) = default;

  virtual ~BasicDevice() = default;
  virtual bool Init();

  operator std::string() const;

  // Todo: Add virtual function to get device status
};

class DeviceManager {
 public:
  static std::unique_ptr<BasicDevice> ConstructDevice(
      const std::string& device_name, const std::string& device_type,
      const std::vector<std::string>& device_path,
      const std::string& env_injector);

  static std::optional<std::tuple<unsigned int, unsigned int, char>>
  GetDeviceFileMajorMinorOpType(const std::string& path);

  static std::vector<std::pair<std::string, std::string>>
  GetDevEnvListByResInNode(
      const crane::grpc::DedicatedResourceInNode& res_in_node);

  static std::vector<std::unique_ptr<BasicDevice>> GetSystemDeviceNvml();
};

// read only after init
inline std::unordered_map<SlotId, std::unique_ptr<BasicDevice>>
    g_this_node_device;

}  // namespace Craned