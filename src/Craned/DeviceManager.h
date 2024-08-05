/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

namespace Craned {

struct BasicDevice {
  // e.g gpu
  std::string name;
  // device type e.g a100
  std::string type;
  std::string path;
  unsigned int major;
  unsigned int minor;
  char op_type;

  BasicDevice(const std::string& device_name, const std::string& device_type,
              const std::string& device_path);
  BasicDevice(const BasicDevice& another) = default;
  virtual ~BasicDevice() = default;
  virtual bool Init();
  operator std::string() const;
  // todo:add virtual function to get device status
};

bool operator==(const BasicDevice& lhs, const BasicDevice& rhs);

class DeviceManager {
 public:
  static std::unique_ptr<BasicDevice> ConstructDevice(
      const std::string& device_name, const std::string& device_type,
      const std::string& device_path);

  static std::optional<std::tuple<unsigned int, unsigned int, char>>
  GetDeviceFileMajorMinorOpType(const std::string& path);

  static std::vector<std::pair<std::string, std::string>>
  GetEnvironmentVariable(
      const crane::grpc::DedicatedResourceInNode& resourceInNode);

  static std::vector<std::unique_ptr<BasicDevice>> GetSystemDeviceNvml();
};

// read only after init
inline std::unordered_map<SlotId, std::unique_ptr<BasicDevice>>
    g_this_node_device;

}  // namespace Craned