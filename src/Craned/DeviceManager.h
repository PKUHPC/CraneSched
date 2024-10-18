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

enum DeviceEnvInjector : uint8_t {
  Nvidia = 0,
  Hip,
  Ascend,

  __DeviceEnvInjector_SIZE,
  InvalidInjector
};

constexpr std::array<std::string_view,
                     DeviceEnvInjector::__DeviceEnvInjector_SIZE>
    DeviceEnvInjectorStr = {
        "nvidia",
        "hip",
        "ascend",
};

constexpr std::array<std::string_view,
                     DeviceEnvInjector::__DeviceEnvInjector_SIZE>
    DeviceEnvNameStr = {
        "CUDA_VISIBLE_DEVICES",
        "HIP_VISIBLE_DEVICES",
        "ASCEND_RT_VISIBLE_DEVICES",
};

DeviceEnvInjector GetDeviceEnvInjectorFromStr(const std::string& str);

struct BasicDevice {
  std::string dev_id;

  // e.g GPU
  std::string name;
  // Device type e.g. A100
  std::string type;

  DeviceEnvInjector env_injector;

  struct DeviceMeta {
    std::string path;
    unsigned int major;
    unsigned int minor;
    char op_type;
  };
  std::vector<DeviceMeta> device_metas;

  BasicDevice(const std::string& device_name, const std::string& device_type,
              const std::vector<std::string>& device_path,
              DeviceEnvInjector env_injector);

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
      DeviceEnvInjector env_injector);

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