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

#include "CommonPublicDefs.h"
// Precompiled header comes first.

namespace Craned::Common {
using namespace std::literals;
enum DeviceEnvInjectorEnum : uint8_t {
  CommonDevice = 0,
  Nvidia,
  Hip,
  Ascend,

  __DeviceEnvInjector_SIZE,
  InvalidInjector
};

constexpr std::array<std::string_view,
                     DeviceEnvInjectorEnum::__DeviceEnvInjector_SIZE>
    DeviceEnvInjectorStr = {
        "common",
        "nvidia",
        "hip",
        "ascend",
};
const inline std::array<std::vector<std::string_view>,
                        DeviceEnvInjectorEnum::__DeviceEnvInjector_SIZE>
    DeviceEnvNameStr = {
        std::vector{"common"sv},
        {"CUDA_VISIBLE_DEVICES"sv, "NVIDIA_VISIBLE_DEVICES"sv},
        {"HIP_VISIBLE_DEVICES"sv},
        {"ASCEND_RT_VISIBLE_DEVICES"sv, "ASCEND_VISIBLE_DEVICES"sv},
};

DeviceEnvInjectorEnum GetDeviceEnvInjectorFromStr(
    const std::optional<std::string>& str);

struct DeviceMetaInConfig {
  std::string name;
  std::string type;
  std::vector<std::string> path;
  std::optional<std::string> EnvInjectorStr;
  std::optional<std::string> CdiName;  // CDI fully-qualified name for this slot
  std::optional<std::string> CniPipeline;  // MetaCNI pipeline name for network devices
};

struct DeviceFileMeta {
  std::string path;
  unsigned int major;
  unsigned int minor;
  char op_type;
};

struct BasicDevice {
  SlotId slot_id;

  // e.g GPU
  std::string name;
  // Device type e.g. A100
  std::string type;

  DeviceEnvInjectorEnum env_injector;
  std::vector<DeviceFileMeta> device_file_metas;

  // CDI fully-qualified name, e.g. "nvidia.com/gpu=0". Set only when
  // configured.
  std::optional<std::string> cdi_name;

  // MetaCNI pipeline name. When set, this device's CDI device name will be
  // injected as a pod annotation for MetaCNI template pipeline expansion.
  std::optional<std::string> cni_pipeline;

  BasicDevice(const std::string& device_name, const std::string& device_type,
              const std::vector<std::string>& device_path,
              DeviceEnvInjectorEnum env_injector,
              std::optional<std::string> cdi_name = std::nullopt,
              std::optional<std::string> cni_pipeline = std::nullopt);

  BasicDevice(const BasicDevice& another) = default;

  virtual ~BasicDevice() = default;
  virtual bool Init();

  explicit operator std::string() const;

  // TODO: Add virtual function to get device status
};

class DeviceManager {
 public:
  static std::unique_ptr<BasicDevice> ConstructDevice(
      const std::string& device_name, const std::string& device_type,
      const std::vector<std::string>& device_path,
      DeviceEnvInjectorEnum env_injector,
      std::optional<std::string> cdi_name = std::nullopt,
      std::optional<std::string> cni_pipeline = std::nullopt);

  static CraneErrCode GetDeviceFileMajorMinorOpType(
      DeviceFileMeta* device_file_meta);

  static Common::EnvMap GetDevEnvMapByResInNode(
      const crane::grpc::DedicatedResourceInNode& res_in_node);

  // Extract the device name from a CDI fully-qualified name.
  // CDI FQN format: "<vendor>/<class>=<device-name>"
  // Returns the part after the last '=', or std::nullopt if malformed.
  static std::optional<std::string> ExtractCdiDeviceName(
      const std::string& cdi_fqn);
};

// read only after init
inline std::unordered_map<SlotId, std::unique_ptr<BasicDevice>>
    g_this_node_device;

}  // namespace Craned::Common