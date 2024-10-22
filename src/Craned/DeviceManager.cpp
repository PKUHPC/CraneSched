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

#include "DeviceManager.h"

#include <sys/sysmacros.h>

#include "crane/String.h"

namespace Craned {

DeviceEnvInjector GetDeviceEnvInjectorFromStr(const std::string& str) {
  static const std::unordered_map<std::string, DeviceEnvInjector>
      EnvStrToEnumMap{
          {"nvidia", DeviceEnvInjector::Nvidia},
          {"hip", DeviceEnvInjector::Hip},
          {"ascend", DeviceEnvInjector::Ascend},
      };
  if (const auto it = EnvStrToEnumMap.find(str); it != EnvStrToEnumMap.end())
    return it->second;
  else
    return DeviceEnvInjector::InvalidInjector;
}

BasicDevice::BasicDevice(const std::string& device_name,
                         const std::string& device_type,
                         const std::vector<std::string>& device_path,
                         DeviceEnvInjector env_injector)
    : name(device_name), type(device_type), env_injector(env_injector) {
  device_metas.reserve(device_path.size());
  for (const auto& dev_path : device_path) {
    device_metas.emplace_back(DeviceMeta{dev_path, 0, 0, 0});
  }
}

bool BasicDevice::Init() {
  for (auto& device_meta : device_metas) {
    const auto& device_major_minor_optype_option =
        DeviceManager::GetDeviceFileMajorMinorOpType(device_meta.path);
    if (!device_major_minor_optype_option.has_value()) return false;
    const auto& device_major_minor_optype =
        device_major_minor_optype_option.value();

    device_meta.major = std::get<0>(device_major_minor_optype);
    device_meta.minor = std::get<1>(device_major_minor_optype);
    device_meta.op_type = std::get<2>(device_major_minor_optype);
  }
  return true;
}

BasicDevice::operator std::string() const {
  std::vector<std::string> device_files;
  for (const auto& device_meta : device_metas) {
    device_files.push_back(device_meta.path);
  }
  return fmt::format("{}:{}:{}", name, type,
                     util::HostNameListToStr(device_files));
}

std::optional<std::tuple<unsigned int, unsigned int, char>>
DeviceManager::GetDeviceFileMajorMinorOpType(const std::string& path) {
  struct stat device_file_info {};
  if (stat(path.c_str(), &device_file_info) == 0) {
    char op_type = 'a';
    if (S_ISBLK(device_file_info.st_mode)) {
      op_type = 'b';
    } else if (S_ISCHR(device_file_info.st_mode)) {
      op_type = 'c';
    }
    return std::make_tuple(major(device_file_info.st_rdev),
                           minor(device_file_info.st_rdev), op_type);
  } else {
    return std::nullopt;
  }
}

std::unique_ptr<BasicDevice> DeviceManager::ConstructDevice(
    const std::string& device_name, const std::string& device_type,
    const std::vector<std::string>& device_path,
    DeviceEnvInjector env_injector) {
  return std::make_unique<BasicDevice>(device_name, device_type, device_path,
                                       env_injector);
}

std::vector<std::pair<std::string, std::string>>
DeviceManager::GetDevEnvListByResInNode(
    const crane::grpc::DedicatedResourceInNode& res_in_node) {
  std::vector<std::pair<std::string, std::string>> env;

  std::unordered_set<std::string> all_res_slots;
  for (const auto& [device_name, device_type_slots_map] :
       res_in_node.name_type_map()) {
    for (const auto& [device_type, slots] :
         device_type_slots_map.type_slots_map())
      all_res_slots.insert(slots.slots().begin(), slots.slots().end());
  }

  std::unordered_map<int, int> injector_count_map;
  for (const auto& [_, device] : g_this_node_device) {
    if (!all_res_slots.contains(device->dev_id)) continue;
    injector_count_map[device->env_injector]++;
  }
  for (const auto [injector_enum, count] : injector_count_map) {
    env.emplace_back(DeviceEnvNameStr[injector_enum],
                     util::GenerateCommaSeparatedString(count));
  }

  return env;
}

}  // namespace Craned