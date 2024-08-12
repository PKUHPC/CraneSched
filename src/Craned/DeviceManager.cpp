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

#include "DeviceManager.h"

#include <sys/stat.h>
#include <sys/sysmacros.h>

#include "crane/String.h"

namespace Craned {

BasicDevice::BasicDevice(const std::string& device_name,
                         const std::string& device_type,
                         const std::vector<std::string>& device_path,
                         const std::string& env_injector)
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
    const std::string& env_injector) {
  return std::make_unique<BasicDevice>(device_name, device_type, device_path,
                                       env_injector);
}

std::vector<std::pair<std::string, std::string>>
DeviceManager::GetDevEnvListByResInNode(
    const crane::grpc::DedicatedResourceInNode& res_in_node) {
  std::vector<std::pair<std::string, std::string>> env;
  std::unordered_set<std::string> all_res_slots;
  for (const auto& [device_name, device_type_slosts_map] :
       res_in_node.name_type_map()) {
    for (const auto& [device_type, slots] :
         device_type_slosts_map.type_slots_map())
      all_res_slots.insert(slots.slots().begin(), slots.slots().end());
  }

  uint32_t cuda_count = 0;
  uint32_t hip_count = 0;
  for (const auto& [_, device] : g_this_node_device) {
    if (!all_res_slots.contains(device->device_metas.front().path)) continue;
    if (device->env_injector == "nvidia")
      ++cuda_count;
    else if (device->env_injector == "hip")
      ++hip_count;
  }
  // nvidia device
  env.emplace_back("CUDA_VISIBLE_DEVICES",
                   util::GenerateCommaSeparatedString(cuda_count));

  // amd/haiguang device
  env.emplace_back("HIP_VISIBLE_DEVICES",
                   util::GenerateCommaSeparatedString(hip_count));

  return env;
}

}  // namespace Craned