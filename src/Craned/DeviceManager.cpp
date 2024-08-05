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

namespace Craned {

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
    const std::string& device_path) {
  if (device_path.find("nvidia") != std::string::npos) {
    // todo: implement nvidia device
    return std::make_unique<BasicDevice>(device_name, device_type, device_path);
  } else {
    return std::make_unique<BasicDevice>(device_name, device_type, device_path);
  }
}
std::vector<std::pair<std::string, std::string>>
DeviceManager::GetEnvironmentVariable(
    const crane::grpc::DedicatedResourceInNode& resourceInNode) {
  std::vector<std::pair<std::string, std::string>> env;
  std::unordered_set<std::string> all_res_slots;
  for (const auto& [device_name, device_type_slosts_map] :
       resourceInNode.name_type_map()) {
    for (const auto& [device_type, slots] :
         device_type_slosts_map.type_slots_map())
      all_res_slots.insert(slots.slots().begin(), slots.slots().end());
  }
  {
    // nvidia device
    uint32_t cuda_count = 0;
    std::ranges::for_each(
        all_res_slots, [&cuda_count](const std::string& device_path) {
          cuda_count += device_path.find("nvidia") != std::string::npos;
        });
    std::vector<int> cuda_visible_device_vars;
    for (int i = 0; i < cuda_count; ++i) {
      cuda_visible_device_vars.push_back(i);
    }
    env.emplace_back("CUDA_VISIBLE_DEVICES",
                     absl::StrJoin(cuda_visible_device_vars, ","));
  }
  return env;
}

BasicDevice::BasicDevice(const std::string& device_name,
                         const std::string& device_type,
                         const std::string& device_path)
    : name(device_name), type(device_type), path(device_path) {}

bool operator==(const BasicDevice& lhs, const BasicDevice& rhs) {
  return lhs.path == rhs.path;
}

bool BasicDevice::Init() {
  const auto& device_major_minor_optype_option =
      DeviceManager::GetDeviceFileMajorMinorOpType(path);
  if (!device_major_minor_optype_option.has_value()) return false;
  const auto& device_major_minor_optype =
      device_major_minor_optype_option.value();

  this->major = std::get<0>(device_major_minor_optype);
  this->minor = std::get<1>(device_major_minor_optype);
  this->op_type = std::get<2>(device_major_minor_optype);
  return true;
}

BasicDevice::operator std::string() const {
  return fmt::format("{}:{}:{}", name, type, path);
}

}  // namespace Craned