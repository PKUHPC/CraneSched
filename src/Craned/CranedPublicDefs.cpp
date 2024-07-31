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

#include "CranedPublicDefs.h"
namespace Craned {

std::optional<std::tuple<unsigned int, unsigned int, char>>
GetDeviceFileMajorMinorOpType(const std::string& path) {
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

bool Device::Init() {
  const auto& device_major_minor_optype_option =
      GetDeviceFileMajorMinorOpType(path);
  if (device_major_minor_optype_option.has_value()) {
    const auto& device_major_minor_optype =
        device_major_minor_optype_option.value();

    this->major = std::get<0>(device_major_minor_optype);
    this->minor = std::get<1>(device_major_minor_optype);
    this->op_type = std::get<2>(device_major_minor_optype);
  } else {
    return false;
  }
  return true;
}

Device::Device(const std::string& device_name, const std::string& device_type,
               const std::string& device_path)
    : name(device_name), type(device_type), path(device_path) {}

Device::operator std::string() const {
  return fmt::format("{}:{}:{}", name, type, path);
};

bool operator==(const Device& lhs, const Device& rhs) {
  return lhs.path == rhs.path;
}

}  // namespace Craned