#include "crane/PublicHeader.h"

AllocatableResource& AllocatableResource::operator+=(
    const AllocatableResource& rhs) {
  cpu_count += rhs.cpu_count;
  memory_bytes += rhs.memory_bytes;
  memory_sw_bytes += rhs.memory_sw_bytes;
  return *this;
}

AllocatableResource& AllocatableResource::operator-=(
    const AllocatableResource& rhs) {
  cpu_count -= rhs.cpu_count;
  memory_bytes -= rhs.memory_bytes;
  memory_sw_bytes -= rhs.memory_sw_bytes;
  return *this;
}

bool operator<=(const AllocatableResource& lhs,
                const AllocatableResource& rhs) {
  if (lhs.cpu_count <= rhs.cpu_count && lhs.memory_bytes <= rhs.memory_bytes &&
      lhs.memory_sw_bytes <= rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator<(const AllocatableResource& lhs, const AllocatableResource& rhs) {
  if (lhs.cpu_count < rhs.cpu_count && lhs.memory_bytes < rhs.memory_bytes &&
      lhs.memory_sw_bytes < rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator==(const AllocatableResource& lhs,
                const AllocatableResource& rhs) {
  if (lhs.cpu_count == rhs.cpu_count && lhs.memory_bytes == rhs.memory_bytes &&
      lhs.memory_sw_bytes == rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator<=(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  for (auto& entry : lhs.devices) {
    const std::string& lhs_device_name = entry.first;
    const uint64_t lhs_device_num = entry.second;
    if (!rhs.devices.contains(lhs_device_name)) {
      if (lhs_device_num != 0)
        return false;
      else
        continue;
    } else if (lhs_device_num > rhs.devices.at(lhs_device_name))
      return false;
  }
  return true;
}
bool operator<(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  // if both lhs and rhs are empty or the num of devices are all zeros,should
  // return false
  bool all_resources_empty = true;
  for (const auto& entry : lhs.devices) {
    const std::string& lhs_device_name = entry.first;
    const uint64_t lhs_device_num = entry.second;
    if (!rhs.devices.contains(lhs_device_name)) {
      if (lhs_device_num != 0)
        return false;
      else
        continue;
    } else if (lhs_device_num >= rhs.devices.at(lhs_device_name))
      return false;
    all_resources_empty = false;
  }
  // should return true,but take care of all_resources_empty
  return all_resources_empty ? false : true;
}

bool operator==(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  for (const auto& entry : lhs.devices) {
    const std::string& lhs_device_name = entry.first;
    const uint64_t lhs_device_num = entry.second;
    if (!rhs.devices.contains(lhs_device_name)) {
      if (lhs_device_num != 0)
        return false;
      else
        continue;
    } else if (lhs_device_num != rhs.devices.at(lhs_device_name))
      return false;
  }
  return true;
}

AllocatableResource::AllocatableResource(
    const crane::grpc::AllocatableResource& value) {
  cpu_count = value.cpu_core_limit();
  memory_bytes = value.memory_limit_bytes();
  memory_sw_bytes = value.memory_sw_limit_bytes();
}

AllocatableResource& AllocatableResource::operator=(
    const crane::grpc::AllocatableResource& value) {
  cpu_count = value.cpu_core_limit();
  memory_bytes = value.memory_limit_bytes();
  memory_sw_bytes = value.memory_sw_limit_bytes();
  return *this;
}

Resources& Resources::operator+=(const Resources& rhs) {
  allocatable_resource += rhs.allocatable_resource;
  dedicated_resource += rhs.dedicated_resource;
  return *this;
}

Resources& Resources::operator-=(const Resources& rhs) {
  allocatable_resource -= rhs.allocatable_resource;
  dedicated_resource -= rhs.dedicated_resource;
  return *this;
}

Resources& Resources::operator+=(const AllocatableResource& rhs) {
  allocatable_resource += rhs;
  return *this;
}

Resources& Resources::operator-=(const AllocatableResource& rhs) {
  allocatable_resource -= rhs;
  return *this;
}

bool operator<=(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource <= rhs.allocatable_resource &&
         lhs.dedicated_resource <= rhs.dedicated_resource;
}

bool operator<(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource < rhs.allocatable_resource &&
         lhs.dedicated_resource < rhs.dedicated_resource;
}

bool operator==(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource == rhs.allocatable_resource &&
         lhs.dedicated_resource == rhs.dedicated_resource;
}

DedicatedResource& DedicatedResource::operator+=(const DedicatedResource& rhs) {
  for (auto& it : rhs.devices) {
    this->devices[it.first] += it.second;
  }
  return *this;
}

DedicatedResource& DedicatedResource::operator-=(const DedicatedResource& rhs) {
  for (auto& it : rhs.devices) {
    this->devices[it.first] -= it.second;
  }
  return *this;
}

DedicatedResource& DedicatedResource::AddResource(
    const std::string& device_name, uint64_t count) {
  this->devices[device_name] += count;
  return *this;
}

DedicatedResource::DedicatedResource(
    const crane::grpc::DedicatedResource& value) {
  for (const auto& entry : value.devices()) {
    this->devices[entry.first] = entry.second;
  }
}

DedicatedResource& DedicatedResource::operator=(
    const crane::grpc::DedicatedResource& value) {
  for (const auto& entry : value.devices()) {
    this->devices[entry.first] = entry.second;
  }
  return *this;
}