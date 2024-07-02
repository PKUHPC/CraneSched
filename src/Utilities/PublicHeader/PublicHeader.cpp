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

AllocatableResource::AllocatableResource(
    const crane::grpc::AllocatableResource& value) {
  cpu_count = cpu_t{value.cpu_core_limit()};
  memory_bytes = value.memory_limit_bytes();
  memory_sw_bytes = value.memory_sw_limit_bytes();
}

AllocatableResource& AllocatableResource::operator=(
    const crane::grpc::AllocatableResource& value) {
  cpu_count = cpu_t{value.cpu_core_limit()};
  memory_bytes = value.memory_limit_bytes();
  memory_sw_bytes = value.memory_sw_limit_bytes();
  return *this;
}

AllocatableResource::operator crane::grpc::AllocatableResource() const {
  auto val = crane::grpc::AllocatableResource();
  val.set_cpu_core_limit(static_cast<double>(this->cpu_count));
  val.set_memory_limit_bytes(this->memory_bytes);
  val.set_memory_sw_limit_bytes(this->memory_sw_bytes);
  return val;
}

Resources& Resources::operator+=(const Resources& rhs) {
  allocatable_resource += rhs.allocatable_resource;
  return *this;
}

Resources& Resources::operator-=(const Resources& rhs) {
  allocatable_resource -= rhs.allocatable_resource;
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

Resources::operator crane::grpc::Resources() const {
  auto val = crane::grpc::Resources();
  *val.mutable_allocatable_resource() = allocatable_resource;
  return val;
}

bool operator<=(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource <= rhs.allocatable_resource;
}

bool operator<(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource < rhs.allocatable_resource;
}

bool operator==(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource == rhs.allocatable_resource;
}