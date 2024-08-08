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

#include <ranges>

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
  for (const auto& [lhs_node_id, lhs_gres] : lhs.craned_id_dres_in_node_map) {
    auto rhs_it = rhs.craned_id_dres_in_node_map.find(lhs_node_id);
    if (rhs_it == rhs.craned_id_dres_in_node_map.end()) return false;

    if (!(lhs_gres <= rhs_it->second)) return false;
  }

  return true;
}

bool operator==(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  return lhs.craned_id_dres_in_node_map == rhs.craned_id_dres_in_node_map;
}

bool operator<=(const DedicatedResourceInNode::Req_t& lhs,
                const DedicatedResourceInNode& rhs) {
  for (const auto& [lhs_name, lhs_name_type_spec] : lhs) {
    const auto& [untyped_req_count, req_type_count_map] = lhs_name_type_spec;
    auto rhs_it = rhs.name_type_slots_map.find(lhs_name);
    if (rhs_it == rhs.name_type_slots_map.end()) {
      if (untyped_req_count != 0 || !req_type_count_map.empty()) return false;
    }

    uint32_t avail_count = 0;

    const auto& rhs_type_slots_map = rhs_it->second.type_slots_map;

    for (const auto& [req_type, req_count] : req_type_count_map) {
      auto rhs_type_it = rhs_type_slots_map.find(req_type);
      if (rhs_type_it == rhs_type_slots_map.end()) return false;

      size_t type_size = rhs_type_it->second.size();
      // E.g. H100:2 of rhs < H100:3 in request
      if (type_size < req_count) return false;

      // Add redundant slots to avail_count for untyped selection
      avail_count += type_size - req_count;
    }

    if (untyped_req_count <= avail_count) continue;

    for (const auto& [rhs_type, rhs_slots] : rhs_type_slots_map) {
      // Skip already counted types
      if (req_type_count_map.contains(rhs_type)) continue;

      avail_count += rhs_slots.size();

      // Stop iteration if the untyped request is satisfied
      if (untyped_req_count <= avail_count) break;
    }

    if (untyped_req_count > avail_count) return false;
  }

  return true;
}

bool operator<=(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs) {
  for (const auto& [lhs_name, lhs_type_slots_map] : lhs.name_type_slots_map) {
    auto rhs_it = rhs.name_type_slots_map.find(lhs_name);
    if (rhs_it == rhs.name_type_slots_map.end()) return false;

    if (!(lhs_type_slots_map <= rhs_it->second)) return false;
  }

  return true;
}

bool operator==(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs) {
  return lhs.name_type_slots_map == rhs.name_type_slots_map;
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
Resources Resources::operator+(const DedicatedResource& rhs) const {
  Resources result(*this);
  result.dedicated_resource += rhs;
  return result;
}

Resources::operator crane::grpc::Resources() const {
  auto val = crane::grpc::Resources();
  *val.mutable_allocatable_resource() =
      static_cast<crane::grpc::AllocatableResource>(allocatable_resource);
  *val.mutable_actual_dedicated_resource() =
      static_cast<crane::grpc::DedicatedResource>(dedicated_resource);
  return val;
}

bool operator<=(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource <= rhs.allocatable_resource &&
         lhs.dedicated_resource <= rhs.dedicated_resource;
}

bool operator==(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource == rhs.allocatable_resource &&
         lhs.dedicated_resource == rhs.dedicated_resource;
}

DedicatedResource& DedicatedResource::operator+=(const DedicatedResource& rhs) {
  for (auto it = rhs.craned_id_dres_in_node_map.cbegin();
       it != rhs.craned_id_dres_in_node_map.cend(); ++it) {
    const std::string& rhs_node_id = it->first;
    AddDedicatedResourceInNode(rhs_node_id, rhs);
  }

  return *this;
}

DedicatedResource& DedicatedResource::operator-=(const DedicatedResource& rhs) {
  for (auto it = rhs.craned_id_dres_in_node_map.cbegin();
       it != rhs.craned_id_dres_in_node_map.cend(); ++it) {
    const std::string& rhs_node_id = it->first;
    SubtractDedicatedResourceInNode(rhs_node_id, rhs);
  }

  return *this;
}

DedicatedResource& DedicatedResource::AddDedicatedResourceInNode(
    const CranedId& craned_id, const DedicatedResource& rhs) {
  auto rhs_node_it = rhs.craned_id_dres_in_node_map.find(craned_id);
  if (rhs_node_it == rhs.craned_id_dres_in_node_map.end()) return *this;

  this->craned_id_dres_in_node_map[craned_id] += rhs_node_it->second;

  return *this;
}

DedicatedResource& DedicatedResource::SubtractDedicatedResourceInNode(
    const CranedId& craned_id, const DedicatedResource& rhs) {
  auto this_node_it = this->craned_id_dres_in_node_map.find(craned_id);
  if (this_node_it == this->craned_id_dres_in_node_map.end()) return *this;

  auto rhs_node_it = rhs.craned_id_dres_in_node_map.find(craned_id);
  if (rhs_node_it == rhs.craned_id_dres_in_node_map.end()) return *this;

  this_node_it->second -= rhs_node_it->second;

  if (this_node_it->second.IsZero())
    this->craned_id_dres_in_node_map.erase(this_node_it);

  return *this;
}

bool DedicatedResource::contains(const CranedId& craned_id) const {
  return craned_id_dres_in_node_map.contains(craned_id);
}

bool DedicatedResource::IsZero() const {
  return craned_id_dres_in_node_map.empty();
}

DedicatedResource::DedicatedResource(
    const crane::grpc::DedicatedResource& rhs) {
  for (const auto& [craned_id, gres] : rhs.each_node_gres()) {
    auto& this_craned_gres_map =
        this->craned_id_dres_in_node_map[craned_id].name_type_slots_map;
    for (const auto& [name, type_slots_map] : gres.name_type_map()) {
      for (const auto& [type, slots] : type_slots_map.type_slots_map())
        this_craned_gres_map[name][type].insert(slots.slots().begin(),
                                                slots.slots().end());
    }
  }
}

DedicatedResource::operator crane::grpc::DedicatedResource() const {
  crane::grpc::DedicatedResource val{};
  for (const auto& [craned_id, gres] : craned_id_dres_in_node_map) {
    auto* each_node_gres = val.mutable_each_node_gres();

    for (const auto& [name, type_slots_map] : gres.name_type_slots_map) {
      auto* name_type_map =
          (*each_node_gres)[craned_id].mutable_name_type_map();

      for (const auto& [type, slots] : type_slots_map.type_slots_map) {
        auto* grpc_type_slots_map =
            (*name_type_map)[name].mutable_type_slots_map();
        (*grpc_type_slots_map)[type].mutable_slots()->Assign(slots.begin(),
                                                             slots.end());
      }
    }
  }
  return val;
}

DedicatedResourceInNode& DedicatedResource::operator[](
    const std::string& craned_id) {
  return this->craned_id_dres_in_node_map[craned_id];
}

const DedicatedResourceInNode& DedicatedResource::at(
    const std::string& craned_id) const {
  return this->craned_id_dres_in_node_map.at(craned_id);
}

DedicatedResourceInNode& DedicatedResource::at(const std::string& craned_id) {
  return this->craned_id_dres_in_node_map.at(craned_id);
}

bool DedicatedResourceInNode::IsZero() const {
  return name_type_slots_map.empty();
}

DedicatedResourceInNode& DedicatedResourceInNode::operator+=(
    const DedicatedResourceInNode& rhs) {
  for (const auto& [rhs_name, rhs_type_slots_map] : rhs.name_type_slots_map)
    this->name_type_slots_map[rhs_name] += rhs_type_slots_map;

  return *this;
}

DedicatedResourceInNode& DedicatedResourceInNode::operator-=(
    const DedicatedResourceInNode& rhs) {
  for (const auto& [rhs_name, rhs_type_slots_map] : rhs.name_type_slots_map) {
    auto lhs_type_slots_map_it = this->name_type_slots_map.find(rhs_name);
    ABSL_ASSERT(lhs_type_slots_map_it != this->name_type_slots_map.end());

    lhs_type_slots_map_it->second -= rhs_type_slots_map;

    if (lhs_type_slots_map_it->second.IsZero())
      this->name_type_slots_map.erase(lhs_type_slots_map_it);
  }

  return *this;
}

TypeSlotsMap& DedicatedResourceInNode::operator[](
    const std::string& device_name) {
  return this->name_type_slots_map[device_name];
}

TypeSlotsMap& DedicatedResourceInNode::at(const std::string& device_name) {
  return this->name_type_slots_map.at(device_name);
}

const TypeSlotsMap& DedicatedResourceInNode::at(
    const std::string& device_name) const {
  return this->name_type_slots_map.at(device_name);
}

bool DedicatedResourceInNode::contains(const std::string& device_name) const {
  return this->name_type_slots_map.contains(device_name);
}

DedicatedResourceInNode::operator crane::grpc::DeviceMap() const {
  crane::grpc::DeviceMap dv_map{};

  for (const auto& [device_name, type_slots_map] : this->name_type_slots_map) {
    auto& mutable_name_type_map = *dv_map.mutable_name_type_map();

    for (const auto& [device_type, slots] : type_slots_map.type_slots_map) {
      // If all slots of a device type are used up,
      // the size of slots in res_avail will be 0.
      // We don't need such devices to show up in the response, so we skip them.
      if (slots.size() == 0) continue;

      auto& mutable_type_count_map =
          *mutable_name_type_map[device_name].mutable_type_count_map();
      mutable_type_count_map[device_type] = slots.size();
    }
  }

  return dv_map;
}

bool TypeSlotsMap::IsZero() const { return type_slots_map.empty(); }

std::set<SlotId>& TypeSlotsMap::operator[](const std::string& type) {
  return type_slots_map[type];
}

const std::set<SlotId>& TypeSlotsMap::at(const std::string& type) const {
  return type_slots_map.at(type);
}

bool TypeSlotsMap::contains(const std::string& type) const {
  return type_slots_map.contains(type);
}

TypeSlotsMap& TypeSlotsMap::operator+=(const TypeSlotsMap& rhs) {
  for (const auto& [rhs_type, rhs_slots] : rhs.type_slots_map)
    this->type_slots_map[rhs_type].insert(rhs_slots.begin(), rhs_slots.end());

  return *this;
}

TypeSlotsMap& TypeSlotsMap::operator-=(const TypeSlotsMap& rhs) {
  for (const auto& [rhs_type, rhs_slots] : rhs.type_slots_map) {
    std::set<SlotId> temp;
    std::ranges::set_difference(this->type_slots_map.at(rhs_type), rhs_slots,
                                std::inserter(temp, temp.begin()));
    if (temp.empty())
      this->type_slots_map.erase(rhs_type);
    else
      this->type_slots_map[rhs_type] = std::move(temp);
  }

  return *this;
}

bool operator==(const TypeSlotsMap& lhs, const TypeSlotsMap& rhs) {
  return lhs.type_slots_map == rhs.type_slots_map;
}

bool operator<=(const TypeSlotsMap& lhs, const TypeSlotsMap& rhs) {
  for (const auto& [lhs_type, lhs_slots] : lhs.type_slots_map) {
    auto rhs_it = rhs.type_slots_map.find(lhs_type);
    if (rhs_it == rhs.type_slots_map.end()) return false;

    if (!std::ranges::includes(rhs_it->second, lhs_slots)) return false;
  }

  return true;
}
