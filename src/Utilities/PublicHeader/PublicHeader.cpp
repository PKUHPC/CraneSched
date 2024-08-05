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

#include <sys/stat.h>
#include <sys/sysmacros.h>

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
  for (const auto& [lhs_node_id, lhs_gres] : lhs.craned_id_gres_map) {
    if (!rhs.contains(lhs_node_id) && !lhs_gres.empty()) {
      return false;
    }

    for (const auto& [lhs_name, lhs_type_slots_map] :
         lhs_gres.name_type_slots_map) {
      if (!rhs.at(lhs_node_id).contains(lhs_name) &&
          !lhs_type_slots_map.empty()) {
        return false;
      }

      for (const auto& [lhs_type, lhs_slots] :
           lhs_type_slots_map.type_slots_map) {
        if (!rhs.at(lhs_node_id).at(lhs_name).contains(lhs_type) &&
            !lhs_slots.empty()) {
          return false;
        }

        if (!std::ranges::includes(
                rhs.at(lhs_node_id).at(lhs_name).at(lhs_type), lhs_slots)) {
          return false;
        }
      }
    }
  }
  return true;
}

bool operator==(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  std::set<CranedId> craned_ids;
  for (const auto& [craned_id, _] : lhs.craned_id_gres_map)
    craned_ids.emplace(craned_id);
  for (const auto& [craned_id, _] : rhs.craned_id_gres_map)
    craned_ids.emplace(craned_id);

  for (const auto& craned_id : craned_ids) {
    // craned gres always not empty
    if (lhs.contains(craned_id) && !rhs.contains(craned_id)) {
      return false;
    } else if (!lhs.contains(craned_id) && rhs.contains(craned_id)) {
      return false;
    } else {
      if (!(lhs.at(craned_id) == rhs.at(craned_id))) return false;
    }
  }
  return true;
}

bool operator<=(
    const std::unordered_map<
        std::string /*name*/,
        std::pair<uint64_t /*untyped req count*/,
                  std::unordered_map<std::string /*type*/,
                                     uint64_t /*type total*/>>>& lhs,
    const DedicatedResourceInNode& rhs) {
  for (const auto& [lhs_name, lhs_name_type_spec] : lhs) {
    const auto& [untyped_req_count, req_type_count_map] = lhs_name_type_spec;
    if (rhs.contains(lhs_name)) {
      uint32_t avail_count = 0;
      const auto& rhs_type_slots_map = rhs.at(lhs_name).type_slots_map;
      for (const auto& [req_type, req_count] : req_type_count_map) {
        if (req_count == 0) continue;
        if (!rhs_type_slots_map.contains(req_type)) {
          return false;
        } else {
          auto type_size = rhs_type_slots_map.at(req_type).size();
          if (type_size < req_count) return false;
          avail_count += type_size - req_count;
        }
      }
      if (untyped_req_count <= avail_count) continue;
      for (const auto& [rhs_type, rhs_slots] : rhs_type_slots_map) {
        if (req_type_count_map.contains(rhs_type)) continue;
        avail_count += rhs_slots.size();
        if (untyped_req_count <= avail_count) break;
      }
      if (untyped_req_count > avail_count) return false;
    } else {
      if (untyped_req_count != 0) return false;
    }
  }
  return true;
}

bool operator<=(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs) {
  for (const auto& [lhs_name, lhs_type_slots_map] : lhs.name_type_slots_map) {
    // type_slots_map always not empty
    if (!rhs.contains(lhs_name) && !lhs_type_slots_map.empty()) {
      return false;
    }

    for (const auto& [lhs_type, lhs_slots] :
         lhs_type_slots_map.type_slots_map) {
      // slots always not empty
      if (!rhs.at(lhs_name).contains(lhs_type)) {
        return false;
      }

      if (!std::ranges::includes(rhs.at(lhs_name).at(lhs_type), lhs_slots)) {
        return false;
      }
    }
  }
  return true;
}

bool operator==(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs) {
  std::set<std::string> names;
  std::set<std::string> types;
  lhs.flat_(names, types);
  rhs.flat_(names, types);
  for (const auto& name : names) {
    // type_slots_map always not empty
    if (lhs.contains(name) && !rhs.contains(name)) {
      return false;
    } else if (!lhs.contains(name) && rhs.contains(name)) {
      return false;
    } else if (lhs.contains(name) && rhs.contains(name)) {
      const auto& lhs_type_slots_map = lhs.at(name);
      const auto& rhs_type_slots_map = rhs.at(name);
      for (const auto& type : types) {
        // slots set always not empty
        if (lhs_type_slots_map.contains(type) &&
            !rhs_type_slots_map.contains(type)) {
          return false;
        } else if (!lhs_type_slots_map.contains(type) &&
                   rhs_type_slots_map.contains(type)) {
          return false;
        } else if (lhs_type_slots_map.contains(type) &&
                   rhs_type_slots_map.contains(type)) {
          if (rhs_type_slots_map.at(type) != lhs_type_slots_map.at(type))
            return false;
        }
      }
    }
  }
  return true;
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
  for (const auto& [rhs_node_id, rhs_name_slots_map] : rhs.craned_id_gres_map) {
    this->craned_id_gres_map[rhs_node_id] += rhs_name_slots_map;
  }

  return *this;
}

DedicatedResource& DedicatedResource::operator-=(const DedicatedResource& rhs) {
  for (const auto& [rhs_node_id, rhs_name_slots_map] : rhs.craned_id_gres_map) {
    ABSL_ASSERT(this->craned_id_gres_map.contains(rhs_node_id));
    this->craned_id_gres_map[rhs_node_id] -= rhs_name_slots_map;
    if (this->craned_id_gres_map[rhs_node_id].empty())
      this->craned_id_gres_map.erase(rhs_node_id);
  }

  return *this;
}
bool DedicatedResource::contains(const CranedId& craned_id) const {
  return craned_id_gres_map.contains(craned_id);
}

bool DedicatedResource::empty() const {
  if (craned_id_gres_map.empty()) return true;
  return std::ranges::all_of(craned_id_gres_map,
                             [](const auto& kv) { return kv.second.empty(); });
}

DedicatedResource::DedicatedResource(
    const crane::grpc::DedicatedResource& rhs) {
  for (const auto& [craned_id, gres] : rhs.each_node_gres()) {
    auto& this_craned_gres_map =
        this->craned_id_gres_map[craned_id].name_type_slots_map;
    for (const auto& [name, type_slots_map] : gres.name_type_map()) {
      for (const auto& [type, slots] : type_slots_map.type_slots_map())
        this_craned_gres_map[name][type].insert(slots.slots().begin(),
                                                slots.slots().end());
    }
  }
}

DedicatedResource::operator crane::grpc::DedicatedResource() const {
  crane::grpc::DedicatedResource val{};
  for (const auto& [craned_id, gres] : craned_id_gres_map) {
    for (const auto& [name, type_slots_map] : gres.name_type_slots_map) {
      {
        for (const auto& [type, slots] : type_slots_map.type_slots_map) {
          (*(*(*val.mutable_each_node_gres())[craned_id]
                  .mutable_name_type_map())[name]
                .mutable_type_slots_map())[type]
              .mutable_slots()
              ->Assign(slots.begin(), slots.end());
        }
      }
    }
  }
  return val;
}

DedicatedResourceInNode& DedicatedResource::operator[](
    const std::string& craned_id) {
  return this->craned_id_gres_map[craned_id];
}

const DedicatedResourceInNode& DedicatedResource::at(
    const std::string& craned_id) const {
  return this->craned_id_gres_map.at(craned_id);
}

DedicatedResourceInNode& DedicatedResource::at(const std::string& craned_id) {
  return this->craned_id_gres_map.at(craned_id);
}

bool DedicatedResourceInNode::empty() const {
  return name_type_slots_map.empty();
}

bool DedicatedResourceInNode::empty(const std::string& device_name) const {
  if (name_type_slots_map.empty() || !name_type_slots_map.contains(device_name))
    return true;
  return name_type_slots_map.at(device_name).empty();
}
bool DedicatedResourceInNode::empty(const std::string& device_name,
                                    const std::string& device_type) const {
  if (name_type_slots_map.empty() ||
      !name_type_slots_map.contains(device_name) ||
      !name_type_slots_map.at(device_name).contains(device_type))
    return true;
  return name_type_slots_map.at(device_name).at(device_type).empty();
}

DedicatedResourceInNode& DedicatedResourceInNode::operator+=(
    const DedicatedResourceInNode& rhs) {
  for (const auto& [rhs_name, rhs_type_slots_map] : rhs.name_type_slots_map) {
    for (const auto& [rhs_type, rhs_slots] : rhs_type_slots_map.type_slots_map)
      this->name_type_slots_map[rhs_name][rhs_type].insert(rhs_slots.begin(),
                                                           rhs_slots.end());
  }
  return *this;
}

DedicatedResourceInNode& DedicatedResourceInNode::operator-=(
    const DedicatedResourceInNode& rhs) {
  for (const auto& [rhs_name, rhs_type_slots_map] : rhs.name_type_slots_map) {
    ABSL_ASSERT(this->contains(rhs_name));

    for (const auto& [rhs_type, rhs_slots] :
         rhs_type_slots_map.type_slots_map) {
      ABSL_ASSERT(this->at(rhs_name).contains(rhs_type));
      auto& this_type_slots_map = this->name_type_slots_map.at(rhs_name);
      std::set<SlotId> temp;
      std::ranges::set_difference(this_type_slots_map.at(rhs_type), rhs_slots,
                                  std::inserter(temp, temp.begin()));
      if (temp.empty()) {
        this_type_slots_map.type_slots_map.erase(rhs_type);
      } else {
        this_type_slots_map[rhs_type] = std::move(temp);
      }
    }

    if (this->name_type_slots_map.at(rhs_name).empty()) {
      this->name_type_slots_map.erase(rhs_name);
    }
  }

  return *this;
}

DedicatedResourceInNode::TypeSlotsMap& DedicatedResourceInNode::operator[](
    const std::string& device_name) {
  return this->name_type_slots_map[device_name];
}

DedicatedResourceInNode::TypeSlotsMap& DedicatedResourceInNode::at(
    const std::string& device_name) {
  return this->name_type_slots_map.at(device_name);
}

const DedicatedResourceInNode::TypeSlotsMap& DedicatedResourceInNode::at(
    const std::string& device_name) const {
  return this->name_type_slots_map.at(device_name);
}

bool DedicatedResourceInNode::contains(const std::string& device_name) const {
  return this->name_type_slots_map.contains(device_name);
}

void DedicatedResourceInNode::flat_(std::set<std::string>& names,
                                    std::set<std::string>& types) const {
  for (const auto& [name, type_slots_map] : this->name_type_slots_map) {
    names.emplace(name);
    for (const auto& [type, _] : type_slots_map.type_slots_map)
      types.emplace(type);
  }
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

bool DedicatedResourceInNode::TypeSlotsMap::empty() const {
  return type_slots_map.empty();
}
std::set<SlotId>& DedicatedResourceInNode::TypeSlotsMap::operator[](
    const std::string& type) {
  return type_slots_map[type];
}
const std::set<SlotId>& DedicatedResourceInNode::TypeSlotsMap::at(
    const std::string& type) const {
  return type_slots_map.at(type);
}
bool DedicatedResourceInNode::TypeSlotsMap::contains(
    const std::string& type) const {
  return type_slots_map.contains(type);
}
