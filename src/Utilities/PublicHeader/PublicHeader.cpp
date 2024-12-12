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

AllocatableResource& AllocatableResource::operator*=(uint32_t rhs) {
  cpu_count *= rhs;
  memory_bytes *= rhs;
  memory_sw_bytes *= rhs;
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
  return lhs.cpu_count < rhs.cpu_count && lhs.memory_bytes < rhs.memory_bytes &&
         lhs.memory_sw_bytes < rhs.memory_sw_bytes;
}

bool operator==(const AllocatableResource& lhs,
                const AllocatableResource& rhs) {
  if (lhs.cpu_count == rhs.cpu_count && lhs.memory_bytes == rhs.memory_bytes &&
      lhs.memory_sw_bytes == rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator<=(const DeviceMap& lhs, const DeviceMap& rhs) {
  for (const auto& [lhs_name, lhs_cnt] : lhs) {
    auto rhs_it = rhs.find(lhs_name);
    if (rhs_it == rhs.end()) return false;

    const auto& [lhs_untyped_cnt, lhs_typed_cnt_map] = lhs_cnt;
    const auto& [rhs_untyped_cnt, rhs_typed_cnt_map] = rhs_it->second;

    uint64_t rhs_avail_count = 0;
    for (const auto& [lhs_type, lhs_type_cnt] : lhs_typed_cnt_map) {
      auto rhs_type_it = rhs_typed_cnt_map.find(lhs_type);
      if (rhs_type_it == rhs_typed_cnt_map.end()) return false;

      if (lhs_type_cnt > rhs_type_it->second) return false;

      rhs_avail_count += rhs_type_it->second - lhs_type_cnt;
    }

    if (lhs_untyped_cnt <= rhs_untyped_cnt + rhs_avail_count) continue;

    for (const auto& [rhs_type, rhs_type_cnt] : rhs_typed_cnt_map) {
      if (lhs_typed_cnt_map.contains(rhs_type)) continue;

      rhs_avail_count += rhs_type_cnt;

      if (lhs_untyped_cnt <= rhs_untyped_cnt + rhs_avail_count) break;
    }

    if (lhs_untyped_cnt > rhs_untyped_cnt + rhs_avail_count) return false;
  }

  return true;
}

bool operator<=(const DeviceMap& lhs, const DedicatedResourceInNode& rhs) {
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

DedicatedResourceInNode Intersection(const DedicatedResourceInNode& lhs,
                                     const DedicatedResourceInNode& rhs) {
  DedicatedResourceInNode result;
  for (const auto& [lhs_name, lhs_type_slots_map] : lhs.name_type_slots_map) {
    auto rhs_it = rhs.name_type_slots_map.find(lhs_name);
    if (rhs_it == rhs.name_type_slots_map.end()) continue;

    TypeSlotsMap intersection =
        Intersection(lhs_type_slots_map, rhs_it->second);
    if (!intersection.IsZero())
      result.name_type_slots_map[lhs_name] = std::move(intersection);
  }

  return result;
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

double AllocatableResource::CpuCount() const {
  return static_cast<double>(cpu_count);
}

bool AllocatableResource::IsZero() const {
  return cpu_count == static_cast<cpu_t>(0) && memory_bytes == 0 &&
         memory_sw_bytes == 0;
}

bool AllocatableResource::IsAnyZero() const {
  return cpu_count == static_cast<cpu_t>(0) || memory_bytes == 0 ||
         memory_sw_bytes == 0;
}

void AllocatableResource::SetToZero() {
  cpu_count = static_cast<cpu_t>(0);
  memory_bytes = 0;
  memory_sw_bytes = 0;
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

DedicatedResourceInNode::DedicatedResourceInNode(
    const crane::grpc::DedicatedResourceInNode& rhs) {
  for (const auto& [name, type_slots_map] : rhs.name_type_map())
    // Implicitly call grpc::DeviceTypeSlotsMap -> TypeSlotsMap conversion
    this->name_type_slots_map.emplace(name, type_slots_map);
}

DedicatedResourceInNode& DedicatedResourceInNode::operator=(
    const crane::grpc::DedicatedResourceInNode& rhs) {
  this->name_type_slots_map.clear();

  for (const auto& [name, type_slots_map] : rhs.name_type_map())
    // Implicitly call grpc::DeviceTypeSlotsMap -> TypeSlotsMap conversion
    this->name_type_slots_map[name] = type_slots_map;

  return *this;
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

DedicatedResourceInNode::operator crane::grpc::DedicatedResourceInNode() const {
  crane::grpc::DedicatedResourceInNode val{};
  auto* grpc_name_type_map = val.mutable_name_type_map();

  for (const auto& [device_name, type_slots_map] : this->name_type_slots_map) {
    auto& grpc_type_slots_map = (*grpc_name_type_map)[device_name];
    grpc_type_slots_map =
        static_cast<crane::grpc::DeviceTypeSlotsMap>(type_slots_map);
  }

  return val;
}

void DedicatedResourceInNode::SetToZero() { name_type_slots_map.clear(); }

crane::grpc::DeviceMap ToGrpcDeviceMap(const DeviceMap& device_map) {
  crane::grpc::DeviceMap grpc_device_map{};

  for (const auto& [device_name, cnt_pair] : device_map) {
    auto& grpc_name_type_map = *grpc_device_map.mutable_name_type_map();

    auto& grpc_type_cnt = grpc_name_type_map[device_name];
    grpc_type_cnt.set_total(cnt_pair.first);

    for (const auto& [dev_type, typed_cnt] : cnt_pair.second) {
      // If all slots of a device type are used up,
      // the size of slots in res_avail will be 0.
      // We don't need such devices to show up in the response, so we skip them.
      if (typed_cnt == 0) [[unlikely]]
        continue;

      auto& grpc_type_count_map = *grpc_type_cnt.mutable_type_count_map();
      grpc_type_count_map[dev_type] = typed_cnt;
    }
  }

  return grpc_device_map;
}

DeviceMap FromGrpcDeviceMap(const crane::grpc::DeviceMap& grpc_device_map) {
  DeviceMap device_map{};

  for (const auto& [device_name, grpc_type_count_map] :
       grpc_device_map.name_type_map()) {
    auto& name_cnt = device_map[device_name];

    // Set untyped cnt.
    name_cnt.first = grpc_type_count_map.total();

    auto& type_slots_map = name_cnt.second;
    for (const auto& [device_type, slots] :
         grpc_type_count_map.type_count_map())
      type_slots_map[device_type] = slots;
  }

  return device_map;
}

void operator+=(DeviceMap& lhs, const DedicatedResourceInNode& rhs) {
  for (const auto& [rhs_name, rhs_type_slots_map] : rhs.name_type_slots_map)
    for (const auto& [rhs_type, rhs_slots] : rhs_type_slots_map.type_slots_map)
      lhs[rhs_name].second[rhs_type] += rhs_slots.size();
}

void operator-=(DeviceMap& lhs, const DedicatedResourceInNode& rhs) {
  for (const auto& [rhs_name, rhs_type_slots_map] : rhs.name_type_slots_map) {
    auto lhs_name_it = lhs.find(rhs_name);
    ABSL_ASSERT(lhs_name_it != lhs.end());

    for (const auto& [rhs_type, rhs_slots] :
         rhs_type_slots_map.type_slots_map) {
      auto& lhs_type_size_map = lhs_name_it->second.second;

      auto lhs_type_it = lhs_type_size_map.find(rhs_type);
      ABSL_ASSERT(lhs_type_it != lhs_type_size_map.end());
      ABSL_ASSERT(rhs_slots.size() <= lhs_type_it->second);

      lhs_type_it->second -= rhs_slots.size();
    }
  }
}

void operator*=(DeviceMap& lhs, uint32_t rhs) {
  for (auto& [_, name_cnt] : lhs) {
    uint64_t& untyped_cnt = name_cnt.first;
    untyped_cnt *= rhs;
    for (auto& [_, typed_cnt] : name_cnt.second) typed_cnt *= rhs;
  }
}

TypeSlotsMap::TypeSlotsMap(const crane::grpc::DeviceTypeSlotsMap& rhs) {
  for (const auto& [type, slots] : rhs.type_slots_map())
    this->type_slots_map[type].insert(slots.slots().begin(),
                                      slots.slots().end());
}

TypeSlotsMap& TypeSlotsMap::operator=(
    const crane::grpc::DeviceTypeSlotsMap& rhs) {
  this->type_slots_map.clear();
  for (const auto& [type, slots] : rhs.type_slots_map())
    this->type_slots_map[type].insert(slots.slots().begin(),
                                      slots.slots().end());
  return *this;
}

TypeSlotsMap::operator crane::grpc::DeviceTypeSlotsMap() const {
  crane::grpc::DeviceTypeSlotsMap val{};
  for (const auto& [type, slots] : type_slots_map) {
    auto* grpc_type_slots_map = val.mutable_type_slots_map();
    auto* grpc_slots = (*grpc_type_slots_map)[type].mutable_slots();
    grpc_slots->Assign(slots.begin(), slots.end());
  }
  return val;
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

TypeSlotsMap Intersection(const TypeSlotsMap& lhs, const TypeSlotsMap& rhs) {
  TypeSlotsMap result;

  for (const auto& [lhs_type, lhs_slots] : lhs.type_slots_map) {
    auto rhs_it = rhs.type_slots_map.find(lhs_type);
    if (rhs_it == rhs.type_slots_map.end()) continue;

    std::set<SlotId> temp;
    std::ranges::set_intersection(lhs_slots, rhs_it->second,
                                  std::inserter(temp, temp.begin()));
    if (!temp.empty()) result.type_slots_map[lhs_type] = std::move(temp);
  }

  return result;
}

ResourceInNode::ResourceInNode(const crane::grpc::ResourceInNode& rhs) {
  allocatable_res = rhs.allocatable_res_in_node();
  dedicated_res = rhs.dedicated_res_in_node();
}

ResourceInNode::operator crane::grpc::ResourceInNode() const {
  crane::grpc::ResourceInNode val{};

  auto* grpc_allocatable_res_in_node = val.mutable_allocatable_res_in_node();
  *grpc_allocatable_res_in_node =
      static_cast<crane::grpc::AllocatableResource>(allocatable_res);

  auto* grpc_dedicated_res_in_node = val.mutable_dedicated_res_in_node();
  *grpc_dedicated_res_in_node =
      static_cast<crane::grpc::DedicatedResourceInNode>(dedicated_res);

  return val;
}

ResourceInNode& ResourceInNode::operator+=(const ResourceInNode& rhs) {
  allocatable_res += rhs.allocatable_res;
  dedicated_res += rhs.dedicated_res;
  return *this;
}

ResourceInNode& ResourceInNode::operator-=(const ResourceInNode& rhs) {
  allocatable_res -= rhs.allocatable_res;
  dedicated_res -= rhs.dedicated_res;
  return *this;
}

bool ResourceInNode::IsZero() const {
  return allocatable_res.IsZero() && dedicated_res.IsZero();
}

void ResourceInNode::SetToZero() {
  allocatable_res.SetToZero();
  dedicated_res.SetToZero();
}

bool operator<=(const ResourceInNode& lhs, const ResourceInNode& rhs) {
  return lhs.allocatable_res <= rhs.allocatable_res &&
         lhs.dedicated_res <= rhs.dedicated_res;
}

bool operator==(const ResourceInNode& lhs, const ResourceInNode& rhs) {
  return lhs.allocatable_res == rhs.allocatable_res &&
         lhs.dedicated_res == rhs.dedicated_res;
}

ResourceV2::ResourceV2(const crane::grpc::ResourceV2& rhs) {
  for (const auto& [node_id, res_in_node] : rhs.each_node_res())
    this->each_node_res_map.emplace(node_id, res_in_node);
}

ResourceV2::operator crane::grpc::ResourceV2() const {
  crane::grpc::ResourceV2 val{};

  auto* grpc_each_node_res = val.mutable_each_node_res();
  for (const auto& [node_id, res_in_node] : this->each_node_res_map) {
    auto& grpc_res_in_node = (*grpc_each_node_res)[node_id];
    grpc_res_in_node = static_cast<crane::grpc::ResourceInNode>(res_in_node);
  }

  return val;
}

ResourceV2& ResourceV2::operator=(const crane::grpc::ResourceV2& rhs) {
  this->each_node_res_map.clear();

  for (const auto& [node_id, res_in_node] : rhs.each_node_res())
    this->each_node_res_map.emplace(node_id, res_in_node);

  return *this;
}

ResourceInNode& ResourceV2::at(const std::string& craned_id) {
  return this->each_node_res_map.at(craned_id);
}

const ResourceInNode& ResourceV2::at(const std::string& craned_id) const {
  return this->each_node_res_map.at(craned_id);
}

ResourceV2& ResourceV2::operator+=(const ResourceV2& rhs) {
  for (const auto& [rhs_node_id, rhs_res_in_node] : rhs.each_node_res_map) {
    this->each_node_res_map[rhs_node_id] += rhs_res_in_node;
  }

  return *this;
}

ResourceV2& ResourceV2::operator-=(const ResourceV2& rhs) {
  for (const auto& [rhs_node_id, rhs_res_in_node] : rhs.each_node_res_map) {
    auto rhs_it = this->each_node_res_map.find(rhs_node_id);
    if (rhs_it == this->each_node_res_map.end()) continue;

    rhs_it->second -= rhs_res_in_node;

    if (rhs_it->second.IsZero()) this->each_node_res_map.erase(rhs_it);
  }

  return *this;
}

ResourceV2& ResourceV2::AddResourceInNode(const std::string& craned_id,
                                          const ResourceInNode& rhs) {
  this->each_node_res_map[craned_id] += rhs;
  return *this;
}

ResourceV2& ResourceV2::SubtractResourceInNode(const std::string& craned_id,
                                               const ResourceInNode& rhs) {
  auto this_node_it = this->each_node_res_map.find(craned_id);
  if (this_node_it == this->each_node_res_map.end()) return *this;

  this_node_it->second -= rhs;

  if (this_node_it->second.IsZero())
    this->each_node_res_map.erase(this_node_it);

  return *this;
}

bool ResourceV2::IsZero() const { return each_node_res_map.empty(); }

void ResourceV2::SetToZero() { each_node_res_map.clear(); }

bool operator<=(const ResourceV2& lhs, const ResourceV2& rhs) {
  for (const auto& [lhs_node_id, lhs_res_in_node] : lhs.each_node_res_map) {
    auto rhs_it = rhs.each_node_res_map.find(lhs_node_id);
    if (rhs_it == rhs.each_node_res_map.end()) return false;

    if (!(lhs_res_in_node <= rhs_it->second)) return false;
  }

  return true;
}

bool operator==(const ResourceV2& lhs, const ResourceV2& rhs) {
  return lhs.each_node_res_map == rhs.each_node_res_map;
}

ResourceView::ResourceView(const crane::grpc::ResourceView& rhs) {
  allocatable_res = rhs.allocatable_res();
  device_map = FromGrpcDeviceMap(rhs.device_map());
}

ResourceView::operator crane::grpc::ResourceView() const {
  crane::grpc::ResourceView val{};

  auto* mutable_allocatable_res = val.mutable_allocatable_res();
  *mutable_allocatable_res =
      static_cast<crane::grpc::AllocatableResource>(allocatable_res);

  auto* mutable_dev_map = val.mutable_device_map();
  *mutable_dev_map = ToGrpcDeviceMap(device_map);

  return val;
}

ResourceView& ResourceView::operator+=(const ResourceV2& rhs) {
  for (const auto& [_, rhs_res_in_node] : rhs.each_node_res_map)
    *this += rhs_res_in_node;

  return *this;
}

ResourceView& ResourceView::operator-=(const ResourceV2& rhs) {
  for (const auto& [_, rhs_res_in_node] : rhs.each_node_res_map)
    *this -= rhs_res_in_node;

  return *this;
}

ResourceView& ResourceView::operator+=(const ResourceInNode& rhs) {
  this->device_map += rhs.dedicated_res;
  this->allocatable_res += rhs.allocatable_res;

  return *this;
}

ResourceView& ResourceView::operator-=(const ResourceInNode& rhs) {
  this->device_map -= rhs.dedicated_res;

  ABSL_ASSERT(rhs.allocatable_res <= this->allocatable_res);
  this->allocatable_res -= rhs.allocatable_res;

  return *this;
}

ResourceView& ResourceView::operator+=(const AllocatableResource& rhs) {
  this->allocatable_res += rhs;
  return *this;
}

ResourceView& ResourceView::operator-=(const AllocatableResource& rhs) {
  ABSL_ASSERT(rhs <= this->allocatable_res);
  this->allocatable_res -= rhs;
  return *this;
}

ResourceView& ResourceView::operator+=(const DedicatedResourceInNode& rhs) {
  this->device_map += rhs;
  return *this;
}

ResourceView& ResourceView::operator-=(const DedicatedResourceInNode& rhs) {
  this->device_map -= rhs;
  return *this;
}

bool ResourceView::IsZero() const {
  return device_map.empty() && allocatable_res.IsZero();
}

void ResourceView::SetToZero() {
  device_map.clear();
  allocatable_res.SetToZero();
}

double ResourceView::CpuCount() const {
  return static_cast<double>(allocatable_res.cpu_count);
}

uint64_t ResourceView::MemoryBytes() const {
  return allocatable_res.memory_bytes;
}

bool ResourceView::GetFeasibleResourceInNode(const ResourceInNode& avail_res,
                                             ResourceInNode* feasible_res) {
  if (avail_res.allocatable_res < this->allocatable_res) return false;

  feasible_res->allocatable_res = this->allocatable_res;

  // chose slot for each node gres request
  for (const auto& [dev_name, name_type_req] : this->device_map) {
    auto dres_avail_it =
        avail_res.dedicated_res.name_type_slots_map.find(dev_name);
    if (dres_avail_it == avail_res.dedicated_res.name_type_slots_map.end())
      return false;

    const auto& dres_avail = dres_avail_it->second;

    // e.g. GPU -> (untyped_cnt: 2 , typed_cnt_map:{A100:2, H100:1})
    uint64_t untyped_cnt = name_type_req.first;
    const auto& typed_cnt_map = name_type_req.second;

    auto& feasible_res_dev_name = feasible_res->dedicated_res[dev_name];

    for (const auto& [dev_type, typed_cnt] : typed_cnt_map) {
      auto avail_slots_it = dres_avail.type_slots_map.find(dev_type);
      if (avail_slots_it == dres_avail.type_slots_map.end()) return false;

      const auto& avail_slots = avail_slots_it->second;
      auto& feasible_res_dev_name_type = feasible_res_dev_name[dev_type];

      if (avail_slots.size() < typed_cnt) return false;

      auto it = avail_slots.begin();
      for (size_t i = 0; i < typed_cnt; ++i, ++it)
        feasible_res_dev_name_type.emplace(*it);

      for (; untyped_cnt > 0 && it != avail_slots.end(); ++it, --untyped_cnt)
        feasible_res_dev_name_type.emplace(*it);
    }

    // If there are still untyped slots to be allocated,
    // we need to find the remaining slots from other types.
    if (untyped_cnt > 0) {
      for (const auto& [type, slots] : dres_avail.type_slots_map) {
        if (typed_cnt_map.contains(type)) continue;

        auto it = slots.begin();
        for (; untyped_cnt > 0 && it != slots.end(); ++it, --untyped_cnt)
          feasible_res_dev_name[type].emplace(*it);

        if (untyped_cnt == 0) break;
      }
    }

    if (untyped_cnt != 0) return false;
  }

  return true;
}

ResourceView operator*(const ResourceView& lhs, uint32_t rhs) {
  ResourceView result(lhs);
  result.allocatable_res *= rhs;
  result.device_map *= rhs;

  return result;
}

bool operator<=(const ResourceView& lhs, const ResourceInNode& rhs) {
  return lhs.device_map <= rhs.dedicated_res &&
         lhs.allocatable_res <= rhs.allocatable_res;
}

bool operator<=(const ResourceView& lhs, const ResourceView& rhs) {
  return lhs.device_map <= rhs.device_map &&
         lhs.allocatable_res <= rhs.allocatable_res;
}

CgroupSpec::CgroupSpec(const crane::grpc::JobSpec& job_spec) {
  job_id = job_spec.job_id();
  uid = job_spec.uid();
  res_in_node = job_spec.res();
  execution_node = job_spec.execution_node();
  recovered = false;
}

CgroupSpec::CgroupSpec(const task_id_t job_id, const uid_t uid,
                       const ResourceInNode& res_in_node,
                       const std::string& execution_node)
    : job_id(job_id),
      uid(uid),
      res_in_node(res_in_node),
      execution_node(execution_node),
      recovered(false) {}

void CgroupSpec::SetJobSpec(crane::grpc::JobSpec* job_spec) {
  job_spec->set_job_id(this->job_id);
  job_spec->set_uid(this->uid);
  *job_spec->mutable_res() = std::move(this->res_in_node);
  job_spec->set_execution_node(this->execution_node);
}
