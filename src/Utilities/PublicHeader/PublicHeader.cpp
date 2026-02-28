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

// ==================== GresCount ====================

GresCount& GresCount::operator+=(const GresCount& rhs) {
  total += rhs.total;
  for (const auto& [type, cnt] : rhs.specified) {
    specified[type] += cnt;
  }
  return *this;
}

GresCount& GresCount::operator-=(const GresCount& rhs) {
  ABSL_ASSERT(rhs.total <= total);
  total -= rhs.total;
  for (const auto& [type, cnt] : rhs.specified) {
    auto it = specified.find(type);
    ABSL_ASSERT(it != specified.end() && cnt <= it->second);
    it->second -= cnt;
    if (it->second == 0) {
      specified.erase(it);
    }
  }
  return *this;
}

GresCount& GresCount::operator*=(uint32_t rhs) {
  total *= rhs;
  for (auto& [_, cnt] : specified) {
    cnt *= rhs;
  }
  return *this;
}

bool operator==(const GresCount& lhs, const GresCount& rhs) {
  return lhs.total == rhs.total && lhs.specified == rhs.specified;
}

bool operator<=(const GresCount& lhs, const GresCount& rhs) {
  // Check total
  if (lhs.total > rhs.total) return false;

  // Check each specified type
  for (const auto& [type, lhs_cnt] : lhs.specified) {
    auto rhs_it = rhs.specified.find(type);
    if (rhs_it == rhs.specified.end()) return false;
    if (lhs_cnt > rhs_it->second) return false;
  }

  return true;
}

uint64_t operator/(const GresCount& lhs, const GresCount& rhs) {
  uint64_t min_quotient = std::numeric_limits<uint64_t>::max();

  // Division for total
  if (rhs.total > 0) {
    min_quotient = std::min(min_quotient, lhs.total / rhs.total);
  }

  // Division for each specified type in rhs
  for (const auto& [type, rhs_cnt] : rhs.specified) {
    if (rhs_cnt == 0) continue;

    auto lhs_it = lhs.specified.find(type);
    if (lhs_it == lhs.specified.end()) {
      return 0;  // lhs doesn't have this type
    }
    min_quotient = std::min(min_quotient, lhs_it->second / rhs_cnt);
  }

  return min_quotient;
}

GresCount GresCountMax(const GresCount& lhs, const GresCount& rhs) {
  GresCount result;
  result.total = std::max(lhs.total, rhs.total);

  // Collect all types
  std::set<std::string> all_types;
  for (const auto& [type, _] : lhs.specified) all_types.insert(type);
  for (const auto& [type, _] : rhs.specified) all_types.insert(type);

  for (const auto& type : all_types) {
    uint64_t l = lhs.specified.count(type) ? lhs.specified.at(type) : 0;
    uint64_t r = rhs.specified.count(type) ? rhs.specified.at(type) : 0;
    result.specified[type] = std::max(l, r);
  }

  return result;
}

GresCount GresCountMin(const GresCount& lhs, const GresCount& rhs) {
  GresCount result;
  result.total = std::min(lhs.total, rhs.total);

  // Only include types present in both
  for (const auto& [type, lhs_cnt] : lhs.specified) {
    auto rhs_it = rhs.specified.find(type);
    if (rhs_it != rhs.specified.end()) {
      result.specified[type] = std::min(lhs_cnt, rhs_it->second);
    }
  }

  return result;
}

GresCount::GresCount(const crane::grpc::GresCount& rhs) {
  total = rhs.total();
  for (const auto& [type, cnt] : rhs.specified()) {
    specified[type] = cnt;
  }
}

GresCount::operator crane::grpc::GresCount() const {
  crane::grpc::GresCount val;
  val.set_total(total);
  for (const auto& [type, cnt] : specified) {
    (*val.mutable_specified())[type] = cnt;
  }
  return val;
}

crane::grpc::GresMap ToGrpcGresMap(const GresMap& gres_map) {
  crane::grpc::GresMap grpc_gres_map;
  for (const auto& [name, gc] : gres_map) {
    (*grpc_gres_map.mutable_name_gres_map())[name] =
        static_cast<crane::grpc::GresCount>(gc);
  }
  return grpc_gres_map;
}

GresMap FromGrpcGresMap(const crane::grpc::GresMap& grpc_gres_map) {
  GresMap gres_map;
  for (const auto& [name, grpc_gc] : grpc_gres_map.name_gres_map()) {
    gres_map.emplace(name, GresCount(grpc_gc));
  }
  return gres_map;
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

// ==================== ResourceView ====================

ResourceView::ResourceView(const crane::grpc::ResourceView& rhs) {
  cpu_count_ = cpu_t{rhs.cpu_count()};
  memory_bytes_ = rhs.memory_bytes();
  memory_sw_bytes_ = rhs.memory_sw_bytes();
  gres_map_ = FromGrpcGresMap(rhs.gres_map());
}

ResourceView& ResourceView::operator=(const crane::grpc::ResourceView& rhs) {
  cpu_count_ = cpu_t{rhs.cpu_count()};
  memory_bytes_ = rhs.memory_bytes();
  memory_sw_bytes_ = rhs.memory_sw_bytes();
  gres_map_ = FromGrpcGresMap(rhs.gres_map());
  return *this;
}

ResourceView::operator crane::grpc::ResourceView() const {
  crane::grpc::ResourceView val{};
  val.set_cpu_count(static_cast<double>(cpu_count_));
  val.set_memory_bytes(memory_bytes_);
  val.set_memory_sw_bytes(memory_sw_bytes_);
  *val.mutable_gres_map() = ToGrpcGresMap(gres_map_);
  return val;
}

ResourceView& ResourceView::operator+=(const ResourceV3& rhs) {
  for (const auto& [_, rhs_res_in_node] : rhs.EachNodeResMap())
    *this += rhs_res_in_node;
  return *this;
}

ResourceView& ResourceView::operator-=(const ResourceV3& rhs) {
  for (const auto& [_, rhs_res_in_node] : rhs.EachNodeResMap())
    *this -= rhs_res_in_node;
  return *this;
}

ResourceView& ResourceView::operator+=(const ResourceInNodeV3& rhs) {
  cpu_count_ += rhs.GetCpuSet().cpu_count;
  memory_bytes_ += rhs.GetMemoryBytes();
  memory_sw_bytes_ += rhs.GetMemorySwBytes();
  *this += rhs.GetGres();
  return *this;
}

ResourceView& ResourceView::operator-=(const ResourceInNodeV3& rhs) {
  ABSL_ASSERT(rhs.GetCpuSet().cpu_count <= cpu_count_);
  ABSL_ASSERT(rhs.GetMemoryBytes() <= memory_bytes_);
  cpu_count_ -= rhs.GetCpuSet().cpu_count;
  memory_bytes_ -= rhs.GetMemoryBytes();
  memory_sw_bytes_ -= rhs.GetMemorySwBytes();
  *this -= rhs.GetGres();
  return *this;
}

ResourceView& ResourceView::operator+=(const DedicatedResourceInNode& rhs) {
  for (const auto& [name, type_slots_map] : rhs.name_type_slots_map) {
    GresCount& gc = gres_map_[name];
    for (const auto& [type, slots] : type_slots_map.type_slots_map) {
      uint64_t cnt = slots.size();
      gc.total += cnt;
      gc.specified[type] += cnt;
    }
  }
  return *this;
}

ResourceView& ResourceView::operator-=(const DedicatedResourceInNode& rhs) {
  for (const auto& [name, type_slots_map] : rhs.name_type_slots_map) {
    auto it = gres_map_.find(name);
    ABSL_ASSERT(it != gres_map_.end());
    GresCount& gc = it->second;
    for (const auto& [type, slots] : type_slots_map.type_slots_map) {
      uint64_t cnt = slots.size();
      ABSL_ASSERT(gc.total >= cnt);
      gc.total -= cnt;
      auto type_it = gc.specified.find(type);
      ABSL_ASSERT(type_it != gc.specified.end() && type_it->second >= cnt);
      type_it->second -= cnt;
      if (type_it->second == 0) gc.specified.erase(type_it);
    }
    if (gc.IsZero()) gres_map_.erase(it);
  }
  return *this;
}

ResourceView& ResourceView::operator+=(const ResourceView& rhs) {
  cpu_count_ += rhs.cpu_count_;
  memory_bytes_ += rhs.memory_bytes_;
  memory_sw_bytes_ += rhs.memory_sw_bytes_;
  for (const auto& [name, gc] : rhs.gres_map_) {
    gres_map_[name] += gc;
  }
  return *this;
}

ResourceView& ResourceView::operator-=(const ResourceView& rhs) {
  ABSL_ASSERT(rhs.cpu_count_ <= cpu_count_);
  ABSL_ASSERT(rhs.memory_bytes_ <= memory_bytes_);
  cpu_count_ -= rhs.cpu_count_;
  memory_bytes_ -= rhs.memory_bytes_;
  memory_sw_bytes_ -= rhs.memory_sw_bytes_;
  for (const auto& [name, rhs_gc] : rhs.gres_map_) {
    auto it = gres_map_.find(name);
    ABSL_ASSERT(it != gres_map_.end());
    it->second -= rhs_gc;
    if (it->second.IsZero()) gres_map_.erase(it);
  }
  return *this;
}

ResourceView& ResourceView::operator*=(uint32_t rhs) {
  cpu_count_ *= rhs;
  memory_bytes_ *= rhs;
  memory_sw_bytes_ *= rhs;
  for (auto& [_, gc] : gres_map_) {
    gc *= rhs;
  }
  return *this;
}

bool ResourceView::IsZero() const {
  return cpu_count_ == cpu_t{0} && memory_bytes_ == 0 &&
         memory_sw_bytes_ == 0 && gres_map_.empty();
}

void ResourceView::SetToZero() {
  cpu_count_ = cpu_t{0};
  memory_bytes_ = 0;
  memory_sw_bytes_ = 0;
  gres_map_.clear();
}

cpu_t ResourceView::GetCpuCount() const { return cpu_count_; }
void ResourceView::SetCpuCount(cpu_t count) { cpu_count_ = count; }

uint64_t ResourceView::GetMemoryBytes() const { return memory_bytes_; }
void ResourceView::SetMemoryBytes(uint64_t bytes) { memory_bytes_ = bytes; }

uint64_t ResourceView::GetMemorySwBytes() const { return memory_sw_bytes_; }
void ResourceView::SetMemorySwBytes(uint64_t bytes) {
  memory_sw_bytes_ = bytes;
}

const GresMap& ResourceView::GetGresMap() const { return gres_map_; }
GresMap& ResourceView::GetGresMap() { return gres_map_; }

double ResourceView::CpuCountDouble() const {
  return static_cast<double>(cpu_count_);
}

uint64_t ResourceView::GpuCount() const {
  auto it = gres_map_.find(kResourceTypeGpu);
  if (it == gres_map_.end()) return 0;
  return it->second.total;
}

bool ResourceView::GetFeasibleResourceInNode(
    const ResourceInNodeV3& avail_res, ResourceInNodeV3* feasible_res) const {
  // Check CPU and memory
  if (cpu_count_ > avail_res.GetCpuSet().cpu_count) return false;
  if (memory_bytes_ > avail_res.GetMemoryBytes()) return false;

  // TODO: Allocate specific CPU core IDs here in the future.
  // Currently only sets cpu_count; craned decides core binding.
  feasible_res->GetCpuSet().cpu_count = cpu_count_;
  feasible_res->SetMemoryBytes(memory_bytes_);
  feasible_res->SetMemorySwBytes(memory_sw_bytes_);

  // Choose slots for each GRES request
  const auto& avail_gres = avail_res.GetGres();
  auto& feasible_gres = feasible_res->GetGres();

  for (const auto& [dev_name, gres_req] : gres_map_) {
    auto dres_avail_it = avail_gres.name_type_slots_map.find(dev_name);
    if (dres_avail_it == avail_gres.name_type_slots_map.end()) return false;

    const auto& dres_avail = dres_avail_it->second;

    // Calculate untyped (generic) request = total - sum(specified)
    uint64_t specified_sum = 0;
    for (const auto& [_, cnt] : gres_req.specified) specified_sum += cnt;
    uint64_t untyped_cnt =
        gres_req.total > specified_sum ? gres_req.total - specified_sum : 0;

    auto& feasible_res_dev_name = feasible_gres[dev_name];

    // First allocate specified types
    for (const auto& [dev_type, typed_cnt] : gres_req.specified) {
      auto avail_slots_it = dres_avail.type_slots_map.find(dev_type);
      if (avail_slots_it == dres_avail.type_slots_map.end()) return false;

      const auto& avail_slots = avail_slots_it->second;
      if (avail_slots.size() < typed_cnt) return false;

      auto& feasible_res_dev_name_type = feasible_res_dev_name[dev_type];
      auto it = avail_slots.begin();
      for (size_t i = 0; i < typed_cnt; ++i, ++it)
        feasible_res_dev_name_type.emplace(*it);

      // Use remaining slots of this type for untyped requests
      for (; untyped_cnt > 0 && it != avail_slots.end(); ++it, --untyped_cnt)
        feasible_res_dev_name_type.emplace(*it);
    }

    // Allocate remaining untyped from other types
    if (untyped_cnt > 0) {
      for (const auto& [type, slots] : dres_avail.type_slots_map) {
        if (gres_req.specified.contains(type)) continue;

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
  result *= rhs;
  return result;
}

ResourceView operator+(const ResourceView& lhs, const ResourceView& rhs) {
  ResourceView result(lhs);
  result += rhs;
  return result;
}

ResourceView operator-(const ResourceView& lhs, const ResourceView& rhs) {
  ResourceView result(lhs);
  result -= rhs;
  return result;
}

bool operator<=(const ResourceView& lhs, const ResourceInNodeV3& rhs) {
  if (lhs.cpu_count_ > rhs.GetCpuSet().cpu_count) return false;
  if (lhs.memory_bytes_ > rhs.GetMemoryBytes()) return false;

  // Check GRES: ResourceView uses GresMap (counts), ResourceInNodeV3 uses
  // DedicatedResourceInNode (slot IDs)
  for (const auto& [name, lhs_gc] : lhs.gres_map_) {
    auto rhs_it = rhs.GetGres().name_type_slots_map.find(name);
    if (rhs_it == rhs.GetGres().name_type_slots_map.end()) return false;

    const auto& rhs_type_slots = rhs_it->second;

    // Check specified types
    for (const auto& [type, lhs_cnt] : lhs_gc.specified) {
      auto type_it = rhs_type_slots.type_slots_map.find(type);
      if (type_it == rhs_type_slots.type_slots_map.end()) return false;
      if (lhs_cnt > type_it->second.size()) return false;
    }

    // Check total
    uint64_t rhs_total = 0;
    for (const auto& [_, slots] : rhs_type_slots.type_slots_map)
      rhs_total += slots.size();
    if (lhs_gc.total > rhs_total) return false;
  }

  return true;
}

bool operator<=(const ResourceView& lhs, const ResourceView& rhs) {
  if (lhs.cpu_count_ > rhs.cpu_count_) return false;
  if (lhs.memory_bytes_ > rhs.memory_bytes_) return false;

  for (const auto& [name, lhs_gc] : lhs.gres_map_) {
    auto rhs_it = rhs.gres_map_.find(name);
    if (rhs_it == rhs.gres_map_.end()) return false;
    if (!(lhs_gc <= rhs_it->second)) return false;
  }

  return true;
}

uint64_t operator/(const ResourceView& lhs, const ResourceView& rhs) {
  uint64_t min_quotient = std::numeric_limits<uint64_t>::max();

  // CPU division
  if (rhs.cpu_count_ > cpu_t{0}) {
    uint64_t cpu_quotient =
        static_cast<uint64_t>(lhs.cpu_count_ / rhs.cpu_count_);
    min_quotient = std::min(min_quotient, cpu_quotient);
  }

  // Memory division
  if (rhs.memory_bytes_ > 0) {
    uint64_t mem_quotient = lhs.memory_bytes_ / rhs.memory_bytes_;
    min_quotient = std::min(min_quotient, mem_quotient);
  }

  // Memory+Swap division
  if (rhs.memory_sw_bytes_ > 0) {
    uint64_t mem_sw_quotient = lhs.memory_sw_bytes_ / rhs.memory_sw_bytes_;
    min_quotient = std::min(min_quotient, mem_sw_quotient);
  }

  // GRES division
  for (const auto& [name, rhs_gc] : rhs.gres_map_) {
    auto lhs_it = lhs.gres_map_.find(name);
    if (lhs_it == lhs.gres_map_.end()) {
      return 0;
    }
    uint64_t gres_quotient = lhs_it->second / rhs_gc;
    min_quotient = std::min(min_quotient, gres_quotient);
  }

  return min_quotient;
}

ResourceView ResourceView::Max(const ResourceView& lhs,
                               const ResourceView& rhs) {
  ResourceView result;
  result.cpu_count_ = std::max(lhs.cpu_count_, rhs.cpu_count_);
  result.memory_bytes_ = std::max(lhs.memory_bytes_, rhs.memory_bytes_);
  result.memory_sw_bytes_ =
      std::max(lhs.memory_sw_bytes_, rhs.memory_sw_bytes_);

  // Collect all GRES names
  std::set<std::string> all_names;
  for (const auto& [name, _] : lhs.gres_map_) all_names.insert(name);
  for (const auto& [name, _] : rhs.gres_map_) all_names.insert(name);

  for (const auto& name : all_names) {
    auto lhs_it = lhs.gres_map_.find(name);
    auto rhs_it = rhs.gres_map_.find(name);

    if (lhs_it != lhs.gres_map_.end() && rhs_it != rhs.gres_map_.end()) {
      result.gres_map_[name] = GresCountMax(lhs_it->second, rhs_it->second);
    } else if (lhs_it != lhs.gres_map_.end()) {
      result.gres_map_[name] = lhs_it->second;
    } else {
      result.gres_map_[name] = rhs_it->second;
    }
  }

  return result;
}

ResourceView ResourceView::Min(const ResourceView& lhs,
                               const ResourceView& rhs) {
  ResourceView result;
  result.cpu_count_ = std::min(lhs.cpu_count_, rhs.cpu_count_);
  result.memory_bytes_ = std::min(lhs.memory_bytes_, rhs.memory_bytes_);
  result.memory_sw_bytes_ =
      std::min(lhs.memory_sw_bytes_, rhs.memory_sw_bytes_);

  // Only include GRES present in both
  for (const auto& [name, lhs_gc] : lhs.gres_map_) {
    auto rhs_it = rhs.gres_map_.find(name);
    if (rhs_it != rhs.gres_map_.end()) {
      result.gres_map_[name] = GresCountMin(lhs_gc, rhs_it->second);
    }
  }

  return result;
}

// ==================== CpuSet ====================

CpuSet::CpuSet(std::set<uint32_t> ids)
    : core_ids(std::move(ids)),
      cpu_count{static_cast<int32_t>(core_ids.size())} {}

CpuSet::CpuSet(cpu_t count) : cpu_count(count) {}

CpuSet& CpuSet::operator+=(const CpuSet& rhs) {
  core_ids.insert(rhs.core_ids.begin(), rhs.core_ids.end());
  cpu_count += rhs.cpu_count;
  return *this;
}

CpuSet& CpuSet::operator-=(const CpuSet& rhs) {
  for (const auto& id : rhs.core_ids) {
    auto it = core_ids.find(id);
    ABSL_ASSERT(it != core_ids.end());
    core_ids.erase(it);
  }
  ABSL_ASSERT(cpu_count >= rhs.cpu_count);
  cpu_count -= rhs.cpu_count;
  return *this;
}

bool CpuSet::IsZero() const {
  return core_ids.empty() && cpu_count == cpu_t{0};
}

void CpuSet::SetToZero() {
  core_ids.clear();
  cpu_count = cpu_t{0};
}

bool CpuSet::IsInteger() const { return !core_ids.empty(); }

// ==================== ResourceInNodeV3 ====================

ResourceInNodeV3& ResourceInNodeV3::operator+=(const ResourceInNodeV3& rhs) {
  cpu_set_ += rhs.cpu_set_;
  memory_bytes_ += rhs.memory_bytes_;
  memory_sw_bytes_ += rhs.memory_sw_bytes_;
  gres_ += rhs.gres_;
  return *this;
}

ResourceInNodeV3& ResourceInNodeV3::operator-=(const ResourceInNodeV3& rhs) {
  cpu_set_ -= rhs.cpu_set_;
  ABSL_ASSERT(memory_bytes_ >= rhs.memory_bytes_);
  memory_bytes_ -= rhs.memory_bytes_;
  memory_sw_bytes_ -= rhs.memory_sw_bytes_;
  gres_ -= rhs.gres_;
  return *this;
}

bool ResourceInNodeV3::IsZero() const {
  return cpu_set_.IsZero() && memory_bytes_ == 0 && memory_sw_bytes_ == 0 &&
         gres_.IsZero();
}

bool ResourceInNodeV3::IsExhausted() const {
  return cpu_set_.cpu_count == cpu_t{0} || memory_bytes_ == 0 ||
         memory_sw_bytes_ == 0;
}

void ResourceInNodeV3::SetToZero() {
  cpu_set_.SetToZero();
  memory_bytes_ = 0;
  memory_sw_bytes_ = 0;
  gres_.SetToZero();
}

void ResourceInNodeV3::ckmin(const ResourceInNodeV3& rhs) {
  cpu_set_.cpu_count = std::min(cpu_set_.cpu_count, rhs.cpu_set_.cpu_count);
  if (!cpu_set_.core_ids.empty() && !rhs.cpu_set_.core_ids.empty()) {
    std::set<uint32_t> inter;
    std::ranges::set_intersection(cpu_set_.core_ids, rhs.cpu_set_.core_ids,
                                  std::inserter(inter, inter.begin()));
    cpu_set_.core_ids = std::move(inter);
  }
  memory_bytes_ = std::min(memory_bytes_, rhs.memory_bytes_);
  memory_sw_bytes_ = std::min(memory_sw_bytes_, rhs.memory_sw_bytes_);
  gres_ = Intersection(gres_, rhs.gres_);
}

const CpuSet& ResourceInNodeV3::GetCpuSet() const { return cpu_set_; }
CpuSet& ResourceInNodeV3::GetCpuSet() { return cpu_set_; }

uint64_t ResourceInNodeV3::GetMemoryBytes() const { return memory_bytes_; }
void ResourceInNodeV3::SetMemoryBytes(uint64_t bytes) { memory_bytes_ = bytes; }

uint64_t ResourceInNodeV3::GetMemorySwBytes() const { return memory_sw_bytes_; }
void ResourceInNodeV3::SetMemorySwBytes(uint64_t bytes) {
  memory_sw_bytes_ = bytes;
}

const DedicatedResourceInNode& ResourceInNodeV3::GetGres() const {
  return gres_;
}
DedicatedResourceInNode& ResourceInNodeV3::GetGres() { return gres_; }

ResourceView ResourceInNodeV3::ToResourceView() const {
  ResourceView view;

  // CPU: total count (integer + fractional)
  view.SetCpuCount(cpu_set_.cpu_count);

  // Memory
  view.SetMemoryBytes(memory_bytes_);
  view.SetMemorySwBytes(memory_sw_bytes_);

  // Convert GRES slots to counts
  for (const auto& [name, type_slots_map] : gres_.name_type_slots_map) {
    GresCount& gc = view.GetGresMap()[name];
    for (const auto& [type, slots] : type_slots_map.type_slots_map) {
      uint64_t cnt = slots.size();
      gc.total += cnt;
      gc.specified[type] += cnt;
    }
  }

  return view;
}

ResourceInNodeV3 operator+(const ResourceInNodeV3& lhs,
                           const ResourceInNodeV3& rhs) {
  ResourceInNodeV3 result(lhs);
  result += rhs;
  return result;
}

ResourceInNodeV3 operator-(const ResourceInNodeV3& lhs,
                           const ResourceInNodeV3& rhs) {
  ResourceInNodeV3 result(lhs);
  result -= rhs;
  return result;
}

bool operator<=(const ResourceInNodeV3& lhs, const ResourceInNodeV3& rhs) {
  if (lhs.GetCpuSet().cpu_count > rhs.GetCpuSet().cpu_count) return false;
  if (lhs.GetMemoryBytes() > rhs.GetMemoryBytes()) return false;
  return lhs.GetGres() <= rhs.GetGres();
}

// ==================== ResourceV3 ====================

ResourceV3& ResourceV3::operator+=(const ResourceV3& rhs) {
  for (const auto& [node_id, res_in_node] : rhs.each_node_res_map_) {
    each_node_res_map_[node_id] += res_in_node;
  }
  return *this;
}

ResourceV3& ResourceV3::operator-=(const ResourceV3& rhs) {
  for (const auto& [node_id, res_in_node] : rhs.each_node_res_map_) {
    auto it = each_node_res_map_.find(node_id);
    if (it == each_node_res_map_.end()) continue;

    it->second -= res_in_node;

    if (it->second.IsZero()) {
      each_node_res_map_.erase(it);
    }
  }
  return *this;
}

ResourceV3& ResourceV3::AddResourceInNode(const std::string& craned_id,
                                          const ResourceInNodeV3& rhs) {
  each_node_res_map_[craned_id] += rhs;
  return *this;
}

ResourceV3& ResourceV3::SubtractResourceInNode(const std::string& craned_id,
                                               const ResourceInNodeV3& rhs) {
  auto it = each_node_res_map_.find(craned_id);
  if (it == each_node_res_map_.end()) return *this;

  it->second -= rhs;

  if (it->second.IsZero()) {
    each_node_res_map_.erase(it);
  }
  return *this;
}

ResourceInNodeV3& ResourceV3::at(const std::string& craned_id) {
  return each_node_res_map_.at(craned_id);
}

const ResourceInNodeV3& ResourceV3::at(const std::string& craned_id) const {
  return each_node_res_map_.at(craned_id);
}

bool ResourceV3::IsZero() const { return each_node_res_map_.empty(); }

void ResourceV3::SetToZero() { each_node_res_map_.clear(); }

ResourceView ResourceV3::View() const noexcept {
  ResourceView view;
  for (const auto& [_, res_in_node] : each_node_res_map_) {
    view += res_in_node.ToResourceView();
  }
  return view;
}

ResourceV3 operator+(const ResourceV3& lhs, const ResourceV3& rhs) {
  ResourceV3 result(lhs);
  result += rhs;
  return result;
}

ResourceV3 operator-(const ResourceV3& lhs, const ResourceV3& rhs) {
  ResourceV3 result(lhs);
  result -= rhs;
  return result;
}

// ==================== ResourceInNodeV3 gRPC Conversion ====================

ResourceInNodeV3::ResourceInNodeV3(const crane::grpc::ResourceInNodeV3& rhs) {
  // CPU
  cpu_set_.core_ids.insert(rhs.cpu_ids().begin(), rhs.cpu_ids().end());
  cpu_set_.cpu_count = cpu_t{rhs.cpu_count()};

  // Memory
  memory_bytes_ = rhs.memory_bytes();
  memory_sw_bytes_ = rhs.memory_sw_bytes();

  // GRES
  gres_ = DedicatedResourceInNode(rhs.gres());
}

ResourceInNodeV3::operator crane::grpc::ResourceInNodeV3() const {
  crane::grpc::ResourceInNodeV3 val;

  // CPU
  val.mutable_cpu_ids()->Assign(cpu_set_.core_ids.begin(),
                                cpu_set_.core_ids.end());
  val.set_cpu_count(static_cast<double>(cpu_set_.cpu_count));

  // Memory
  val.set_memory_bytes(memory_bytes_);
  val.set_memory_sw_bytes(memory_sw_bytes_);

  // GRES
  *val.mutable_gres() =
      static_cast<crane::grpc::DedicatedResourceInNode>(gres_);

  return val;
}

// ==================== ResourceV3 gRPC Conversion ====================

ResourceV3::ResourceV3(const crane::grpc::ResourceV3& rhs) {
  for (const auto& [node_id, res_in_node] : rhs.each_node_res_map()) {
    each_node_res_map_.emplace(node_id, ResourceInNodeV3(res_in_node));
  }
}

ResourceV3::operator crane::grpc::ResourceV3() const {
  crane::grpc::ResourceV3 val;

  for (const auto& [node_id, res_in_node] : each_node_res_map_) {
    (*val.mutable_each_node_res_map())[node_id] =
        static_cast<crane::grpc::ResourceInNodeV3>(res_in_node);
  }

  return val;
}
