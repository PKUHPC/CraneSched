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

bool operator==(const Device& lhs, const Device& rhs) {
  return lhs.path == rhs.path;
}

bool operator<=(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  for (const auto& [lhs_node_id, lhs_gres] : lhs.craned_id_gres_map) {
    if (!rhs.contains(lhs_node_id)) {
      if (lhs_gres.empty())
        continue;
      else
        return false;
    }

    for (const auto& [lhs_name, lhs_type_slots_map] :
         lhs_gres.name_type_slots_map) {
      if (!rhs.at(lhs_node_id).contains(lhs_name)) {
        if (lhs_type_slots_map.empty())
          continue;
        else
          return false;
      }

      for (const auto& [lhs_type, lhs_slots] :
           lhs_type_slots_map.type_slots_map) {
        if (!rhs.at(lhs_node_id).at(lhs_name).contains(lhs_type)) {
          if (lhs_slots.empty())
            continue;
          else
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

template <typename T>
concept HasContainsMethod = requires(T t, const std::string& str) {
  { t.contains(str) } -> std::same_as<bool>;
};

template <typename T>
concept HasAtMethod = requires(T t, const std::string& str) {
  { t.at(str) };
};

template <typename T>
concept HasEmptyMethod = requires(T t) {
  { t.empty() } -> std::same_as<bool>;
};

template <typename T>
  requires HasContainsMethod<T> && HasAtMethod<T> &&
           HasEmptyMethod<
               decltype(std::declval<T>().at(std::declval<std::string>()))>

int checkEmpty(const T& lhs, const T& rhs, const std::string& key, bool& eq) {
  if (lhs.contains(key) && !rhs.contains(key)) {
    if (!lhs.at(key).empty()) {
      return -1;
    } else {
      return 1;
    }
  } else if (!lhs.contains(key) && rhs.contains(key)) {
    if (!rhs.at(key).empty()) eq = false;
    return 1;
  } else if (!lhs.contains(key) && !rhs.contains(key)) {
    return 1;
  }
  return 0;
}

bool operator<(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  std::set<CranedId> craned_ids;
  std::set<std::string> names;
  std::set<std::string> types;
  bool all_element_equ_or_empty = true;
  lhs.flat_(craned_ids, names, types);
  rhs.flat_(craned_ids, names, types);
  for (const auto& craned_id : craned_ids) {
    auto val = checkEmpty(lhs, rhs, craned_id, all_element_equ_or_empty);
    if (val == 1) {
      continue;
    } else if (val == -1) {
      return false;
    }
    const auto& lhs_res_in_node = lhs.at(craned_id);
    const auto& rhs_res_in_node = rhs.at(craned_id);
    for (const auto& name : names) {
      val = checkEmpty(lhs_res_in_node, rhs_res_in_node, name,
                       all_element_equ_or_empty);
      if (val == 1) {
        continue;
      } else if (val == -1) {
        return false;
      }
      const auto& lhs_type_slots_map = lhs_res_in_node.at(craned_id);
      const auto& rhs_type_slots_map = rhs_res_in_node.at(craned_id);
      for (const auto& type : types) {
        val = checkEmpty(lhs_type_slots_map, rhs_type_slots_map, type,
                         all_element_equ_or_empty);
        if (val == 1) {
          continue;
        } else if (val == -1) {
          return false;
        } else {
          auto lhs_size = lhs_type_slots_map.at(type).size();
          auto rhs_size = rhs_type_slots_map.at(type).size();
          if (lhs_size > rhs_size) {
            return false;
          } else {
            if (std::ranges::includes(rhs_type_slots_map.at(type),
                                      lhs_type_slots_map.at(type))) {
              if (lhs_size < rhs_size) all_element_equ_or_empty = false;
            } else
              return false;
          }
        }
      }
    }
  }

  if (all_element_equ_or_empty) return false;
  return true;
}

bool operator==(const DedicatedResource& lhs, const DedicatedResource& rhs) {
  std::set<CranedId> craned_ids;
  for (const auto& [craned_id, _] : lhs.craned_id_gres_map)
    craned_ids.emplace(craned_id);
  for (const auto& [craned_id, _] : rhs.craned_id_gres_map)
    craned_ids.emplace(craned_id);

  for (const auto& craned_id : craned_ids) {
    if (lhs.contains(craned_id) && !rhs.contains(craned_id)) {
      if (lhs.at(craned_id).empty())
        continue;
      else
        return false;
    } else if (!lhs.contains(craned_id) && rhs.contains(craned_id)) {
      if (rhs.at(craned_id).empty())
        continue;
      else
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
        if (untyped_req_count >= avail_count) break;
      }
      if (untyped_req_count >= avail_count) return false;
    } else {
      if (untyped_req_count != 0) return false;
    }
  }
  return true;
}

bool operator<=(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs) {
  for (const auto& [lhs_name, lhs_type_slots_map] : lhs.name_type_slots_map) {
    if (!rhs.contains(lhs_name)) {
      if (lhs_type_slots_map.empty())
        continue;
      else
        return false;
    }

    for (const auto& [lhs_type, lhs_slots] :
         lhs_type_slots_map.type_slots_map) {
      if (!rhs.at(lhs_name).contains(lhs_type)) {
        if (lhs_slots.empty())
          continue;
        else
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
    if (lhs.contains(name) && !rhs.contains(name)) {
      if (!lhs.at(name).empty())
        return false;
      else
        continue;
    } else if (!lhs.contains(name) && rhs.contains(name)) {
      if (!rhs.at(name).empty())
        return false;
      else
        continue;
    } else if (lhs.contains(name) && rhs.contains(name)) {
      const auto& lhs_type_slots_map = lhs.at(name);
      const auto& rhs_type_slots_map = rhs.at(name);
      for (const auto& type : types) {
        if (lhs_type_slots_map.contains(type) &&
            !rhs_type_slots_map.contains(type)) {
          if (!lhs_type_slots_map.at(type).empty())
            return false;
          else
            continue;
        } else if (!lhs_type_slots_map.contains(type) &&
                   rhs_type_slots_map.contains(type)) {
          if (!rhs_type_slots_map.at(type).empty())
            return false;
          else
            continue;
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

bool operator<(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource < rhs.allocatable_resource &&
         lhs.dedicated_resource < rhs.dedicated_resource;
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
    if (!this->craned_id_gres_map.contains(rhs_node_id)) continue;
    this->craned_id_gres_map[rhs_node_id] -= rhs_name_slots_map;
  }

  return *this;
}
bool DedicatedResource::contains(const CranedId& craned_id) const {
  return craned_id_gres_map.contains(craned_id);
}

bool DedicatedResource::Empty() const {
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
void DedicatedResource::flat_(std::set<CranedId>& craned_ids,
                              std::set<std::string>& names,
                              std::set<std::string>& types) const {
  for (const auto& [craned_id, res_in_node] : this->craned_id_gres_map) {
    craned_ids.emplace(craned_id);
    res_in_node.flat_(names, types);
  }
}

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

bool Device::Init(const std::string& device_name,
                  const std::string& device_type,
                  const std::string& device_path) {
  this->type = device_type;
  this->name = device_name;
  const auto& device_major_minor_optype_option =
      GetDeviceFileMajorMinorOpType(device_path);
  if (device_major_minor_optype_option.has_value()) {
    const auto& device_major_minor_optype =
        device_major_minor_optype_option.value();

    this->major = std::get<0>(device_major_minor_optype);
    this->minor = std::get<1>(device_major_minor_optype);
    this->op_type = std::get<2>(device_major_minor_optype);
    this->path = device_path;
  } else {
    return false;
  }
  return true;
}
Device::Device(const std::string& device_name, const std::string& device_type,
               const std::string& device_path)
    : name(device_name), type(device_type), path(device_path){};

bool DedicatedResourceInNode::empty() const {
  if (name_type_slots_map.empty()) return true;
  return std::ranges::all_of(name_type_slots_map,
                             [](const auto& kv) { return kv.second.empty(); });
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
      !name_type_slots_map.at(device_name).type_slots_map.contains(device_type))
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
    auto& this_type_slots_map = this->name_type_slots_map.at(rhs_name);
    for (const auto& [rhs_type, rhs_slots] :
         rhs_type_slots_map.type_slots_map) {
      std::set<SlotId> temp;
      std::ranges::set_difference(this_type_slots_map.at(rhs_type), rhs_slots,
                                  std::inserter(temp, temp.begin()));
      this_type_slots_map[rhs_type] = std::move(temp);
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
void DedicatedResourceInNode::setGrpcDeviceMap(
    crane::grpc::DeviceMap* device_map) const {
  for (const auto& [device_name, type_slots_map] : this->name_type_slots_map) {
    auto* mutable_name_type_map = device_map->mutable_name_type_map();
    if (!mutable_name_type_map->contains(device_name)) {
      mutable_name_type_map->emplace(device_name, crane::grpc::TypeCountMap{});
    }
    for (const auto& [device_type, slots] : type_slots_map.type_slots_map) {
      if (slots.size() == 0) continue;
      auto* mutable_type_count_map =
          mutable_name_type_map->at(device_name).mutable_type_count_map();
      if (!mutable_type_count_map->contains(device_type))
        mutable_type_count_map->emplace(device_type, 1);
      else
        mutable_type_count_map->at(device_type) += 1;
    }
  }
}

bool DedicatedResourceInNode::TypeSlotsMap::empty() const {
  return std::ranges::all_of(type_slots_map,
                             [](const auto& kv) { return kv.second.empty(); });
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
