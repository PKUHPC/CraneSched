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

#include "CranedMetaContainer.h"

#include "CranedKeeper.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

void CranedMetaContainerSimpleImpl::CranedUp(const CranedId& craned_id) {
  CRANE_ASSERT(craned_id_part_ids_map_.contains(craned_id));
  auto& part_ids = craned_id_part_ids_map_.at(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  CRANE_ASSERT(craned_meta_map_.Contains(craned_id));
  auto node_meta = craned_meta_map_[craned_id];
  node_meta->alive = true;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.res_total += node_meta->res_total;
    part_global_meta.res_avail += node_meta->res_total;
    part_global_meta.alive_craned_cnt++;
  }
  g_thread_pool->detach_task([this, craned_id]() {
    auto stub = g_craned_keeper->GetCranedStub(craned_id);
    if (stub != nullptr && !stub->Invalid()) {
      DedicatedResourceInNode resource;
      stub->QueryActualGres(resource);
      this->AddDedicatedResource(craned_id, resource);
    }
  });
}

void CranedMetaContainerSimpleImpl::CranedDown(const CranedId& craned_id) {
  CRANE_ASSERT(craned_id_part_ids_map_.contains(craned_id));
  auto& part_ids = craned_id_part_ids_map_.at(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  CRANE_ASSERT(craned_meta_map_.Contains(craned_id));
  auto node_meta = craned_meta_map_[craned_id];
  node_meta->alive = false;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.res_total -= node_meta->res_total;
    part_global_meta.res_avail -= node_meta->res_avail;
    part_global_meta.res_in_use -= node_meta->res_in_use;

    part_global_meta.alive_craned_cnt--;
  }

  node_meta->res_total.dedicated_resource -=
      node_meta->res_total.dedicated_resource;

  node_meta->res_avail.allocatable_resource +=
      node_meta->res_in_use.allocatable_resource;

  // clear dedicated_resources,will be set when craned up
  node_meta->res_avail.dedicated_resource -=
      node_meta->res_avail.dedicated_resource;
  // Set the rse_in_use of dead craned to 0
  node_meta->res_in_use -= node_meta->res_in_use;
  node_meta->running_task_resource_map.clear();
}

bool CranedMetaContainerSimpleImpl::CheckCranedOnline(
    const CranedId& craned_id) {
  CRANE_ASSERT(craned_meta_map_.Contains(craned_id));
  auto node_meta = craned_meta_map_.GetValueExclusivePtr(craned_id);
  return node_meta->alive;
}

CranedMetaContainerInterface::PartitionMetaPtr
CranedMetaContainerSimpleImpl::GetPartitionMetasPtr(PartitionId partition_id) {
  return partition_metas_map_.GetValueExclusivePtr(partition_id);
}

CranedMetaContainerInterface::CranedMetaPtr
CranedMetaContainerSimpleImpl::GetCranedMetaPtr(CranedId craned_id) {
  return craned_meta_map_.GetValueExclusivePtr(craned_id);
}

CranedMetaContainerInterface::AllPartitionsMetaMapConstPtr
CranedMetaContainerSimpleImpl::GetAllPartitionsMetaMapConstPtr() {
  return partition_metas_map_.GetMapConstSharedPtr();
}

CranedMetaContainerInterface::CranedMetaMapConstPtr
CranedMetaContainerSimpleImpl::GetCranedMetaMapConstPtr() {
  return craned_meta_map_.GetMapConstSharedPtr();
}

void CranedMetaContainerSimpleImpl::MallocResourceFromNodes(
    const std::list<CranedId>& node_ids, task_id_t task_id,
    const Resources& resources) {
  // Build PartitionId -> set<CranedId> hash map.
  TreeMap<PartitionId, TreeSet<CranedId>> part_id_craned_ids_map;

  for (auto& node_id : node_ids) {
    if (!craned_id_part_ids_map_.contains(node_id)) {
      CRANE_ERROR("Malloc resource on {} not in the craned map", node_id);
      return;
    }

    auto& part_ids = craned_id_part_ids_map_.at(node_id);
    for (auto& part_id : part_ids)
      part_id_craned_ids_map[part_id].emplace(node_id);
  }

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_id_craned_ids_map.size());

  // TODO: Duplicate lock acquiring!
  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (auto it = part_id_craned_ids_map.begin();
       it != part_id_craned_ids_map.end(); ++it) {
    PartitionId const& part_id = it->first;
    CRANE_TRACE("Hold partition exclusive lock for {}.", part_id);

    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());
  }

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_gmeta = partition_meta->partition_global_meta;
    PartitionId const& part_id = part_gmeta.name;

    part_gmeta.res_avail.allocatable_resource -=
        resources.allocatable_resource *
        part_id_craned_ids_map.at(part_id).size();
    part_gmeta.res_in_use.allocatable_resource +=
        resources.allocatable_resource *
        part_id_craned_ids_map.at(part_id).size();

    part_gmeta.res_avail.dedicated_resource -= resources.dedicated_resource;
    part_gmeta.res_in_use.dedicated_resource += resources.dedicated_resource;
  }

  for (const CranedId& node_id : node_ids) {
    if (!craned_meta_map_.Contains(node_id)) {
      CRANE_ERROR("Try to malloc resource from an unknown craned {}", node_id);
      return;
    }

    // Then acquire craned meta lock.
    auto node_meta = craned_meta_map_[node_id];

    node_meta->res_avail.allocatable_resource -= resources.allocatable_resource;
    node_meta->res_in_use.allocatable_resource +=
        resources.allocatable_resource;

    if (resources.dedicated_resource.contains(node_id)) {
      node_meta->res_avail.dedicated_resource[node_id] -=
          resources.dedicated_resource.at(node_id);
      node_meta->res_in_use.dedicated_resource[node_id] +=
          resources.dedicated_resource.at(node_id);
    }

    node_meta->running_task_resource_map.emplace(task_id, resources);
  }
}

void CranedMetaContainerSimpleImpl::FreeResourceFromNodes(
    std::list<CranedId> const& node_ids, uint32_t task_id,
    Resources const& resources) {
  // Build PartitionId -> set<CranedId> hash map.
  TreeMap<PartitionId, TreeSet<CranedId>> part_id_craned_ids_map;

  for (auto& node_id : node_ids) {
    if (!craned_id_part_ids_map_.contains(node_id)) {
      CRANE_ERROR("Free resource on {} not in the craned map", node_id);
      return;
    }

    auto& part_ids = craned_id_part_ids_map_.at(node_id);
    for (auto& part_id : part_ids)
      part_id_craned_ids_map[part_id].emplace(node_id);
  }

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_id_craned_ids_map.size());

  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (auto it = part_id_craned_ids_map.begin();
       it != part_id_craned_ids_map.end(); ++it) {
    PartitionId const& part_id = it->first;
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());
  }

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_gmeta = partition_meta->partition_global_meta;
    PartitionId const& part_id = part_gmeta.name;

    part_gmeta.res_avail.allocatable_resource +=
        resources.allocatable_resource *
        part_id_craned_ids_map.at(part_id).size();
    part_gmeta.res_in_use.allocatable_resource -=
        resources.allocatable_resource *
        part_id_craned_ids_map.at(part_id).size();

    part_gmeta.res_avail.dedicated_resource += resources.dedicated_resource;
    part_gmeta.res_in_use.dedicated_resource -= resources.dedicated_resource;
  }

  for (const CranedId& node_id : node_ids) {
    if (!craned_meta_map_.Contains(node_id)) {
      CRANE_ERROR("Try to free resource from an unknown craned {}", node_id);
      return;
    }

    // Then acquire craned meta lock.
    auto node_meta = craned_meta_map_[node_id];
    if (!node_meta->alive) {
      CRANE_DEBUG(
          "Crane {} has already been down. Ignore FreeResourceFromNodes.",
          node_meta->static_meta.hostname);
      return;
    }

    node_meta->res_avail.allocatable_resource += resources.allocatable_resource;
    node_meta->res_in_use.allocatable_resource -=
        resources.allocatable_resource;

    if (resources.dedicated_resource.contains(node_id)) {
      node_meta->res_avail.dedicated_resource[node_id] +=
          resources.dedicated_resource.at(node_id);
      node_meta->res_in_use.dedicated_resource[node_id] -=
          resources.dedicated_resource.at(node_id);
    }

    node_meta->running_task_resource_map.erase(task_id);
  }
}

void CranedMetaContainerSimpleImpl::InitFromConfig(const Config& config) {
  HashMap<CranedId, CranedMeta> craned_map;
  HashMap<PartitionId, PartitionMeta> partition_map;

  for (auto&& [craned_name, node_ptr] : config.Nodes) {
    CRANE_TRACE("Parsing node {}", craned_name);

    auto& craned_meta = craned_map[craned_name];

    auto& static_meta = craned_meta.static_meta;
    static_meta.res.allocatable_resource.cpu_count =
        cpu_t(config.Nodes.at(craned_name)->cpu);
    static_meta.res.allocatable_resource.memory_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    static_meta.res.allocatable_resource.memory_sw_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    static_meta.res.dedicated_resource =
        config.Nodes.at(craned_name)->dedicated_resource;
    static_meta.hostname = craned_name;
    static_meta.port = std::strtoul(kCranedDefaultPort, nullptr, 10);

    craned_meta.res_total.allocatable_resource =
        static_meta.res.allocatable_resource;
    craned_meta.res_avail.allocatable_resource =
        static_meta.res.allocatable_resource;
  }

  for (auto&& [part_name, partition] : config.Partitions) {
    CRANE_TRACE("Parsing partition {}", part_name);

    Resources part_res;

    auto& part_meta = partition_map[part_name];

    for (auto&& craned_name : partition.nodes) {
      auto& craned_meta = craned_map[craned_name];
      craned_meta.static_meta.partition_ids.emplace_back(part_name);

      craned_id_part_ids_map_[craned_name].emplace_back(part_name);

      part_meta.craned_ids.emplace(craned_name);

      CRANE_DEBUG(
          "Add the resource of Craned {} (cpu: {}, mem: {}, gres: {}) to "
          "partition [{}]'s global resource.",
          craned_name,
          craned_meta.static_meta.res.allocatable_resource.cpu_count,
          util::ReadableMemory(
              craned_meta.static_meta.res.allocatable_resource.memory_bytes),
          util::ReadableGres(craned_meta.static_meta.res.dedicated_resource),
          part_name);

      part_res += craned_meta.static_meta.res;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.res_total_inc_dead = part_res;
    part_meta.partition_global_meta.node_cnt = part_meta.craned_ids.size();
    part_meta.partition_global_meta.nodelist_str = partition.nodelist_str;

    CRANE_DEBUG(
        "partition [{}]'s Global resource now: (cpu: {}, mem: {}, gres: {}). "
        "It has {} craneds.",
        part_name,
        part_meta.partition_global_meta.res_total_inc_dead.allocatable_resource
            .cpu_count,
        util::ReadableMemory(part_meta.partition_global_meta.res_total_inc_dead
                                 .allocatable_resource.memory_bytes),
        util::ReadableGres(part_meta.partition_global_meta.res_total_inc_dead
                               .dedicated_resource),
        part_meta.partition_global_meta.node_cnt);
  }

  craned_meta_map_.InitFromMap(std::move(craned_map));
  partition_metas_map_.InitFromMap(std::move(partition_map));
}

crane::grpc::QueryCranedInfoReply
CranedMetaContainerSimpleImpl::QueryAllCranedInfo() {
  crane::grpc::QueryCranedInfoReply reply;
  auto* list = reply.mutable_craned_info_list();

  auto craned_map = craned_meta_map_.GetMapConstSharedPtr();
  for (auto&& [craned_index, craned_meta_ptr] : *craned_map) {
    auto craned_meta = craned_meta_ptr.GetExclusivePtr();

    auto* craned_info = list->Add();
    SetGrpcCranedInfoByCranedMeta_(*craned_meta, craned_info);
  }
  return reply;
}

crane::grpc::QueryCranedInfoReply
CranedMetaContainerSimpleImpl::QueryCranedInfo(const std::string& node_name) {
  crane::grpc::QueryCranedInfoReply reply;
  auto* list = reply.mutable_craned_info_list();

  if (!craned_meta_map_.Contains(node_name)) {
    return reply;
  }

  auto craned_meta = craned_meta_map_.GetValueExclusivePtr(node_name);

  auto* craned_info = list->Add();
  SetGrpcCranedInfoByCranedMeta_(*craned_meta, craned_info);

  return reply;
}

crane::grpc::QueryPartitionInfoReply
CranedMetaContainerSimpleImpl::QueryAllPartitionInfo() {
  crane::grpc::QueryPartitionInfoReply reply;
  auto* list = reply.mutable_partition_info();

  auto partition_map = partition_metas_map_.GetMapConstSharedPtr();

  for (auto&& [part_name, part_meta_ptr] : *partition_map) {
    auto* part_info = list->Add();
    auto part_meta = part_meta_ptr.GetExclusivePtr();

    part_info->set_name(part_meta->partition_global_meta.name);
    part_info->set_total_nodes(part_meta->partition_global_meta.node_cnt);
    part_info->set_alive_nodes(
        part_meta->partition_global_meta.alive_craned_cnt);

    *part_info->mutable_res_total() = static_cast<crane::grpc::Resources>(
        part_meta->partition_global_meta.res_total);
    *part_info->mutable_res_avail() = static_cast<crane::grpc::Resources>(
        part_meta->partition_global_meta.res_avail);
    *part_info->mutable_res_alloc() = static_cast<crane::grpc::Resources>(
        part_meta->partition_global_meta.res_in_use);

    if (part_meta->partition_global_meta.alive_craned_cnt > 0)
      part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
    else
      part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

    part_info->set_hostlist(part_meta->partition_global_meta.nodelist_str);
  }

  return reply;
}

crane::grpc::QueryPartitionInfoReply
CranedMetaContainerSimpleImpl::QueryPartitionInfo(
    const std::string& partition_name) {
  crane::grpc::QueryPartitionInfoReply reply;
  auto* list = reply.mutable_partition_info();

  if (!partition_metas_map_.Contains(partition_name)) return reply;

  auto part_meta = partition_metas_map_.GetValueExclusivePtr(partition_name);

  auto* part_info = list->Add();
  part_info->set_name(part_meta->partition_global_meta.name);
  part_info->set_total_nodes(part_meta->partition_global_meta.node_cnt);
  part_info->set_alive_nodes(part_meta->partition_global_meta.alive_craned_cnt);

  if (part_meta->partition_global_meta.alive_craned_cnt > 0)
    part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
  else
    part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

  part_info->set_hostlist(part_meta->partition_global_meta.nodelist_str);

  *part_info->mutable_res_total() = static_cast<crane::grpc::Resources>(
      part_meta->partition_global_meta.res_total);
  *part_info->mutable_res_avail() = static_cast<crane::grpc::Resources>(
      part_meta->partition_global_meta.res_avail);
  *part_info->mutable_res_alloc() = static_cast<crane::grpc::Resources>(
      part_meta->partition_global_meta.res_in_use);

  return reply;
}

crane::grpc::QueryClusterInfoReply
CranedMetaContainerSimpleImpl::QueryClusterInfo(
    const crane::grpc::QueryClusterInfoRequest& request) {
  crane::grpc::QueryClusterInfoReply reply;
  auto* partition_list = reply.mutable_partitions();

  std::unordered_set<std::string> filter_partitions_set(
      request.filter_partitions().begin(), request.filter_partitions().end());
  bool no_partition_constraint = request.filter_partitions().empty();
  auto partition_rng_filter_name = [no_partition_constraint,
                                    &filter_partitions_set](auto& it) {
    auto part_meta = it.second.GetExclusivePtr();
    return no_partition_constraint ||
           filter_partitions_set.contains(
               part_meta->partition_global_meta.name);
  };

  std::string hosts = absl::StrJoin(request.filter_nodes(), ",");
  std::list<std::string> hosts_list;
  util::ParseHostList(hosts, &hosts_list);
  std::unordered_set<std::string> req_nodes;
  for (auto& host : hosts_list) req_nodes.insert(std::move(host));

  bool no_craned_hostname_constraint = request.filter_nodes().empty();
  auto craned_rng_filter_hostname = [&](CranedMetaRawMap::const_iterator it) {
    auto craned_meta = it->second.GetExclusivePtr();
    return no_craned_hostname_constraint ||
           req_nodes.contains(craned_meta->static_meta.hostname);
  };

  if (request.filter_craned_control_states().empty() ||
      request.filter_craned_resource_states().empty())
    return reply;

  const int control_state_num = crane::grpc::CranedControlState_ARRAYSIZE;
  bool control_filters[control_state_num] = {false};
  for (const auto& it : request.filter_craned_control_states())
    control_filters[static_cast<int>(it)] = true;

  const int resource_state_num = crane::grpc::CranedResourceState_ARRAYSIZE;
  bool resource_filters[resource_state_num] = {false};
  for (const auto& it : request.filter_craned_resource_states())
    resource_filters[static_cast<int>(it)] = true;

  // Ensure that the map global read lock is held during the following filtering
  // operations and partition_metas_map_ must be locked before craned_meta_map_
  auto partition_map = partition_metas_map_.GetMapConstSharedPtr();
  auto craned_map = craned_meta_map_.GetMapConstSharedPtr();

  auto partition_rng =
      *partition_map | ranges::views::filter(partition_rng_filter_name);
  ranges::for_each(partition_rng, [&](auto& it) {
    PartitionId part_id = it.first;

    auto* part_info = partition_list->Add();

    // we make a copy of these craned ids to lower overall latency.
    // The amortized cost is 1 copy for each craned node.
    // Although an extra copying cost is introduced,
    // the time of accessing partition_metas_map_ is minimized and
    // the copying cost is taken only by this grpc thread handling
    // cinfo request.
    // Since we assume the number of cpu cores is sufficient on the
    // machine running CraneCtld, it's ok to pay 1 core cpu time
    // for overall low latency.
    std::unordered_set<CranedId> craned_ids;
    {
      auto part_meta = it.second.GetExclusivePtr();
      std::string partition_name = part_meta->partition_global_meta.name;
      if (partition_name == g_config.DefaultPartition) {
        partition_name.append("*");
      }
      part_info->set_name(std::move(partition_name));

      part_info->set_state(part_meta->partition_global_meta.alive_craned_cnt > 0
                               ? crane::grpc::PartitionState::PARTITION_UP
                               : crane::grpc::PartitionState::PARTITION_DOWN);
      craned_ids = part_meta->craned_ids;
    }

    std::list<std::string> craned_name_lists[control_state_num]
                                            [resource_state_num];

    auto craned_rng =
        craned_ids |
        ranges::views::transform(
            [&](CranedId const& craned_id) -> CranedMetaRawMap::const_iterator {
              return craned_map->find(craned_id);
            }) |
        ranges::views::filter(craned_rng_filter_hostname);

    ranges::for_each(craned_rng, [&](CranedMetaRawMap::const_iterator it) {
      auto craned_meta = it->second.GetExclusivePtr();
      auto& res_total = craned_meta->res_total.allocatable_resource;
      auto& res_in_use = craned_meta->res_in_use.allocatable_resource;
      auto& res_avail = craned_meta->res_avail.allocatable_resource;
      crane::grpc::CranedControlState control_state;
      if (craned_meta->drain) {
        control_state = crane::grpc::CranedControlState::CRANE_DRAIN;
      } else {
        control_state = crane::grpc::CranedControlState::CRANE_NONE;
      }
      crane::grpc::CranedResourceState resource_state;
      if (craned_meta->alive) {
        if (res_in_use.cpu_count == cpu_t(0) && res_in_use.memory_bytes == 0 &&
            craned_meta->res_in_use.dedicated_resource.empty()) {
          resource_state = crane::grpc::CranedResourceState::CRANE_IDLE;
        } else if (res_avail.cpu_count == cpu_t(0) ||
                   res_avail.memory_bytes == 0 ||
                   (!craned_meta->res_total.dedicated_resource.empty() &&
                    craned_meta->res_in_use.dedicated_resource ==
                        craned_meta->res_total.dedicated_resource)) {
          resource_state = crane::grpc::CranedResourceState::CRANE_ALLOC;
        } else {
          resource_state = crane::grpc::CranedResourceState::CRANE_MIX;
        }
      } else {
        resource_state = crane::grpc::CranedResourceState::CRANE_DOWN;
      }
      if (control_filters[static_cast<int>(control_state)] &&
          resource_filters[static_cast<int>(resource_state)]) {
        craned_name_lists[static_cast<int>(control_state)]
                         [static_cast<int>(resource_state)]
                             .emplace_back(craned_meta->static_meta.hostname);
      }
    });

    auto* craned_lists = part_info->mutable_craned_lists();
    for (int i = 0; i < control_state_num; i++) {
      for (int j = 0; j < resource_state_num; j++) {
        auto* craned_list = craned_lists->Add();
        craned_list->set_control_state(crane::grpc::CranedControlState(i));
        craned_list->set_resource_state(crane::grpc::CranedResourceState(j));
        craned_list->set_count(craned_name_lists[i][j].size());
        craned_list->set_craned_list_regex(
            util::HostNameListToStr(craned_name_lists[i][j]));
      }
    }
  });

  return reply;
}

crane::grpc::ModifyCranedStateReply
CranedMetaContainerSimpleImpl::ChangeNodeState(
    const crane::grpc::ModifyCranedStateRequest& request) {
  crane::grpc::ModifyCranedStateReply reply;

  if (!craned_meta_map_.Contains(request.craned_id())) {
    reply.set_ok(false);
    reply.set_reason("Invalid node name specified.");
    return reply;
  }

  auto craned_meta = craned_meta_map_[request.craned_id()];

  if (craned_meta->alive) {
    if (request.new_state() == crane::grpc::CranedControlState::CRANE_DRAIN) {
      craned_meta->drain = true;
      craned_meta->state_reason = request.reason();
      reply.set_ok(true);
    } else if (request.new_state() ==
               crane::grpc::CranedControlState::CRANE_NONE) {
      craned_meta->drain = false;
      craned_meta->state_reason.clear();
      reply.set_ok(true);
    } else {
      reply.set_ok(false);
      reply.set_reason("Invalid state.");
    }
  } else {
    reply.set_ok(false);
    reply.set_reason("Can't change the state of a DOWN node!");
  }

  return reply;
}

void CranedMetaContainerSimpleImpl::AddDedicatedResource(
    const CranedId& node_id, const DedicatedResourceInNode& resource) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to free resource from an unknown craned {}", node_id);
    return;
  }

  auto& part_ids = craned_id_part_ids_map_.at(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];
  if (!node_meta->alive) return;

  auto& node_dedicated_res_total = node_meta->res_total.dedicated_resource;
  auto& node_dedicated_res_avail = node_meta->res_avail.dedicated_resource;

  // find how many resource should add,under the constraint of configured count
  const auto& constraint = node_meta->static_meta.res.dedicated_resource;
  DedicatedResource res_to_add;

  for (const auto& [device_name, type_slots_map] :
       resource.name_type_slots_map) {
    if (!constraint.at(node_id).contains(device_name)) {
      CRANE_TRACE(
          "Node #{} try to report {},which is no in the config of ctld ",
          node_id, device_name);
      continue;
    }
    auto& this_name_constraint = constraint.at(node_id).at(device_name);
    auto& this_name_avail_current =
        node_dedicated_res_avail[node_id][device_name];
    auto& this_name_to_add = res_to_add[node_id][device_name];
    std::set<std::string> tmp;
    for (const auto& [type, slots] : type_slots_map.type_slots_map) {
      if (!constraint.at(node_id).at(device_name).contains(type)) {
        CRANE_TRACE(
            "Node #{} try to report {}:{},which is no in the config of ctld ",
            node_id, device_name, type);
        continue;
      }
      tmp.clear();
      std::ranges::set_difference(this_name_constraint.at(type),
                                  this_name_avail_current[type],
                                  std::inserter(tmp, tmp.begin()));
      std::ranges::set_intersection(
          tmp, slots,
          std::inserter(this_name_to_add[type],
                        this_name_to_add[type].begin()));
    }
  }

  node_dedicated_res_total += res_to_add;
  node_dedicated_res_avail += res_to_add;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.res_total.dedicated_resource += res_to_add;
    part_global_meta.res_avail.dedicated_resource += res_to_add;
  }
}

void CranedMetaContainerSimpleImpl::SetGrpcCranedInfoByCranedMeta_(
    const CranedMeta& craned_meta, crane::grpc::CranedInfo* craned_info) {
  const std::string& craned_index = craned_meta.static_meta.hostname;

  *craned_info->mutable_res_total() =
      static_cast<crane::grpc::Resources>(craned_meta.res_total);
  *craned_info->mutable_res_avail() =
      static_cast<crane::grpc::Resources>(craned_meta.res_avail);
  *craned_info->mutable_res_alloc() =
      static_cast<crane::grpc::Resources>(craned_meta.res_in_use);

  craned_info->set_hostname(craned_meta.static_meta.hostname);

  craned_info->set_running_task_num(
      craned_meta.running_task_resource_map.size());

  if (craned_meta.drain) {
    craned_info->set_control_state(
        crane::grpc::CranedControlState::CRANE_DRAIN);
  } else {
    craned_info->set_control_state(crane::grpc::CranedControlState::CRANE_NONE);
  }

  if (craned_meta.alive) {
    if (craned_meta.res_in_use.allocatable_resource.cpu_count == cpu_t(0) &&
        craned_meta.res_in_use.allocatable_resource.memory_bytes == 0 &&
        craned_meta.res_in_use.dedicated_resource.empty())
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_IDLE);
    else if (craned_meta.res_avail.allocatable_resource.cpu_count == cpu_t(0) ||
             craned_meta.res_avail.allocatable_resource.memory_bytes == 0 ||
             (!craned_meta.res_total.dedicated_resource.empty() &&
              craned_meta.res_in_use.dedicated_resource ==
                  craned_meta.res_total.dedicated_resource))
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_ALLOC);
    else
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_MIX);
  } else
    craned_info->set_resource_state(
        crane::grpc::CranedResourceState::CRANE_DOWN);

  craned_info->mutable_partition_names()->Assign(
      craned_meta.static_meta.partition_ids.begin(),
      craned_meta.static_meta.partition_ids.end());
}

}  // namespace Ctld