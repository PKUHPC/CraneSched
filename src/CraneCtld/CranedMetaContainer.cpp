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
    part_global_meta.m_resource_total_ += node_meta->res_total;
    part_global_meta.m_resource_avail_ += node_meta->res_total;
    part_global_meta.alive_craned_cnt++;
  }
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
    part_global_meta.m_resource_total_ -= node_meta->res_total;
    part_global_meta.m_resource_avail_ -= node_meta->res_avail;
    part_global_meta.m_resource_in_use_ -= node_meta->res_in_use;

    part_global_meta.alive_craned_cnt--;
  }

  // Set the rse_in_use of dead craned to 0
  node_meta->res_avail += node_meta->res_in_use;
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

void CranedMetaContainerSimpleImpl::MallocResourceFromNode(
    CranedId node_id, task_id_t task_id, const Resources& resources) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to malloc resource from an unknown craned {}", node_id);
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
  node_meta->running_task_resource_map.emplace(task_id, resources);
  node_meta->res_avail -= resources;
  node_meta->res_in_use += resources;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.m_resource_avail_ -= resources;
    part_global_meta.m_resource_in_use_ += resources;
  }
}

void CranedMetaContainerSimpleImpl::FreeResourceFromNode(CranedId craned_id,
                                                         uint32_t task_id) {
  if (!craned_meta_map_.Contains(craned_id)) {
    CRANE_ERROR("Try to free resource from an unknown craned {}", craned_id);
    return;
  }

  auto& part_ids = craned_id_part_ids_map_.at(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[craned_id];
  if (!node_meta->alive) {
    CRANE_DEBUG("Crane {} has already been down. Ignore FreeResourceFromNode.",
                node_meta->static_meta.hostname);
    return;
  }

  auto resource_iter = node_meta->running_task_resource_map.find(task_id);
  if (resource_iter == node_meta->running_task_resource_map.end()) {
    CRANE_ERROR("Try to free resource from an unknown task {} on craned {}",
                task_id, craned_id);
    return;
  }
  Resources const& resources = resource_iter->second;

  node_meta->res_avail += resources;
  node_meta->res_in_use -= resources;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.m_resource_avail_ += resources;
    part_global_meta.m_resource_in_use_ -= resources;
  }

  node_meta->running_task_resource_map.erase(resource_iter);
}

void CranedMetaContainerSimpleImpl::InitFromConfig(const Config& config) {
  HashMap<CranedId, CranedMeta> craned_map;
  HashMap<PartitionId, PartitionMeta> partition_map;

  for (auto&& [craned_name, node_ptr] : config.Nodes) {
    CRANE_TRACE("Parsing node {}", craned_name);

    auto& craned_meta = craned_map[craned_name];

    auto& static_meta = craned_meta.static_meta;
    static_meta.res.allocatable_resource.cpu_count =
        config.Nodes.at(craned_name)->cpu;
    static_meta.res.allocatable_resource.memory_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    static_meta.res.allocatable_resource.memory_sw_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    static_meta.hostname = craned_name;
    static_meta.port = std::strtoul(kCranedDefaultPort, nullptr, 10);

    craned_meta.res_total = static_meta.res;
    craned_meta.res_avail = static_meta.res;
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
          "Add the resource of Craned {} (cpu: {}, mem: {}) to partition "
          "[{}]'s global resource.",
          craned_name,
          craned_meta.static_meta.res.allocatable_resource.cpu_count,
          util::ReadableMemory(
              craned_meta.static_meta.res.allocatable_resource.memory_bytes),
          part_name);

      part_res += craned_meta.static_meta.res;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.m_resource_total_inc_dead_ = part_res;
    part_meta.partition_global_meta.node_cnt = part_meta.craned_ids.size();
    part_meta.partition_global_meta.nodelist_str = partition.nodelist_str;

    CRANE_DEBUG(
        "partition [{}]'s Global resource now: cpu: {}, mem: {}). It has {} "
        "craneds.",
        part_name,
        part_meta.partition_global_meta.m_resource_total_inc_dead_
            .allocatable_resource.cpu_count,
        util::ReadableMemory(
            part_meta.partition_global_meta.m_resource_total_inc_dead_
                .allocatable_resource.memory_bytes),
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
    auto* craned_info = list->Add();
    auto craned_meta = craned_meta_ptr.GetExclusivePtr();

    auto& alloc_res_total = craned_meta->res_total.allocatable_resource;
    auto& alloc_res_in_use = craned_meta->res_in_use.allocatable_resource;
    auto& alloc_res_avail = craned_meta->res_avail.allocatable_resource;

    craned_info->set_hostname(craned_meta->static_meta.hostname);
    craned_info->set_cpu(alloc_res_total.cpu_count);
    craned_info->set_alloc_cpu(alloc_res_in_use.cpu_count);
    craned_info->set_free_cpu(alloc_res_avail.cpu_count);
    craned_info->set_real_mem(alloc_res_total.memory_bytes);
    craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
    craned_info->set_free_mem(alloc_res_avail.memory_bytes);
    craned_info->set_running_task_num(
        craned_meta->running_task_resource_map.size());
    if (craned_meta->alive)
      craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
    else
      craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

    craned_info->mutable_partition_names()->Assign(
        craned_meta->static_meta.partition_ids.begin(),
        craned_meta->static_meta.partition_ids.end());
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
  auto& alloc_res_total = craned_meta->res_total.allocatable_resource;
  auto& alloc_res_in_use = craned_meta->res_in_use.allocatable_resource;
  auto& alloc_res_avail = craned_meta->res_avail.allocatable_resource;

  craned_info->set_hostname(craned_meta->static_meta.hostname);
  craned_info->set_cpu(alloc_res_total.cpu_count);
  craned_info->set_alloc_cpu(alloc_res_in_use.cpu_count);
  craned_info->set_free_cpu(alloc_res_avail.cpu_count);
  craned_info->set_real_mem(alloc_res_total.memory_bytes);
  craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  craned_info->set_free_mem(alloc_res_avail.memory_bytes);
  craned_info->set_running_task_num(
      craned_meta->running_task_resource_map.size());
  if (craned_meta->alive)
    craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
  else
    craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

  craned_info->mutable_partition_names()->Assign(
      craned_meta->static_meta.partition_ids.begin(),
      craned_meta->static_meta.partition_ids.end());

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

    auto& alloc_res_total =
        part_meta->partition_global_meta.m_resource_total_inc_dead_
            .allocatable_resource;
    auto& alloc_res_avail =
        part_meta->partition_global_meta.m_resource_avail_.allocatable_resource;
    auto& alloc_res_in_use = part_meta->partition_global_meta.m_resource_in_use_
                                 .allocatable_resource;
    part_info->set_name(part_meta->partition_global_meta.name);
    part_info->set_total_nodes(part_meta->partition_global_meta.node_cnt);
    part_info->set_alive_nodes(
        part_meta->partition_global_meta.alive_craned_cnt);
    part_info->set_total_cpu(alloc_res_total.cpu_count);
    part_info->set_avail_cpu(alloc_res_avail.cpu_count);
    part_info->set_alloc_cpu(alloc_res_in_use.cpu_count);
    part_info->set_total_mem(alloc_res_total.memory_bytes);
    part_info->set_avail_mem(alloc_res_avail.memory_bytes);
    part_info->set_alloc_mem(alloc_res_in_use.memory_bytes);

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
  auto& res_total = part_meta->partition_global_meta.m_resource_total_inc_dead_
                        .allocatable_resource;
  auto& res_avail =
      part_meta->partition_global_meta.m_resource_avail_.allocatable_resource;
  auto& res_in_use =
      part_meta->partition_global_meta.m_resource_in_use_.allocatable_resource;
  part_info->set_name(part_meta->partition_global_meta.name);
  part_info->set_total_nodes(part_meta->partition_global_meta.node_cnt);
  part_info->set_alive_nodes(part_meta->partition_global_meta.alive_craned_cnt);
  part_info->set_total_cpu(res_total.cpu_count);
  part_info->set_avail_cpu(res_avail.cpu_count);
  part_info->set_alloc_cpu(res_in_use.cpu_count);
  part_info->set_total_mem(res_total.memory_bytes);
  part_info->set_avail_mem(res_avail.memory_bytes);
  part_info->set_alloc_mem(res_in_use.memory_bytes);

  if (part_meta->partition_global_meta.alive_craned_cnt > 0)
    part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
  else
    part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

  part_info->set_hostlist(part_meta->partition_global_meta.nodelist_str);

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

  std::unordered_set<std::string> req_nodes(request.filter_nodes().begin(),
                                            request.filter_nodes().end());

  std::unordered_set<int> filter_craned_states_set(
      request.filter_craned_states().begin(),
      request.filter_craned_states().end());
  bool no_craned_state_constraint = request.filter_craned_states().empty();
  auto craned_rng_filter_state = [&](CranedMetaRawMap::const_iterator it) {
    if (no_craned_state_constraint) return true;

    auto craned_meta = it->second.GetExclusivePtr();
    auto& res_total = craned_meta->res_total.allocatable_resource;
    auto& res_in_use = craned_meta->res_in_use.allocatable_resource;
    auto& res_avail = craned_meta->res_avail.allocatable_resource;
    if (craned_meta->alive) {
      if (res_in_use.cpu_count == 0 && res_in_use.memory_bytes == 0) {
        return filter_craned_states_set.contains(
            crane::grpc::CranedState::CRANE_IDLE);
      } else if (res_avail.cpu_count == 0 && res_avail.memory_bytes == 0)
        return filter_craned_states_set.contains(
            crane::grpc::CranedState::CRANE_ALLOC);
      else
        return filter_craned_states_set.contains(
            crane::grpc::CranedState::CRANE_MIX);
    } else {
      return filter_craned_states_set.contains(
          crane::grpc::CranedState::CRANE_DOWN);
    }
  };

  bool no_craned_hostname_constraint = request.filter_nodes().empty();
  auto craned_rng_filter_hostname = [&](CranedMetaRawMap::const_iterator it) {
    auto craned_meta = it->second.GetExclusivePtr();
    return no_craned_hostname_constraint ||
           req_nodes.contains(craned_meta->static_meta.hostname);
  };

  auto craned_rng_filter_only_responding =
      [&](CranedMetaRawMap::const_iterator it) {
        auto craned_meta = it->second.GetExclusivePtr();
        return !request.filter_only_responding_nodes() || craned_meta->alive;
      };

  auto craned_rng_filter_only_down = [&](CranedMetaRawMap::const_iterator it) {
    auto craned_meta = it->second.GetExclusivePtr();
    return !request.filter_only_down_nodes() || !craned_meta->alive;
  };

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

    auto* craned_lists = part_info->mutable_craned_lists();

    auto* idle_craned_list = craned_lists->Add();
    auto* mix_craned_list = craned_lists->Add();
    auto* alloc_craned_list = craned_lists->Add();
    auto* down_craned_list = craned_lists->Add();

    idle_craned_list->set_state(crane::grpc::CranedState::CRANE_IDLE);
    mix_craned_list->set_state(crane::grpc::CranedState::CRANE_MIX);
    alloc_craned_list->set_state(crane::grpc::CranedState::CRANE_ALLOC);
    down_craned_list->set_state(crane::grpc::CranedState::CRANE_DOWN);

    std::list<std::string> idle_craned_name_list, mix_craned_name_list,
        alloc_craned_name_list, down_craned_name_list;

    auto craned_rng =
        craned_ids |
        ranges::views::transform(
            [&](CranedId const& craned_id) -> CranedMetaRawMap::const_iterator {
              return craned_map->find(craned_id);
            }) |
        ranges::views::filter(craned_rng_filter_hostname) |
        ranges::views::filter(craned_rng_filter_state) |
        ranges::views::filter(craned_rng_filter_only_down) |
        ranges::views::filter(craned_rng_filter_only_responding);

    ranges::for_each(craned_rng, [&](CranedMetaRawMap::const_iterator it) {
      auto craned_meta = it->second.GetExclusivePtr();
      auto& res_total = craned_meta->res_total.allocatable_resource;
      auto& res_in_use = craned_meta->res_in_use.allocatable_resource;
      auto& res_avail = craned_meta->res_avail.allocatable_resource;
      if (craned_meta->alive) {
        if (res_in_use.cpu_count == 0 && res_in_use.memory_bytes == 0) {
          idle_craned_name_list.emplace_back(craned_meta->static_meta.hostname);
          idle_craned_list->set_count(idle_craned_name_list.size());
        } else if (res_avail.cpu_count == 0 || res_avail.memory_bytes == 0) {
          alloc_craned_name_list.emplace_back(
              craned_meta->static_meta.hostname);
          alloc_craned_list->set_count(alloc_craned_name_list.size());
        } else {
          mix_craned_name_list.emplace_back(craned_meta->static_meta.hostname);
          mix_craned_list->set_count(mix_craned_name_list.size());
        }
      } else {
        down_craned_name_list.emplace_back(craned_meta->static_meta.hostname);
        down_craned_list->set_count(down_craned_name_list.size());
      }
    });

    idle_craned_list->set_craned_list_regex(
        util::HostNameListToStr(idle_craned_name_list));
    mix_craned_list->set_craned_list_regex(
        util::HostNameListToStr(mix_craned_name_list));
    alloc_craned_list->set_craned_list_regex(
        util::HostNameListToStr(alloc_craned_name_list));
    down_craned_list->set_craned_list_regex(
        util::HostNameListToStr(down_craned_name_list));
  });

  return reply;
}

}  // namespace Ctld