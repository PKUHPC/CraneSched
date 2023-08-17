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
  LockGuard guard(mtx_);

  CRANE_ASSERT(craned_meta_map_.contains(craned_id));
  CranedMeta& node_meta = craned_meta_map_.at(craned_id);
  node_meta.alive = true;

  CRANE_ASSERT(craned_id_part_ids_map_.contains(craned_id));
  for (PartitionId const& part_id : craned_id_part_ids_map_.at(craned_id)) {
    PartitionGlobalMeta& part_global_meta =
        partition_metas_map_.at(part_id).partition_global_meta;
    part_global_meta.m_resource_total_ += node_meta.res_total;
    part_global_meta.m_resource_avail_ += node_meta.res_total;
    part_global_meta.alive_craned_cnt++;
  }
}

void CranedMetaContainerSimpleImpl::CranedDown(CranedId craned_id) {
  LockGuard guard(mtx_);

  CRANE_ASSERT(craned_meta_map_.contains(craned_id));
  CranedMeta& node_meta = craned_meta_map_.at(craned_id);
  node_meta.alive = false;

  CRANE_ASSERT(craned_id_part_ids_map_.contains(craned_id));
  for (PartitionId const& part_id : craned_id_part_ids_map_.at(craned_id)) {
    PartitionGlobalMeta& part_global_meta =
        partition_metas_map_.at(part_id).partition_global_meta;
    part_global_meta.m_resource_total_ -= node_meta.res_total;
    part_global_meta.m_resource_avail_ -= node_meta.res_total;
    part_global_meta.alive_craned_cnt--;

    part_global_meta.m_resource_in_use_ -= node_meta.res_in_use;
  }

  // Set the rse_in_use of dead craned to 0
  node_meta.res_in_use -= node_meta.res_in_use;
}

CranedMetaContainerInterface::PartitionMetasPtr
CranedMetaContainerSimpleImpl::GetPartitionMetasPtr(PartitionId partition_id) {
  mtx_.lock();

  auto iter = partition_metas_map_.find(partition_id);
  if (iter == partition_metas_map_.end()) {
    mtx_.unlock();
    return PartitionMetasPtr{nullptr};
  }
  return PartitionMetasPtr(&iter->second, &mtx_);
}

CranedMetaContainerInterface::CranedMetaPtr
CranedMetaContainerSimpleImpl::GetCranedMetaPtr(CranedId craned_id) {
  mtx_.lock();

  auto craned_meta_iter = craned_meta_map_.find(craned_id);
  if (craned_meta_iter == craned_meta_map_.end()) {
    mtx_.unlock();
    return CranedMetaPtr{nullptr};
  }

  return CranedMetaPtr{&craned_meta_iter->second, &mtx_};
}

CranedMetaContainerInterface::AllPartitionsMetaMapPtr
CranedMetaContainerSimpleImpl::GetAllPartitionsMetaMapPtr() {
  mtx_.lock();
  return AllPartitionsMetaMapPtr{&partition_metas_map_, &mtx_};
}

CranedMetaContainerInterface::CranedMetaMapPtr
CranedMetaContainerSimpleImpl::GetCranedMetaMapPtr() {
  mtx_.lock();
  return CranedMetaMapPtr{&craned_meta_map_, &mtx_};
}

void CranedMetaContainerSimpleImpl::MallocResourceFromNode(
    CranedId node_id, task_id_t task_id, const Resources& resources) {
  LockGuard guard(mtx_);

  auto node_meta_iter = craned_meta_map_.find(node_id);
  if (node_meta_iter == craned_meta_map_.end()) {
    CRANE_ERROR("Try to malloc resource from an unknown craned {}", node_id);
    return;
  }

  CranedMeta& node_meta = node_meta_iter->second;
  node_meta.running_task_resource_map.emplace(task_id, resources);
  node_meta.res_avail -= resources;
  node_meta.res_in_use += resources;

  for (PartitionId const& part_id : craned_id_part_ids_map_.at(node_id)) {
    PartitionGlobalMeta& part_global_meta =
        partition_metas_map_.at(part_id).partition_global_meta;
    part_global_meta.m_resource_avail_ -= resources;
    part_global_meta.m_resource_in_use_ += resources;
  }
}

void CranedMetaContainerSimpleImpl::FreeResourceFromNode(CranedId craned_id,
                                                         uint32_t task_id) {
  LockGuard guard(mtx_);

  auto node_meta_iter = craned_meta_map_.find(craned_id);
  if (node_meta_iter == craned_meta_map_.end()) {
    CRANE_ERROR("Try to free resource from an unknown craned {}", craned_id);
    return;
  }

  CranedMeta& node_meta = node_meta_iter->second;
  auto resource_iter = node_meta.running_task_resource_map.find(task_id);
  if (resource_iter == node_meta_iter->second.running_task_resource_map.end()) {
    CRANE_ERROR("Try to free resource from an unknown task {} on craned {}",
                task_id, craned_id);
    return;
  }

  Resources const& resources = resource_iter->second;

  node_meta.res_avail += resources;
  node_meta.res_in_use -= resources;

  for (PartitionId const& part_id : craned_id_part_ids_map_.at(craned_id)) {
    PartitionGlobalMeta& part_global_meta =
        partition_metas_map_.at(part_id).partition_global_meta;
    part_global_meta.m_resource_avail_ += resources;
    part_global_meta.m_resource_in_use_ -= resources;
  }

  node_meta.running_task_resource_map.erase(resource_iter);
}

void CranedMetaContainerSimpleImpl::InitFromConfig(const Config& config) {
  LockGuard guard(mtx_);

  for (auto&& [craned_name, node_ptr] : config.Nodes) {
    CRANE_TRACE("Parsing node {}", craned_name);

    auto& craned_meta = craned_meta_map_[craned_name];

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

    auto& part_meta = partition_metas_map_[part_name];

    for (auto&& craned_name : partition.nodes) {
      auto& craned_meta = craned_meta_map_[craned_name];
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
}

bool CranedMetaContainerSimpleImpl::CheckCranedAllowed(
    const std::string& hostname) {
  LockGuard guard(mtx_);

  return craned_meta_map_.contains(hostname);
}

crane::grpc::QueryCranedInfoReply
CranedMetaContainerSimpleImpl::QueryAllCranedInfo() {
  LockGuard guard(mtx_);

  crane::grpc::QueryCranedInfoReply reply;
  auto* list = reply.mutable_craned_info_list();

  for (auto&& [craned_index, craned_meta] : craned_meta_map_) {
    auto* craned_info = list->Add();
    auto& alloc_res_total = craned_meta.res_total.allocatable_resource;
    auto& alloc_res_in_use = craned_meta.res_in_use.allocatable_resource;
    auto& alloc_res_avail = craned_meta.res_avail.allocatable_resource;

    craned_info->set_hostname(craned_meta.static_meta.hostname);
    craned_info->set_cpu(alloc_res_total.cpu_count);
    craned_info->set_alloc_cpu(alloc_res_in_use.cpu_count);
    craned_info->set_free_cpu(alloc_res_avail.cpu_count);
    craned_info->set_real_mem(alloc_res_total.memory_bytes);
    craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
    craned_info->set_free_mem(alloc_res_avail.memory_bytes);
    craned_info->set_running_task_num(
        craned_meta.running_task_resource_map.size());
    if (craned_meta.alive)
      craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
    else
      craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

    craned_info->mutable_partition_names()->Assign(
        craned_meta.static_meta.partition_ids.begin(),
        craned_meta.static_meta.partition_ids.end());
  }

  return reply;
}

crane::grpc::QueryCranedInfoReply
CranedMetaContainerSimpleImpl::QueryCranedInfo(const std::string& node_name) {
  LockGuard guard(mtx_);

  crane::grpc::QueryCranedInfoReply reply;
  auto* list = reply.mutable_craned_info_list();

  auto it = craned_meta_map_.find(node_name);
  if (it == craned_meta_map_.end()) {
    return reply;
  }

  CranedMeta const& craned_meta = it->second;

  auto* craned_info = list->Add();
  auto& alloc_res_total = craned_meta.res_total.allocatable_resource;
  auto& alloc_res_in_use = craned_meta.res_in_use.allocatable_resource;
  auto& alloc_res_avail = craned_meta.res_avail.allocatable_resource;

  craned_info->set_hostname(craned_meta.static_meta.hostname);
  craned_info->set_cpu(alloc_res_total.cpu_count);
  craned_info->set_alloc_cpu(alloc_res_in_use.cpu_count);
  craned_info->set_free_cpu(alloc_res_avail.cpu_count);
  craned_info->set_real_mem(alloc_res_total.memory_bytes);
  craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  craned_info->set_free_mem(alloc_res_avail.memory_bytes);
  craned_info->set_running_task_num(
      craned_meta.running_task_resource_map.size());
  if (craned_meta.alive)
    craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
  else
    craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

  craned_info->mutable_partition_names()->Assign(
      craned_meta.static_meta.partition_ids.begin(),
      craned_meta.static_meta.partition_ids.end());

  return reply;
}

crane::grpc::QueryPartitionInfoReply
CranedMetaContainerSimpleImpl::QueryAllPartitionInfo() {
  LockGuard guard(mtx_);

  crane::grpc::QueryPartitionInfoReply reply;
  auto* list = reply.mutable_partition_info();

  for (auto&& [part_name, part_meta] : partition_metas_map_) {
    auto* part_info = list->Add();
    auto& alloc_res_total =
        part_meta.partition_global_meta.m_resource_total_inc_dead_
            .allocatable_resource;
    auto& alloc_res_avail =
        part_meta.partition_global_meta.m_resource_avail_.allocatable_resource;
    auto& alloc_res_in_use =
        part_meta.partition_global_meta.m_resource_in_use_.allocatable_resource;
    part_info->set_name(part_meta.partition_global_meta.name);
    part_info->set_total_nodes(part_meta.partition_global_meta.node_cnt);
    part_info->set_alive_nodes(
        part_meta.partition_global_meta.alive_craned_cnt);
    part_info->set_total_cpu(alloc_res_total.cpu_count);
    part_info->set_avail_cpu(alloc_res_avail.cpu_count);
    part_info->set_alloc_cpu(alloc_res_in_use.cpu_count);
    part_info->set_total_mem(alloc_res_total.memory_bytes);
    part_info->set_avail_mem(alloc_res_avail.memory_bytes);
    part_info->set_alloc_mem(alloc_res_in_use.memory_bytes);

    if (part_meta.partition_global_meta.alive_craned_cnt > 0)
      part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
    else
      part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

    part_info->set_hostlist(part_meta.partition_global_meta.nodelist_str);
  }

  return reply;
}

crane::grpc::QueryPartitionInfoReply
CranedMetaContainerSimpleImpl::QueryPartitionInfo(
    const std::string& partition_name) {
  LockGuard guard(mtx_);

  crane::grpc::QueryPartitionInfoReply reply;
  auto* list = reply.mutable_partition_info();

  auto it = partition_metas_map_.find(partition_name);
  if (it == partition_metas_map_.end()) return reply;

  PartitionMeta const& part_meta = it->second;

  auto* part_info = list->Add();
  auto& res_total = part_meta.partition_global_meta.m_resource_total_inc_dead_
                        .allocatable_resource;
  auto& res_avail =
      part_meta.partition_global_meta.m_resource_avail_.allocatable_resource;
  auto& res_in_use =
      part_meta.partition_global_meta.m_resource_in_use_.allocatable_resource;
  part_info->set_name(part_meta.partition_global_meta.name);
  part_info->set_total_nodes(part_meta.partition_global_meta.node_cnt);
  part_info->set_alive_nodes(part_meta.partition_global_meta.alive_craned_cnt);
  part_info->set_total_cpu(res_total.cpu_count);
  part_info->set_avail_cpu(res_avail.cpu_count);
  part_info->set_alloc_cpu(res_in_use.cpu_count);
  part_info->set_total_mem(res_total.memory_bytes);
  part_info->set_avail_mem(res_avail.memory_bytes);
  part_info->set_alloc_mem(res_in_use.memory_bytes);

  if (part_meta.partition_global_meta.alive_craned_cnt > 0)
    part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
  else
    part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

  part_info->set_hostlist(part_meta.partition_global_meta.nodelist_str);

  return reply;
}

crane::grpc::QueryClusterInfoReply
CranedMetaContainerSimpleImpl::QueryClusterInfo(
    const crane::grpc::QueryClusterInfoRequest& request) {
  LockGuard guard(mtx_);
  crane::grpc::QueryClusterInfoReply reply;
  auto* partition_list = reply.mutable_partitions();

  std::unordered_set<std::string> filter_partitions_set(
      request.filter_partitions().begin(), request.filter_partitions().end());
  bool no_partition_constraint = request.filter_partitions().empty();
  auto partition_rng_filter_name = [no_partition_constraint,
                                    &filter_partitions_set](auto& it) {
    auto part_meta = it.second;
    return no_partition_constraint ||
           filter_partitions_set.contains(part_meta.partition_global_meta.name);
  };

  std::unordered_set<std::string> req_nodes(request.filter_nodes().begin(),
                                            request.filter_nodes().end());

  std::unordered_set<int> filter_craned_states_set(
      request.filter_craned_states().begin(),
      request.filter_craned_states().end());
  bool no_craned_state_constraint = request.filter_craned_states().empty();
  auto craned_rng_filter_state = [&](CranedMetaMap::iterator it) {
    if (no_craned_state_constraint) return true;

    CranedMeta& craned_meta = it->second;
    auto& res_total = craned_meta.res_total.allocatable_resource;
    auto& res_in_use = craned_meta.res_in_use.allocatable_resource;
    auto& res_avail = craned_meta.res_avail.allocatable_resource;
    if (craned_meta.alive) {
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
  auto craned_rng_filter_hostname = [&](CranedMetaMap::iterator it) {
    auto& craned_meta = it->second;
    return no_craned_hostname_constraint ||
           req_nodes.contains(craned_meta.static_meta.hostname);
  };

  auto craned_rng_filter_only_responding = [&](CranedMetaMap::iterator it) {
    CranedMeta& craned_meta = it->second;
    return !request.filter_only_responding_nodes() || craned_meta.alive;
  };

  auto craned_rng_filter_only_down = [&](CranedMetaMap::iterator it) {
    CranedMeta& craned_meta = it->second;
    return !request.filter_only_down_nodes() || !craned_meta.alive;
  };

  auto partition_rng =
      partition_metas_map_ | ranges::views::filter(partition_rng_filter_name);
  ranges::for_each(partition_rng, [&](auto& it) {
    PartitionId part_id = it.first;
    PartitionMeta& part_meta = it.second;

    auto* part_info = partition_list->Add();

    std::string partition_name = part_meta.partition_global_meta.name;
    if (partition_name == g_config.DefaultPartition) {
      partition_name.append("*");
    }
    part_info->set_name(std::move(partition_name));

    part_info->set_state(part_meta.partition_global_meta.alive_craned_cnt > 0
                             ? crane::grpc::PartitionState::PARTITION_UP
                             : crane::grpc::PartitionState::PARTITION_DOWN);

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
        part_meta.craned_ids |
        ranges::views::transform(
            [&](CranedId const& craned_id) -> CranedMetaMap::iterator {
              return craned_meta_map_.find(craned_id);
            }) |
        ranges::views::filter(craned_rng_filter_hostname) |
        ranges::views::filter(craned_rng_filter_state) |
        ranges::views::filter(craned_rng_filter_only_down) |
        ranges::views::filter(craned_rng_filter_only_responding);
    ranges::for_each(craned_rng, [&](CranedMetaMap::iterator it) {
      CranedMeta& craned_meta = it->second;
      auto& res_total = craned_meta.res_total.allocatable_resource;
      auto& res_in_use = craned_meta.res_in_use.allocatable_resource;
      auto& res_avail = craned_meta.res_avail.allocatable_resource;
      if (craned_meta.alive) {
        if (res_in_use.cpu_count == 0 && res_in_use.memory_bytes == 0) {
          idle_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
          idle_craned_list->set_count(idle_craned_name_list.size());
        } else if (res_avail.cpu_count == 0 || res_avail.memory_bytes == 0) {
          alloc_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
          alloc_craned_list->set_count(alloc_craned_name_list.size());
        } else {
          mix_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
          mix_craned_list->set_count(mix_craned_name_list.size());
        }
      } else {
        down_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
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