#include "CranedMetaContainer.h"

namespace Ctld {

void CranedMetaContainerSimpleImpl::CranedUp(const CranedId& craned_id) {
  LockGuard guard(mtx_);

  CRANE_ASSERT(partition_metas_map_.count(craned_id.partition_id) > 0);
  auto& part_meta = partition_metas_map_.at(craned_id.partition_id);

  CRANE_ASSERT(part_meta.craned_meta_map.count(craned_id.craned_index) > 0);
  auto& node_meta = part_meta.craned_meta_map.at(craned_id.craned_index);
  node_meta.alive = true;

  part_meta.partition_global_meta.m_resource_total_ += node_meta.res_total;
  part_meta.partition_global_meta.m_resource_avail_ += node_meta.res_total;
  part_meta.partition_global_meta.alive_craned_cnt++;
}

void CranedMetaContainerSimpleImpl::CranedDown(CranedId craned_id) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(craned_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    CRANE_ERROR(
        "Deleting non-existent craned {} in CranedDown: Partition not found",
        craned_id);
    return;
  }

  CranedMetaMap& craned_meta_map = part_metas_iter->second.craned_meta_map;
  auto craned_meta_iter = craned_meta_map.find(craned_id.craned_index);
  if (craned_meta_iter == craned_meta_map.end()) {
    CRANE_ERROR(
        "Deleting non-existent craned {} in CranedDown: Craned not found in "
        "the partition",
        craned_id);
    return;
  }

  PartitionGlobalMeta& part_meta =
      part_metas_iter->second.partition_global_meta;
  CranedMeta& craned_meta = craned_meta_iter->second;
  craned_meta.alive = false;

  part_meta.m_resource_avail_ -= craned_meta.res_avail;
  part_meta.m_resource_total_ -= craned_meta.res_total;
  part_meta.m_resource_in_use_ -= craned_meta.res_in_use;
  part_meta.alive_craned_cnt++;
}

CranedMetaContainerInterface::PartitionMetasPtr
CranedMetaContainerSimpleImpl::GetPartitionMetasPtr(uint32_t partition_id) {
  mtx_.lock();

  auto iter = partition_metas_map_.find(partition_id);
  if (iter == partition_metas_map_.end()) {
    mtx_.unlock();
    return PartitionMetasPtr{nullptr};
  }
  return PartitionMetasPtr(&iter->second, &mtx_);
}

bool CranedMetaContainerSimpleImpl::GetPartitionId(
    const std::string& partition_name, uint32_t* partition_id) {
  LockGuard guard(mtx_);
  auto iter = partition_name_id_map_.find(partition_name);
  if (iter == partition_name_id_map_.end()) {
    return false;
  }

  *partition_id = iter->second;
  return true;
}

bool CranedMetaContainerSimpleImpl::PartitionExists(
    const std::string& partition_name) {
  LockGuard guard(mtx_);

  return partition_name_id_map_.count(partition_name) > 0;
}

CranedMetaContainerInterface::CranedMetaPtr
CranedMetaContainerSimpleImpl::GetNodeMetaPtr(CranedId node_id) {
  mtx_.lock();

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // No such partition.
    mtx_.unlock();
    return CranedMetaPtr{nullptr};
  }

  auto node_meta_iter =
      part_metas_iter->second.craned_meta_map.find(node_id.craned_index);
  if (node_meta_iter == part_metas_iter->second.craned_meta_map.end()) {
    // No such node in this partition.
    mtx_.unlock();
    return CranedMetaPtr{nullptr};
  }

  return CranedMetaPtr{&node_meta_iter->second, &mtx_};
}

CranedMetaContainerInterface::AllPartitionsMetaMapPtr
CranedMetaContainerSimpleImpl::GetAllPartitionsMetaMapPtr() {
  mtx_.lock();
  return AllPartitionsMetaMapPtr{&partition_metas_map_, &mtx_};
}

void CranedMetaContainerSimpleImpl::MallocResourceFromNode(
    CranedId node_id, uint32_t task_id, const Resources& resources) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(node_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // No such partition.
    return;
  }

  auto node_meta_iter =
      part_metas_iter->second.craned_meta_map.find(node_id.craned_index);
  if (node_meta_iter == part_metas_iter->second.craned_meta_map.end()) {
    // No such node in this partition.
    return;
  }

  node_meta_iter->second.running_task_resource_map.emplace(task_id, resources);
  part_metas_iter->second.partition_global_meta.m_resource_avail_ -= resources;
  part_metas_iter->second.partition_global_meta.m_resource_in_use_ += resources;
  node_meta_iter->second.res_avail -= resources;
  node_meta_iter->second.res_in_use += resources;
}

void CranedMetaContainerSimpleImpl::FreeResourceFromNode(CranedId craned_id,
                                                         uint32_t task_id) {
  LockGuard guard(mtx_);

  auto part_metas_iter = partition_metas_map_.find(craned_id.partition_id);
  if (part_metas_iter == partition_metas_map_.end()) {
    // No such partition.
    return;
  }

  auto node_meta_iter =
      part_metas_iter->second.craned_meta_map.find(craned_id.craned_index);
  if (node_meta_iter == part_metas_iter->second.craned_meta_map.end()) {
    // No such node in this partition.
    return;
  }

  auto resource_iter =
      node_meta_iter->second.running_task_resource_map.find(task_id);
  if (resource_iter == node_meta_iter->second.running_task_resource_map.end()) {
    // Invalid task_id
    return;
  }

  const Resources& resources = resource_iter->second;
  part_metas_iter->second.partition_global_meta.m_resource_avail_ += resources;
  part_metas_iter->second.partition_global_meta.m_resource_in_use_ -= resources;
  node_meta_iter->second.res_avail += resources;
  node_meta_iter->second.res_in_use -= resources;

  node_meta_iter->second.running_task_resource_map.erase(resource_iter);
}

void CranedMetaContainerSimpleImpl::InitFromConfig(const Config& config) {
  LockGuard guard(mtx_);

  uint32_t part_seq = 0;

  for (auto&& [part_name, partition] : config.Partitions) {
    CRANE_TRACE("Parsing partition {}", part_name);

    Resources part_res;

    partition_name_id_map_[part_name] = part_seq;
    auto& part_meta = partition_metas_map_[part_seq];
    uint32_t craned_index = 0;

    for (auto&& craned_name : partition.nodes) {
      CRANE_TRACE("Parsing node {}", craned_name);

      auto& craned_meta = part_meta.craned_meta_map[craned_index];

      auto& static_meta = craned_meta.static_meta;
      static_meta.res.allocatable_resource.cpu_count =
          config.Nodes.at(craned_name)->cpu;
      static_meta.res.allocatable_resource.memory_bytes =
          config.Nodes.at(craned_name)->memory_bytes;
      static_meta.res.allocatable_resource.memory_sw_bytes =
          config.Nodes.at(craned_name)->memory_bytes;
      static_meta.hostname = craned_name;
      static_meta.port = std::strtoul(kCranedDefaultPort, nullptr, 10);
      static_meta.partition_id = part_seq;
      static_meta.partition_name = part_name;

      craned_meta.res_total = static_meta.res;
      craned_meta.res_avail = static_meta.res;

      node_hostname_part_id_map_[craned_name] = part_seq;
      part_id_host_index_map_[std::make_pair(part_seq, craned_name)] =
          craned_index;

      CRANE_DEBUG(
          "Add the resource of Craned #{} {} (cpu: {}, mem: {}) to partition "
          "[{}]'s global resource.",
          craned_index, craned_name,
          static_meta.res.allocatable_resource.cpu_count,
          util::ReadableMemory(
              static_meta.res.allocatable_resource.memory_bytes),
          static_meta.partition_name);

      craned_index++;

      part_res += static_meta.res;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.m_resource_total_inc_dead_ = part_res;
    part_meta.partition_global_meta.node_cnt = craned_index;
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
        craned_index);
    part_seq++;
  }
}

bool CranedMetaContainerSimpleImpl::CheckCranedAllowed(
    const std::string& hostname) {
  LockGuard guard(mtx_);

  if (node_hostname_part_id_map_.count(hostname) > 0) return true;

  return false;
}

bool CranedMetaContainerSimpleImpl::GetCraneId(const std::string& hostname,
                                               CranedId* craned_id) {
  LockGuard guard(mtx_);

  auto it1 = node_hostname_part_id_map_.find(hostname);
  if (it1 == node_hostname_part_id_map_.end()) return false;
  uint32_t part_id = it1->second;

  auto it2 = part_id_host_index_map_.find(std::make_pair(part_id, hostname));
  if (it2 == part_id_host_index_map_.end()) return false;
  uint32_t craned_index = it2->second;

  craned_id->partition_id = part_id;
  craned_id->craned_index = craned_index;

  return true;
}

crane::grpc::QueryCranedInfoReply
CranedMetaContainerSimpleImpl::QueryAllCranedInfo() {
  LockGuard guard(mtx_);

  crane::grpc::QueryCranedInfoReply reply;
  auto* list = reply.mutable_craned_info_list();

  for (auto&& [part_name, part_meta] : partition_metas_map_) {
    for (auto&& [craned_index, craned_meta] : part_meta.craned_meta_map) {
      auto* craned_info = list->Add();
      auto& alloc_res_total = craned_meta.res_total.allocatable_resource;
      auto& alloc_res_in_use = craned_meta.res_in_use.allocatable_resource;
      auto& alloc_res_avail = craned_meta.res_avail.allocatable_resource;

      craned_info->set_hostname(craned_meta.static_meta.hostname);
      craned_info->set_cpus(alloc_res_total.cpu_count);
      craned_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
      craned_info->set_free_cpus(alloc_res_avail.cpu_count);
      craned_info->set_real_mem(alloc_res_total.memory_bytes);
      craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
      craned_info->set_free_mem(alloc_res_avail.memory_bytes);
      craned_info->set_partition_name(craned_meta.static_meta.partition_name);
      craned_info->set_running_task_num(
          craned_meta.running_task_resource_map.size());
      if (craned_meta.alive)
        craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
      else
        craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);
    }
  }

  return reply;
}

crane::grpc::QueryCranedInfoReply
CranedMetaContainerSimpleImpl::QueryCranedInfo(const std::string& node_name) {
  LockGuard guard(mtx_);

  crane::grpc::QueryCranedInfoReply reply;
  auto* list = reply.mutable_craned_info_list();

  auto it1 = node_hostname_part_id_map_.find(node_name);
  if (it1 == node_hostname_part_id_map_.end()) {
    return reply;
  }

  uint32_t part_id = it1->second;
  auto key = std::make_pair(part_id, node_name);
  auto it2 = part_id_host_index_map_.find(key);
  if (it2 == part_id_host_index_map_.end()) {
    return reply;
  }
  uint32_t node_index = it2->second;

  auto& node_meta = partition_metas_map_[part_id].craned_meta_map[node_index];
  auto* node_info = list->Add();
  auto& alloc_res_total = node_meta.res_total.allocatable_resource;
  auto& alloc_res_in_use = node_meta.res_in_use.allocatable_resource;
  auto& alloc_res_avail = node_meta.res_avail.allocatable_resource;

  node_info->set_hostname(node_meta.static_meta.hostname);
  node_info->set_cpus(alloc_res_total.cpu_count);
  node_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
  node_info->set_free_cpus(alloc_res_avail.cpu_count);
  node_info->set_real_mem(alloc_res_total.memory_bytes);
  node_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  node_info->set_free_mem(alloc_res_avail.memory_bytes);
  node_info->set_partition_name(node_meta.static_meta.partition_name);
  node_info->set_running_task_num(node_meta.running_task_resource_map.size());
  if (node_meta.alive)
    node_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
  else
    node_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

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
    part_info->set_total_cpus(alloc_res_total.cpu_count);
    part_info->set_avail_cpus(alloc_res_avail.cpu_count);
    part_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
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

  auto it = partition_name_id_map_.find(partition_name);
  if (it == partition_name_id_map_.end()) return reply;

  auto& part_meta = partition_metas_map_.at(it->second);

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
  part_info->set_total_cpus(res_total.cpu_count);
  part_info->set_avail_cpus(res_avail.cpu_count);
  part_info->set_alloc_cpus(res_in_use.cpu_count);
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
  auto craned_rng_filter_state = [&](auto& it) {
    if (no_craned_state_constraint) return true;

    CranedMeta& craned_meta = it.second;
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
  auto craned_rng_filter_hostname = [&](auto& it) {
    auto& craned_meta = it.second;
    return no_craned_hostname_constraint ||
           req_nodes.contains(craned_meta.static_meta.hostname);
  };

  auto craned_rng_filter_only_responding = [&](auto& it) {
    CranedMeta& craned_meta = it.second;
    return !request.filter_only_responding_nodes() || craned_meta.alive;
  };

  auto craned_rng_filter_only_down = [&](auto& it) {
    CranedMeta& craned_meta = it.second;
    return !request.filter_only_down_nodes() || !craned_meta.alive;
  };

  auto partition_rng =
      partition_metas_map_ | ranges::views::filter(partition_rng_filter_name);
  ranges::for_each(partition_rng, [&](auto& it) {
    uint32_t part_id = it.first;
    PartitionMetas& part_meta = it.second;

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
        alloc_craned_name_list, down_craned_name_list,abstract_info_count_list,summarize_craned_name_list;

    auto craned_rng = part_meta.craned_meta_map |
                      ranges::views::filter(craned_rng_filter_hostname) |
                      ranges::views::filter(craned_rng_filter_state) |
                      ranges::views::filter(craned_rng_filter_only_down) |
                      ranges::views::filter(craned_rng_filter_only_responding);
    ranges::for_each(craned_rng, [&](auto& it) {
      CranedMeta& craned_meta = it.second;
      auto& res_total = craned_meta.res_total.allocatable_resource;
      auto& res_in_use = craned_meta.res_in_use.allocatable_resource;
      auto& res_avail = craned_meta.res_avail.allocatable_resource;
      if (craned_meta.alive) {
        if (res_in_use.cpu_count == 0 && res_in_use.memory_bytes == 0) {
          idle_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
          idle_craned_list->set_count(idle_craned_name_list.size());
        } else if (res_avail.cpu_count == 0 && res_avail.memory_bytes == 0) {
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
    abstract_info_count_list.emplace_back(std::to_string(alloc_craned_list->count()+mix_craned_list->count()));
    abstract_info_count_list.emplace_back(std::to_string(idle_craned_list->count()));
    abstract_info_count_list.emplace_back(std::to_string(down_craned_list->count()));
    abstract_info_count_list.emplace_back(std::to_string(down_craned_list->count()+mix_craned_list->count()+alloc_craned_list->count()+idle_craned_list->count()));
    part_info->set_abstract_info(boost::join(abstract_info_count_list,"/"));
    if ( request.filter_nodes_on_centric_format() ) {
      idle_craned_list->set_craned_list_regex(
          boost::join(idle_craned_name_list,","));
      mix_craned_list->set_craned_list_regex(
          boost::join(mix_craned_name_list,","));
      alloc_craned_list->set_craned_list_regex(
          boost::join(alloc_craned_name_list,","));
      down_craned_list->set_craned_list_regex(
          boost::join(down_craned_name_list,","));
    }
    else
    {
      idle_craned_list->set_craned_list_regex(
          util::HostNameListToStr(idle_craned_name_list));
      mix_craned_list->set_craned_list_regex(
          util::HostNameListToStr(mix_craned_name_list));
      alloc_craned_list->set_craned_list_regex(
          util::HostNameListToStr(alloc_craned_name_list));
      down_craned_list->set_craned_list_regex(
          util::HostNameListToStr(down_craned_name_list));
      summarize_craned_name_list.splice(summarize_craned_name_list.end(),idle_craned_name_list);
      summarize_craned_name_list.splice(summarize_craned_name_list.end(),mix_craned_name_list);
      summarize_craned_name_list.splice(summarize_craned_name_list.end(),alloc_craned_name_list);
      summarize_craned_name_list.splice(summarize_craned_name_list.end(),down_craned_name_list);
      part_info->set_craned_abstract_nodes_regex(util::HostNameListToStr(summarize_craned_name_list));
    }
  });

  return reply;
}

}  // namespace Ctld