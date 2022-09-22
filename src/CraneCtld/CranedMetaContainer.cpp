#include "CranedMetaContainer.h"

#include "crane/String.h"

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

crane::grpc::QueryCranedInfoReply*
CranedMetaContainerSimpleImpl::QueryAllCranedInfo() {
  LockGuard guard(mtx_);

  auto* reply = new crane::grpc::QueryCranedInfoReply;
  auto* list = reply->mutable_craned_info_list();

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
        craned_info->set_state(crane::grpc::CranedInfo_CranedState_IDLE);
      else
        craned_info->set_state(crane::grpc::CranedInfo_CranedState_DOWN);
    }
  }

  return reply;
}

crane::grpc::QueryCranedInfoReply*
CranedMetaContainerSimpleImpl::QueryCranedInfo(const std::string& node_name) {
  LockGuard guard(mtx_);

  auto* reply = new crane::grpc::QueryCranedInfoReply;
  auto* list = reply->mutable_craned_info_list();

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
    node_info->set_state(crane::grpc::CranedInfo_CranedState_IDLE);
  else
    node_info->set_state(crane::grpc::CranedInfo_CranedState_DOWN);

  return reply;
}

crane::grpc::QueryPartitionInfoReply*
CranedMetaContainerSimpleImpl::QueryAllPartitionInfo() {
  LockGuard guard(mtx_);

  auto* reply = new crane::grpc::QueryPartitionInfoReply;
  auto* list = reply->mutable_partition_info();

  for (auto&& [part_name, part_meta] : partition_metas_map_) {
    auto* part_info = list->Add();
    auto& alloc_res_total =
        part_meta.partition_global_meta.m_resource_total_inc_dead_
            .allocatable_resource;
    auto& alloc_res_avail =
        part_meta.partition_global_meta.m_resource_total_.allocatable_resource;
    auto& alloc_res_in_use =
        part_meta.partition_global_meta.m_resource_in_use_.allocatable_resource;
    auto& alloc_res_free =
        part_meta.partition_global_meta.m_resource_avail_.allocatable_resource;
    part_info->set_name(part_meta.partition_global_meta.name);
    part_info->set_total_nodes(part_meta.partition_global_meta.node_cnt);
    part_info->set_alive_nodes(
        part_meta.partition_global_meta.alive_craned_cnt);
    part_info->set_total_cpus(alloc_res_total.cpu_count);
    part_info->set_avail_cpus(alloc_res_avail.cpu_count);
    part_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
    part_info->set_free_cpus(alloc_res_free.cpu_count);
    part_info->set_total_mem(alloc_res_total.memory_bytes);
    part_info->set_avail_mem(alloc_res_avail.memory_bytes);
    part_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
    part_info->set_free_mem(alloc_res_free.memory_bytes);

    if (part_meta.partition_global_meta.alive_craned_cnt > 0)
      part_info->set_state(crane::grpc::PartitionInfo_PartitionState_UP);
    else
      part_info->set_state(crane::grpc::PartitionInfo_PartitionState_DOWN);

    part_info->set_hostlist(part_meta.partition_global_meta.nodelist_str);
  }

  return reply;
}

crane::grpc::QueryPartitionInfoReply*
CranedMetaContainerSimpleImpl::QueryPartitionInfo(
    const std::string& partition_name) {
  LockGuard guard(mtx_);

  auto* reply = new crane::grpc::QueryPartitionInfoReply;
  auto* list = reply->mutable_partition_info();

  auto it = partition_name_id_map_.find(partition_name);
  if (it == partition_name_id_map_.end()) return reply;

  auto& part_meta = partition_metas_map_.at(it->second);

  auto* part_info = list->Add();
  auto& alloc_res_total = part_meta.partition_global_meta
                              .m_resource_total_inc_dead_.allocatable_resource;
  auto& alloc_res_avail =
      part_meta.partition_global_meta.m_resource_total_.allocatable_resource;
  auto& alloc_res_in_use =
      part_meta.partition_global_meta.m_resource_in_use_.allocatable_resource;
  auto& alloc_res_free =
      part_meta.partition_global_meta.m_resource_avail_.allocatable_resource;
  part_info->set_name(part_meta.partition_global_meta.name);
  part_info->set_total_nodes(part_meta.partition_global_meta.node_cnt);
  part_info->set_alive_nodes(part_meta.partition_global_meta.alive_craned_cnt);
  part_info->set_total_cpus(alloc_res_total.cpu_count);
  part_info->set_avail_cpus(alloc_res_avail.cpu_count);
  part_info->set_alloc_cpus(alloc_res_in_use.cpu_count);
  part_info->set_free_cpus(alloc_res_free.cpu_count);
  part_info->set_total_mem(alloc_res_total.memory_bytes);
  part_info->set_avail_mem(alloc_res_avail.memory_bytes);
  part_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  part_info->set_free_mem(alloc_res_free.memory_bytes);

  if (part_meta.partition_global_meta.alive_craned_cnt > 0)
    part_info->set_state(crane::grpc::PartitionInfo_PartitionState_UP);
  else
    part_info->set_state(crane::grpc::PartitionInfo_PartitionState_DOWN);

  part_info->set_hostlist(part_meta.partition_global_meta.nodelist_str);

  return reply;
}

crane::grpc::QueryClusterInfoReply*
CranedMetaContainerSimpleImpl::QueryClusterInfo() {
  LockGuard guard(mtx_);
  auto* reply = new crane::grpc::QueryClusterInfoReply;
  auto* partition_craned_list = reply->mutable_partition_craned();

  for (auto&& [part_name, part_meta] : partition_metas_map_) {
    auto* part_craned_info = partition_craned_list->Add();
    part_craned_info->set_name(part_meta.partition_global_meta.name);
    if (part_meta.partition_global_meta.alive_craned_cnt > 0)
      part_craned_info->set_state(crane::grpc::PartitionInfo_PartitionState_UP);
    else
      part_craned_info->set_state(
          crane::grpc::PartitionInfo_PartitionState_DOWN);

    auto* common_craned_state_list =
        part_craned_info->mutable_common_craned_state_list();

    auto* idle_craned_list = common_craned_state_list->Add();
    idle_craned_list->set_state("idle");
    auto* mix_craned_list = common_craned_state_list->Add();
    mix_craned_list->set_state("mix");
    auto* alloc_craned_list = common_craned_state_list->Add();
    alloc_craned_list->set_state("alloc");
    auto* down_craned_list = common_craned_state_list->Add();
    down_craned_list->set_state("down");

    std::list<std::string> idle_craned_name_list, mix_craned_name_list,
        alloc_craned_name_list, down_craned_name_list;

    for (auto&& [craned_index, craned_meta] : part_meta.craned_meta_map) {
      auto& alloc_res_total = craned_meta.res_total.allocatable_resource;
      auto& alloc_res_in_use = craned_meta.res_in_use.allocatable_resource;
      auto& alloc_res_avail = craned_meta.res_avail.allocatable_resource;

      if (craned_meta.alive) {
        if (alloc_res_in_use.cpu_count == 0 &&
            alloc_res_in_use.memory_bytes == 0) {
          idle_craned_list->set_craned_num(idle_craned_list->craned_num() + 1);
          idle_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
        } else if (alloc_res_avail.cpu_count == 0 &&
                   alloc_res_avail.memory_bytes == 0) {
          alloc_craned_list->set_craned_num(alloc_craned_list->craned_num() +
                                            1);
          alloc_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
        } else {
          mix_craned_list->set_craned_num(mix_craned_list->craned_num() + 1);
          mix_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
        }
      } else {
        down_craned_list->set_craned_num(down_craned_list->craned_num() + 1);
        down_craned_name_list.emplace_back(craned_meta.static_meta.hostname);
      }
    }

    idle_craned_list->set_craned_list(
        util::HostNameListToStr(idle_craned_name_list));
    mix_craned_list->set_craned_list(
        util::HostNameListToStr(mix_craned_name_list));
    alloc_craned_list->set_craned_list(
        util::HostNameListToStr(alloc_craned_name_list));
    down_craned_list->set_craned_list(
        util::HostNameListToStr(down_craned_name_list));
  }

  return reply;
}

}  // namespace Ctld