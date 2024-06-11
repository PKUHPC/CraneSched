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

namespace Ctld {

void CranedMetaContainerSimpleImpl::CranedUp(const CranedId& craned_id) {
  std::vector<std::string> part_ids;
  {
    auto crane_map = craned_meta_map_.GetMapConstSharedPtr();
    CRANE_ASSERT(crane_map->contains(craned_id));
    part_ids = crane_map->at(craned_id).GetExclusivePtr()->partition_ids;
  }

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
  std::vector<std::string> part_ids;
  {
    auto crane_map = craned_meta_map_.GetMapConstSharedPtr();
    CRANE_ASSERT(crane_map->contains(craned_id));
    part_ids = crane_map->at(craned_id).GetExclusivePtr()->partition_ids;
  }

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
    CranedId crane_id, task_id_t task_id, const Resources& resources) {
  std::vector<std::string> part_ids;
  {
    auto crane_map = craned_meta_map_.GetMapConstSharedPtr();
    if (!crane_map->contains(crane_id)) {
      CRANE_ERROR("Try to malloc resource from an unknown craned {}", crane_id);
      return;
    }
    part_ids = crane_map->at(crane_id).GetExclusivePtr()->partition_ids;
  }

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_metas_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[crane_id];
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
  std::vector<std::string> part_ids;
  {
    auto crane_map = craned_meta_map_.GetMapConstSharedPtr();
    if (!crane_map->contains(craned_id)) {
      CRANE_INFO("Try to free resource from a deleted craned {}", craned_id);
      return;
    }
    part_ids = crane_map->at(craned_id).GetExclusivePtr()->partition_ids;
  }

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

result::result<void, std::string> CranedMetaContainerSimpleImpl::AddPartition(
    PartitionMeta&& partition) {
  auto part_map = partition_metas_map_.GetMapExclusivePtr();
  auto craned_map = craned_meta_map_.GetMapExclusivePtr();

  std::string name = partition.partition_global_meta.name;

  if (part_map->contains(name)) {
    return result::fail(fmt::format("Partition {} already exists.", name));
  }

  PartitionGlobalMeta& part_global_meta = partition.partition_global_meta;

  for (const auto& node : partition.craned_ids) {
    auto node_ptr = craned_map->find(node);
    if (node_ptr == craned_map->end()) {
      return result::fail(fmt::format("Node {} not existed!", node));
    } else {
      auto node_raw_ptr = node_ptr->second.RawPtr();

      part_global_meta.m_resource_total_inc_dead_ += node_raw_ptr->res_total;
      if (node_raw_ptr->alive) {
        part_global_meta.m_resource_total_ += node_raw_ptr->res_total;
        part_global_meta.m_resource_avail_ += node_raw_ptr->res_avail;
        part_global_meta.m_resource_in_use_ += node_raw_ptr->res_in_use;
        part_global_meta.alive_craned_cnt++;
      }
    }
  }

  for (const auto& node : partition.craned_ids) {
    auto node_raw_ptr = craned_map->at(node).RawPtr();
    node_raw_ptr->partition_ids.emplace_back(name);
    std::sort(node_raw_ptr->partition_ids.begin(),
              node_raw_ptr->partition_ids.end());
  }

  part_map->emplace(name, std::move(partition));

  return {};
}

result::result<void, std::string> CranedMetaContainerSimpleImpl::AddNode(
    CranedMeta&& node) {
  auto part_map = partition_metas_map_.GetMapExclusivePtr();
  auto craned_map = craned_meta_map_.GetMapExclusivePtr();

  CranedId craned_id = node.static_meta.hostname;

  if (craned_map->contains(craned_id)) {
    return result::fail(fmt::format("Node {} already exists", craned_id));
  }

  for (const auto& part : node.partition_ids) {
    if (!part_map->contains(part)) {
      return result::fail(fmt::format("Partition {} not exists", part));
    }
  }

  // Because a new node has been added, we do not need to check if it already
  // exists in a partition
  for (const auto& part : node.partition_ids) {
    auto part_raw_ptr = part_map->at(part).RawPtr();

    part_raw_ptr->craned_ids.emplace(craned_id);
    part_raw_ptr->partition_global_meta.m_resource_total_inc_dead_ +=
        node.res_total;
    part_raw_ptr->partition_global_meta.node_cnt++;

    std::string node_str = util::HostNameListToStr(part_raw_ptr->craned_ids);
    part_raw_ptr->partition_global_meta.nodelist_str = node_str;
  }

  craned_map->emplace(craned_id, std::move(node));
  g_craned_keeper->PutNodeIntoUnavailList(craned_id);

  return {};
}

result::result<void, std::string>
CranedMetaContainerSimpleImpl::DeletePartition(PartitionId name) {
  auto part_map = partition_metas_map_.GetMapExclusivePtr();
  auto craned_map = craned_meta_map_.GetMapExclusivePtr();

  auto it = part_map->find(name);
  if (it == part_map->end()) {
    return result::fail(fmt::format("Partition {} not exists", name));
  }

  for (const auto& node : it->second.RawPtr()->craned_ids) {
    auto crane_ptr = craned_map->at(node).RawPtr();
    crane_ptr->partition_ids.erase(
        std::remove(crane_ptr->partition_ids.begin(),
                    crane_ptr->partition_ids.end(), name),
        crane_ptr->partition_ids.end());
  }

  part_map->erase(name);

  return {};
}

result::result<void, std::string> CranedMetaContainerSimpleImpl::DeleteNode(
    CranedId name) {
  auto craned_ptr = craned_meta_map_[name];

  if (!craned_ptr) {
    return result::fail(fmt::format("Node {} not exists", name));
  }

  craned_ptr->status = crane::grpc::CranedState::CRANE_DRAIN;
  g_craned_keeper->SetDeactivate(name);

  std::vector<task_id_t> job_ids;
  for (const auto& [job, res] : craned_ptr->running_task_resource_map) {
    job_ids.emplace_back(job);
  }
  if (!job_ids.empty())
    g_craned_keeper->GetCranedStub(name)->TerminateTasks(job_ids);

  m_wait_jobs_terminated_queue_.enqueue(name);

  return {};
}

result::result<void, std::string>
CranedMetaContainerSimpleImpl::UpdatePartition(PartitionMeta&& new_partition) {
  PartitionId name = new_partition.partition_global_meta.name;
  auto pre_part_ptr = partition_metas_map_[name];
  auto craned_map = craned_meta_map_.GetMapExclusivePtr();

  // The process of parameter verification should be executed first
  if (!pre_part_ptr) {
    return result::fail(fmt::format("Partition {} not exists.", name));
  }
  PartitionGlobalMeta& part_global_meta = pre_part_ptr->partition_global_meta;

  if (!new_partition.craned_ids.empty()) {
    std::vector<std::string> delete_craned, add_craned;

    for (const auto& node : pre_part_ptr->craned_ids) {
      if (!new_partition.craned_ids.contains(node))
        delete_craned.emplace_back(node);
    }

    for (const auto& node : new_partition.craned_ids) {
      if (!pre_part_ptr->craned_ids.contains(node)) {
        if (!craned_map->contains(node))
          return result::fail(fmt::format("Node {} not existed!", node));
        add_craned.emplace_back(node);
      }
    }

    for (auto&& node : add_craned) {
      auto node_raw_ptr = craned_map->at(node).RawPtr();

      part_global_meta.m_resource_total_inc_dead_ += node_raw_ptr->res_total;
      if (node_raw_ptr->alive) {
        part_global_meta.m_resource_total_ += node_raw_ptr->res_total;
        part_global_meta.m_resource_avail_ += node_raw_ptr->res_avail;
        part_global_meta.m_resource_in_use_ += node_raw_ptr->res_in_use;
        part_global_meta.alive_craned_cnt++;
      }

      node_raw_ptr->partition_ids.emplace_back(name);
      std::sort(node_raw_ptr->partition_ids.begin(),
                node_raw_ptr->partition_ids.end());
    }

    for (auto&& node : delete_craned) {
      auto node_raw_ptr = craned_map->at(node).RawPtr();

      part_global_meta.m_resource_total_inc_dead_ -= node_raw_ptr->res_total;
      if (node_raw_ptr->alive) {
        part_global_meta.m_resource_total_ -= node_raw_ptr->res_total;
        part_global_meta.m_resource_avail_ -= node_raw_ptr->res_avail;
        part_global_meta.m_resource_in_use_ -= node_raw_ptr->res_in_use;
        part_global_meta.alive_craned_cnt--;
      }

      node_raw_ptr->partition_ids.erase(
          std::remove(node_raw_ptr->partition_ids.begin(),
                      node_raw_ptr->partition_ids.end(), name),
          node_raw_ptr->partition_ids.end());
      std::sort(node_raw_ptr->partition_ids.begin(),
                node_raw_ptr->partition_ids.end());
    }

    pre_part_ptr->craned_ids = new_partition.craned_ids;
    part_global_meta.nodelist_str =
        new_partition.partition_global_meta.nodelist_str;
    part_global_meta.node_cnt = new_partition.partition_global_meta.node_cnt;
  }

  if (!new_partition.partition_global_meta.allow_accounts.empty()) {
    part_global_meta.allow_accounts =
        new_partition.partition_global_meta.allow_accounts;
  }

  if (!new_partition.partition_global_meta.deny_accounts.empty()) {
    part_global_meta.deny_accounts =
        new_partition.partition_global_meta.deny_accounts;
  }

  if (new_partition.partition_global_meta.priority >= 0) {
    part_global_meta.priority = new_partition.partition_global_meta.priority;
  }

  return {};
}

result::result<void, std::string> CranedMetaContainerSimpleImpl::UpdateNode(
    CranedMeta&& node) {
  CranedId craned_id = node.static_meta.hostname;

  auto part_map = partition_metas_map_.GetMapExclusivePtr();
  auto craned_ptr = GetCranedMetaPtr(craned_id);
  if (!craned_ptr) {
    return result::fail(fmt::format("Node {} not exists", craned_id));
  }

  if (node.res_total.allocatable_resource.cpu_count > cpu_t{0}) {
    if (node.res_total.allocatable_resource.cpu_count >
        craned_ptr->static_meta.res_physical.allocatable_resource.cpu_count) {
      return result::fail(fmt::format(
          "The number of CPUs exceeds the number of physical cores ({} cpus)",
          craned_ptr->static_meta.res_physical.allocatable_resource.cpu_count));
    } else if (node.res_total.allocatable_resource.cpu_count <
               craned_ptr->res_in_use.allocatable_resource.cpu_count) {
      return result::fail(
          fmt::format("The number of CPUs is less than the current number of "
                      "cores in use ({} cpus)",
                      craned_ptr->res_in_use.allocatable_resource.cpu_count));
    }
  }

  if (node.res_total.allocatable_resource.memory_bytes > 0) {
    if (node.res_total.allocatable_resource.memory_bytes >
        craned_ptr->static_meta.res_physical.allocatable_resource
            .memory_bytes) {
      return result::fail(fmt::format(
          "The memory size is smaller than physical memory size ({} B)",
          craned_ptr->static_meta.res_physical.allocatable_resource
              .memory_bytes));
    } else if (node.res_total.allocatable_resource.memory_bytes <
               craned_ptr->res_in_use.allocatable_resource.memory_bytes) {
      return result::fail(fmt::format(
          "The memory size is smaller than the current memory being used ({} "
          "B)",
          craned_ptr->res_in_use.allocatable_resource.memory_bytes));
    }
  }

  if (node.res_total.allocatable_resource.cpu_count > cpu_t{0}) {
    cpu_t diff = node.res_total.allocatable_resource.cpu_count -
                 craned_ptr->res_total.allocatable_resource.cpu_count;
    craned_ptr->res_total.allocatable_resource.cpu_count =
        node.res_total.allocatable_resource.cpu_count;

    craned_ptr->res_avail.allocatable_resource.cpu_count += diff;
    for (const auto& part : craned_ptr->partition_ids) {
      auto part_ptr = part_map->at(part).RawPtr();
      part_ptr->partition_global_meta.m_resource_total_inc_dead_
          .allocatable_resource.cpu_count += diff;
      part_ptr->partition_global_meta.m_resource_total_.allocatable_resource
          .cpu_count += diff;
      part_ptr->partition_global_meta.m_resource_avail_.allocatable_resource
          .cpu_count += diff;
    }
  }

  if (node.res_total.allocatable_resource.memory_bytes > 0) {
    uint64_t diff = node.res_total.allocatable_resource.memory_bytes -
                    craned_ptr->res_total.allocatable_resource.memory_bytes;
    craned_ptr->res_total.allocatable_resource.memory_bytes =
        craned_ptr->res_total.allocatable_resource.memory_sw_bytes =
            node.res_total.allocatable_resource.memory_bytes;

    craned_ptr->res_avail.allocatable_resource.memory_bytes += diff;
    for (const auto& part : craned_ptr->partition_ids) {
      auto part_ptr = part_map->at(part).RawPtr();
      part_ptr->partition_global_meta.m_resource_total_inc_dead_
          .allocatable_resource.memory_bytes += diff;
      part_ptr->partition_global_meta.m_resource_total_.allocatable_resource
          .memory_bytes += diff;
      part_ptr->partition_global_meta.m_resource_avail_.allocatable_resource
          .memory_bytes += diff;
    }
  }

  return {};
}

void CranedMetaContainerSimpleImpl::RegisterPhysicalResource(
    const CranedId& name, double cpu, uint64_t memory_bytes) {
  auto craned_ptr = craned_meta_map_[name];
  craned_ptr->static_meta.res_physical.allocatable_resource.cpu_count =
      cpu_t(cpu);
  craned_ptr->static_meta.res_physical.allocatable_resource.memory_bytes =
      memory_bytes;
}

void CranedMetaContainerSimpleImpl::WaitJobsTerminatedCb_() {
  size_t approximate_size = m_wait_jobs_terminated_queue_.size_approx();
  std::vector<CranedId> craned_to_wait;
  craned_to_wait.resize(approximate_size);

  m_wait_jobs_terminated_queue_.try_dequeue_bulk(craned_to_wait.begin(),
                                                 approximate_size);

  bool found = false;
  for (const auto& craned : craned_to_wait) {
    if (craned_meta_map_[craned]->running_task_resource_map.empty()) {
      found = true;
      m_delete_craned_queue_.enqueue(craned);
    } else {
      m_wait_jobs_terminated_queue_.enqueue(craned);
    }
  }
  if (found) m_clean_deletion_queue_handle_->send();
}

void CranedMetaContainerSimpleImpl::CleanDeletionQueueCb_() {
  size_t approximate_size = m_delete_craned_queue_.size_approx();
  std::vector<CranedId> craned_to_delete;
  craned_to_delete.resize(approximate_size);

  m_delete_craned_queue_.try_dequeue_bulk(craned_to_delete.begin(),
                                          approximate_size);

  for (const auto& craned : craned_to_delete) {
    g_craned_keeper->ResetConnection(
        craned);  // call CranedDown() automatically
  }

  // Waiting for all CranedDown() calls to end
  for (const auto& craned : craned_to_delete) {
    while (true) {
      if (!craned_meta_map_[craned]->alive) {
        break;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  }

  auto part_map = partition_metas_map_.GetMapExclusivePtr();
  auto craned_map = craned_meta_map_.GetMapExclusivePtr();

  for (const auto& craned : craned_to_delete) {
    auto craned_ptr = craned_map->at(craned).RawPtr();
    for (const auto& part : craned_ptr->partition_ids) {
      PartitionMeta* part_ptr = part_map->at(part).RawPtr();
      part_ptr->partition_global_meta.node_cnt--;
      part_ptr->partition_global_meta.m_resource_total_inc_dead_ -=
          craned_ptr->res_total;  // copy of res
      part_ptr->craned_ids.erase(craned);
      part_ptr->partition_global_meta.nodelist_str =
          util::HostNameListToStr(part_ptr->craned_ids);
    }
    craned_map->erase(craned);
  }
}

void CranedMetaContainerSimpleImpl::DeleteCranedThread_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("DeleteCraned");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in craned keeper loop.");
  }

  uvw_loop->run();
}

void CranedMetaContainerSimpleImpl::InitFromConfig(const Config& config) {
  HashMap<CranedId, CranedMeta> craned_map;
  HashMap<PartitionId, PartitionMeta> partition_map;

  for (auto&& [craned_name, node_ptr] : config.Nodes) {
    CRANE_TRACE("Parsing node {}", craned_name);

    auto& craned_meta = craned_map[craned_name];

    craned_meta.res_total.allocatable_resource.cpu_count =
        cpu_t(config.Nodes.at(craned_name)->cpu);
    craned_meta.res_total.allocatable_resource.memory_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    craned_meta.res_total.allocatable_resource.memory_sw_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    craned_meta.static_meta.hostname = craned_name;
    craned_meta.static_meta.port =
        std::strtoul(kCranedDefaultPort, nullptr, 10);

    craned_meta.res_avail = craned_meta.res_total;
  }

  for (auto&& [part_name, partition] : config.Partitions) {
    CRANE_TRACE("Parsing partition {}", part_name);

    Resources part_res;

    auto& part_meta = partition_map[part_name];

    for (auto&& craned_name : partition.nodes) {
      auto& craned_meta = craned_map[craned_name];
      craned_meta.partition_ids.emplace_back(part_name);

      part_meta.craned_ids.emplace(craned_name);

      CRANE_DEBUG(
          "Add the resource of Craned {} (cpu: {}, mem: {}) to partition "
          "[{}]'s global resource.",
          craned_name, craned_meta.res_total.allocatable_resource.cpu_count,
          util::ReadableMemory(
              craned_meta.res_total.allocatable_resource.memory_bytes),
          part_name);

      part_res += craned_meta.res_total;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.m_resource_total_inc_dead_ = part_res;
    part_meta.partition_global_meta.node_cnt = part_meta.craned_ids.size();
    part_meta.partition_global_meta.nodelist_str = partition.nodelist_str;
    part_meta.partition_global_meta.priority = partition.priority;

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

  for (auto& [name, craned_meta] : craned_map) {
    std::sort(craned_meta.partition_ids.begin(),
              craned_meta.partition_ids.end());
  }

  craned_meta_map_.InitFromMap(std::move(craned_map));
  partition_metas_map_.InitFromMap(std::move(partition_map));

  std::shared_ptr<uvw::loop> uvw_delete_craned_loop = uvw::loop::create();
  m_wait_jobs_terminated_handle_ =
      uvw_delete_craned_loop->resource<uvw::timer_handle>();
  m_wait_jobs_terminated_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        WaitJobsTerminatedCb_();
      });
  m_wait_jobs_terminated_handle_->start(std::chrono::milliseconds(5000),
                                        std::chrono::milliseconds(5000));

  m_clean_deletion_queue_handle_ =
      uvw_delete_craned_loop->resource<uvw::async_handle>();
  m_clean_deletion_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanDeletionQueueCb_();
      });

  m_delete_craned_thread_ =
      std::thread([this, loop = std::move(uvw_delete_craned_loop)]() {
        DeleteCranedThread_(loop);
      });
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
    craned_info->set_cpu(static_cast<double>(alloc_res_total.cpu_count));
    craned_info->set_alloc_cpu(static_cast<double>(alloc_res_in_use.cpu_count));
    craned_info->set_free_cpu(static_cast<double>(alloc_res_avail.cpu_count));
    craned_info->set_real_mem(alloc_res_total.memory_bytes);
    craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
    craned_info->set_free_mem(alloc_res_avail.memory_bytes);
    craned_info->set_running_task_num(
        craned_meta->running_task_resource_map.size());
    if (craned_meta->alive) {
      if (craned_meta->drain) {
        craned_info->set_state(crane::grpc::CranedState::CRANE_DRAIN);
      } else if (craned_meta->res_in_use.allocatable_resource.cpu_count ==
                     cpu_t(0) &&
                 craned_meta->res_in_use.allocatable_resource.memory_bytes == 0)
        craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
      else if (craned_meta->res_avail.allocatable_resource.cpu_count ==
                   cpu_t(0) ||
               craned_meta->res_avail.allocatable_resource.memory_bytes == 0)
        craned_info->set_state(crane::grpc::CranedState::CRANE_ALLOC);
      else
        craned_info->set_state(crane::grpc::CranedState::CRANE_MIX);
    } else
      craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

    craned_info->mutable_partition_names()->Assign(
        craned_meta->partition_ids.begin(), craned_meta->partition_ids.end());
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
  craned_info->set_cpu(static_cast<double>(alloc_res_total.cpu_count));
  craned_info->set_alloc_cpu(static_cast<double>(alloc_res_in_use.cpu_count));
  craned_info->set_free_cpu(static_cast<double>(alloc_res_avail.cpu_count));
  craned_info->set_real_mem(alloc_res_total.memory_bytes);
  craned_info->set_alloc_mem(alloc_res_in_use.memory_bytes);
  craned_info->set_free_mem(alloc_res_avail.memory_bytes);
  craned_info->set_running_task_num(
      craned_meta->running_task_resource_map.size());
  if (craned_meta->alive) {
    if (craned_meta->drain) {
      craned_info->set_state(crane::grpc::CranedState::CRANE_DRAIN);
    } else if (craned_meta->res_in_use.allocatable_resource.cpu_count ==
                   cpu_t(0) &&
               craned_meta->res_in_use.allocatable_resource.memory_bytes == 0)
      craned_info->set_state(crane::grpc::CranedState::CRANE_IDLE);
    else if (craned_meta->res_avail.allocatable_resource.cpu_count ==
                 cpu_t(0) ||
             craned_meta->res_avail.allocatable_resource.memory_bytes == 0)
      craned_info->set_state(crane::grpc::CranedState::CRANE_ALLOC);
    else
      craned_info->set_state(crane::grpc::CranedState::CRANE_MIX);
  } else
    craned_info->set_state(crane::grpc::CranedState::CRANE_DOWN);

  craned_info->mutable_partition_names()->Assign(
      craned_meta->partition_ids.begin(), craned_meta->partition_ids.end());

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
    part_info->set_total_cpu(static_cast<double>(alloc_res_total.cpu_count));
    part_info->set_avail_cpu(static_cast<double>(alloc_res_avail.cpu_count));
    part_info->set_alloc_cpu(static_cast<double>(alloc_res_in_use.cpu_count));
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
  part_info->set_total_cpu(static_cast<double>(res_total.cpu_count));
  part_info->set_avail_cpu(static_cast<double>(res_avail.cpu_count));
  part_info->set_alloc_cpu(static_cast<double>(res_in_use.cpu_count));
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

  bool filter_idle = false, filter_alloc = false, filter_mix = false,
       filter_down = false, filter_drain = false;

  if (request.filter_craned_states().empty()) return reply;
  for (const auto& it : request.filter_craned_states()) {
    switch (it) {
      case crane::grpc::CranedState::CRANE_IDLE:
        filter_idle = true;
        break;
      case crane::grpc::CranedState::CRANE_ALLOC:
        filter_alloc = true;
        break;
      case crane::grpc::CranedState::CRANE_MIX:
        filter_mix = true;
        break;
      case crane::grpc::CranedState::CRANE_DOWN:
        filter_down = true;
        break;
      case crane::grpc::CranedState::CRANE_DRAIN:
        filter_drain = true;
      default:
        break;
    }
  }

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
    std::set<CranedId> craned_ids;
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
    auto* drain_craned_list = craned_lists->Add();

    idle_craned_list->set_state(crane::grpc::CranedState::CRANE_IDLE);
    mix_craned_list->set_state(crane::grpc::CranedState::CRANE_MIX);
    alloc_craned_list->set_state(crane::grpc::CranedState::CRANE_ALLOC);
    down_craned_list->set_state(crane::grpc::CranedState::CRANE_DOWN);
    drain_craned_list->set_state(crane::grpc::CranedState::CRANE_DRAIN);

    std::list<std::string> idle_craned_name_list, mix_craned_name_list,
        alloc_craned_name_list, down_craned_name_list, drain_craned_name_list;

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
      if (craned_meta->alive) {
        if (craned_meta->drain) {
          if (filter_drain) {
            drain_craned_name_list.emplace_back(
                craned_meta->static_meta.hostname);
            drain_craned_list->set_count(drain_craned_name_list.size());
          }
        } else if (res_in_use.cpu_count == cpu_t(0) &&
                   res_in_use.memory_bytes == 0) {
          if (filter_idle) {
            idle_craned_name_list.emplace_back(
                craned_meta->static_meta.hostname);
            idle_craned_list->set_count(idle_craned_name_list.size());
          }
        } else if (res_avail.cpu_count == cpu_t(0) ||
                   res_avail.memory_bytes == 0) {
          if (filter_alloc) {
            alloc_craned_name_list.emplace_back(
                craned_meta->static_meta.hostname);
            alloc_craned_list->set_count(alloc_craned_name_list.size());
          }
        } else {
          if (filter_mix) {
            mix_craned_name_list.emplace_back(
                craned_meta->static_meta.hostname);
            mix_craned_list->set_count(mix_craned_name_list.size());
          }
        }
      } else {
        if (filter_down) {
          down_craned_name_list.emplace_back(craned_meta->static_meta.hostname);
          down_craned_list->set_count(down_craned_name_list.size());
        }
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
    drain_craned_list->set_craned_list_regex(
        util::HostNameListToStr(drain_craned_name_list));
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
    if (request.new_state() == crane::grpc::CranedState::CRANE_DRAIN) {
      craned_meta->drain = true;
      craned_meta->state_reason = request.reason();
      g_craned_keeper->SetDeactivate(request.craned_id());
      reply.set_ok(true);
    } else if (request.new_state() == crane::grpc::CranedState::CRANE_IDLE) {
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

}  // namespace Ctld