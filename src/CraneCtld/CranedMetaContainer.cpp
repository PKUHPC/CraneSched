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

#include "CranedMetaContainer.h"

#include "CranedKeeper.h"
#include "TaskScheduler.h"
#include "crane/PluginClient.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

void CranedMetaContainer::CranedUp(
    const CranedId& craned_id,
    const crane::grpc::CranedRemoteMeta& remote_meta) {
  CRANE_ASSERT(craned_id_part_ids_map_.contains(craned_id));
  auto& part_ids = craned_id_part_ids_map_.at(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  CRANE_ASSERT(craned_meta_map_.Contains(craned_id));
  auto node_meta = craned_meta_map_[craned_id];
  auto stub = g_craned_keeper->GetCranedStub(craned_id);
  if (stub != nullptr) {
    if (!stub->SetConnected(req->token())) {
      CRANE_DEBUG("Craned {} try to up,but insufficient token {} got.",
                  craned_id, req->token());
    }
    stub.reset();
  }
  node_meta->alive = true;

  node_meta->remote_meta = CranedRemoteMeta(remote_meta);

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.alive_craned_cnt++;
  }

  CRANE_INFO("Craned {} is up now.", craned_id);
}

void CranedMetaContainer::CranedDown(const CranedId& craned_id) {
  CRANE_ASSERT(craned_id_part_ids_map_.contains(craned_id));
  auto& part_ids = craned_id_part_ids_map_.at(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids) {
    auto& raw_part_meta = raw_part_metas_map->at(part_id);
    part_meta_ptrs.emplace_back(raw_part_meta.GetExclusivePtr());
  }

  // Then acquire craned meta lock.
  CRANE_ASSERT(craned_meta_map_.Contains(craned_id));
  auto node_meta = craned_meta_map_[craned_id];

  if (!node_meta->alive) {
    CRANE_TRACE("Craned {} down, but it's not alive, skip clean.", craned_id);
    return;
  }
  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.alive_craned_cnt--;
  }
  node_meta->alive = false;
}

bool CranedMetaContainer::CheckCranedOnline(const CranedId& craned_id) {
  auto craned_meta_ptr = craned_meta_map_.GetValueExclusivePtr(craned_id);
  CRANE_ASSERT(craned_meta_ptr);

  return craned_meta_ptr->alive;
}

int CranedMetaContainer::GetOnlineCranedCount() {
  int count = 0;
  auto map_ptr = craned_meta_map_.GetMapConstSharedPtr();
  for (const auto& v : *map_ptr | std::ranges::views::values)
    if (v.GetExclusivePtr()->alive) count++;
  return count;
}

CranedMetaContainer::PartitionMetaPtr CranedMetaContainer::GetPartitionMetasPtr(
    const PartitionId& partition_id) {
  return partition_meta_map_.GetValueExclusivePtr(partition_id);
}

CranedMetaContainer::CranedMetaPtr CranedMetaContainer::GetCranedMetaPtr(
    const CranedId& craned_id) {
  return craned_meta_map_.GetValueExclusivePtr(craned_id);
}

CranedMetaContainer::ResvMetaPtr CranedMetaContainer::GetResvMetaPtr(
    const ResvId& name) {
  return resv_meta_map_.GetValueExclusivePtr(name);
}

CranedMetaContainer::AllPartitionsMetaMapConstPtr
CranedMetaContainer::GetAllPartitionsMetaMapConstPtr() {
  return partition_meta_map_.GetMapConstSharedPtr();
}

CranedMetaContainer::CranedMetaMapConstPtr
CranedMetaContainer::GetCranedMetaMapConstPtr() {
  return craned_meta_map_.GetMapConstSharedPtr();
}

CranedMetaContainer::ResvMetaMapConstPtr
CranedMetaContainer::GetResvMetaMapConstPtr() {
  return resv_meta_map_.GetMapConstSharedPtr();
}

CranedMetaContainer::ResvMetaMapPtr CranedMetaContainer::GetResvMetaMapPtr() {
  return resv_meta_map_.GetMapSharedPtr();
}

void CranedMetaContainer::MallocResourceFromNode(CranedId node_id,
                                                 task_id_t task_id,
                                                 const ResourceV2& resources) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to malloc resource from an unknown craned {}", node_id);
    return;
  }

  auto& part_ids = craned_id_part_ids_map_.at(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  const ResourceInNode& task_node_res = resources.at(node_id);

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];

  node_meta->rn_task_res_map.emplace(task_id, task_node_res);

  node_meta->res_avail -= task_node_res;
  node_meta->res_in_use += task_node_res;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;

    part_global_meta.res_avail -= task_node_res;
    part_global_meta.res_in_use += task_node_res;
  }
}

void CranedMetaContainer::FreeResourceFromNode(CranedId node_id,
                                               uint32_t task_id) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to free resource from an unknown craned {}", node_id);
    return;
  }

  auto& part_ids = craned_id_part_ids_map_.at(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];

  auto resource_iter = node_meta->rn_task_res_map.find(task_id);
  if (resource_iter == node_meta->rn_task_res_map.end()) {
    CRANE_ERROR("Try to free resource from an unknown task {} on craned {}",
                task_id, node_id);
    return;
  }

  ResourceInNode const& resources = resource_iter->second;

  node_meta->res_avail += resources;
  node_meta->res_in_use -= resources;
  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;

    part_global_meta.res_avail += resources;
    part_global_meta.res_in_use -= resources;
  }

  node_meta->rn_task_res_map.erase(resource_iter);
}

void CranedMetaContainer::MallocResourceFromResv(
    ResvId resv_id, task_id_t task_id, const LogicalPartition::RnTaskRes& res) {
  if (!resv_meta_map_.Contains(resv_id)) {
    CRANE_ERROR("Try to malloc resource from an unknown reservation {}",
                resv_id);
    return;
  }

  const auto& resv_meta = resv_meta_map_[resv_id];

  resv_meta->logical_part.res_avail -= res.resources;
  resv_meta->logical_part.res_in_use += res.resources;

  resv_meta->logical_part.rn_task_res_map.emplace(task_id, res);
}

void CranedMetaContainer::FreeResourceFromResv(ResvId reservation_id,
                                               task_id_t task_id) {
  if (!resv_meta_map_.Contains(reservation_id)) {
    CRANE_ERROR("Try to free resource from an unknown reservation {}",
                reservation_id);
    return;
  }

  const auto& resv_meta = resv_meta_map_[reservation_id];

  auto resource_iter = resv_meta->logical_part.rn_task_res_map.find(task_id);
  if (resource_iter == resv_meta->logical_part.rn_task_res_map.end()) {
    CRANE_ERROR(
        "Try to free resource from an unknown task {} on reservation {}",
        task_id, reservation_id);
    return;
  }

  ResourceV2 const& resources = resource_iter->second.resources;

  resv_meta->logical_part.res_avail += resources;
  resv_meta->logical_part.res_in_use -= resources;

  resv_meta->logical_part.rn_task_res_map.erase(resource_iter);
}

void CranedMetaContainer::InitFromConfig(const Config& config) {
  HashMap<CranedId, CranedMeta> craned_map;
  HashMap<PartitionId, PartitionMeta> partition_map;

  for (auto&& [craned_name, node_ptr] : config.Nodes) {
    CRANE_TRACE("Parsing node {}", craned_name);

    auto& craned_meta = craned_map[craned_name];
    craned_meta.remote_meta.craned_version = "unknown";
    craned_meta.remote_meta.sys_rel_info.name = "unknown";

    auto& static_meta = craned_meta.static_meta;
    static_meta.res.allocatable_res.cpu_count =
        cpu_t(config.Nodes.at(craned_name)->cpu);
    static_meta.res.allocatable_res.memory_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    static_meta.res.allocatable_res.memory_sw_bytes =
        config.Nodes.at(craned_name)->memory_bytes;
    static_meta.res.dedicated_res =
        config.Nodes.at(craned_name)->dedicated_resource;
    static_meta.hostname = craned_name;
    static_meta.port = std::strtoul(
        g_config.CranedListenConf.CranedListenPort.c_str(), nullptr, 10);

    craned_meta.res_total += static_meta.res;
    craned_meta.res_avail += static_meta.res;
    craned_meta.res_in_use.SetToZero();
  }

  for (auto&& [part_name, partition] : config.Partitions) {
    CRANE_TRACE("Parsing partition {}", part_name);

    ResourceView part_res;

    auto& part_meta = partition_map[part_name];

    for (auto&& craned_name : partition.nodes) {
      auto& craned_meta = craned_map[craned_name];
      craned_meta.static_meta.partition_ids.emplace_back(part_name);

      craned_id_part_ids_map_[craned_name].emplace_back(part_name);

      part_meta.craned_ids.emplace(craned_name);
      part_meta.partition_global_meta.res_avail += craned_meta.static_meta.res;
      part_meta.partition_global_meta.res_total += craned_meta.static_meta.res;

      CRANE_DEBUG(
          "Add the resource of Craned {} (cpu: {}, mem: {}, gres: {}) to "
          "partition [{}]'s global resource.",
          craned_name, craned_meta.static_meta.res.allocatable_res.cpu_count,
          util::ReadableMemory(
              craned_meta.static_meta.res.allocatable_res.memory_bytes),
          util::ReadableDresInNode(craned_meta.static_meta.res), part_name);

      part_res += craned_meta.static_meta.res;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.res_total_inc_dead = part_res;
    part_meta.partition_global_meta.res_in_use.SetToZero();
    part_meta.partition_global_meta.node_cnt = part_meta.craned_ids.size();
    part_meta.partition_global_meta.nodelist_str = partition.nodelist_str;
    part_meta.partition_global_meta.allowed_accounts =
        partition.allowed_accounts;
    part_meta.partition_global_meta.denied_accounts = partition.denied_accounts;

    CRANE_DEBUG(
        "partition [{}]'s Global resource now: (cpu: {}, mem: {}, "
        "gres: {}). "
        "It has {} craneds.",
        part_name,
        part_meta.partition_global_meta.res_total_inc_dead.CpuCount(),
        util::ReadableMemory(
            part_meta.partition_global_meta.res_total_inc_dead.MemoryBytes()),
        util::ReadableTypedDeviceMap(
            part_meta.partition_global_meta.res_total_inc_dead.GetDeviceMap()),
        part_meta.partition_global_meta.node_cnt);
  }

  craned_meta_map_.InitFromMap(std::move(craned_map));
  partition_meta_map_.InitFromMap(std::move(partition_map));
}

crane::grpc::QueryCranedInfoReply CranedMetaContainer::QueryAllCranedInfo() {
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

crane::grpc::QueryCranedInfoReply CranedMetaContainer::QueryCranedInfo(
    const CranedId& node_name) {
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
CranedMetaContainer::QueryAllPartitionInfo() {
  crane::grpc::QueryPartitionInfoReply reply;
  auto* list = reply.mutable_partition_info_list();

  auto partition_map = partition_meta_map_.GetMapConstSharedPtr();

  for (auto&& [part_name, part_meta_ptr] : *partition_map) {
    auto* part_info = list->Add();
    auto part_meta = part_meta_ptr.GetExclusivePtr();

    part_info->set_name(part_meta->partition_global_meta.name);
    part_info->set_total_nodes(part_meta->partition_global_meta.node_cnt);
    part_info->set_alive_nodes(
        part_meta->partition_global_meta.alive_craned_cnt);
    auto* allowed_accounts = part_info->mutable_allowed_accounts();
    for (const auto& account_name :
         part_meta->partition_global_meta.allowed_accounts) {
      allowed_accounts->Add()->assign(account_name);
    }

    auto* denied_accounts = part_info->mutable_denied_accounts();
    for (const auto& account_name :
         part_meta->partition_global_meta.denied_accounts) {
      denied_accounts->Add()->assign(account_name);
    }

    *part_info->mutable_res_total() = static_cast<crane::grpc::ResourceView>(
        part_meta->partition_global_meta.res_total);
    *part_info->mutable_res_avail() = static_cast<crane::grpc::ResourceView>(
        part_meta->partition_global_meta.res_avail);
    *part_info->mutable_res_alloc() = static_cast<crane::grpc::ResourceView>(
        part_meta->partition_global_meta.res_in_use);
    part_info->set_default_mem_per_cpu(
        g_config.Partitions[part_name].default_mem_per_cpu);
    part_info->set_max_mem_per_cpu(
        g_config.Partitions[part_name].max_mem_per_cpu);

    if (part_meta->partition_global_meta.alive_craned_cnt > 0)
      part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
    else
      part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

    part_info->set_hostlist(part_meta->partition_global_meta.nodelist_str);
  }

  return reply;
}

crane::grpc::QueryPartitionInfoReply CranedMetaContainer::QueryPartitionInfo(
    const PartitionId& partition_name) {
  crane::grpc::QueryPartitionInfoReply reply;
  auto* list = reply.mutable_partition_info_list();

  if (!partition_meta_map_.Contains(partition_name)) return reply;

  auto part_meta = partition_meta_map_.GetValueExclusivePtr(partition_name);

  auto* part_info = list->Add();
  part_info->set_name(part_meta->partition_global_meta.name);
  part_info->set_total_nodes(part_meta->partition_global_meta.node_cnt);
  part_info->set_alive_nodes(part_meta->partition_global_meta.alive_craned_cnt);
  auto* allowed_accounts = part_info->mutable_allowed_accounts();
  for (const auto& account_name :
       part_meta->partition_global_meta.allowed_accounts) {
    allowed_accounts->Add()->assign(account_name);
  }
  auto* denied_accounts = part_info->mutable_denied_accounts();
  for (const auto& account_name :
       part_meta->partition_global_meta.denied_accounts) {
    denied_accounts->Add()->assign(account_name);
  }
  if (part_meta->partition_global_meta.alive_craned_cnt > 0)
    part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
  else
    part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

  part_info->set_hostlist(part_meta->partition_global_meta.nodelist_str);

  part_info->set_default_mem_per_cpu(
      g_config.Partitions[partition_name].default_mem_per_cpu);
  part_info->set_max_mem_per_cpu(
      g_config.Partitions[partition_name].max_mem_per_cpu);

  *part_info->mutable_res_total() = static_cast<crane::grpc::ResourceView>(
      part_meta->partition_global_meta.res_total);
  *part_info->mutable_res_avail() = static_cast<crane::grpc::ResourceView>(
      part_meta->partition_global_meta.res_avail);
  *part_info->mutable_res_alloc() = static_cast<crane::grpc::ResourceView>(
      part_meta->partition_global_meta.res_in_use);

  return reply;
}

crane::grpc::QueryReservationInfoReply CranedMetaContainer::QueryAllResvInfo() {
  crane::grpc::QueryReservationInfoReply reply;
  auto* list = reply.mutable_reservation_info_list();

  auto resv_map_ptr = resv_meta_map_.GetMapConstSharedPtr();
  for (auto&& [resv_id, resv_meta] : *resv_map_ptr) {
    const auto& resv_meta_ptr = resv_meta.GetExclusivePtr();

    auto* reservation_info = list->Add();

    reservation_info->set_reservation_name(resv_id);
    reservation_info->mutable_start_time()->set_seconds(
        absl::ToUnixSeconds(resv_meta_ptr->logical_part.start_time));
    reservation_info->mutable_duration()->set_seconds(
        absl::ToUnixSeconds(resv_meta_ptr->logical_part.end_time) -
        absl::ToUnixSeconds(resv_meta_ptr->logical_part.start_time));
    reservation_info->set_partition(resv_meta_ptr->part_id);
    reservation_info->set_craned_regex(
        util::HostNameListToStr(resv_meta_ptr->logical_part.craned_ids));

    ResourceView res_total;
    ResourceView res_avail;
    ResourceView res_alloc;

    res_total += resv_meta_ptr->logical_part.res_total;
    res_avail += resv_meta_ptr->logical_part.res_avail;
    res_alloc += resv_meta_ptr->logical_part.res_in_use;

    reservation_info->mutable_res_total()->CopyFrom(
        static_cast<crane::grpc::ResourceView>(res_total));
    reservation_info->mutable_res_avail()->CopyFrom(
        static_cast<crane::grpc::ResourceView>(res_avail));
    reservation_info->mutable_res_alloc()->CopyFrom(
        static_cast<crane::grpc::ResourceView>(res_alloc));

    if (resv_meta_ptr->accounts_black_list) {
      for (auto const& account : resv_meta_ptr->accounts) {
        reservation_info->add_denied_accounts()->assign(account);
      }
    } else {
      for (auto const& account : resv_meta_ptr->accounts) {
        reservation_info->add_allowed_accounts()->assign(account);
      }
    }
    if (resv_meta_ptr->users_black_list) {
      for (auto const& user : resv_meta_ptr->users) {
        reservation_info->add_denied_users()->assign(user);
      }
    } else {
      for (auto const& user : resv_meta_ptr->users) {
        reservation_info->add_allowed_users()->assign(user);
      }
    }
  }
  return reply;
}

crane::grpc::QueryReservationInfoReply CranedMetaContainer::QueryResvInfo(
    const ResvId& resv_name) {
  crane::grpc::QueryReservationInfoReply reply;
  auto* list = reply.mutable_reservation_info_list();

  if (!resv_meta_map_.Contains(resv_name)) {
    return reply;
  }

  const auto& reservation_meta = resv_meta_map_.GetValueExclusivePtr(resv_name);

  auto* reservation_info = list->Add();

  reservation_info->set_reservation_name(resv_name);
  reservation_info->mutable_start_time()->set_seconds(
      absl::ToUnixSeconds(reservation_meta->logical_part.start_time));
  reservation_info->mutable_duration()->set_seconds(
      absl::ToUnixSeconds(reservation_meta->logical_part.end_time) -
      absl::ToUnixSeconds(reservation_meta->logical_part.start_time));
  reservation_info->set_partition(reservation_meta->part_id);
  reservation_info->set_craned_regex(
      util::HostNameListToStr(reservation_meta->logical_part.craned_ids));

  ResourceView res_total;
  ResourceView res_avail;
  ResourceView res_alloc;

  res_total += reservation_meta->logical_part.res_total;
  res_avail += reservation_meta->logical_part.res_avail;
  res_alloc += reservation_meta->logical_part.res_in_use;

  reservation_info->mutable_res_total()->CopyFrom(
      static_cast<crane::grpc::ResourceView>(res_total));
  reservation_info->mutable_res_avail()->CopyFrom(
      static_cast<crane::grpc::ResourceView>(res_avail));
  reservation_info->mutable_res_alloc()->CopyFrom(
      static_cast<crane::grpc::ResourceView>(res_alloc));

  if (reservation_meta->accounts_black_list) {
    for (auto const& account : reservation_meta->accounts) {
      reservation_info->add_denied_accounts()->assign(account);
    }
  } else {
    for (auto const& account : reservation_meta->accounts) {
      reservation_info->add_allowed_accounts()->assign(account);
    }
  }
  if (reservation_meta->users_black_list) {
    for (auto const& user : reservation_meta->users) {
      reservation_info->add_denied_users()->assign(user);
    }
  } else {
    for (auto const& user : reservation_meta->users) {
      reservation_info->add_allowed_users()->assign(user);
    }
  }
  return reply;
}

crane::grpc::QueryClusterInfoReply CranedMetaContainer::QueryClusterInfo(
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
      request.filter_craned_resource_states().empty()) {
    reply.set_ok(true);
    return reply;
  }

  const int control_state_num = crane::grpc::CranedControlState_ARRAYSIZE;
  bool control_filters[control_state_num] = {false};
  for (const auto& it : request.filter_craned_control_states())
    control_filters[static_cast<int>(it)] = true;

  const int resource_state_num = crane::grpc::CranedResourceState_ARRAYSIZE;
  bool resource_filters[resource_state_num] = {false};
  for (const auto& it : request.filter_craned_resource_states())
    resource_filters[static_cast<int>(it)] = true;

  // Ensure that the map global read lock is held during the following filtering
  // operations and partition_meta_map_ must be locked before craned_meta_map_
  auto partition_map = partition_meta_map_.GetMapConstSharedPtr();
  auto craned_map = craned_meta_map_.GetMapConstSharedPtr();

  auto partition_rng =
      *partition_map | ranges::views::filter(partition_rng_filter_name);
  ranges::for_each(partition_rng, [&](auto& it) {
    PartitionId part_id = it.first;

    auto* part_info = partition_list->Add();

    // we make a copy of these craned ids to lower overall latency.
    // The amortized cost is 1 copy for each craned node.
    // Although an extra copying cost is introduced,
    // the time of accessing partition_meta_map_ is minimized and
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

      auto& res_total = craned_meta->res_total;
      auto& res_in_use = craned_meta->res_in_use;
      auto& res_avail = craned_meta->res_avail;

      crane::grpc::CranedControlState control_state;
      if (craned_meta->drain) {
        control_state = crane::grpc::CranedControlState::CRANE_DRAIN;
      } else {
        control_state = crane::grpc::CranedControlState::CRANE_NONE;
      }
      crane::grpc::CranedResourceState resource_state;
      if (craned_meta->alive) {
        if (res_in_use.IsZero()) {
          resource_state = crane::grpc::CranedResourceState::CRANE_IDLE;
        } else if (res_avail.allocatable_res.IsAnyZero()) {
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

  reply.set_ok(true);
  return reply;
}

crane::grpc::ModifyCranedStateReply CranedMetaContainer::ChangeNodeState(
    const crane::grpc::ModifyCranedStateRequest& request) {
  crane::grpc::ModifyCranedStateReply reply;
  std::vector<crane::grpc::plugin::CranedEventInfo> event_list;
  crane::grpc::plugin::CranedEventInfo event;

  if (g_config.Plugin.Enabled) {
    // Generate timestamp
    absl::Time now = absl::Now();
    int64_t seconds = absl::ToUnixSeconds(now);
    int32_t nanos = static_cast<int32_t>(absl::ToUnixNanos(now) % 1000000000);

    auto timestamp = std::make_unique<::google::protobuf::Timestamp>();
    timestamp->set_seconds(seconds);
    timestamp->set_nanos(nanos);

    event.set_cluster_name(g_config.CraneClusterName);
    event.set_uid(request.uid());
    event.set_reason(request.reason());
    event.set_allocated_start_time(timestamp.release());
  }

  for (auto craned_id : request.craned_ids()) {
    if (!craned_meta_map_.Contains(craned_id)) {
      reply.add_not_modified_nodes(craned_id);
      reply.add_not_modified_reasons("Invalid node name specified.");
      continue;
    }

    auto craned_meta = craned_meta_map_[craned_id];

    if (craned_meta->alive) {
      if (request.new_state() == crane::grpc::CranedControlState::CRANE_DRAIN) {
        if (g_config.Plugin.Enabled && craned_meta->drain != true) {
          // Set node event info
          event.set_node_name(craned_id);
          event.set_state(crane::grpc::CranedControlState::CRANE_DRAIN);
          event_list.emplace_back(event);
        }

        craned_meta->drain = true;
        craned_meta->state_reason = request.reason();
        reply.add_modified_nodes(craned_id);
      } else if (request.new_state() ==
                 crane::grpc::CranedControlState::CRANE_NONE) {
        if (g_config.Plugin.Enabled && craned_meta->drain != false) {
          // Set node event info
          event.set_node_name(craned_id);
          event.set_state(crane::grpc::CranedControlState::CRANE_NONE);
          event_list.emplace_back(event);
        }

        craned_meta->drain = false;
        craned_meta->state_reason.clear();
        reply.add_modified_nodes(craned_id);
      } else {
        reply.add_not_modified_nodes(craned_id);
        reply.add_not_modified_reasons("Invalid state.");
      }
    } else {
      reply.add_not_modified_nodes(craned_id);
      reply.add_not_modified_reasons("Can't change the state of a DOWN node!");
    }
  }

  if (g_config.Plugin.Enabled && !event_list.empty()) {
    g_plugin_client->NodeEventHookAsync(std::move(event_list));
  }
  return reply;
}

CraneExpected<void> CranedMetaContainer::ModifyPartitionAcl(
    const std::string& partition_name, bool is_allowed_list,
    std::unordered_set<std::string>&& accounts) {
  CraneExpected<void> result{};

  auto part_metas_map = partition_meta_map_.GetMapSharedPtr();

  const auto part_meta_iter = part_metas_map->find(partition_name);

  if (part_meta_iter == part_metas_map->end())
    return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);

  auto part_meta = part_meta_iter->second.GetExclusivePtr();
  auto& allowed_accounts = part_meta->partition_global_meta.allowed_accounts;
  auto& denied_accounts = part_meta->partition_global_meta.denied_accounts;

  if (is_allowed_list) {
    allowed_accounts = std::move(accounts);
  } else {
    denied_accounts = std::move(accounts);
  }

  return result;
}

CraneExpected<void> CranedMetaContainer::CheckIfAccountIsAllowedInPartition(
    const std::string& partition_name, const std::string& account_name) {
  auto part_metas_map = partition_meta_map_.GetMapSharedPtr();

  const auto part_meta_iter = part_metas_map->find(partition_name);

  if (part_meta_iter == part_metas_map->end()) {
    CRANE_DEBUG(
        "the partition {} does not exist, submission of the task is "
        "prohibited.",
        partition_name);
    return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);
  }

  auto part_meta = part_meta_iter->second.GetExclusivePtr();
  const auto& allowed_accounts =
      part_meta->partition_global_meta.allowed_accounts;

  const auto& denied_accounts =
      part_meta->partition_global_meta.denied_accounts;

  if (!allowed_accounts.empty()) {
    if (!allowed_accounts.contains(account_name)) {
      CRANE_DEBUG(
          "The account {} is not in the AllowedAccounts of the partition {}"
          "specified for the task, submission of the task is prohibited.",
          account_name, partition_name);
      return std::unexpected(CraneErrCode::ERR_NOT_IN_ALLOWED_LIST);
    }
  } else if (!denied_accounts.empty()) {
    if (denied_accounts.contains(account_name)) {
      CRANE_DEBUG(
          "The account {} is in the DeniedAccounts of the partition {}"
          "specified for the task, submission of the task is prohibited.",
          account_name, partition_name);
      return std::unexpected(CraneErrCode::ERR_IN_DENIED_LIST);
    }
  }

  return {};
}

void CranedMetaContainer::AddDedicatedResource(
    const CranedId& node_id, const DedicatedResourceInNode& resource) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to free resource from an unknown craned {}", node_id);
    return;
  }

  auto& part_ids = craned_id_part_ids_map_.at(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];
  if (!node_meta->alive) return;

  // Find how many resource should add,
  // under the constraint of configured count
  const auto& constraint = node_meta->static_meta.res.dedicated_res;

  DedicatedResourceInNode intersection = Intersection(constraint, resource);

  node_meta->res_total.dedicated_res += intersection;
  node_meta->res_avail.dedicated_res += intersection;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.res_avail += intersection;
    part_global_meta.res_total += intersection;
  }
}

void CranedMetaContainer::SetGrpcCranedInfoByCranedMeta_(
    const CranedMeta& craned_meta, crane::grpc::CranedInfo* craned_info) {
  const std::string& craned_index = craned_meta.static_meta.hostname;

  *craned_info->mutable_res_total() =
      static_cast<crane::grpc::ResourceInNode>(craned_meta.res_total);
  *craned_info->mutable_res_avail() =
      static_cast<crane::grpc::ResourceInNode>(craned_meta.res_avail);
  *craned_info->mutable_res_alloc() =
      static_cast<crane::grpc::ResourceInNode>(craned_meta.res_in_use);

  craned_info->set_hostname(craned_meta.static_meta.hostname);
  craned_info->set_craned_version(craned_meta.remote_meta.craned_version);
  craned_info->mutable_craned_start_time()->set_seconds(
      ToUnixSeconds(craned_meta.remote_meta.craned_start_time));
  craned_info->mutable_system_boot_time()->set_seconds(
      ToUnixSeconds(craned_meta.remote_meta.system_boot_time));
  craned_info->mutable_last_busy_time()->set_seconds(
      ToUnixSeconds(craned_meta.last_busy_time));

  std::string system_desc =
      fmt::format("{} {} {}", craned_meta.remote_meta.sys_rel_info.name,
                  craned_meta.remote_meta.sys_rel_info.release,
                  craned_meta.remote_meta.sys_rel_info.version);
  craned_info->set_system_desc(system_desc);

  craned_info->set_running_task_num(craned_meta.rn_task_res_map.size());

  if (craned_meta.drain) {
    craned_info->set_control_state(
        crane::grpc::CranedControlState::CRANE_DRAIN);
  } else {
    craned_info->set_control_state(crane::grpc::CranedControlState::CRANE_NONE);
  }

  if (craned_meta.alive) {
    if (craned_meta.res_in_use.IsZero())
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_IDLE);
    else if (craned_meta.res_avail.allocatable_res.IsAnyZero())
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