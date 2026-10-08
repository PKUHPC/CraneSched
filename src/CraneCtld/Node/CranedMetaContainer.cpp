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

#include "Node/CranedMetaContainer.h"

#include <absl/strings/match.h>
#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include "RpcService/CranedKeeper.h"
#include "crane/PluginClient.h"
#include "protos/PublicDefs.pb.h"
namespace Ctld {

void CranedMetaContainer::CranedUp(
    const CranedId& craned_id,
    const crane::grpc::CranedRemoteMeta& remote_meta) {
  if (g_config.Plugin.Enabled && g_plugin_client != nullptr) {
    std::vector<crane::NetworkInterface> interfaces;
    for (const auto& interface : remote_meta.network_interfaces()) {
      interfaces.emplace_back(interface);
    }
    g_plugin_client->RegisterCranedHookAsync(craned_id, interfaces);
  }

  auto part_ids = GetNodePartitions_(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[craned_id];
  if (!node_meta) return;
  if (node_meta->alive) {
    CRANE_TRACE("Craned {} is trying to up, but it's already alive, skip it.",
                craned_id);
    return;
  }

  node_meta->alive = true;
  node_meta->has_registered = true;
  if (!node_meta->drain) node_meta->outage.reset();

  node_meta->remote_meta = CranedRemoteMeta(remote_meta);
  if (remote_meta.has_node_topo_info() &&
      remote_meta.node_topo_info().sockets() != 0) {
    uint32_t reported = remote_meta.node_topo_info().sockets();
    if (reported != node_meta->static_meta.node_topo_info.sockets) {
      CRANE_WARN(
          "Craned {} reports sockets={} but config has sockets={}; "
          "using reported value.",
          craned_id, reported, node_meta->static_meta.node_topo_info.sockets);
    }
    node_meta->static_meta.node_topo_info.sockets = reported;
  }
  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.res_avail += node_meta->res_avail;
    part_global_meta.alive_craned_cnt++;
  }

  CRANE_INFO("Craned {} is up now.", craned_id);
}

void CranedMetaContainer::CranedDown(const CranedId& craned_id) {
  auto part_ids = GetNodePartitions_(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  LockResReduceEvents();

  auto raw_part_metas_map = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids) {
    auto& raw_part_meta = raw_part_metas_map->at(part_id);
    part_meta_ptrs.emplace_back(raw_part_meta.GetExclusivePtr());
  }

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[craned_id];
  if (!node_meta || !node_meta->alive) {
    UnlockResReduceEvents();
    CRANE_TRACE("Craned {} trying to down, but it's not alive, skip clean.",
                craned_id);
    return;
  }
  node_meta->alive = false;
  node_meta->craned_down_time = absl::Now();
  if (node_meta->power_state == crane::grpc::CRANE_POWER_ACTIVE ||
      node_meta->power_state == crane::grpc::CRANE_POWER_IDLE)
    node_meta->outage = NodeOutage{getuid(), node_meta->craned_down_time};

  AddResReduceEventsAndUnlock(
      {std::make_pair(absl::InfinitePast(), std::vector<CranedId>{craned_id})});

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;

    part_global_meta.res_avail -= node_meta->res_avail;
    part_global_meta.alive_craned_cnt--;
  }

  CRANE_INFO("Craned {} is down now.", craned_id);
}

bool CranedMetaContainer::CheckCranedOnline(const CranedId& craned_id) {
  auto craned_meta_ptr = craned_meta_map_.GetValueExclusivePtr(craned_id);
  return craned_meta_ptr && craned_meta_ptr->alive;
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

CranedMetaContainer::ResvMetaMapExclusivePtr
CranedMetaContainer::GetResvMetaMapExclusivePtr() {
  return resv_meta_map_.GetMapExclusivePtr();
}

void CranedMetaContainer::MallocResourceFromNode(CranedId node_id,
                                                 job_id_t job_id,
                                                 const ResourceV3& resources) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to malloc resource from an unknown craned {}", node_id);
    return;
  }

  auto part_ids = GetNodePartitions_(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  const ResourceInNodeV3& job_node_res = resources.At(node_id);

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];
  if (!node_meta) return;

  node_meta->rn_job_res_map.emplace(job_id, job_node_res);

  CRANE_TRACE(
      "[RESTRACK] MallocNode: job={} node={} "
      "before: avail_cpu={}, in_use_cpu={}, "
      "alloc_cpu={}",
      job_id, node_id,
      static_cast<double>(node_meta->res_avail.GetCpuSet().cpu_count),
      static_cast<double>(node_meta->res_in_use.GetCpuSet().cpu_count),
      static_cast<double>(job_node_res.GetCpuSet().cpu_count));

  node_meta->res_avail -= job_node_res;
  node_meta->res_in_use += job_node_res;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;

    if (node_meta->alive) part_global_meta.res_avail -= job_node_res;
    part_global_meta.res_in_use += job_node_res;
  }
}

void CranedMetaContainer::FreeResourceFromNode(CranedId node_id,
                                               uint32_t job_id) {
  if (!craned_meta_map_.Contains(node_id)) {
    CRANE_ERROR("Try to free resource from an unknown craned {}", node_id);
    return;
  }

  auto part_ids = GetNodePartitions_(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];
  if (!node_meta) return;

  auto resource_iter = node_meta->rn_job_res_map.find(job_id);
  if (resource_iter == node_meta->rn_job_res_map.end()) {
    CRANE_ERROR("Try to free resource from an unknown job {} on craned {}",
                job_id, node_id);
    return;
  }

  ResourceInNodeV3 const& resources = resource_iter->second;

  CRANE_TRACE(
      "[RESTRACK] FreeNode: job={} node={} "
      "before: avail_cpu={}, in_use_cpu={}, "
      "free_cpu={}",
      job_id, node_id,
      static_cast<double>(node_meta->res_avail.GetCpuSet().cpu_count),
      static_cast<double>(node_meta->res_in_use.GetCpuSet().cpu_count),
      static_cast<double>(resources.GetCpuSet().cpu_count));

  node_meta->res_avail += resources;
  node_meta->res_in_use -= resources;
  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;

    if (node_meta->alive) part_global_meta.res_avail += resources;
    part_global_meta.res_in_use -= resources;
  }

  node_meta->rn_job_res_map.erase(resource_iter);
}

void CranedMetaContainer::MallocResourceFromResv(ResvId resv_id,
                                                 job_id_t job_id,
                                                 const ResourceV3& res) {
  auto resv_meta = resv_meta_map_.GetValueExclusivePtr(resv_id);
  if (!resv_meta) {
    CRANE_ERROR("Try to malloc resource from an unknown reservation {}",
                resv_id);
    return;
  }

  CRANE_TRACE(
      "[RESTRACK] MallocResv: resv={} job={} "
      "resv_rn_jobs={}",
      resv_id, job_id, resv_meta->rn_job_res_map.size());

  resv_meta->res_avail -= res;

  resv_meta->rn_job_res_map.emplace(job_id, res);
}

void CranedMetaContainer::FreeResourceFromResv(ResvId resv_id,
                                               job_id_t job_id) {
  auto resv_meta = resv_meta_map_.GetValueExclusivePtr(resv_id);
  if (!resv_meta) {
    CRANE_ERROR("Try to free resource from an unknown reservation {}", resv_id);
    return;
  }

  auto iter = resv_meta->rn_job_res_map.find(job_id);
  if (iter == resv_meta->rn_job_res_map.end()) {
    CRANE_ERROR("Try to free resource from an unknown job {} on reservation {}",
                job_id, resv_id);
    return;
  }

  CRANE_TRACE(
      "[RESTRACK] FreeResv: resv={} job={} "
      "resv_rn_jobs={}",
      resv_id, job_id, resv_meta->rn_job_res_map.size());

  CRANE_DEBUG("[Job #{}] Freeing resource from reservation {}", job_id,
              resv_id);

  ResourceV3 const& resources = iter->second;

  resv_meta->res_avail += resources;

  resv_meta->rn_job_res_map.erase(iter);
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
    uint32_t cpu_cnt = config.Nodes.at(craned_name)->cpu;
    static_meta.res.GetCpuSet().cpu_count = cpu_t(cpu_cnt);
    for (uint32_t i = 0; i < cpu_cnt; ++i)
      static_meta.res.GetCpuSet().core_ids.insert(i);
    static_meta.res.SetMemoryBytes(config.Nodes.at(craned_name)->memory_bytes);
    static_meta.res.SetMemorySwBytes(
        config.Nodes.at(craned_name)->memory_bytes);
    static_meta.res.GetGres() =
        config.Nodes.at(craned_name)->dedicated_resource;
    static_meta.node_topo_info.sockets =
        config.Nodes.at(craned_name)->node_topo_info.sockets;
    static_meta.hostname = craned_name;
    static_meta.node_hostname = config.Nodes.at(craned_name)->node_hostname;
    static_meta.node_addr = config.Nodes.at(craned_name)->node_addr;
    static_meta.port = std::strtoul(
        g_config.CranedListenConf.CranedListenPort.c_str(), nullptr, 10);
    static_meta.is_future = config.Nodes.at(craned_name)->is_future;
    static_meta.features = config.Nodes.at(craned_name)->features;

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

      part_meta.craned_ids.emplace(craned_name);

      if (craned_meta.static_meta.is_future) {
        CRANE_DEBUG(
            "FUTURE node {} is not added to partition [{}]'s global resource "
            "until a craned is mapped to it.",
            craned_name, part_name);
        continue;
      }

      part_meta.partition_global_meta.res_total += craned_meta.static_meta.res;

      CRANE_DEBUG(
          "Add the resource of Craned {} (cpu: {}, mem: {}, gres: {}) to "
          "partition [{}]'s global resource.",
          craned_name, craned_meta.static_meta.res.GetCpuSet().cpu_count,
          util::ReadableMemory(craned_meta.static_meta.res.GetMemoryBytes()),
          util::ReadableDresInNode(craned_meta.static_meta.res.GetGres()),
          part_name);

      part_res += craned_meta.static_meta.res;
    }

    part_meta.partition_global_meta.name = part_name;
    part_meta.partition_global_meta.res_total_inc_dead = part_res;
    part_meta.partition_global_meta.res_in_use.SetToZero();
    part_meta.partition_global_meta.node_cnt = part_meta.craned_ids.size();
    part_meta.partition_global_meta.nodelist_str = partition.nodelist_str;
    LoadPartitionAclFromConfig_(part_name, part_meta.partition_global_meta);

    CRANE_DEBUG(
        "partition [{}]'s Global resource now: (cpu: {}, mem: {}, "
        "gres: {}). "
        "It has {} craneds.",
        part_name,
        part_meta.partition_global_meta.res_total_inc_dead.CpuCountDouble(),
        util::ReadableMemory(part_meta.partition_global_meta.res_total_inc_dead
                                 .GetMemoryBytes()),
        util::ReadableGresMap(
            part_meta.partition_global_meta.res_total_inc_dead.GetGresMap()),
        part_meta.partition_global_meta.node_cnt);
  }

  for (auto& [id, node] : craned_map) node.static_meta.partition_ids.sort();
  craned_meta_map_.InitFromMap(std::move(craned_map));
  partition_meta_map_.InitFromMap(std::move(partition_map));
  RestoreNodeState_();
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

  const CranedId craned_id = ResolveCranedIdAlias(node_name);
  auto craned_meta = craned_meta_map_.GetValueExclusivePtr(craned_id);
  if (!craned_meta) return reply;

  auto* craned_info = list->Add();
  SetGrpcCranedInfoByCranedMeta_(*craned_meta, craned_info);

  return reply;
}

crane::grpc::QueryPartitionInfoReply
CranedMetaContainer::QueryAllPartitionInfo() {
  crane::grpc::QueryPartitionInfoReply reply;
  auto partitions = partition_meta_map_.GetMapConstSharedPtr();
  for (const auto& name : g_config.PartitionOrder) {
    auto meta = partitions->at(name).GetExclusivePtr();
    SetGrpcPartitionInfoByPartitionMeta_(*meta,
                                         reply.add_partition_info_list());
  }
  return reply;
}

crane::grpc::QueryPartitionInfoReply CranedMetaContainer::QueryPartitionInfo(
    const PartitionId& partition_name) {
  crane::grpc::QueryPartitionInfoReply reply;
  auto meta = partition_meta_map_.GetValueExclusivePtr(partition_name);
  if (meta)
    SetGrpcPartitionInfoByPartitionMeta_(*meta,
                                         reply.add_partition_info_list());
  return reply;
}

void CranedMetaContainer::SetGrpcPartitionInfoByPartitionMeta_(
    const PartitionMeta& partition, crane::grpc::PartitionInfo* part_info) {
  const auto& meta = partition.partition_global_meta;
  const auto& config = g_config.Partitions.at(meta.name);
  part_info->set_name(meta.name);
  part_info->set_total_nodes(meta.node_cnt);
  part_info->set_alive_nodes(meta.alive_craned_cnt);
  auto* allowed_accounts = part_info->mutable_allowed_accounts();
  for (const auto& account_name : meta.allowed_accounts) {
    allowed_accounts->Add()->assign(account_name);
  }
  auto* denied_accounts = part_info->mutable_denied_accounts();
  for (const auto& account_name : meta.denied_accounts) {
    denied_accounts->Add()->assign(account_name);
  }
  if (meta.alive_craned_cnt > 0)
    part_info->set_state(crane::grpc::PartitionState::PARTITION_UP);
  else
    part_info->set_state(crane::grpc::PartitionState::PARTITION_DOWN);

  part_info->set_hostlist(meta.nodelist_str);

  part_info->set_default_mem_per_cpu(config.default_mem_per_cpu);
  part_info->set_max_mem_per_cpu(config.max_mem_per_cpu);
  part_info->set_default_mem_per_node(config.default_mem_per_node);
  part_info->set_max_mem_per_node(config.max_mem_per_node);

  *part_info->mutable_res_total() =
      static_cast<crane::grpc::ResourceView>(meta.res_total);
  *part_info->mutable_res_avail() =
      static_cast<crane::grpc::ResourceView>(meta.res_avail);
  *part_info->mutable_res_alloc() =
      static_cast<crane::grpc::ResourceView>(meta.res_in_use);

  part_info->set_is_default(meta.name == g_config.DefaultPartition);
  part_info->set_priority(config.priority);
  part_info->set_preempt_mode(g_config.Preempt.PreemptMode);
}

void CranedMetaContainer::UpdateNodeTelemetry(
    const crane::grpc::CranedPingRequest& request) {
  auto meta = GetCranedMetaPtr(request.craned_id());
  if (!meta || !meta->alive) return;
  meta->remote_meta.cpu_load =
      request.has_cpu_load() ? std::optional{request.cpu_load()} : std::nullopt;
  meta->remote_meta.free_memory_mb =
      request.has_free_memory_mb() ? std::optional{request.free_memory_mb()}
                                   : std::nullopt;
}

crane::grpc::QueryReservationInfoReply CranedMetaContainer::QueryAllResvInfo() {
  crane::grpc::QueryReservationInfoReply reply;
  auto resv_map = resv_meta_map_.GetMapConstSharedPtr();
  for (const auto& [id, value] : *resv_map) {
    const auto meta = value.GetExclusivePtr();
    SetGrpcReservationInfoByResvMeta_(*meta, reply.add_reservation_info_list());
  }
  return reply;
}

crane::grpc::QueryReservationInfoReply CranedMetaContainer::QueryResvInfo(
    const ResvId& resv_id) {
  crane::grpc::QueryReservationInfoReply reply;
  auto meta = resv_meta_map_.GetValueExclusivePtr(resv_id);
  if (meta)
    SetGrpcReservationInfoByResvMeta_(*meta, reply.add_reservation_info_list());
  return reply;
}

void CranedMetaContainer::SetGrpcReservationInfoByResvMeta_(
    const ResvMeta& meta, crane::grpc::ReservationInfo* info) {
  info->set_reservation_name(meta.name);
  info->mutable_start_time()->set_seconds(absl::ToUnixSeconds(meta.start_time));
  info->mutable_duration()->set_seconds(absl::ToUnixSeconds(meta.end_time) -
                                        absl::ToUnixSeconds(meta.start_time));
  info->set_partition(meta.part_id);
  info->set_creation_order(meta.creation_order);
  std::list<std::string> allocated_nodes;
  for (const auto& [id, resources] : meta.res_total.EachNodeResMap()) {
    allocated_nodes.push_back(id);
    info->add_allocated_nodes(id);
  }
  // Keep the legacy expression usable by existing clients, with actual
  // allocation membership rather than the reservation's candidate nodes.
  info->set_craned_regex(util::HostNameListToStr(allocated_nodes));

  ResourceView total, available;
  total += meta.res_total;
  available += meta.res_avail;
  *info->mutable_res_total() = static_cast<crane::grpc::ResourceView>(total);
  info->mutable_res_total()->set_cpu_count(
      ConvertCpuCountForClient(total.GetCpuCount()));
  *info->mutable_res_avail() =
      static_cast<crane::grpc::ResourceView>(available);
  info->mutable_res_avail()->set_cpu_count(
      ConvertCpuCountForClient(available.GetCpuCount()));
  total -= meta.res_avail;
  *info->mutable_res_alloc() = static_cast<crane::grpc::ResourceView>(total);
  info->mutable_res_alloc()->set_cpu_count(
      ConvertCpuCountForClient(total.GetCpuCount()));

  for (const auto& account : meta.accounts)
    (meta.accounts_black_list ? info->add_denied_accounts()
                              : info->add_allowed_accounts())
        ->assign(account);
  for (const auto& user : meta.users)
    (meta.users_black_list ? info->add_denied_users()
                           : info->add_allowed_users())
        ->assign(user);
}

crane::grpc::QueryClusterInfoReply CranedMetaContainer::QueryClusterInfo(
    const crane::grpc::QueryClusterInfoRequest& request,
    const AllocationCleanupMap& cleanup_jobs) {
  crane::grpc::QueryClusterInfoReply reply;
  reply.set_ok(true);
  reply.set_cluster_name(g_config.CraneClusterName);
  const auto now = absl::Now();
  reply.mutable_observed_at()->set_seconds(absl::ToUnixSeconds(now));
  reply.mutable_observed_at()->set_nanos(absl::ToInt64Nanoseconds(
      now - absl::FromUnixSeconds(absl::ToUnixSeconds(now))));
  // Each domain uses its own locks; this is not a cross-domain atomic snapshot.
  if (request.include_reservations()) {
    auto reservations = QueryAllResvInfo();
    reply.mutable_reservations()->Swap(
        reservations.mutable_reservation_info_list());
  }
  std::unordered_set<std::string> parts(request.filter_partitions().begin(),
                                        request.filter_partitions().end());
  if (request.include_partitions()) {
    auto partitions = partition_meta_map_.GetMapConstSharedPtr();
    for (const auto& name : g_config.PartitionOrder) {
      if (!parts.empty() && !parts.contains(name)) continue;
      auto meta = partitions->at(name).GetExclusivePtr();
      SetGrpcPartitionInfoByPartitionMeta_(*meta, reply.add_partitions());
    }
  }
  if (!request.include_nodes()) return reply;

  std::list<std::string> hosts;
  util::ParseHostList(absl::StrJoin(request.filter_nodes(), ","), &hosts);
  std::unordered_set<std::string> selected;
  for (const auto& host : hosts) selected.insert(ResolveCranedIdAlias(host));
  auto nodes = craned_meta_map_.GetMapConstSharedPtr();
  // Nodes created at runtime follow the configured ones.
  std::list<CranedId> names;
  for (const auto& [name, _] : *nodes) names.emplace_back(name);
  OrderCranedIdListByNodeOrder(&names);
  for (const auto& name : names) {
    if (!request.filter_nodes().empty() && !selected.contains(name)) continue;
    auto meta = nodes->at(name).GetExclusivePtr();
    if (!parts.empty() &&
        !ranges::any_of(meta->static_meta.partition_ids,
                        [&](const auto& part) { return parts.contains(part); }))
      continue;
    auto* node = reply.add_nodes();
    SetGrpcCranedInfoByCranedMeta_(*meta, node);
    if (request.include_allocation_cleanup()) {
      ResourceInNodeV3 cleanup_resources;
      for (const auto& [id, resources] : meta->rn_job_res_map) {
        const auto cleanup = cleanup_jobs.find(id);
        if (cleanup == cleanup_jobs.end()) continue;
        cleanup_resources += resources;
        if (cleanup->second.contains(name)) node->set_cleanup_pending(true);
      }
      *node->mutable_cleanup_resources() =
          static_cast<crane::grpc::ResourceInNodeV3>(cleanup_resources);
    }
  }
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

  if (request.new_state() == crane::grpc::CranedControlState::CRANE_DRAIN) {
    LockResReduceEvents();
  }

  std::vector<CranedId> affected_nodes;

  for (auto craned_id : request.craned_ids()) {
    auto craned_meta = craned_meta_map_[craned_id];
    if (!craned_meta) {
      reply.add_not_modified_nodes(craned_id);
      reply.add_not_modified_reasons("Invalid node name specified.");
      continue;
    }

    if (craned_meta->static_meta.is_future && !craned_meta->future_mapped) {
      reply.add_not_modified_nodes(craned_id);
      reply.add_not_modified_reasons(
          "Node is a FUTURE placeholder and not mapped yet.");
      continue;
    }

    if (craned_meta->alive) {
      if (request.new_state() == crane::grpc::CranedControlState::CRANE_DRAIN) {
        if (craned_meta->drain == false) {
          if (g_config.Plugin.Enabled) {
            // Set node event info
            event.set_node_name(craned_id);
            event.set_control_state(
                crane::grpc::CranedControlState::CRANE_DRAIN);
            event_list.emplace_back(event);
          }
          craned_meta->drain = true;
          affected_nodes.push_back(craned_id);
        }

        craned_meta->state_reason = request.reason();
        craned_meta->state_reason_uid = request.uid();
        craned_meta->state_reason_time = absl::Now();
        craned_meta->outage.reset();
        reply.add_modified_nodes(craned_id);
      } else if (request.new_state() ==
                 crane::grpc::CranedControlState::CRANE_NONE) {
        if (craned_meta->drain == true) {
          if (g_config.Plugin.Enabled) {
            // Set node event info
            event.set_node_name(craned_id);
            event.set_control_state(
                crane::grpc::CranedControlState::CRANE_NONE);
            event_list.emplace_back(event);
          }
          craned_meta->drain = false;
          craned_meta->state_reason.clear();
          craned_meta->state_reason_uid.reset();
          craned_meta->state_reason_time = absl::UnixEpoch();
          craned_meta->outage.reset();
        }

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
  if (request.new_state() == crane::grpc::CranedControlState::CRANE_DRAIN) {
    AddResReduceEventsAndUnlock(
        {std::make_pair(absl::InfinitePast(), std::move(affected_nodes))});
  }

  if (g_config.Plugin.Enabled && !event_list.empty()) {
    g_plugin_client->NodeEventHookAsync(std::move(event_list));
  }
  return reply;
}

bool CranedMetaContainer::UpdateNodeDrainState(const std::string& craned_id,
                                               bool is_drain,
                                               const std::string& reason) {
  if (is_drain) LockResReduceEvents();

  CRANE_DEBUG("Updating node '{}' state to {}, reason {}.", craned_id, is_drain,
              reason);

  if (!craned_meta_map_.Contains(craned_id)) {
    CRANE_ERROR("Unknown craned_id '{}', cannot update drain state.",
                craned_id);
    if (is_drain) UnlockResReduceEvents();
    return false;
  }

  auto craned_meta = craned_meta_map_[craned_id];

  if (!craned_meta || !craned_meta->alive) {
    CRANE_ERROR("craned '{}' is DOWN; refuse to change drain state.",
                craned_id);
    if (is_drain) UnlockResReduceEvents();
    return false;
  }

  if (craned_meta->drain == is_drain) {
    if (is_drain) UnlockResReduceEvents();
    return true;
  }

  craned_meta->drain = is_drain;
  craned_meta->state_reason = reason;
  craned_meta->state_reason_uid.reset();
  craned_meta->state_reason_time =
      reason.empty() ? absl::UnixEpoch() : absl::Now();
  craned_meta->outage.reset();

  if (is_drain) {
    AddResReduceEventsAndUnlock({std::make_pair(
        absl::InfinitePast(), std::vector<CranedId>{craned_id})});
  }

  return true;
}

std::list<PartitionId> CranedMetaContainer::GetNodePartitions_(
    const CranedId& node_id) {
  auto node = craned_meta_map_[node_id];
  return node ? node->static_meta.partition_ids : std::list<PartitionId>{};
}

crane::grpc::DynamicNodeDefinition CranedMetaContainer::NodeDefinition_(
    const CranedMeta& node) {
  crane::grpc::DynamicNodeDefinition definition;
  const auto& meta = node.static_meta;
  definition.set_name(meta.hostname);
  definition.set_cpu(static_cast<uint32_t>(meta.res.GetCpuSet().cpu_count));
  definition.set_memory_bytes(meta.res.GetMemoryBytes());
  definition.set_sockets(meta.node_topo_info.sockets);
  for (const auto& feature : meta.features) definition.add_features(feature);
  for (const auto& partition : meta.partition_ids)
    definition.add_partitions(partition);
  return definition;
}

std::string CranedMetaContainer::ValidateNodeDefinition_(
    const crane::grpc::DynamicNodeDefinition& definition) {
  const auto& name = definition.name();
  if (name.empty() || name.find_first_not_of(
                          "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                          "0123456789-_.") != std::string::npos)
    return "Invalid node name.";
  if (craned_meta_map_.Contains(name) ||
      g_config.CranedIdByAlias.contains(name))
    return "Node name or alias already exists.";
  if (definition.cpu() == 0 || definition.memory_bytes() == 0 ||
      definition.sockets() == 0 || definition.sockets() > definition.cpu() ||
      definition.cpu() % definition.sockets() != 0)
    return "CPU and memory must be positive; sockets must evenly divide CPUs.";
  if (definition.partitions().empty())
    return "At least one partition must be specified.";
  std::unordered_set<std::string> partitions;
  for (const auto& partition : definition.partitions()) {
    if (!partition_meta_map_.Contains(partition))
      return fmt::format("Unknown partition {}.", partition);
    if (!partitions.emplace(partition).second) return "Duplicate partition.";
  }
  std::unordered_set<std::string> features;
  for (const auto& feature : definition.features()) {
    if (feature.empty() ||
        feature.find_first_of(" ,\t\r\n") != std::string::npos)
      return "Invalid feature.";
    if (!features.emplace(absl::AsciiStrToLower(feature)).second)
      return "Duplicate feature.";
  }
  return {};
}

void CranedMetaContainer::InsertDynamicNode_(
    const crane::grpc::DynamicNodeDefinition& definition) {
  CranedMeta node;
  auto& meta = node.static_meta;
  meta.hostname = definition.name();
  meta.node_hostname = definition.name();
  meta.node_addr = definition.name();
  meta.port = std::strtoul(g_config.CranedListenConf.CranedListenPort.c_str(),
                           nullptr, 10);
  meta.is_future = true;
  meta.dynamic = true;
  meta.features.assign(definition.features().begin(),
                       definition.features().end());
  meta.partition_ids.assign(definition.partitions().begin(),
                            definition.partitions().end());
  meta.partition_ids.sort();
  meta.node_topo_info.sockets = definition.sockets();
  meta.res.GetCpuSet().cpu_count = cpu_t(definition.cpu());
  for (uint32_t cpu = 0; cpu < definition.cpu(); ++cpu)
    meta.res.GetCpuSet().core_ids.insert(cpu);
  meta.res.SetMemoryBytes(definition.memory_bytes());
  meta.res.SetMemorySwBytes(definition.memory_bytes());
  node.res_total = meta.res;
  node.res_avail = meta.res;
  node.remote_meta.craned_version = "unknown";
  node.remote_meta.sys_rel_info.name = "unknown";

  auto partitions = partition_meta_map_.GetMapSharedPtr();
  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> partition_locks;
  for (const auto& id : meta.partition_ids)
    partition_locks.emplace_back(partitions->at(id).GetExclusivePtr());
  auto nodes = craned_meta_map_.GetMapExclusivePtr();
  nodes->emplace(definition.name(), std::move(node));
  for (auto& partition : partition_locks) {
    partition->craned_ids.emplace(definition.name());
    auto& global = partition->partition_global_meta;
    global.node_cnt = partition->craned_ids.size();
    global.nodelist_str = util::HostNameListToStr(partition->craned_ids);
  }
}

bool CranedMetaContainer::SaveNodeState_(
    const crane::grpc::NodeStateSnapshot& snapshot) {
  const std::string path = g_config.CraneCtldDbPath.string() + ".nodes";
  const std::string temporary = path + ".tmp";
  const std::string bytes = snapshot.SerializeAsString();
  int fd =
      open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd == -1) {
    CRANE_ERROR("Cannot open node state {}: {}", temporary, strerror(errno));
    return false;
  }
  size_t written = 0;
  while (written < bytes.size()) {
    ssize_t n = write(fd, bytes.data() + written, bytes.size() - written);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    written += n;
  }
  bool ok = written == bytes.size() && fsync(fd) == 0;
  if (close(fd) != 0) ok = false;
  if (ok) ok = rename(temporary.c_str(), path.c_str()) == 0;
  if (!ok) {
    CRANE_ERROR("Cannot save node state {}: {}", path, strerror(errno));
    unlink(temporary.c_str());
    return false;
  }
  auto directory = std::filesystem::path(path).parent_path();
  if (directory.empty()) directory = ".";
  fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd == -1 || fsync(fd) != 0)
    CRANE_ERROR("Cannot sync node state directory {}: {}", directory.string(),
                strerror(errno));
  if (fd != -1) close(fd);
  return true;
}

void CranedMetaContainer::RestoreNodeState_() {
  const std::string path = g_config.CraneCtldDbPath.string() + ".nodes";
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    if (!std::filesystem::exists(path)) return;
    CRANE_ERROR("Cannot open node state {}.", path);
    std::exit(1);
  }
  if (!m_node_state_.ParseFromIstream(&file)) {
    CRANE_ERROR("Cannot parse node state {}.", path);
    std::exit(1);
  }
  std::unordered_set<std::string> restored;
  for (const auto& state : m_node_state_.nodes()) {
    const auto& definition = state.definition();
    std::string error;
    if (!restored.emplace(definition.name()).second)
      error = "Duplicate persisted node.";
    else if (state.dynamic())
      error = ValidateNodeDefinition_(definition);
    else {
      auto node = craned_meta_map_[definition.name()];
      if (!node || !node->static_meta.is_future)
        error = "Persisted FUTURE node is absent from configuration.";
      else if (definition.cpu() !=
                   static_cast<uint32_t>(
                       node->static_meta.res.GetCpuSet().cpu_count) ||
               definition.memory_bytes() !=
                   node->static_meta.res.GetMemoryBytes())
        error = "Persisted FUTURE node resources differ from configuration.";
    }
    if (!error.empty()) {
      CRANE_ERROR("Cannot restore node {}: {}", definition.name(), error);
      std::exit(1);
    }
    if (state.dynamic()) InsertDynamicNode_(definition);
    if (!state.node_hostname().empty() && !state.node_addr().empty())
      ClaimFutureNode_(definition.name(), state.node_hostname(),
                       state.node_addr());
    else if (!state.node_hostname().empty() || !state.node_addr().empty()) {
      CRANE_ERROR("Incomplete persisted mapping for node {}.",
                  definition.name());
      std::exit(1);
    }
  }
}

crane::grpc::CreateNodesReply CranedMetaContainer::CreateNodes(
    const crane::grpc::CreateNodesRequest& request) {
  absl::MutexLock lock(&m_node_lifecycle_mtx_);
  crane::grpc::CreateNodesReply reply;
  for (const auto& definition : request.nodes()) {
    auto error = ValidateNodeDefinition_(definition);
    if (error.empty()) {
      auto snapshot = m_node_state_;
      auto* state = snapshot.add_nodes();
      *state->mutable_definition() = definition;
      state->set_dynamic(true);
      if (!SaveNodeState_(snapshot))
        error = "Failed to persist node definition.";
      else {
        InsertDynamicNode_(definition);
        m_node_state_ = std::move(snapshot);
        reply.add_created_nodes(definition.name());
        CRANE_INFO("User {} created FUTURE node {}.", request.uid(),
                   definition.name());
      }
    }
    if (!error.empty()) {
      reply.add_not_created_nodes(definition.name());
      reply.add_not_created_reasons(error);
    }
  }
  return reply;
}

crane::grpc::DeleteNodesReply CranedMetaContainer::DeleteNodes(
    const crane::grpc::DeleteNodesRequest& request) {
  absl::MutexLock lock(&m_node_lifecycle_mtx_);
  auto keeper_lock = g_craned_keeper->GetLifecycleLock();
  crane::grpc::DeleteNodesReply reply;
  for (const auto& id : request.node_names()) {
    LockResReduceEvents();
    auto part_ids = GetNodePartitions_(id);
    auto partitions = partition_meta_map_.GetMapSharedPtr();
    std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr>
        partition_locks;
    for (const auto& part_id : part_ids)
      partition_locks.emplace_back(partitions->at(part_id).GetExclusivePtr());
    auto nodes = craned_meta_map_.GetMapExclusivePtr();
    auto it = nodes->find(id);
    std::string error;
    if (it == nodes->end())
      error = "Unknown node.";
    else {
      const auto& node = *it->second.RawPtr();
      if (!node.static_meta.dynamic)
        error = "Only manually created nodes can be deleted.";
      else if (!node.rn_job_res_map.empty())
        error = "Node has allocated or completing jobs.";
      else if (!node.resv_in_node_map.empty())
        error = "Node belongs to a reservation.";
      else {
        auto snapshot = m_node_state_;
        auto* states = snapshot.mutable_nodes();
        for (int i = 0; i < states->size(); ++i) {
          if (states->Get(i).definition().name() == id) {
            states->DeleteSubrange(i, 1);
            break;
          }
        }
        if (!SaveNodeState_(snapshot))
          error = "Failed to persist node deletion.";
        else {
          g_craned_keeper->RetireCraned(id);
          for (auto& partition : partition_locks) {
            auto& global = partition->partition_global_meta;
            if (node.future_mapped) {
              global.res_total -= node.res_total;
              if (node.alive) global.res_avail -= node.res_avail;
              global.res_total_inc_dead -= node.static_meta.res;
            }
            if (node.alive) --global.alive_craned_cnt;
            partition->craned_ids.erase(id);
            global.node_cnt = partition->craned_ids.size();
            global.nodelist_str =
                util::HostNameListToStr(partition->craned_ids);
          }
          nodes->erase(it);
          m_node_state_ = std::move(snapshot);
          reply.add_deleted_nodes(id);
          CRANE_INFO("User {} deleted node {}.", request.uid(), id);
        }
      }
    }
    if (error.empty())
      AddResReduceEventsAndUnlock(
          {std::make_pair(absl::InfinitePast(), std::vector<CranedId>{id})});
    else {
      UnlockResReduceEvents();
      reply.add_not_deleted_nodes(id);
      reply.add_not_deleted_reasons(error);
    }
  }
  return reply;
}

crane::grpc::CranedMapFutureNodeReply CranedMetaContainer::MapFutureNode(
    const crane::grpc::CranedMapFutureNodeRequest& request,
    const std::string& craned_addr) {
  crane::grpc::CranedMapFutureNodeReply reply;

  absl::MutexLock lock(&m_node_lifecycle_mtx_);
  auto keeper_lock = g_craned_keeper->GetLifecycleLock();
  if (request.hostname().empty() || craned_addr.empty()) {
    reply.set_reason("Hostname and address are required.");
    return reply;
  }

  constexpr double kBytesPerGB = 1024 * 1024 * 1024;
  double real_mem_gb =
      static_cast<double>(request.memory_bytes()) / kBytesPerGB;

  auto persist_mapping = [&](const CranedId& id) {
    auto snapshot = m_node_state_;
    bool found = false;
    for (auto& state : *snapshot.mutable_nodes()) {
      if (state.definition().name() == id) {
        state.set_node_hostname(request.hostname());
        state.set_node_addr(craned_addr);
        found = true;
        break;
      }
    }
    if (!found) {
      auto node = craned_meta_map_[id];
      if (!node) return false;
      auto* state = snapshot.add_nodes();
      *state->mutable_definition() = NodeDefinition_(*node);
      state->set_dynamic(node->static_meta.dynamic);
      state->set_node_hostname(request.hostname());
      state->set_node_addr(craned_addr);
    }
    if (!SaveNodeState_(snapshot)) return false;
    m_node_state_ = std::move(snapshot);
    return true;
  };

  // A restarted craned on an already mapped machine reuses its previous
  // mapping instead of claiming (and thus leaking) another node.
  CranedId selected_id;
  {
    auto craned_map = craned_meta_map_.GetMapConstSharedPtr();
    for (const auto& [craned_id, craned_meta_ptr] : *craned_map) {
      auto craned_meta = craned_meta_ptr.GetExclusivePtr();
      if (!craned_meta->static_meta.is_future || !craned_meta->future_mapped)
        continue;
      if (craned_meta->static_meta.node_hostname != request.hostname())
        continue;

      selected_id = craned_id;
      break;
    }
  }
  if (!selected_id.empty()) {
    {
      auto node = craned_meta_map_[selected_id];
      const auto& meta = node->static_meta;
      if (request.cpu() !=
              static_cast<uint32_t>(meta.res.GetCpuSet().cpu_count) ||
          real_mem_gb + kMemoryToleranceGB <
              static_cast<double>(meta.res.GetMemoryBytes()) / kBytesPerGB ||
          (!request.feature().empty() && std::ranges::none_of(
                                             meta.features,
                                             [&](const auto& feature) {
                                               return absl::EqualsIgnoreCase(
                                                   feature, request.feature());
                                             }))) {
        reply.set_reason(
            "Hardware or feature no longer matches the mapped FUTURE node.");
        return reply;
      }
      if (node->alive && meta.node_addr != craned_addr) {
        reply.set_reason("FUTURE node is already online at another address.");
        return reply;
      }
    }
    if (!persist_mapping(selected_id)) {
      reply.set_ok(false);
      reply.set_reason("Failed to persist FUTURE mapping.");
      return reply;
    }
    craned_meta_map_[selected_id]->static_meta.node_addr = craned_addr;
    reply.mutable_definition()->CopyFrom(
        NodeDefinition_(*craned_meta_map_[selected_id]));
    CRANE_INFO("Craned {} at {} reuses its mapping to FUTURE node {}.",
               request.hostname(), craned_addr, selected_id);
    reply.set_ok(true);
    reply.set_craned_id(selected_id);
    return reply;
  }

  // Pick the matching node with the smallest hostname for deterministic
  // mapping.
  {
    auto craned_map = craned_meta_map_.GetMapConstSharedPtr();
    for (const auto& [craned_id, craned_meta_ptr] : *craned_map) {
      if (!selected_id.empty() && craned_id >= selected_id) continue;

      auto craned_meta = craned_meta_ptr.GetExclusivePtr();
      const auto& static_meta = craned_meta->static_meta;
      if (!static_meta.is_future || craned_meta->future_mapped) continue;

      if (!request.feature().empty() &&
          std::ranges::find_if(static_meta.features, [&](const auto& f) {
            return absl::EqualsIgnoreCase(f, request.feature());
          }) == static_meta.features.end())
        continue;

      if (request.cpu() !=
          static_cast<uint32_t>(static_meta.res.GetCpuSet().cpu_count))
        continue;

      double config_mem_gb =
          static_cast<double>(static_meta.res.GetMemoryBytes()) / kBytesPerGB;
      if (real_mem_gb + kMemoryToleranceGB < config_mem_gb) continue;

      selected_id = craned_id;
    }
  }

  if (selected_id.empty()) {
    reply.set_ok(false);
    reply.set_reason(
        fmt::format("No matching unmapped FUTURE node (cpu: {}, mem: {:.3f}GB, "
                    "feature: '{}').",
                    request.cpu(), real_mem_gb, request.feature()));
    return reply;
  }

  if (!persist_mapping(selected_id)) {
    reply.set_ok(false);
    reply.set_reason("Failed to persist FUTURE mapping.");
    return reply;
  }
  ClaimFutureNode_(selected_id, request.hostname(), craned_addr);
  reply.mutable_definition()->CopyFrom(
      NodeDefinition_(*craned_meta_map_[selected_id]));

  CRANE_INFO("Craned {} at {} is mapped to FUTURE node {}.", request.hostname(),
             craned_addr, selected_id);

  reply.set_ok(true);
  reply.set_craned_id(selected_id);
  return reply;
}

void CranedMetaContainer::ClaimFutureNode_(const CranedId& craned_id,
                                           const std::string& node_hostname,
                                           const std::string& node_addr) {
  auto part_ids = GetNodePartitions_(craned_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[craned_id];
  node_meta->future_mapped = true;
  if (!node_hostname.empty())
    node_meta->static_meta.node_hostname = node_hostname;
  node_meta->static_meta.node_addr = node_addr;

  for (auto& partition_meta : part_meta_ptrs) {
    PartitionGlobalMeta& part_global_meta =
        partition_meta->partition_global_meta;
    part_global_meta.res_total += node_meta->static_meta.res;
    part_global_meta.res_total_inc_dead += node_meta->static_meta.res;
  }
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

void CranedMetaContainer::LoadPartitionAclFromConfig_(
    const std::string& part_name, PartitionGlobalMeta& meta) {
  auto it = g_config.Partitions.find(part_name);
  if (it != g_config.Partitions.end()) {
    meta.allowed_accounts = it->second.allowed_accounts;
    meta.denied_accounts = it->second.denied_accounts;
  } else {
    meta.allowed_accounts.clear();
    meta.denied_accounts.clear();
  }
}

void CranedMetaContainer::ResetAllPartitionAcls(bool reload_from_config) {
  auto part_metas_map = partition_meta_map_.GetMapSharedPtr();
  for (auto& [part_name, part_meta_ptr] : *part_metas_map) {
    auto part_meta = part_meta_ptr.GetExclusivePtr();
    if (reload_from_config) {
      LoadPartitionAclFromConfig_(part_name, part_meta->partition_global_meta);
    } else {
      part_meta->partition_global_meta.allowed_accounts.clear();
      part_meta->partition_global_meta.denied_accounts.clear();
    }
  }
}

CraneExpected<void> CranedMetaContainer::CheckIfAccountIsAllowedInPartition(
    const std::string& partition_name, const std::string& account_name) {
  auto part_metas_map = partition_meta_map_.GetMapSharedPtr();

  const auto part_meta_iter = part_metas_map->find(partition_name);

  if (part_meta_iter == part_metas_map->end()) {
    CRANE_DEBUG(
        "the partition {} does not exist, submission of the job is "
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
          "specified for the job, submission of the job is prohibited.",
          account_name, partition_name);
      return std::unexpected(CraneErrCode::ERR_NOT_IN_ALLOWED_LIST);
    }
  } else if (!denied_accounts.empty()) {
    if (denied_accounts.contains(account_name)) {
      CRANE_DEBUG(
          "The account {} is in the DeniedAccounts of the partition {}"
          "specified for the job, submission of the job is prohibited.",
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

  auto part_ids = GetNodePartitions_(node_id);

  std::vector<util::Synchronized<PartitionMeta>::ExclusivePtr> part_meta_ptrs;
  part_meta_ptrs.reserve(part_ids.size());

  auto raw_part_metas_map_ = partition_meta_map_.GetMapSharedPtr();

  // Acquire all partition locks first.
  for (PartitionId const& part_id : part_ids)
    part_meta_ptrs.emplace_back(
        raw_part_metas_map_->at(part_id).GetExclusivePtr());

  // Then acquire craned meta lock.
  auto node_meta = craned_meta_map_[node_id];
  if (!node_meta || !node_meta->alive) return;

  // Find how many resource should add,
  // under the constraint of configured count
  const auto& constraint = node_meta->static_meta.res.GetGres();

  DedicatedResourceInNode intersection = Intersection(constraint, resource);

  node_meta->res_total.GetGres() += intersection;
  node_meta->res_avail.GetGres() += intersection;

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
      static_cast<crane::grpc::ResourceInNodeV3>(craned_meta.res_total);
  *craned_info->mutable_res_avail() =
      static_cast<crane::grpc::ResourceInNodeV3>(craned_meta.res_avail);
  *craned_info->mutable_res_alloc() =
      static_cast<crane::grpc::ResourceInNodeV3>(craned_meta.res_in_use);

  craned_info->set_state_reason(craned_meta.state_reason);
  if (craned_meta.state_reason_uid)
    craned_info->set_state_reason_uid(*craned_meta.state_reason_uid);
  if (craned_meta.state_reason_time != absl::UnixEpoch())
    craned_info->mutable_state_reason_time()->set_seconds(
        absl::ToUnixSeconds(craned_meta.state_reason_time));
  if (craned_meta.outage) {
    craned_info->mutable_outage()->set_uid(craned_meta.outage->uid);
    craned_info->mutable_outage()->mutable_time()->set_seconds(
        absl::ToUnixSeconds(craned_meta.outage->time));
  }
  craned_info->set_responding(craned_meta.alive);
  craned_info->set_has_registered(craned_meta.has_registered);
  craned_info->set_port(craned_meta.static_meta.port);
  if (craned_meta.remote_meta.cpu_load)
    craned_info->set_cpu_load(*craned_meta.remote_meta.cpu_load);
  if (craned_meta.remote_meta.free_memory_mb)
    craned_info->set_free_memory_mb(*craned_meta.remote_meta.free_memory_mb);
  // Nodes created at runtime are absent from g_config.Nodes and have no GRES.
  if (auto config_it = g_config.Nodes.find(craned_index);
      config_it == g_config.Nodes.end()) {
    craned_info->set_configured_sockets(
        craned_meta.static_meta.node_topo_info.sockets);
  } else {
    craned_info->set_configured_sockets(
        config_it->second->node_topo_info.sockets);

    for (const auto& resource_name : g_config.GresOrder)
      for (const auto& resource : config_it->second->gres_order) {
        if (resource.name != resource_name) continue;
        auto* ordered = craned_info->add_gres_order();
        ordered->set_name(resource.name);
        ordered->set_type(resource.type);
        for (const auto& device : resource.devices)
          ordered->add_devices(device);
      }
  }

  craned_info->set_dynamic(craned_meta.static_meta.dynamic);
  craned_info->set_hostname(craned_meta.static_meta.hostname);
  craned_info->set_node_hostname(craned_meta.static_meta.node_hostname);
  craned_info->set_node_addr(craned_meta.static_meta.node_addr);
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

  craned_info->set_running_job_num(craned_meta.rn_job_res_map.size());

  if (craned_meta.drain) {
    craned_info->set_control_state(
        crane::grpc::CranedControlState::CRANE_DRAIN);
  } else {
    craned_info->set_control_state(crane::grpc::CranedControlState::CRANE_NONE);
  }

  // Set power state
  craned_info->set_power_state(craned_meta.power_state);

  if (craned_meta.static_meta.is_future && !craned_meta.future_mapped) {
    craned_info->set_resource_state(
        crane::grpc::CranedResourceState::CRANE_FUTURE);
  } else if (craned_meta.alive) {
    if (craned_meta.res_in_use.IsZero())
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_IDLE);
    else if (craned_meta.res_avail.IsExhausted())
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_ALLOC);
    else
      craned_info->set_resource_state(
          crane::grpc::CranedResourceState::CRANE_MIX);
  } else {
    craned_info->set_resource_state(
        crane::grpc::CranedResourceState::CRANE_DOWN);
  }

  craned_info->set_dynamic_mapped(craned_meta.static_meta.is_future &&
                                  craned_meta.future_mapped);
  craned_info->mutable_features()->Assign(
      craned_meta.static_meta.features.begin(),
      craned_meta.static_meta.features.end());

  craned_info->mutable_partition_names()->Assign(
      craned_meta.static_meta.partition_ids.begin(),
      craned_meta.static_meta.partition_ids.end());

  // Set physical CPU socket count from static configuration.
  craned_info->mutable_node_topo_info()->set_sockets(
      craned_meta.static_meta.node_topo_info.sockets);
}

}  // namespace Ctld
