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

#pragma once

#include "CtldPreCompiledHeader.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

/**
 * The static information on a Craned (the static part of CranedMeta). This
 * structure is provided when a new Craned node is to be registered in
 * CranedMetaContainer.
 */
struct CranedStaticMeta {
  std::string hostname;  // the hostname corresponds to the node index
  uint32_t port;

  std::list<std::string> partition_ids;  // Partitions to which
                                         // this craned belongs to
  ResourceInNodeV3 res;

  NodeTopoInfo node_topo_info;
};

struct CranedRemoteMeta {
  DedicatedResourceInNode dres_in_node;
  SystemRelInfo sys_rel_info;
  std::string craned_version;
  absl::Time craned_start_time;
  absl::Time system_boot_time;

  std::vector<crane::grpc::NetworkInterface> network_interfaces;

  CranedRemoteMeta() = default;
  explicit CranedRemoteMeta(const crane::grpc::CranedRemoteMeta& grpc_meta);
};

/**
 * Represent the runtime status on a Craned node.
 * A Node is uniquely identified by (partition id, node index).
 */
struct CranedMeta {
  CranedStaticMeta static_meta;
  CranedRemoteMeta remote_meta;

  bool alive{false};
  crane::grpc::CranedPowerState power_state{
      crane::grpc::CranedPowerState::CRANE_POWER_IDLE};

  // total = avail + in-use
  ResourceInNodeV3 res_total;  // A copy of res in CranedStaticMeta,
  ResourceInNodeV3 res_avail;
  ResourceInNodeV3 res_in_use;

  bool drain{false};
  std::string state_reason{""};
  absl::Time last_busy_time;
  absl::Time craned_down_time;

  absl::flat_hash_map<job_id_t, ResourceInNodeV3> rn_job_res_map;

  absl::flat_hash_map<ResvId, std::pair<absl::Time, absl::Time>>
      resv_in_node_map;
};

struct ResvMeta {
  ResvId name;
  PartitionId part_id;
  absl::Time start_time;
  absl::Time end_time;

  bool accounts_black_list{false};
  bool users_black_list{false};
  std::unordered_set<std::string> accounts;
  std::unordered_set<std::string> users;

  absl::flat_hash_set<CranedId> craned_ids;
  ResourceV3 res_total;
  ResourceV3 res_avail;
  absl::flat_hash_map<job_id_t, ResourceV3> rn_job_res_map;
  absl::flat_hash_set<job_id_t> pd_job_ids;
};

struct PartitionGlobalMeta {
  // total = avail + in-use
  ResourceView res_total;
  ResourceView res_avail;
  ResourceView res_in_use;

  // Include resources in unavailable nodes.
  ResourceView res_total_inc_dead;

  std::string name;
  std::string nodelist_str;

  std::unordered_set<std::string> allowed_accounts;
  std::unordered_set<std::string> denied_accounts;

  uint32_t node_cnt;
  uint32_t alive_craned_cnt;
};

struct PartitionMeta {
  PartitionGlobalMeta partition_global_meta;
  std::unordered_set<CranedId> craned_ids;
};

}  // namespace Ctld
