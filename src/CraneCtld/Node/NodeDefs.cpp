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

#include "Node/NodeDefs.h"

namespace Ctld {

CranedRemoteMeta::CranedRemoteMeta(
    const crane::grpc::CranedRemoteMeta& grpc_meta)
    : dres_in_node(grpc_meta.dres_in_node()) {
  this->sys_rel_info.name = grpc_meta.sys_rel_info().name();
  this->sys_rel_info.release = grpc_meta.sys_rel_info().release();
  this->sys_rel_info.version = grpc_meta.sys_rel_info().version();
  this->craned_start_time =
      absl::FromUnixSeconds(grpc_meta.craned_start_time().seconds());
  this->system_boot_time =
      absl::FromUnixSeconds(grpc_meta.system_boot_time().seconds());

  this->network_interfaces.clear();
  for (const auto& interface : grpc_meta.network_interfaces()) {
    this->network_interfaces.emplace_back(interface);
  }
  if (grpc_meta.has_reported_spec())
    this->reported_spec = grpc_meta.reported_spec();
  this->physical_hostname = grpc_meta.physical_hostname();
}

std::optional<crane::grpc::CranedPowerState> CranedPowerStateFromDynamic(
    crane::grpc::DynamicNodePowerState state) {
  switch (state) {
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_OFF:
    return crane::grpc::CRANE_POWER_POWEREDOFF;
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON:
    return crane::grpc::CRANE_POWER_POWERING_ON;
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF:
    return crane::grpc::CRANE_POWER_POWERING_OFF;
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_SLEEPING:
    return crane::grpc::CRANE_POWER_SLEEPING;
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP:
    return crane::grpc::CRANE_POWER_WAKING_UP;
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING:
    return crane::grpc::CRANE_POWER_TO_SLEEPING;
  default:
    return std::nullopt;
  }
}

std::optional<crane::grpc::DynamicNodePowerState>
DynamicNodePowerStateFromCraned(crane::grpc::CranedPowerState state) {
  switch (state) {
  case crane::grpc::CRANE_POWER_POWEREDOFF:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_OFF;
  case crane::grpc::CRANE_POWER_POWERING_ON:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON;
  case crane::grpc::CRANE_POWER_WAKING_UP:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP;
  case crane::grpc::CRANE_POWER_POWERING_OFF:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF;
  case crane::grpc::CRANE_POWER_SLEEPING:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_SLEEPING;
  case crane::grpc::CRANE_POWER_TO_SLEEPING:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING;
  case crane::grpc::CRANE_POWER_ACTIVE:
  case crane::grpc::CRANE_POWER_IDLE:
    return crane::grpc::DYNAMIC_NODE_POWER_STATE_ON;
  default:
    return std::nullopt;
  }
}

}  // namespace Ctld
