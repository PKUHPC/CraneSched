/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <string_view>

#include "crane/Logger.h"

inline ConfiguredNodeTopology NormalizeConfiguredNodeTopology(
    ConfiguredNodeTopology config, const std::string& node_name) {
  auto& topo = config.topology;

  auto reset_zero = [&](uint32_t& value, std::string_view field) {
    if (value != 0) return;
    CRANE_ERROR("Invalid {}=0 for node '{}'. Resetting to 1.", field,
                node_name);
    value = 1;
  };

  // Slurm initializes all topology dimensions to 1. Missing config fields are
  // normalized from config only; hwloc actual values must not back-fill them.
  reset_zero(topo.boards, "boards");
  reset_zero(topo.sockets, "sockets");
  reset_zero(topo.cores_per_socket, "cores_per_socket");
  reset_zero(topo.threads_per_core, "threads_per_core");
  reset_zero(config.sockets_per_board, "sockets_per_board");

  if (config.sockets_configured && config.sockets_per_board_configured) {
    CRANE_WARN(
        "Both sockets={} and sockets_per_board={} are configured for node "
        "'{}'. Using sockets_per_board.",
        topo.sockets, config.sockets_per_board, node_name);
  }

  if (config.sockets_per_board_configured) {
    topo.sockets = topo.boards * config.sockets_per_board;
  } else if (!config.sockets_configured && config.cpu_configured) {
    const uint32_t cores_threads =
        topo.cores_per_socket * topo.threads_per_core;
    if (cores_threads != 0 && topo.total_cpus % cores_threads == 0) {
      topo.sockets = topo.total_cpus / cores_threads;
    } else {
      topo.sockets = topo.boards;
    }
  }

  const uint32_t full_product =
      topo.sockets * topo.cores_per_socket * topo.threads_per_core;

  if (topo.sockets < topo.boards) {
    CRANE_ERROR("Sockets({}) < Boards({}) for node '{}'. Resetting Boards to 1.",
                topo.sockets, topo.boards, node_name);
    topo.boards = 1;
  }

  if (!config.cpu_configured || topo.total_cpus == 0) {
    topo.total_cpus =
        topo.sockets * topo.cores_per_socket * topo.threads_per_core;
    config.cpu_configured = true;
    return config;
  }

  // Slurm compatibility cases: CPUs may equal sockets, sockets*cores, or the
  // full sockets*cores*threads product. Other values are reset to the product.
  const uint32_t socket_product = topo.sockets;
  const uint32_t core_product = topo.sockets * topo.cores_per_socket;
  if (topo.total_cpus != socket_product && topo.total_cpus != core_product &&
      topo.total_cpus != full_product) {
    CRANE_WARN(
        "CPU={} for node '{}' is inconsistent with topology "
        "sockets={} cores_per_socket={} threads_per_core={}. Resetting CPU "
        "to {}.",
        topo.total_cpus, node_name, topo.sockets, topo.cores_per_socket,
        topo.threads_per_core, full_product);
    topo.total_cpus = full_product;
  }

  return config;
}
