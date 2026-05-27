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

#include "CpuTopoDetector.h"

#include "PreCompiledHeader.h"

namespace Craned::Common {

NodeTopoInfo CpuTopoDetector::Detect(uint32_t cfg_sockets, uint32_t cfg_cores,
                                     uint32_t cfg_threads) {
#ifdef CRANE_ENABLE_HWLOC
  hwloc_topology_t topology;
  if (hwloc_topology_init(&topology) != 0) {
    CRANE_WARN(
        "hwloc_topology_init() failed, falling back to /proc/cpuinfo");
    return ReconcileWithConfig_(ParseFromProcCpuinfo_(), cfg_sockets,
                                cfg_cores, cfg_threads);
  }

  if (!LoadTopology_(&topology)) {
    hwloc_topology_destroy(topology);
    CRANE_WARN("hwloc topology load failed, falling back to /proc/cpuinfo");
    return ReconcileWithConfig_(ParseFromProcCpuinfo_(), cfg_sockets,
                                cfg_cores, cfg_threads);
  }

  NodeTopoInfo result = BuildFromHwloc_(topology);
  hwloc_topology_destroy(topology);

  CRANE_INFO(
      "hwloc detected: boards={} sockets={} cores_per_socket={} "
      "threads_per_core={} total_cpus={}",
      result.boards, result.sockets, result.cores_per_socket,
      result.threads_per_core, result.total_cpus);

  return ReconcileWithConfig_(result, cfg_sockets, cfg_cores, cfg_threads);
#else
  CRANE_WARN("hwloc disabled at compile time, using /proc/cpuinfo");
  return ReconcileWithConfig_(ParseFromProcCpuinfo_(), cfg_sockets, cfg_cores,
                              cfg_threads);
#endif
}

#ifdef CRANE_ENABLE_HWLOC

bool CpuTopoDetector::LoadTopology_(hwloc_topology_t* topology) {
  hwloc_topology_set_flags(*topology, HWLOC_TOPOLOGY_FLAG_WHOLE_SYSTEM);

#if HWLOC_API_VERSION >= 0x00020000
  hwloc_topology_set_type_filter(*topology, HWLOC_OBJ_L1CACHE,
                                 HWLOC_TYPE_FILTER_KEEP_NONE);
  hwloc_topology_set_type_filter(*topology, HWLOC_OBJ_L2CACHE,
                                 HWLOC_TYPE_FILTER_KEEP_NONE);
  hwloc_topology_set_type_filter(*topology, HWLOC_OBJ_L4CACHE,
                                 HWLOC_TYPE_FILTER_KEEP_NONE);
  hwloc_topology_set_type_filter(*topology, HWLOC_OBJ_L5CACHE,
                                 HWLOC_TYPE_FILTER_KEEP_NONE);
  hwloc_topology_set_type_filter(*topology, HWLOC_OBJ_MISC,
                                 HWLOC_TYPE_FILTER_KEEP_NONE);
#else
  hwloc_topology_ignore_type(*topology, HWLOC_OBJ_CACHE);
  hwloc_topology_ignore_type(*topology, HWLOC_OBJ_MISC);
#endif

  if (hwloc_topology_load(*topology) != 0) {
    CRANE_WARN("hwloc_topology_load() failed");
    return false;
  }
  return true;
}

NodeTopoInfo CpuTopoDetector::BuildFromHwloc_(hwloc_topology_t topology) {
  NodeTopoInfo result;

  // Detect Board count: if the root's first child is a Group, count Groups
  hwloc_obj_t root = hwloc_get_root_obj(topology);
  hwloc_obj_t root_child = hwloc_get_next_child(topology, root, nullptr);
  if (root_child && root_child->type == HWLOC_OBJ_GROUP) {
    int depth = root_child->depth;
    int nboards = hwloc_get_nbobjs_by_depth(topology, depth);
    result.boards = static_cast<uint32_t>(std::max(1, nboards));
  }

  // Detect socket count (skip sockets with no cores, e.g. KNL empty NUMA)
  int depth_sock =
      hwloc_get_type_depth(topology, HWLOC_OBJ_SOCKET);
  if (depth_sock == HWLOC_TYPE_DEPTH_UNKNOWN ||
      depth_sock == HWLOC_TYPE_DEPTH_MULTIPLE) {
    // No socket topology, treat whole machine as 1 socket
    result.sockets = 1;
  } else {
    int sock_cnt = hwloc_get_nbobjs_by_depth(topology, depth_sock);
    uint32_t valid_socks = 0;
    for (int i = 0; i < sock_cnt; i++) {
      hwloc_obj_t sock = hwloc_get_obj_by_depth(topology, depth_sock, i);
      if (hwloc_get_nbobjs_inside_cpuset_by_type(
              topology, sock->cpuset, HWLOC_OBJ_CORE) > 0)
        valid_socks++;
    }
    result.sockets = std::max(1u, valid_socks);
  }

  // Total cores and PUs
  int total_cores = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_CORE);
  if (total_cores == 0) total_cores = 1;

  int total_pus = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
  if (total_pus == 0) total_pus = total_cores;

  result.total_cpus = static_cast<uint32_t>(total_pus);
  result.threads_per_core =
      static_cast<uint32_t>(total_pus / total_cores);
  result.cores_per_socket =
      static_cast<uint32_t>(total_cores / result.sockets);

  return result;
}

#endif  // CRANE_ENABLE_HWLOC

NodeTopoInfo CpuTopoDetector::ParseFromProcCpuinfo_() {
  NodeTopoInfo result;

  std::ifstream f("/proc/cpuinfo");
  if (!f.is_open()) {
    CRANE_WARN("Cannot open /proc/cpuinfo, using default topology (1x1x1)");
    return result;
  }

  std::set<int> physical_ids;
  std::set<std::pair<int, int>> core_ids;  // (physical_id, core_id)
  int total_pus = 0;
  int cur_physical = -1;
  int cur_core = -1;

  std::string line;
  while (std::getline(f, line)) {
    if (line.empty()) {
      if (cur_physical >= 0 && cur_core >= 0) {
        physical_ids.insert(cur_physical);
        core_ids.emplace(cur_physical, cur_core);
        total_pus++;
      }
      cur_physical = -1;
      cur_core = -1;
      continue;
    }
    auto sep = line.find(':');
    if (sep == std::string::npos) continue;
    std::string key = line.substr(0, sep);
    std::string val = line.substr(sep + 1);
    // Trim whitespace
    while (!key.empty() && key.back() == ' ') key.pop_back();
    while (!val.empty() && val.front() == ' ') val.erase(val.begin());

    if (key == "physical id") cur_physical = std::stoi(val);
    else if (key == "core id") cur_core = std::stoi(val);
  }
  // Handle last block if file doesn't end with blank line
  if (cur_physical >= 0 && cur_core >= 0) {
    physical_ids.insert(cur_physical);
    core_ids.emplace(cur_physical, cur_core);
    total_pus++;
  }

  if (physical_ids.empty()) {
    // Single-socket or no physical id fields (VMs etc.)
    result.sockets = 1;
    result.cores_per_socket = static_cast<uint32_t>(core_ids.size());
    result.total_cpus = static_cast<uint32_t>(total_pus);
    if (result.cores_per_socket == 0) result.cores_per_socket = 1;
    result.threads_per_core =
        result.total_cpus / result.cores_per_socket;
  } else {
    result.sockets =
        static_cast<uint32_t>(std::max<size_t>(1, physical_ids.size()));
    int total_cores = static_cast<int>(core_ids.size());
    if (total_cores == 0) total_cores = 1;
    result.total_cpus = static_cast<uint32_t>(std::max(1, total_pus));
    result.cores_per_socket =
        static_cast<uint32_t>(total_cores / result.sockets);
    if (result.cores_per_socket == 0) result.cores_per_socket = 1;
    result.threads_per_core = result.total_cpus /
                              (result.sockets * result.cores_per_socket);
  }
  if (result.threads_per_core == 0) result.threads_per_core = 1;

  CRANE_INFO(
      "/proc/cpuinfo: boards={} sockets={} cores_per_socket={} "
      "threads_per_core={} total_cpus={}",
      result.boards, result.sockets, result.cores_per_socket,
      result.threads_per_core, result.total_cpus);

  return result;
}

NodeTopoInfo CpuTopoDetector::ReconcileWithConfig_(
    const NodeTopoInfo& detected, uint32_t cfg_sockets, uint32_t cfg_cores,
    uint32_t cfg_threads) {
  if (cfg_sockets == 0 && cfg_cores == 0 && cfg_threads == 0)
    return detected;

  NodeTopoInfo result = detected;
  if (cfg_sockets > 0) result.sockets = cfg_sockets;
  if (cfg_cores > 0) result.cores_per_socket = cfg_cores;
  if (cfg_threads > 0) result.threads_per_core = cfg_threads;

  uint32_t expected = result.sockets * result.cores_per_socket *
                      result.threads_per_core;
  if (expected != detected.total_cpus && detected.total_cpus > 0) {
    CRANE_WARN(
        "Config topology ({}×{}×{}={}) differs from detected ({} cpus)",
        result.sockets, result.cores_per_socket, result.threads_per_core,
        expected, detected.total_cpus);
  }

  return result;
}

}  // namespace Craned::Common
