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

#include "ResourceAllocators.h"

namespace Craned {

bool AllocatableResourceAllocator::Allocate(const AllocatableResource& resource,
                                            util::Cgroup* cg) {
  bool ok;
  ok = cg->SetCpuCoreLimit(resource.cpu_count);
  ok &= cg->SetMemoryLimitBytes(resource.memory_bytes);

  // Depending on the system configuration, the following two options may not
  // be enabled, so we ignore the result of them.
  cg->SetMemorySoftLimitBytes(resource.memory_sw_bytes);
  cg->SetMemorySwLimitBytes(resource.memory_sw_bytes);
  return ok;
}

bool AllocatableResourceAllocator::Allocate(
    const crane::grpc::AllocatableResource& resource, util::Cgroup* cg) {
  bool ok;
  ok = cg->SetCpuCoreLimit(resource.cpu_core_limit());
  ok &= cg->SetMemoryLimitBytes(resource.memory_limit_bytes());

  // Depending on the system configuration, the following two options may not
  // be enabled, so we ignore the result of them.
  cg->SetMemorySoftLimitBytes(resource.memory_sw_limit_bytes());
  cg->SetMemorySwLimitBytes(resource.memory_sw_limit_bytes());
  return ok;
}

bool DedicatedResourceAllocator::Allocate(
    const crane::grpc::DedicatedResource& request_resource, util::Cgroup* cg) {
  auto devices = Craned::g_this_node_device;
  std::unordered_set<std::string> all_request_slots;
  if (request_resource.each_node_gres().contains(g_config.Hostname)) {
    for (const auto& [_, request_slots] : request_resource.each_node_gres()
                                              .at(g_config.Hostname)
                                              .name_slots_map()) {
      all_request_slots.insert(request_slots.slot().cbegin(),
                               request_slots.slot().cend());
    };
  }
  std::ranges::for_each(devices, [&all_request_slots](Device& dev) {
    dev.alloc = all_request_slots.contains(dev.path);
  });
  if (!cg->SetDeviceAccess(devices, true, true, true)) return false;
  return true;
}

}  // namespace Craned
