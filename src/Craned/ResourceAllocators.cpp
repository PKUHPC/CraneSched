#include "ResourceAllocators.h"

namespace Craned {

bool AllocatableResourceAllocator::Allocate(const AllocatableResource &resource,
                                            util::Cgroup *cg) {
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
    const crane::grpc::AllocatableResource &resource, util::Cgroup *cg) {
  bool ok;
  ok = cg->SetCpuCoreLimit(resource.cpu_core_limit());
  ok &= cg->SetMemoryLimitBytes(resource.memory_limit_bytes());

  // Depending on the system configuration, the following two options may not
  // be enabled, so we ignore the result of them.
  cg->SetMemorySoftLimitBytes(resource.memory_sw_limit_bytes());
  cg->SetMemorySwLimitBytes(resource.memory_sw_limit_bytes());
  return ok;
}

bool DedicatedResourceAllocator::Allocate(const DedicatedResource& resource, util::Cgroup* cg){
  for(auto& it:resource.devices){
    if(!cg->SetDeviceDeny(DedicatedResource::DeviceType(util::CgroupUtil::getDeviceType(it.first)),it.second)){
      return false;
    }
  }
  return true;
}

bool DedicatedResourceAllocator::Allocate(const crane::grpc::DedicatedResource& resource,
                        util::Cgroup* cg){
  for(auto& it:resource.devices()){
    if(!cg->SetDeviceDeny(DedicatedResource::DeviceType(util::CgroupUtil::getDeviceType(it.first)),it.second)){
      return false;
    }
  }
  return true;
}

}  // namespace Craned
