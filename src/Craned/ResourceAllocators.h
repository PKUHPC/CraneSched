#pragma once

#include "cgroup.linux.h"
#include "crane/PublicHeader.h"

namespace Craned {

class AllocatableResourceAllocator {
 public:
  static bool Allocate(const AllocatableResource& resource, util::Cgroup* cg);
  static bool Allocate(const crane::grpc::AllocatableResource& resource,
                       util::Cgroup* cg);
};

}  // namespace Craned
