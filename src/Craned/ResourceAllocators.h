#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "cgroup.linux.h"
#include "crane/PublicHeader.h"

namespace Craned {

class AllocatableResourceAllocator {
 public:
  static bool Allocate(const AllocatableResource& resource, util::Cgroup* cg);
  static bool Allocate(const crane::grpc::AllocatableResource& resource,
                       util::Cgroup* cg);
};

class DedicatedResourceAllocator
{
private:
    /* data */
public:
    static bool Allocate(const DedicatedResource& resource, util::Cgroup* cg);
    static bool Allocate(const crane::grpc::DedicatedResource& resource,
                        util::Cgroup* cg);
};



}  // namespace Craned
