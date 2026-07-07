#pragma once

#include "crane/Tracing.h"
#include "protos/Crane.pb.h"

namespace crane {

inline void FillRuntimeTraceConfigProto(
    ::crane::grpc::RuntimeTraceConfig* proto,
    RuntimeTraceConfig config = GetRuntimeTraceConfig()) {
  proto->set_compiled_with_tracing(config.compiled_with_tracing);
  proto->set_compiled_max_level(
      std::string{TraceLevelToString(config.compiled_max_level)});
  proto->set_runtime_enabled(config.enabled);
  proto->set_runtime_level(
      std::string{TraceLevelToString(config.runtime_level)});
  proto->set_effective_level(
      std::string{TraceLevelToString(config.effective_level)});
  proto->set_clamped(config.clamped);
}

inline RuntimeTraceConfig ApplyRuntimeTraceConfig(
    const ::crane::grpc::RuntimeTraceConfig& proto) {
  TraceLevel level;
  if (!TraceLevelFromString(proto.runtime_level(), &level))
    level = TraceLevel::Debug;
  return ApplyRuntimeTraceConfig(proto.runtime_enabled(), level);
}

}  // namespace crane
