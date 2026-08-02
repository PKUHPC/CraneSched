/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "crane/Tracing.h"

namespace crane {

inline constexpr std::string_view kExecutionFlowIdEnvironmentVariable =
    "CRANE_EXECUTION_FLOW_ID";
inline constexpr std::string_view kExecutionFlowSchema = "v1";

#ifdef CRANE_ENABLE_EXECUTION_FLOW

bool IsValidExecutionFlowId(std::string_view flow_id);
std::optional<std::string> ParseExecutionFlowId(std::string_view flow_id);
std::string MakeExecutionFlowServiceInstance(std::string_view logical_instance,
                                             uint64_t process_id,
                                             uint64_t process_start_unix_nanos);
std::string_view ExecutionFlowJobUnsupportedReason(bool is_batch,
                                                   bool is_container,
                                                   bool is_array,
                                                   bool no_requeue,
                                                   int32_t requeue_count);
bool IsAllowedExecutionFlowAttribute(std::string_view key);

template <typename Environment>
std::string ExecutionFlowIdFromEnvironment(const Environment& environment) {
  auto it = environment.find(std::string{kExecutionFlowIdEnvironmentVariable});
  if (it == environment.end()) return {};
  auto parsed = ParseExecutionFlowId(it->second);
  return parsed.value_or(std::string{});
}

bool ExecutionFlowEnabled();
bool ExecutionFlowHeartbeatRunning();
void InitializeExecutionFlow(bool enabled, uint32_t heartbeat_interval_seconds,
                             std::string service, std::string instance,
                             bool emit_heartbeat);
void ReconcileExecutionFlowWithTracing();
void ShutdownExecutionFlow();

class ExecutionFlowPoint {
 public:
  ExecutionFlowPoint(std::string_view point, std::string_view flow_id,
                     std::string_view traceparent = {});

  ExecutionFlowPoint(const ExecutionFlowPoint&) = delete;
  ExecutionFlowPoint& operator=(const ExecutionFlowPoint&) = delete;
  ExecutionFlowPoint(ExecutionFlowPoint&&) = delete;
  ExecutionFlowPoint& operator=(ExecutionFlowPoint&&) = delete;
  ~ExecutionFlowPoint();

  template <typename T>
  void SetAttribute(std::string_view key, const T& value) {
    if (span_ && IsAllowedExecutionFlowAttribute(key))
      span_->SetAttribute(std::string{key}, value);
  }

  void SetError() {
    if (span_) span_->SetStatus(StatusCode::kError);
  }

  [[nodiscard]] bool IsActive() const { return span_ != nullptr; }

 private:
  bool emitter_acquired_{false};
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::common::SteadyTimestamp point_time_;
};

#  define CRANE_EXECUTION_FLOW_INITIALIZE(enabled, interval, service,      \
                                          instance, emit_heartbeat)        \
    ::crane::InitializeExecutionFlow(enabled, interval, service, instance, \
                                     emit_heartbeat)
#  define CRANE_EXECUTION_FLOW_RECONCILE()       \
    ::crane::ReconcileExecutionFlowWithTracing()
#  define CRANE_EXECUTION_FLOW_SHUTDOWN() ::crane::ShutdownExecutionFlow()

// Arguments occur only in the enabled expansion so a compiled-out flow point
// cannot evaluate IDs or attributes at the call site.
#  define CRANE_FLOW_POINT(point, flow_id, traceparent, ...)           \
    do {                                                               \
      if (::crane::ExecutionFlowEnabled()) {                           \
        ::crane::ExecutionFlowPoint _crane_flow_point_(point, flow_id, \
                                                       traceparent);   \
        if (_crane_flow_point_.IsActive()) {                           \
          __VA_ARGS__                                                  \
        }                                                              \
      }                                                                \
    } while (0)

#  define CRANE_FLOW_SET_ATTR(key, value)       \
    _crane_flow_point_.SetAttribute(key, value)
#  define CRANE_FLOW_SET_ERROR() _crane_flow_point_.SetError()

#else

#  define CRANE_EXECUTION_FLOW_INITIALIZE(enabled, interval, service, \
                                          instance, emit_heartbeat)   \
    (void)0
#  define CRANE_EXECUTION_FLOW_RECONCILE() (void)0
#  define CRANE_EXECUTION_FLOW_SHUTDOWN() (void)0
#  define CRANE_FLOW_POINT(point, flow_id, traceparent, ...) (void)0
#  define CRANE_FLOW_SET_ATTR(key, value) (void)0
#  define CRANE_FLOW_SET_ERROR() (void)0

#endif

}  // namespace crane
