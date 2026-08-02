/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include "crane/ExecutionFlow.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <ranges>
#include <thread>
#include <utility>

#include "crane/TracerManager.h"

namespace crane {
namespace {

std::atomic<bool> g_execution_flow_configured{false};
std::atomic<uint64_t> g_execution_flow_sequence{0};
std::mutex g_execution_flow_lifecycle_mutex;
std::mutex g_execution_flow_emitters_mutex;
std::condition_variable g_execution_flow_emitters_cv;
uint64_t g_execution_flow_emitters{0};
std::mutex g_execution_flow_mutex;
std::condition_variable g_execution_flow_cv;
std::thread g_execution_flow_heartbeat_thread;
bool g_execution_flow_stopping{false};
bool g_execution_flow_emit_heartbeat{true};
uint32_t g_heartbeat_interval_seconds{5};
std::string g_execution_flow_service;
std::string g_execution_flow_logical_instance;
std::string g_execution_flow_instance;

bool AcquireExecutionFlowEmitter() {
  std::lock_guard lock{g_execution_flow_emitters_mutex};
  if (!ExecutionFlowEnabled()) return false;
  ++g_execution_flow_emitters;
  return true;
}

void ReleaseExecutionFlowEmitter() {
  std::lock_guard lock{g_execution_flow_emitters_mutex};
  assert(g_execution_flow_emitters > 0);
  --g_execution_flow_emitters;
  if (g_execution_flow_emitters == 0) g_execution_flow_emitters_cv.notify_all();
}

class ExecutionFlowEmitterLease {
 public:
  ExecutionFlowEmitterLease() : acquired_{AcquireExecutionFlowEmitter()} {}
  ~ExecutionFlowEmitterLease() {
    if (acquired_) ReleaseExecutionFlowEmitter();
  }

  explicit operator bool() const { return acquired_; }

 private:
  bool acquired_;
};

template <typename Span>
void SetCommonAttributes(Span* span, std::string_view point) {
  std::string service;
  std::string logical_instance;
  std::string instance;
  {
    std::lock_guard lock{g_execution_flow_mutex};
    service = g_execution_flow_service;
    logical_instance = g_execution_flow_logical_instance;
    instance = g_execution_flow_instance;
  }
  span->SetAttribute("flow_schema", std::string{kExecutionFlowSchema});
  span->SetAttribute("point", std::string{point});
  span->SetAttribute("producer", service);
  span->SetAttribute("service_logical_instance", logical_instance);
  span->SetAttribute("service_instance", instance);
  span->SetAttribute("event_sequence",
                     static_cast<int64_t>(g_execution_flow_sequence.fetch_add(
                         1, std::memory_order_relaxed)));
}

void EmitPipelineHeartbeat() {
  ExecutionFlowEmitterLease emitter;
  if (!emitter) return;

  constexpr std::string_view kPoint = "flow/v1/pipeline/heartbeat";
  auto tracer = TracerManager::GetInstance().GetTracerSafe();
  if (!ShouldCreateTraceSpan(kPoint) || !tracer) return;

  const auto system_time = std::chrono::system_clock::now();
  const auto steady_time = std::chrono::steady_clock::now();
  opentelemetry::trace::StartSpanOptions start_options;
  start_options.start_system_time = system_time;
  start_options.start_steady_time = steady_time;
  auto span = tracer->StartSpan(std::string{kPoint}, start_options);
  if (!span) return;
  SetCommonAttributes(span.get(), "pipeline/heartbeat");
  opentelemetry::trace::EndSpanOptions end_options;
  end_options.end_steady_time = steady_time;
  span->End(end_options);
}

void HeartbeatLoop() {
  std::unique_lock lock{g_execution_flow_mutex};
  while (!g_execution_flow_stopping) {
    auto interval = std::chrono::seconds(g_heartbeat_interval_seconds);
    lock.unlock();
    EmitPipelineHeartbeat();
    lock.lock();
    g_execution_flow_cv.wait_for(lock, interval,
                                 [] { return g_execution_flow_stopping; });
  }
}

void StopHeartbeatLocked() {
  {
    std::lock_guard lock{g_execution_flow_mutex};
    g_execution_flow_stopping = true;
  }
  g_execution_flow_cv.notify_all();
  if (g_execution_flow_heartbeat_thread.joinable())
    g_execution_flow_heartbeat_thread.join();
}

void DisableExecutionFlowAndDrain() {
  {
    std::lock_guard lock{g_execution_flow_emitters_mutex};
    g_execution_flow_configured.store(false, std::memory_order_release);
  }
  StopHeartbeatLocked();

  std::unique_lock lock{g_execution_flow_emitters_mutex};
  g_execution_flow_emitters_cv.wait(
      lock, [] { return g_execution_flow_emitters == 0; });
}

void StartHeartbeatLocked() {
  if (!ExecutionFlowEnabled() || g_execution_flow_heartbeat_thread.joinable())
    return;
  {
    std::lock_guard lock{g_execution_flow_mutex};
    g_execution_flow_stopping = false;
  }
  g_execution_flow_heartbeat_thread = std::thread(HeartbeatLoop);
}

void ReconcileHeartbeatLocked() {
  if (ExecutionFlowEnabled() && g_execution_flow_emit_heartbeat)
    StartHeartbeatLocked();
  else
    StopHeartbeatLocked();
}

}  // namespace

bool IsValidExecutionFlowId(std::string_view flow_id) {
  return flow_id.size() == 32 &&
         std::ranges::all_of(flow_id, [](unsigned char ch) {
           return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         });
}

std::optional<std::string> ParseExecutionFlowId(std::string_view flow_id) {
  if (!IsValidExecutionFlowId(flow_id)) return std::nullopt;
  return std::string{flow_id};
}

std::string MakeExecutionFlowServiceInstance(
    std::string_view logical_instance, uint64_t process_id,
    uint64_t process_start_unix_nanos) {
  std::string instance{logical_instance};
  instance.append("#pid=");
  instance.append(std::to_string(process_id));
  instance.append(":start=");
  instance.append(std::to_string(process_start_unix_nanos));
  return instance;
}

std::string_view ExecutionFlowJobUnsupportedReason(bool is_batch,
                                                   bool is_container,
                                                   bool is_array,
                                                   bool no_requeue,
                                                   int32_t requeue_count) {
  if (is_container) return "container-job";
  if (!is_batch) return "non-batch-job";
  if (is_array) return "array-job";
  if (!no_requeue) return "requeue-enabled";
  if (requeue_count != 0) return "requeue-attempt";
  return {};
}

bool IsAllowedExecutionFlowAttribute(std::string_view key) {
  static constexpr std::array<std::string_view, 9> kAllowedAttributes{
      "job_id",    "step_id", "task_id", "attempt",    "node_id",
      "operation", "outcome", "status",  "reason_code"};
  return std::ranges::find(kAllowedAttributes, key) != kAllowedAttributes.end();
}

bool ExecutionFlowEnabled() {
  return g_execution_flow_configured.load(std::memory_order_acquire) &&
         g_tracing_enabled.load(std::memory_order_acquire);
}

bool ExecutionFlowHeartbeatRunning() {
  std::lock_guard lock{g_execution_flow_lifecycle_mutex};
  return g_execution_flow_heartbeat_thread.joinable();
}

void InitializeExecutionFlow(bool enabled, uint32_t heartbeat_interval_seconds,
                             std::string service, std::string instance,
                             bool emit_heartbeat) {
  std::lock_guard lifecycle_lock{g_execution_flow_lifecycle_mutex};
  DisableExecutionFlowAndDrain();

  const auto process_start_unix_nanos = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  auto process_instance = MakeExecutionFlowServiceInstance(
      instance, static_cast<uint64_t>(getpid()), process_start_unix_nanos);

  {
    std::lock_guard lock{g_execution_flow_mutex};
    g_execution_flow_service = std::move(service);
    g_execution_flow_logical_instance = std::move(instance);
    g_execution_flow_instance = std::move(process_instance);
    g_execution_flow_emit_heartbeat = emit_heartbeat;
    g_heartbeat_interval_seconds =
        std::max<uint32_t>(1, heartbeat_interval_seconds);
    g_execution_flow_sequence.store(0, std::memory_order_relaxed);
  }

  g_execution_flow_configured.store(enabled, std::memory_order_release);
  ReconcileHeartbeatLocked();
}

void ReconcileExecutionFlowWithTracing() {
  std::lock_guard lifecycle_lock{g_execution_flow_lifecycle_mutex};
  ReconcileHeartbeatLocked();
}

void ShutdownExecutionFlow() {
  std::lock_guard lifecycle_lock{g_execution_flow_lifecycle_mutex};
  DisableExecutionFlowAndDrain();
}

ExecutionFlowPoint::ExecutionFlowPoint(std::string_view point,
                                       std::string_view flow_id,
                                       std::string_view traceparent) {
  emitter_acquired_ = AcquireExecutionFlowEmitter();
  if (!emitter_acquired_) return;
  auto parsed = ParseExecutionFlowId(flow_id);
  if (!parsed.has_value()) return;

  std::string span_name = "flow/v1/";
  span_name.append(point);
  auto tracer = TracerManager::GetInstance().GetTracerSafe();
  if (!ShouldCreateTraceSpan(span_name) || !tracer) return;

  const auto system_time = std::chrono::system_clock::now();
  const auto steady_time = std::chrono::steady_clock::now();
  opentelemetry::trace::StartSpanOptions start_options;
  start_options.start_system_time = system_time;
  start_options.start_steady_time = steady_time;
  auto parent = DeserializeTraceParent(traceparent);
  if (parent.IsValid()) start_options.parent = parent;
  span_ = tracer->StartSpan(span_name, start_options);

  if (!span_) return;
  point_time_ = steady_time;
  SetCommonAttributes(span_.get(), point);
  span_->SetAttribute("flow_id", *parsed);
  span_->SetAttribute("attempt", int64_t{0});
}

ExecutionFlowPoint::~ExecutionFlowPoint() {
  if (span_) {
    opentelemetry::trace::EndSpanOptions end_options;
    end_options.end_steady_time = point_time_;
    span_->End(end_options);
  }
  if (emitter_acquired_) ReleaseExecutionFlowEmitter();
}

}  // namespace crane
