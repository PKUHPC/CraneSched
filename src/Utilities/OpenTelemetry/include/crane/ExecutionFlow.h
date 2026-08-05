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
#include <utility>

namespace crane {

namespace detail {
class FlowContextBuilder;
class ExecutionFlowPoint;
}  // namespace detail

inline constexpr std::string_view kExecutionFlowIdEnvironmentVariable =
    "CRANE_EXECUTION_FLOW_ID";

class FlowId {
 public:
  [[nodiscard]] static std::optional<FlowId> Parse(std::string_view value) {
    if (value.size() != 32) return std::nullopt;
    for (const unsigned char ch : value) {
      if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
        return std::nullopt;
    }
    return FlowId{std::string{value}};
  }
  [[nodiscard]] std::string_view Value() const { return value_; }

 private:
  explicit FlowId(std::string value) : value_(std::move(value)) {}
  std::string value_;
};

class FlowContext {
 public:
  [[nodiscard]] static std::optional<FlowContext> Create(
      std::string_view flow_id, std::string_view traceparent);
  [[nodiscard]] static std::optional<FlowContext> FromId(
      const FlowId& flow_id, std::string_view traceparent);

  FlowContext& Job(int64_t value) {
    job_id_ = value;
    return *this;
  }
  FlowContext& Step(int64_t value) {
    step_id_ = value;
    return *this;
  }
  FlowContext& Task(int64_t value) {
    task_id_ = value;
    return *this;
  }

 private:
  friend class detail::ExecutionFlowPoint;
  friend class detail::FlowContextBuilder;
  friend class FlowEmitter;

  FlowContext& Node(std::string_view value) {
    node_id_ = value;
    return *this;
  }
  FlowContext& Status(int64_t value) {
    status_ = value;
    return *this;
  }

  FlowContext(FlowId flow_id, std::string traceparent, std::string producer,
              std::string service_logical_instance,
              std::string service_instance)
      : flow_id_(std::move(flow_id)),
        traceparent_(std::move(traceparent)),
        producer_(std::move(producer)),
        service_logical_instance_(std::move(service_logical_instance)),
        service_instance_(std::move(service_instance)) {}

  FlowId flow_id_;
  std::string traceparent_;
  std::string producer_;
  std::string service_logical_instance_;
  std::string service_instance_;
  std::optional<int64_t> job_id_;
  std::optional<int64_t> step_id_;
  std::optional<int64_t> task_id_;
  std::optional<int64_t> status_;
  std::optional<std::string> node_id_;
};

// Configuration and correlation types are part of the stable domain-facing
// API.  The implementation is compiled out when execution flow is disabled;
// callers must not need conditional members or declarations for that build.
struct ExecutionFlowRuntimeConfig {
  bool Enabled{false};
  uint32_t HeartbeatIntervalSeconds{5};
};

// Product-level reasons why a job cannot participate in the Batch execution
// flow contract.  This deliberately does not expose the generated wire-schema
// reason catalog, which also contains FrontEnd pipeline validation failures.
enum class FlowUnsupportedReason {
  kArrayJob,
  kCancelledBeforeAllocation,
  kContainerJob,
  kExtraCommonStep,
  kNonBatchJob,
  kRequeueAttempt,
  kRequeueEnabled,
};

inline constexpr bool kExecutionFlowCompiledIn =
#ifdef CRANE_ENABLE_EXECUTION_FLOW
    true;
#else
    false;
#endif

[[nodiscard]] inline std::optional<FlowId> ExecutionFlowIdFromString(
    std::string_view value) {
#ifdef CRANE_ENABLE_EXECUTION_FLOW
  return FlowId::Parse(value);
#else
  (void)value;
  return std::nullopt;
#endif
}

[[nodiscard]] inline std::optional<FlowContext> MakeExecutionFlowContext(
    const std::optional<FlowId>& flow_id, std::string_view traceparent,
    std::optional<int64_t> job_id = std::nullopt,
    std::optional<int64_t> step_id = std::nullopt,
    std::optional<int64_t> task_id = std::nullopt) {
#ifdef CRANE_ENABLE_EXECUTION_FLOW
  if (!flow_id) return std::nullopt;
  auto context = FlowContext::FromId(*flow_id, traceparent);
  if (!context) return std::nullopt;
  if (job_id) context->Job(*job_id);
  if (step_id) context->Step(*step_id);
  if (task_id) context->Task(*task_id);
  return context;
#else
  // Keep this branch free of parsing and allocation.  In particular, a
  // disabled build must not make correlation data observable to business code.
  (void)flow_id;
  (void)traceparent;
  (void)job_id;
  (void)step_id;
  (void)task_id;
  return std::nullopt;
#endif
}

template <typename Environment>
[[nodiscard]] inline std::optional<FlowId> ExecutionFlowIdFromEnvironment(
    const Environment& environment) {
#ifdef CRANE_ENABLE_EXECUTION_FLOW
  auto it = environment.find(std::string{kExecutionFlowIdEnvironmentVariable});
  if (it == environment.end()) return std::nullopt;
  return FlowId::Parse(it->second);
#else
  (void)environment;
  return std::nullopt;
#endif
}

#ifdef CRANE_ENABLE_EXECUTION_FLOW

std::string MakeExecutionFlowServiceInstance(std::string_view logical_instance,
                                             uint64_t process_id,
                                             uint64_t process_start_unix_nanos);
std::optional<FlowUnsupportedReason> ExecutionFlowJobUnsupportedReason(
    bool is_batch, bool is_container, bool is_array, bool no_requeue,
    int32_t requeue_count);

bool ExecutionFlowEnabled();
bool ExecutionFlowHeartbeatRunning();
void InitializeExecutionFlow(bool enabled, uint32_t heartbeat_interval_seconds,
                             std::string service, std::string instance,
                             bool emit_heartbeat);
void ReconcileExecutionFlowWithTracing();
void ShutdownExecutionFlow();

#else
inline bool ExecutionFlowEnabled() noexcept { return false; }
inline bool ExecutionFlowHeartbeatRunning() noexcept { return false; }
inline void InitializeExecutionFlow(bool, uint32_t, std::string, std::string,
                                    bool) noexcept {}
inline void ReconcileExecutionFlowWithTracing() noexcept {}
inline void ShutdownExecutionFlow() noexcept {}

inline std::string MakeExecutionFlowServiceInstance(
    std::string_view logical_instance, uint64_t, uint64_t) {
  return std::string{logical_instance};
}

inline std::optional<FlowUnsupportedReason> ExecutionFlowJobUnsupportedReason(
    bool, bool, bool, bool, int32_t) {
  return std::nullopt;
}
#endif

#ifndef CRANE_ENABLE_EXECUTION_FLOW
inline std::optional<FlowContext> FlowContext::Create(
    std::string_view flow_id, std::string_view traceparent) {
  (void)flow_id;
  (void)traceparent;
  return std::nullopt;
}

inline std::optional<FlowContext> FlowContext::FromId(
    const FlowId& flow_id, std::string_view traceparent) {
  (void)flow_id;
  (void)traceparent;
  return std::nullopt;
}
#endif

#ifdef CRANE_ENABLE_EXECUTION_FLOW
#  define CRANE_FLOW_EMITTER_METHOD(name, ...) static void name(__VA_ARGS__);
#else
#  define CRANE_FLOW_EMITTER_METHOD(name, ...) \
    static void name(__VA_ARGS__) noexcept {}
#endif

class FlowEmitter {
 public:
  // Product call sites must invoke semantic point methods through
  // CRANE_FLOW_EMIT. The macro owns both the compile-time and runtime gates,
  // so disabled instrumentation does not evaluate its arguments. The helper
  // methods below are not point emissions and may be called directly.
#ifdef CRANE_ENABLE_EXECUTION_FLOW
  [[nodiscard]] static bool CorrelationRequired(bool already_captured);
  [[nodiscard]] static std::string CorrelationValue(
      const std::optional<FlowId>& flow_id);
  [[nodiscard]] static ExecutionFlowRuntimeConfig ChildRuntimeConfig(
      const std::optional<FlowId>& flow_id,
      uint32_t heartbeat_interval_seconds);
#else
  [[nodiscard]] static bool CorrelationRequired(bool) noexcept { return false; }
  [[nodiscard]] static std::string CorrelationValue(
      const std::optional<FlowId>&) noexcept {
    return {};
  }
  [[nodiscard]] static ExecutionFlowRuntimeConfig ChildRuntimeConfig(
      const std::optional<FlowId>&, uint32_t) noexcept {
    return {};
  }
#endif

  CRANE_FLOW_EMITTER_METHOD(JobAccepted, std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(JobAllocated, std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(JobAllocationPersisted,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(JobAllocRpcSucceeded,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(JobAllocRpcFailed,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(JobAllocCranedDown,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(JobEmbeddedPersisted,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(JobFreeResult, std::optional<FlowContext> context,
                            std::string_view node_id, bool succeeded)
  CRANE_FLOW_EMITTER_METHOD(JobMongoPersisted,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(JobResourcesReleased,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(JobTerminal, std::optional<FlowContext> context,
                            int64_t status)
  CRANE_FLOW_EMITTER_METHOD(JobUnsupportedAtSubmit,
                            std::optional<FlowContext> context,
                            FlowUnsupportedReason reason)
  CRANE_FLOW_EMITTER_METHOD(JobUnsupportedAtPendingCancel,
                            std::optional<FlowContext> context,
                            FlowUnsupportedReason reason)
  CRANE_FLOW_EMITTER_METHOD(JobUnsupportedAtStepSubmit,
                            std::optional<FlowContext> context,
                            FlowUnsupportedReason reason, int64_t step_id)
  CRANE_FLOW_EMITTER_METHOD(JobUnsupportedAtRequeue,
                            std::optional<FlowContext> context,
                            FlowUnsupportedReason reason)

  CRANE_FLOW_EMITTER_METHOD(StatusReceived, std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(StatusApplied, std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(StatusSendResult,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status,
                            bool succeeded)

  CRANE_FLOW_EMITTER_METHOD(DaemonAllConfigured,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(StepAllConfigured,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(DaemonAllCompleting,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(StepAllCompleting,
                            std::optional<FlowContext> context)
  CRANE_FLOW_EMITTER_METHOD(DaemonAllTerminal,
                            std::optional<FlowContext> context, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(StepAllTerminal, std::optional<FlowContext> context,
                            int64_t status)
  CRANE_FLOW_EMITTER_METHOD(StepAllocRpcSucceeded,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepAllocRpcFailed,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepAllocCranedDown,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepExecuteRequested,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepExecuteRpcSucceeded,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepExecuteRpcRejected,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepExecuteRpcFailed,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepExecuteCranedDown,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepFreeRpcSucceeded,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepFreeRpcFailed,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(StepFreeCranedDown,
                            std::optional<FlowContext> context,
                            std::string_view node_id)

  CRANE_FLOW_EMITTER_METHOD(CranedJobInstalled,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedJobRemoved,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedStepInstallAccepted,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedStepInstalled,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedStepExecuteAccepted,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedStepFreeAccepted,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedJobCleanupStarted,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedJobCleanupFinished,
                            std::optional<FlowContext> context,
                            std::string_view node_id, bool succeeded)
  CRANE_FLOW_EMITTER_METHOD(CranedStepCleanupStarted,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedStepCleanupFinished,
                            std::optional<FlowContext> context,
                            std::string_view node_id, bool succeeded)
  CRANE_FLOW_EMITTER_METHOD(CranedSupervisorForked,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedSupervisorReady,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(CranedSupervisorExitObserved,
                            std::optional<FlowContext> context,
                            std::string_view node_id)

  CRANE_FLOW_EMITTER_METHOD(SupervisorStepInitialized,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status,
                            bool succeeded)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepExecuteStarted,
                            std::optional<FlowContext> context,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(SupervisorTaskPrepared,
                            std::optional<FlowContext> context, int64_t task_id,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(SupervisorTaskSpawned,
                            std::optional<FlowContext> context, int64_t task_id,
                            std::string_view node_id)
  CRANE_FLOW_EMITTER_METHOD(SupervisorTaskExitObserved,
                            std::optional<FlowContext> context, int64_t task_id,
                            std::string_view node_id, bool signaled,
                            bool failed)
  CRANE_FLOW_EMITTER_METHOD(SupervisorTaskFinalized,
                            std::optional<FlowContext> context, int64_t task_id,
                            std::string_view node_id, int64_t status,
                            bool succeeded)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepAllTasksFinalized,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepCompletingQueued,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepCompletingPreStartFailure,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepCompletingEarlyCancel,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepShutdownReceived,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepEpilogFinished,
                            std::optional<FlowContext> context,
                            std::string_view node_id, bool configured,
                            bool succeeded)
  CRANE_FLOW_EMITTER_METHOD(SupervisorStepExiting,
                            std::optional<FlowContext> context,
                            std::string_view node_id, int64_t status)
};

#undef CRANE_FLOW_EMITTER_METHOD

#ifdef CRANE_ENABLE_EXECUTION_FLOW
#  define CRANE_EXECUTION_FLOW_INITIALIZE(enabled, interval, service,      \
                                          instance, emit_heartbeat)        \
    ::crane::InitializeExecutionFlow(enabled, interval, service, instance, \
                                     emit_heartbeat)
#  define CRANE_EXECUTION_FLOW_RECONCILE()       \
    ::crane::ReconcileExecutionFlowWithTracing()
#  define CRANE_EXECUTION_FLOW_SHUTDOWN() ::crane::ShutdownExecutionFlow()
#  define CRANE_EXECUTION_FLOW_IF_COMPILED(...) \
    do {                                        \
      __VA_ARGS__                               \
    } while (0)

#  define CRANE_FLOW_EMIT(method, ...)             \
    do {                                           \
      if (::crane::ExecutionFlowEnabled()) {       \
        ::crane::FlowEmitter::method(__VA_ARGS__); \
      }                                            \
    } while (0)

#else

#  define CRANE_EXECUTION_FLOW_INITIALIZE(enabled, interval, service, \
                                          instance, emit_heartbeat)   \
    (void)0
#  define CRANE_EXECUTION_FLOW_RECONCILE() (void)0
#  define CRANE_EXECUTION_FLOW_SHUTDOWN() (void)0
#  define CRANE_EXECUTION_FLOW_IF_COMPILED(...) (void)0
#  define CRANE_FLOW_EMIT(method, ...) (void)0

#endif

}  // namespace crane
