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
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

#include "crane/ExecutionFlowSchema.h"
#include "crane/TracerManager.h"
#include "crane/Tracing.h"

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

struct FlowProducerIdentity {
  std::string producer;
  std::string service_logical_instance;
  std::string service_instance;
};

FlowProducerIdentity SnapshotFlowProducerIdentity() {
  std::lock_guard lock{g_execution_flow_mutex};
  return {
      .producer = g_execution_flow_service,
      .service_logical_instance = g_execution_flow_logical_instance,
      .service_instance = g_execution_flow_instance,
  };
}

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
void SetCommonAttributes(Span* span, std::string_view point,
                         const FlowProducerIdentity& identity) {
  span->SetAttribute(
      FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kFlowSchema).data(),
      std::string{kExecutionFlowSchemaVersion});
  span->SetAttribute(
      FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kPoint).data(),
      std::string{point});
  span->SetAttribute(
      FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kProducer).data(),
      identity.producer);
  span->SetAttribute(
      FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kServiceLogicalInstance)
          .data(),
      identity.service_logical_instance);
  span->SetAttribute(
      FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kServiceInstance).data(),
      identity.service_instance);
  span->SetAttribute(
      FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kEventSequence).data(),
      static_cast<int64_t>(
          g_execution_flow_sequence.fetch_add(1, std::memory_order_relaxed)));
}

void EmitPipelineHeartbeat() {
  ExecutionFlowEmitterLease emitter;
  if (!emitter) return;

  constexpr std::string_view kPoint = kExecutionFlowHeartbeatPoint;
  auto tracer = TracerManager::GetInstance().GetTracerSafe();
  if (!ShouldCreateTraceSpan(kPoint) || !tracer) return;

  const auto system_time = std::chrono::system_clock::now();
  const auto steady_time = std::chrono::steady_clock::now();
  opentelemetry::trace::StartSpanOptions start_options;
  start_options.start_system_time = system_time;
  start_options.start_steady_time = steady_time;
  auto span = tracer->StartSpan(std::string{kPoint}, start_options);
  if (!span) return;
  SetCommonAttributes(span.get(),
                      kPoint.substr(kExecutionFlowWirePrefix.size()),
                      SnapshotFlowProducerIdentity());
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

namespace detail {

class FlowContextBuilder {
 public:
  static void Node(std::optional<FlowContext>& context,
                   std::string_view node_id) {
    if (context) context->Node(node_id);
  }

  static void Status(std::optional<FlowContext>& context, int64_t status) {
    if (context) context->Status(status);
  }

  static void NodeAndStatus(std::optional<FlowContext>& context,
                            std::string_view node_id, int64_t status) {
    if (context) context->Node(node_id).Status(status);
  }
};

class ExecutionFlowPoint {
 public:
  ExecutionFlowPoint(FlowPoint point, const FlowContext& context,
                     std::optional<FlowOperation> operation,
                     std::optional<FlowOutcome> outcome,
                     std::optional<FlowReasonCode> reason, bool failed) {
    FlowAttributeMask present = FlowAttributeBit(FlowAttribute::kAttempt);
    if (context.job_id_) present |= FlowAttributeBit(FlowAttribute::kJobId);
    if (context.step_id_) present |= FlowAttributeBit(FlowAttribute::kStepId);
    if (context.task_id_) present |= FlowAttributeBit(FlowAttribute::kTaskId);
    if (context.node_id_) present |= FlowAttributeBit(FlowAttribute::kNodeId);
    if (context.status_) present |= FlowAttributeBit(FlowAttribute::kStatus);
    if (operation) present |= FlowAttributeBit(FlowAttribute::kOperation);
    if (outcome) present |= FlowAttributeBit(FlowAttribute::kOutcome);
    if (reason) present |= FlowAttributeBit(FlowAttribute::kReasonCode);

    const auto expected_producer = FlowPointProducer(point);
    if (expected_producer.empty() || context.producer_ != expected_producer ||
        context.service_logical_instance_.empty() ||
        context.service_instance_.empty() ||
        !FlowAttributesContain(present, FlowPointRequiredAttributes(point)))
      return;

    emitter_acquired_ = AcquireExecutionFlowEmitter();
    if (!emitter_acquired_) return;

    std::string span_name{kExecutionFlowWirePrefix};
    span_name.append(FlowPointName(point));
    auto tracer = TracerManager::GetInstance().GetTracerSafe();
    if (!ShouldCreateTraceSpan(span_name) || !tracer) return;

    const auto system_time = std::chrono::system_clock::now();
    const auto steady_time = std::chrono::steady_clock::now();
    opentelemetry::trace::StartSpanOptions start_options;
    start_options.start_system_time = system_time;
    start_options.start_steady_time = steady_time;
    auto parent = DeserializeTraceParent(context.traceparent_);
    if (parent.IsValid()) start_options.parent = parent;
    span_ = tracer->StartSpan(span_name, start_options);
    if (!span_) return;

    point_time_ = steady_time;
    SetCommonAttributes(span_.get(), FlowPointName(point),
                        FlowProducerIdentity{
                            context.producer_,
                            context.service_logical_instance_,
                            context.service_instance_,
                        });
    span_->SetAttribute(
        FlowEnvelopeAttributeName(FlowEnvelopeAttribute::kFlowId).data(),
        std::string{context.flow_id_.Value()});
    span_->SetAttribute(FlowAttributeName(FlowAttribute::kAttempt).data(),
                        int64_t{0});
    if (context.job_id_)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kJobId).data(),
                          *context.job_id_);
    if (context.step_id_)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kStepId).data(),
                          *context.step_id_);
    if (context.task_id_)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kTaskId).data(),
                          *context.task_id_);
    if (context.node_id_)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kNodeId).data(),
                          *context.node_id_);
    if (context.status_)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kStatus).data(),
                          *context.status_);
    if (operation)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kOperation).data(),
                          FlowOperationName(*operation));
    if (outcome)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kOutcome).data(),
                          FlowOutcomeName(*outcome));
    if (reason)
      span_->SetAttribute(FlowAttributeName(FlowAttribute::kReasonCode).data(),
                          FlowReasonCodeName(*reason));
    if (failed) span_->SetStatus(StatusCode::kError);
  }

  ExecutionFlowPoint(const ExecutionFlowPoint&) = delete;
  ExecutionFlowPoint& operator=(const ExecutionFlowPoint&) = delete;
  ExecutionFlowPoint(ExecutionFlowPoint&&) = delete;
  ExecutionFlowPoint& operator=(ExecutionFlowPoint&&) = delete;

  ~ExecutionFlowPoint() {
    if (span_) {
      opentelemetry::trace::EndSpanOptions end_options;
      end_options.end_steady_time = point_time_;
      span_->End(end_options);
    }
    if (emitter_acquired_) ReleaseExecutionFlowEmitter();
  }

 private:
  bool emitter_acquired_{false};
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::common::SteadyTimestamp point_time_;
};

}  // namespace detail

std::optional<FlowContext> FlowContext::Create(std::string_view flow_id,
                                               std::string_view traceparent) {
  if (!ExecutionFlowEnabled()) return std::nullopt;
  auto parsed = FlowId::Parse(flow_id);
  if (!parsed) return std::nullopt;
  auto identity = SnapshotFlowProducerIdentity();
  return FlowContext{std::move(*parsed), std::string{traceparent},
                     std::move(identity.producer),
                     std::move(identity.service_logical_instance),
                     std::move(identity.service_instance)};
}

std::optional<FlowContext> FlowContext::FromId(const FlowId& flow_id,
                                               std::string_view traceparent) {
  if (!ExecutionFlowEnabled()) return std::nullopt;
  auto identity = SnapshotFlowProducerIdentity();
  return FlowContext{flow_id, std::string{traceparent},
                     std::move(identity.producer),
                     std::move(identity.service_logical_instance),
                     std::move(identity.service_instance)};
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

std::optional<FlowUnsupportedReason> ExecutionFlowJobUnsupportedReason(
    bool is_batch, bool is_container, bool is_array, bool no_requeue,
    int32_t requeue_count) {
  if (is_container) return FlowUnsupportedReason::kContainerJob;
  if (!is_batch) return FlowUnsupportedReason::kNonBatchJob;
  if (is_array) return FlowUnsupportedReason::kArrayJob;
  if (!no_requeue) return FlowUnsupportedReason::kRequeueEnabled;
  if (requeue_count != 0) return FlowUnsupportedReason::kRequeueAttempt;
  return std::nullopt;
}

bool ExecutionFlowEnabled() {
  return g_execution_flow_configured.load(std::memory_order_acquire) &&
         g_tracing_enabled.load(std::memory_order_acquire);
}

bool FlowEmitter::CorrelationRequired(bool already_captured) {
  return ExecutionFlowEnabled() && !already_captured;
}

std::string FlowEmitter::CorrelationValue(
    const std::optional<FlowId>& flow_id) {
  if (!ExecutionFlowEnabled() || !flow_id) return {};
  return std::string{flow_id->Value()};
}

ExecutionFlowRuntimeConfig FlowEmitter::ChildRuntimeConfig(
    const std::optional<FlowId>& flow_id, uint32_t heartbeat_interval_seconds) {
  if (!ExecutionFlowEnabled() || !flow_id) return {};
  return {.Enabled = true,
          .HeartbeatIntervalSeconds =
              std::max<uint32_t>(1, heartbeat_interval_seconds)};
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

void EmitPoint(FlowPoint point, std::optional<FlowContext> context,
               std::optional<FlowOperation> operation = std::nullopt,
               std::optional<FlowOutcome> outcome = std::nullopt,
               std::optional<FlowReasonCode> reason = std::nullopt,
               bool failed = false) {
  if (!context) return;
  detail::ExecutionFlowPoint event{point,   *context, operation,
                                   outcome, reason,   failed};
}

void FlowEmitter::JobAccepted(std::optional<FlowContext> context) {
  EmitPoint(FlowPoint::kCtldJobAccepted, std::move(context),
            FlowOperation::kSubmit, FlowOutcome::kAccepted);
}

void FlowEmitter::JobAllocated(std::optional<FlowContext> context,
                               std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCtldJobAllocated, std::move(context),
            FlowOperation::kAllocate, FlowOutcome::kSuccess);
}

void FlowEmitter::JobAllocationPersisted(std::optional<FlowContext> context) {
  EmitPoint(FlowPoint::kCtldJobAllocationPersisted, std::move(context),
            FlowOperation::kEmbeddedDbCommit, FlowOutcome::kSuccess);
}

static void EmitJobAllocRpcResult_(std::optional<FlowContext> context,
                                   std::string_view node_id,
                                   FlowOutcome outcome) {
  detail::FlowContextBuilder::Node(context, node_id);
  EmitPoint(FlowPoint::kCtldJobAllocRpcResult, std::move(context),
            FlowOperation::kAllocJobs, outcome, std::nullopt,
            outcome != FlowOutcome::kSuccess);
}

void FlowEmitter::JobAllocRpcSucceeded(std::optional<FlowContext> context,
                                       std::string_view node_id) {
  EmitJobAllocRpcResult_(std::move(context), node_id, FlowOutcome::kSuccess);
}

void FlowEmitter::JobAllocRpcFailed(std::optional<FlowContext> context,
                                    std::string_view node_id) {
  EmitJobAllocRpcResult_(std::move(context), node_id, FlowOutcome::kRpcFailure);
}

void FlowEmitter::JobAllocCranedDown(std::optional<FlowContext> context,
                                     std::string_view node_id) {
  EmitJobAllocRpcResult_(std::move(context), node_id, FlowOutcome::kCranedDown);
}

void FlowEmitter::JobEmbeddedPersisted(std::optional<FlowContext> context) {
  EmitPoint(FlowPoint::kCtldJobEmbeddedPersisted, std::move(context),
            FlowOperation::kEmbeddedDbFinalize, FlowOutcome::kSuccess);
}

void FlowEmitter::JobFreeResult(std::optional<FlowContext> context,
                                std::string_view node_id, bool succeeded) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCtldJobFreeResult, std::move(context),
            FlowOperation::kFreeJobs,
            succeeded ? FlowOutcome::kSuccess : FlowOutcome::kFailure,
            std::nullopt, !succeeded);
}

void FlowEmitter::JobMongoPersisted(std::optional<FlowContext> context) {
  EmitPoint(FlowPoint::kCtldJobMongoPersisted, std::move(context),
            FlowOperation::kMongodbFinalize, FlowOutcome::kSuccess);
}

void FlowEmitter::JobResourcesReleased(std::optional<FlowContext> context) {
  EmitPoint(FlowPoint::kCtldJobResourcesReleased, std::move(context),
            FlowOperation::kReleaseResources, FlowOutcome::kCompleted);
}

void FlowEmitter::JobTerminal(std::optional<FlowContext> context,
                              int64_t status) {
  if (context) context->Status(status);
  EmitPoint(FlowPoint::kCtldJobTerminal, std::move(context),
            FlowOperation::kJobTerminal, FlowOutcome::kPersisted);
}

static FlowReasonCode WireReasonFor(FlowUnsupportedReason reason) {
  switch (reason) {
  case FlowUnsupportedReason::kArrayJob:
    return FlowReasonCode::kArrayJob;
  case FlowUnsupportedReason::kCancelledBeforeAllocation:
    return FlowReasonCode::kCancelledBeforeAllocation;
  case FlowUnsupportedReason::kContainerJob:
    return FlowReasonCode::kContainerJob;
  case FlowUnsupportedReason::kExtraCommonStep:
    return FlowReasonCode::kExtraCommonStep;
  case FlowUnsupportedReason::kNonBatchJob:
    return FlowReasonCode::kNonBatchJob;
  case FlowUnsupportedReason::kRequeueAttempt:
    return FlowReasonCode::kRequeueAttempt;
  case FlowUnsupportedReason::kRequeueEnabled:
    return FlowReasonCode::kRequeueEnabled;
  }
  return FlowReasonCode::kUnknownPoint;
}

static void EmitJobUnsupported(std::optional<FlowContext> context,
                               FlowOperation operation,
                               FlowUnsupportedReason reason,
                               std::optional<int64_t> step_id = std::nullopt) {
  if (context && step_id) context->Step(*step_id);
  EmitPoint(FlowPoint::kCtldJobUnsupported, std::move(context), operation,
            FlowOutcome::kUnsupported, WireReasonFor(reason));
}

void FlowEmitter::JobUnsupportedAtSubmit(std::optional<FlowContext> context,
                                         FlowUnsupportedReason reason) {
  EmitJobUnsupported(std::move(context), FlowOperation::kSubmit, reason);
}

void FlowEmitter::JobUnsupportedAtPendingCancel(
    std::optional<FlowContext> context, FlowUnsupportedReason reason) {
  EmitJobUnsupported(std::move(context), FlowOperation::kCancelPendingJob,
                     reason);
}

void FlowEmitter::JobUnsupportedAtStepSubmit(std::optional<FlowContext> context,
                                             FlowUnsupportedReason reason,
                                             int64_t step_id) {
  EmitJobUnsupported(std::move(context), FlowOperation::kSubmitCommonStep,
                     reason, step_id);
}

void FlowEmitter::JobUnsupportedAtRequeue(std::optional<FlowContext> context,
                                          FlowUnsupportedReason reason) {
  EmitJobUnsupported(std::move(context), FlowOperation::kRequeue, reason);
}

void FlowEmitter::StatusReceived(std::optional<FlowContext> context,
                                 std::string_view node_id, int64_t status) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kCtldStatusReceived, std::move(context),
            FlowOperation::kStatusChange);
}

void FlowEmitter::StatusApplied(std::optional<FlowContext> context,
                                std::string_view node_id, int64_t status) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kCtldStatusApplied, std::move(context),
            FlowOperation::kStateMachine, FlowOutcome::kApplied);
}

void FlowEmitter::StatusSendResult(std::optional<FlowContext> context,
                                   std::string_view node_id, int64_t status,
                                   bool succeeded) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kCranedStatusSendResult, std::move(context),
            FlowOperation::kSendStatusChange,
            succeeded ? FlowOutcome::kSuccess : FlowOutcome::kFailure,
            std::nullopt, !succeeded);
}

void EmitStepAllConfigured(std::optional<FlowContext> context,
                           FlowOperation operation) {
  EmitPoint(FlowPoint::kCtldStepAllConfigured, std::move(context), operation,
            FlowOutcome::kSuccess);
}

void FlowEmitter::DaemonAllConfigured(std::optional<FlowContext> context) {
  EmitStepAllConfigured(std::move(context), FlowOperation::kDaemonConfigure);
}

void FlowEmitter::StepAllConfigured(std::optional<FlowContext> context) {
  EmitStepAllConfigured(std::move(context), FlowOperation::kStepConfigure);
}

void EmitStepAllCompleting(std::optional<FlowContext> context,
                           FlowOperation operation) {
  EmitPoint(FlowPoint::kCtldStepAllCompleting, std::move(context), operation,
            FlowOutcome::kRequested);
}

void FlowEmitter::DaemonAllCompleting(std::optional<FlowContext> context) {
  EmitStepAllCompleting(std::move(context), FlowOperation::kDaemonCleanup);
}

void FlowEmitter::StepAllCompleting(std::optional<FlowContext> context) {
  EmitStepAllCompleting(std::move(context), FlowOperation::kStepCleanup);
}

static void EmitStepAllTerminal_(std::optional<FlowContext> context,
                                 int64_t status, FlowOperation operation) {
  detail::FlowContextBuilder::Status(context, status);
  EmitPoint(FlowPoint::kCtldStepAllTerminal, std::move(context), operation);
}

void FlowEmitter::DaemonAllTerminal(std::optional<FlowContext> context,
                                    int64_t status) {
  EmitStepAllTerminal_(std::move(context), status,
                       FlowOperation::kDaemonTerminal);
}

void FlowEmitter::StepAllTerminal(std::optional<FlowContext> context,
                                  int64_t status) {
  EmitStepAllTerminal_(std::move(context), status,
                       FlowOperation::kStepTerminal);
}

static void EmitStepAllocRpcResult_(std::optional<FlowContext> context,
                                    std::string_view node_id,
                                    FlowOutcome outcome) {
  detail::FlowContextBuilder::Node(context, node_id);
  EmitPoint(FlowPoint::kCtldStepAllocRpcResult, std::move(context),
            FlowOperation::kAllocSteps, outcome, std::nullopt,
            outcome != FlowOutcome::kSuccess);
}

void FlowEmitter::StepAllocRpcSucceeded(std::optional<FlowContext> context,
                                        std::string_view node_id) {
  EmitStepAllocRpcResult_(std::move(context), node_id, FlowOutcome::kSuccess);
}

void FlowEmitter::StepAllocRpcFailed(std::optional<FlowContext> context,
                                     std::string_view node_id) {
  EmitStepAllocRpcResult_(std::move(context), node_id,
                          FlowOutcome::kRpcFailure);
}

void FlowEmitter::StepAllocCranedDown(std::optional<FlowContext> context,
                                      std::string_view node_id) {
  EmitStepAllocRpcResult_(std::move(context), node_id,
                          FlowOutcome::kCranedDown);
}

void FlowEmitter::StepExecuteRequested(std::optional<FlowContext> context,
                                       std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCtldStepExecuteRequested, std::move(context),
            FlowOperation::kExecuteSteps, FlowOutcome::kRequested);
}

static void EmitStepExecuteRpcResult_(std::optional<FlowContext> context,
                                      std::string_view node_id,
                                      FlowOutcome outcome) {
  detail::FlowContextBuilder::Node(context, node_id);
  EmitPoint(FlowPoint::kCtldStepExecuteRpcResult, std::move(context),
            FlowOperation::kExecuteSteps, outcome, std::nullopt,
            outcome != FlowOutcome::kSuccess);
}

void FlowEmitter::StepExecuteRpcSucceeded(std::optional<FlowContext> context,
                                          std::string_view node_id) {
  EmitStepExecuteRpcResult_(std::move(context), node_id, FlowOutcome::kSuccess);
}

void FlowEmitter::StepExecuteRpcRejected(std::optional<FlowContext> context,
                                         std::string_view node_id) {
  EmitStepExecuteRpcResult_(std::move(context), node_id,
                            FlowOutcome::kRejected);
}

void FlowEmitter::StepExecuteRpcFailed(std::optional<FlowContext> context,
                                       std::string_view node_id) {
  EmitStepExecuteRpcResult_(std::move(context), node_id,
                            FlowOutcome::kRpcFailure);
}

void FlowEmitter::StepExecuteCranedDown(std::optional<FlowContext> context,
                                        std::string_view node_id) {
  EmitStepExecuteRpcResult_(std::move(context), node_id,
                            FlowOutcome::kCranedDown);
}

static void EmitStepFreeRpcResult_(std::optional<FlowContext> context,
                                   std::string_view node_id,
                                   FlowOutcome outcome) {
  detail::FlowContextBuilder::Node(context, node_id);
  EmitPoint(FlowPoint::kCtldStepFreeRpcResult, std::move(context),
            FlowOperation::kFreeSteps, outcome, std::nullopt,
            outcome != FlowOutcome::kSuccess);
}

void FlowEmitter::StepFreeRpcSucceeded(std::optional<FlowContext> context,
                                       std::string_view node_id) {
  EmitStepFreeRpcResult_(std::move(context), node_id, FlowOutcome::kSuccess);
}

void FlowEmitter::StepFreeRpcFailed(std::optional<FlowContext> context,
                                    std::string_view node_id) {
  EmitStepFreeRpcResult_(std::move(context), node_id, FlowOutcome::kRpcFailure);
}

void FlowEmitter::StepFreeCranedDown(std::optional<FlowContext> context,
                                     std::string_view node_id) {
  EmitStepFreeRpcResult_(std::move(context), node_id, FlowOutcome::kCranedDown);
}

void FlowEmitter::CranedJobInstalled(std::optional<FlowContext> context,
                                     std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedJobInstalled, std::move(context),
            FlowOperation::kInstallJob, FlowOutcome::kSuccess);
}

void FlowEmitter::CranedJobRemoved(std::optional<FlowContext> context,
                                   std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedJobRemoved, std::move(context),
            FlowOperation::kRemoveJob, FlowOutcome::kSuccess);
}

void FlowEmitter::CranedStepInstallAccepted(std::optional<FlowContext> context,
                                            std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedStepInstallAccepted, std::move(context),
            FlowOperation::kInstallStep, FlowOutcome::kAccepted);
}

void FlowEmitter::CranedStepInstalled(std::optional<FlowContext> context,
                                      std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedStepInstalled, std::move(context),
            FlowOperation::kInstallStep, FlowOutcome::kSuccess);
}

void FlowEmitter::CranedStepExecuteAccepted(std::optional<FlowContext> context,
                                            std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedStepExecuteAccepted, std::move(context),
            FlowOperation::kExecuteStep, FlowOutcome::kAccepted);
}

void FlowEmitter::CranedStepFreeAccepted(std::optional<FlowContext> context,
                                         std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedStepFreeAccepted, std::move(context),
            FlowOperation::kFreeStep, FlowOutcome::kAccepted);
}

static void EmitCranedCleanupStarted_(FlowPoint point,
                                      std::optional<FlowContext> context,
                                      std::string_view node_id,
                                      FlowOperation operation) {
  detail::FlowContextBuilder::Node(context, node_id);
  EmitPoint(point, std::move(context), operation);
}

static void EmitCranedCleanupFinished_(FlowPoint point,
                                       std::optional<FlowContext> context,
                                       std::string_view node_id,
                                       FlowOperation operation,
                                       bool succeeded) {
  detail::FlowContextBuilder::Node(context, node_id);
  EmitPoint(point, std::move(context), operation,
            succeeded ? FlowOutcome::kSuccess : FlowOutcome::kFailure,
            std::nullopt, !succeeded);
}

void FlowEmitter::CranedJobCleanupStarted(std::optional<FlowContext> context,
                                          std::string_view node_id) {
  EmitCranedCleanupStarted_(FlowPoint::kCranedStepCleanupStarted,
                            std::move(context), node_id,
                            FlowOperation::kJobCleanup);
}

void FlowEmitter::CranedJobCleanupFinished(std::optional<FlowContext> context,
                                           std::string_view node_id,
                                           bool succeeded) {
  EmitCranedCleanupFinished_(FlowPoint::kCranedStepCleanupFinished,
                             std::move(context), node_id,
                             FlowOperation::kJobCleanup, succeeded);
}

void FlowEmitter::CranedStepCleanupStarted(std::optional<FlowContext> context,
                                           std::string_view node_id) {
  EmitCranedCleanupStarted_(FlowPoint::kCranedStepCleanupStarted,
                            std::move(context), node_id,
                            FlowOperation::kStepCleanup);
}

void FlowEmitter::CranedStepCleanupFinished(std::optional<FlowContext> context,
                                            std::string_view node_id,
                                            bool succeeded) {
  EmitCranedCleanupFinished_(FlowPoint::kCranedStepCleanupFinished,
                             std::move(context), node_id,
                             FlowOperation::kStepCleanup, succeeded);
}

void FlowEmitter::CranedSupervisorForked(std::optional<FlowContext> context,
                                         std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedSupervisorForked, std::move(context),
            FlowOperation::kForkSupervisor, FlowOutcome::kSuccess);
}

void FlowEmitter::CranedSupervisorReady(std::optional<FlowContext> context,
                                        std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedSupervisorReady, std::move(context),
            FlowOperation::kInitializeSupervisor, FlowOutcome::kSuccess);
}

void FlowEmitter::CranedSupervisorExitObserved(
    std::optional<FlowContext> context, std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kCranedSupervisorExitObserved, std::move(context),
            FlowOperation::kObserveSupervisorExit, FlowOutcome::kExited);
}

void FlowEmitter::SupervisorStepInitialized(std::optional<FlowContext> context,
                                            std::string_view node_id,
                                            int64_t status, bool succeeded) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kSupervisorStepInitialized, std::move(context),
            FlowOperation::kInitialize,
            succeeded ? FlowOutcome::kSuccess : FlowOutcome::kFailure,
            std::nullopt, !succeeded);
}

void FlowEmitter::SupervisorStepExecuteStarted(
    std::optional<FlowContext> context, std::string_view node_id) {
  if (context) context->Node(node_id);
  EmitPoint(FlowPoint::kSupervisorStepExecuteStarted, std::move(context),
            FlowOperation::kExecuteStep, FlowOutcome::kStarted);
}

void FlowEmitter::SupervisorTaskPrepared(std::optional<FlowContext> context,
                                         int64_t task_id,
                                         std::string_view node_id) {
  if (context) context->Task(task_id).Node(node_id);
  EmitPoint(FlowPoint::kSupervisorTaskPrepared, std::move(context),
            FlowOperation::kPrepareTask, FlowOutcome::kSuccess);
}

void FlowEmitter::SupervisorTaskSpawned(std::optional<FlowContext> context,
                                        int64_t task_id,
                                        std::string_view node_id) {
  if (context) context->Task(task_id).Node(node_id);
  EmitPoint(FlowPoint::kSupervisorTaskSpawned, std::move(context),
            FlowOperation::kSpawnTask, FlowOutcome::kSuccess);
}

void FlowEmitter::SupervisorTaskExitObserved(std::optional<FlowContext> context,
                                             int64_t task_id,
                                             std::string_view node_id,
                                             bool signaled, bool failed) {
  if (context) context->Task(task_id).Node(node_id);
  EmitPoint(FlowPoint::kSupervisorTaskExitObserved, std::move(context),
            FlowOperation::kObserveTaskExit,
            signaled ? FlowOutcome::kSignal : FlowOutcome::kExit, std::nullopt,
            failed);
}

void FlowEmitter::SupervisorTaskFinalized(std::optional<FlowContext> context,
                                          int64_t task_id,
                                          std::string_view node_id,
                                          int64_t status, bool succeeded) {
  if (context) context->Task(task_id).Node(node_id).Status(status);
  EmitPoint(FlowPoint::kSupervisorTaskFinalized, std::move(context),
            FlowOperation::kFinalizeTask,
            succeeded ? FlowOutcome::kSuccess : FlowOutcome::kCleanupFailure,
            std::nullopt, !succeeded);
}

void FlowEmitter::SupervisorStepAllTasksFinalized(
    std::optional<FlowContext> context, std::string_view node_id,
    int64_t status) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kSupervisorStepAllTasksFinalized, std::move(context),
            FlowOperation::kFinalizeStep);
}

static void EmitSupervisorStepCompleting_(std::optional<FlowContext> context,
                                          std::string_view node_id,
                                          int64_t status, FlowOutcome outcome) {
  detail::FlowContextBuilder::NodeAndStatus(context, node_id, status);
  EmitPoint(FlowPoint::kSupervisorStepCompletingEnqueued, std::move(context),
            FlowOperation::kEnqueueStatus, outcome, std::nullopt,
            outcome == FlowOutcome::kPreStartFailure);
}

void FlowEmitter::SupervisorStepCompletingQueued(
    std::optional<FlowContext> context, std::string_view node_id,
    int64_t status) {
  EmitSupervisorStepCompleting_(std::move(context), node_id, status,
                                FlowOutcome::kQueued);
}

void FlowEmitter::SupervisorStepCompletingPreStartFailure(
    std::optional<FlowContext> context, std::string_view node_id,
    int64_t status) {
  EmitSupervisorStepCompleting_(std::move(context), node_id, status,
                                FlowOutcome::kPreStartFailure);
}

void FlowEmitter::SupervisorStepCompletingEarlyCancel(
    std::optional<FlowContext> context, std::string_view node_id,
    int64_t status) {
  EmitSupervisorStepCompleting_(std::move(context), node_id, status,
                                FlowOutcome::kEarlyCancel);
}

void FlowEmitter::SupervisorStepShutdownReceived(
    std::optional<FlowContext> context, std::string_view node_id,
    int64_t status) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kSupervisorStepShutdownReceived, std::move(context),
            FlowOperation::kShutdownSupervisor);
}

void FlowEmitter::SupervisorStepEpilogFinished(
    std::optional<FlowContext> context, std::string_view node_id,
    bool configured, bool succeeded) {
  if (context) context->Node(node_id);
  const auto outcome = !configured ? FlowOutcome::kSkipped
                       : succeeded ? FlowOutcome::kSuccess
                                   : FlowOutcome::kFailure;
  EmitPoint(FlowPoint::kSupervisorStepEpilogFinished, std::move(context),
            FlowOperation::kSupervisorEpilog, outcome, std::nullopt,
            outcome == FlowOutcome::kFailure);
}

void FlowEmitter::SupervisorStepExiting(std::optional<FlowContext> context,
                                        std::string_view node_id,
                                        int64_t status) {
  if (context) context->Node(node_id).Status(status);
  EmitPoint(FlowPoint::kSupervisorStepExiting, std::move(context),
            FlowOperation::kExitSupervisor);
}

}  // namespace crane
