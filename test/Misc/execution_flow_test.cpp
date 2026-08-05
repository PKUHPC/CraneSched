#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include "TraceSpanExport.h"
#include "crane/ExecutionFlow.h"
#include "crane/ExecutionFlowSchema.h"
#include "crane/TracerManager.h"
#include "crane/Tracing.h"
#include "gtest/gtest.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"

template <typename Context>
concept HasPublicFlowNodeMutation =
    requires(Context& context) { context.Node("node0"); };

template <typename Context>
concept HasPublicFlowStatusMutation =
    requires(Context& context) { context.Status(0); };

static_assert(!HasPublicFlowNodeMutation<crane::FlowContext>);
static_assert(!HasPublicFlowStatusMutation<crane::FlowContext>);

namespace {

struct FilteringExporterState {
  std::vector<std::optional<crane::grpc::plugin::SpanInfo>> results;
};

class FilteringExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit FilteringExporter(std::shared_ptr<FilteringExporterState> state)
      : state_(std::move(state)) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override {
    return std::make_unique<opentelemetry::sdk::trace::SpanData>();
  }

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override {
    for (const auto& recordable : spans) {
      const auto& span =
          static_cast<const opentelemetry::sdk::trace::SpanData&>(*recordable);
      state_->results.emplace_back(
          crane::otel::detail::PrepareSpanForExport(span));
    }
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool Shutdown(std::chrono::microseconds) noexcept override { return true; }
  bool ForceFlush(std::chrono::microseconds) noexcept override { return true; }

 private:
  std::shared_ptr<FilteringExporterState> state_;
};

}  // namespace

#ifdef CRANE_ENABLE_EXECUTION_FLOW
namespace {

struct RecordingExporterState {
  struct Point {
    std::string name;
    std::string producer;
    std::string operation;
    std::string outcome;
    crane::FlowAttributeMask attributes{0};
  };

  std::mutex mutex;
  std::vector<std::string> names;
  std::vector<std::string> producers;
  std::vector<std::string> operations;
  std::vector<int64_t> step_ids;
  std::vector<Point> points;
};

class RecordingExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit RecordingExporter(std::shared_ptr<RecordingExporterState> state)
      : state_(std::move(state)) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override {
    return std::make_unique<opentelemetry::sdk::trace::SpanData>();
  }

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override {
    std::lock_guard lock{state_->mutex};
    for (const auto& recordable : spans) {
      const auto& span =
          static_cast<const opentelemetry::sdk::trace::SpanData&>(*recordable);
      state_->names.emplace_back(span.GetName());
      const auto& attributes = span.GetAttributes();
      auto record_string = [this, &attributes](
                               std::string_view key,
                               std::vector<std::string>* destination) {
        const auto it = attributes.find(std::string{key});
        if (it == attributes.end()) return;
        std::visit(
            [destination](const auto& value) {
              using Value = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<Value, std::string>)
                destination->emplace_back(value);
            },
            it->second);
      };
      record_string("producer", &state_->producers);
      record_string("operation", &state_->operations);

      RecordingExporterState::Point point{.name = std::string{span.GetName()}};
      auto point_string = [&attributes](std::string_view key) {
        std::string result;
        const auto it = attributes.find(std::string{key});
        if (it == attributes.end()) return result;
        std::visit(
            [&result](const auto& value) {
              using Value = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<Value, std::string>) result = value;
            },
            it->second);
        return result;
      };
      point.producer = point_string("producer");
      point.operation = point_string("operation");
      point.outcome = point_string("outcome");
      for (const auto [key, attribute] : {
               std::pair{"attempt", crane::FlowAttribute::kAttempt},
               std::pair{"job_id", crane::FlowAttribute::kJobId},
               std::pair{"node_id", crane::FlowAttribute::kNodeId},
               std::pair{"operation", crane::FlowAttribute::kOperation},
               std::pair{"outcome", crane::FlowAttribute::kOutcome},
               std::pair{"reason_code", crane::FlowAttribute::kReasonCode},
               std::pair{"status", crane::FlowAttribute::kStatus},
               std::pair{"step_id", crane::FlowAttribute::kStepId},
               std::pair{"task_id", crane::FlowAttribute::kTaskId},
           }) {
        if (attributes.find(key) != attributes.end())
          point.attributes |= crane::FlowAttributeBit(attribute);
      }
      state_->points.emplace_back(std::move(point));

      const auto step_it = attributes.find("step_id");
      if (step_it != attributes.end()) {
        std::visit(
            [this](const auto& value) {
              using Value = std::decay_t<decltype(value)>;
              if constexpr (std::is_same_v<Value, int64_t>)
                state_->step_ids.emplace_back(value);
            },
            step_it->second);
      }
    }
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool Shutdown(std::chrono::microseconds) noexcept override { return true; }
  bool ForceFlush(std::chrono::microseconds) noexcept override { return true; }

 private:
  std::shared_ptr<RecordingExporterState> state_;
};

void ExpectSemanticPoint(const std::shared_ptr<RecordingExporterState>& state,
                         std::size_t before, crane::FlowPoint expected_point,
                         crane::FlowOperation expected_operation,
                         std::optional<crane::FlowOutcome> expected_outcome) {
  std::lock_guard lock{state->mutex};
  ASSERT_EQ(state->points.size(), before + 1)
      << crane::FlowPointName(expected_point);
  const auto& point = state->points.back();
  EXPECT_EQ(point.name, std::string{crane::kExecutionFlowWirePrefix} +
                            std::string{crane::FlowPointName(expected_point)});
  EXPECT_EQ(point.producer, crane::FlowPointProducer(expected_point));
  EXPECT_EQ(point.operation, crane::FlowOperationName(expected_operation));
  EXPECT_EQ(point.outcome, expected_outcome
                               ? crane::FlowOutcomeName(*expected_outcome)
                               : std::string_view{});
  EXPECT_TRUE(crane::FlowAttributesContain(
      point.attributes, crane::FlowPointRequiredAttributes(expected_point)));
}

struct BlockingExporterState {
  std::mutex mutex;
  std::condition_variable cv;
  bool export_started{false};
  bool release_export{false};
};

class BlockingExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit BlockingExporter(std::shared_ptr<BlockingExporterState> state)
      : state_(std::move(state)) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override {
    return std::make_unique<opentelemetry::sdk::trace::SpanData>();
  }

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&) noexcept
      override {
    std::unique_lock lock{state_->mutex};
    state_->export_started = true;
    state_->cv.notify_all();
    state_->cv.wait(lock, [this] { return state_->release_export; });
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool Shutdown(std::chrono::microseconds) noexcept override { return true; }
  bool ForceFlush(std::chrono::microseconds) noexcept override { return true; }

 private:
  std::shared_ptr<BlockingExporterState> state_;
};

}  // namespace
#endif

TEST(ExecutionFlowTest, FlowEmitterUsesExplicitCoreTraceClass) {
  EXPECT_EQ(crane::ClassifyTraceSpanName("flow/v1/ctld/job/accepted"),
            crane::TraceSpanClass::Other);
  EXPECT_TRUE(crane::TraceLevelAllowsSpan(crane::TraceLevel::Basic,
                                          crane::TraceSpanClass::Core, false));
}

#ifdef CRANE_ENABLE_EXECUTION_FLOW
TEST(ExecutionFlowTest, RuntimeDisabledEmitterDoesNotEvaluateArguments) {
  crane::ShutdownExecutionFlow();
  crane::ApplyRuntimeTraceConfig(false, crane::TraceLevel::Basic);
  int evaluations = 0;
  CRANE_FLOW_EMIT(JobFreeResult, (++evaluations, std::nullopt),
                  (++evaluations, std::string_view{"node0"}),
                  (++evaluations, true));
  EXPECT_EQ(evaluations, 0);
}
#else
TEST(ExecutionFlowTest, CompiledOutEmitterDoesNotEvaluateArguments) {
  int evaluations = 0;
  CRANE_FLOW_EMIT(JobFreeResult, (++evaluations, std::nullopt),
                  (++evaluations, std::string_view{"node0"}),
                  (++evaluations, true));
  EXPECT_EQ(evaluations, 0);
}

TEST(ExecutionFlowTest, CompiledOutContextFactoriesExposeNoCorrelation) {
  constexpr std::string_view kFlowId = "0123456789abcdef0123456789abcdef";
  EXPECT_FALSE(crane::FlowContext::Create(kFlowId, "traceparent").has_value());

  auto parsed = crane::FlowId::Parse(kFlowId);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_FALSE(crane::FlowContext::FromId(*parsed, "traceparent").has_value());
}
#endif

#ifdef CRANE_ENABLE_EXECUTION_FLOW
class ExecutionFlowRuntimeTest : public testing::Test {
 protected:
  void SetUp() override {
    crane::ShutdownExecutionFlow();
    crane::ApplyRuntimeTraceConfig(false, crane::TraceLevel::Basic);
  }

  void TearDown() override {
    crane::ShutdownExecutionFlow();
    crane::ApplyRuntimeTraceConfig(false, crane::TraceLevel::Basic);
  }
};

TEST_F(ExecutionFlowRuntimeTest,
       SemanticEmitterMarksCoreSpansAndExporterStripsMarker) {
  auto state = std::make_shared<FilteringExporterState>();
  auto& tracer_manager = crane::TracerManager::GetInstance();
  tracer_manager.Shutdown();
  ASSERT_TRUE(tracer_manager.Initialize(
      "ExecutionFlowExporterTest", std::make_unique<FilteringExporter>(state)));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 3600, "cranectld", "ctld", false);

  auto context = crane::FlowContext::Create("0123456789abcdef0123456789abcdef",
                                            std::string_view{});
  ASSERT_TRUE(context.has_value());
  context->Job(42);
  CRANE_FLOW_EMIT(JobAccepted, context);

  auto tracer = tracer_manager.GetTracerSafe();
  ASSERT_TRUE(tracer);
  auto unmarked = tracer->StartSpan("flow/v1/ctld/job/accepted");
  unmarked->SetAttribute("flow_id",
                         std::string{"fedcba9876543210fedcba9876543210"});
  unmarked->End();
  tracer.reset();
  crane::ShutdownExecutionFlow();
  tracer_manager.Shutdown();

  ASSERT_EQ(state->results.size(), 2);
  ASSERT_TRUE(state->results[0].has_value());
  EXPECT_EQ(state->results[0]->name(), "flow/v1/ctld/job/accepted");
  const auto& attributes = state->results[0]->attributes();
  EXPECT_NE(attributes.find("flow_id"), attributes.end());
  EXPECT_EQ(
      attributes.find(std::string{crane::otel::detail::kSpanClassAttribute}),
      attributes.end());
  EXPECT_FALSE(state->results[1].has_value());
}

TEST_F(ExecutionFlowRuntimeTest,
       ConfiguredFlowFollowsTracingTransitionsIdempotently) {
  crane::InitializeExecutionFlow(true, 1, "craned", "node0", true);
  EXPECT_FALSE(crane::ExecutionFlowEnabled());
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());

  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_FALSE(crane::ExecutionFlowEnabled());
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());

  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_TRUE(crane::ExecutionFlowEnabled());
  EXPECT_TRUE(crane::ExecutionFlowHeartbeatRunning());

  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_TRUE(crane::ExecutionFlowEnabled());
  EXPECT_TRUE(crane::ExecutionFlowHeartbeatRunning());

  crane::ApplyRuntimeTraceConfig(false, crane::TraceLevel::Debug);
  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_FALSE(crane::ExecutionFlowEnabled());
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());

  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());

  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Detailed);
  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_TRUE(crane::ExecutionFlowEnabled());
  EXPECT_TRUE(crane::ExecutionFlowHeartbeatRunning());

  crane::ShutdownExecutionFlow();
  crane::ShutdownExecutionFlow();
  EXPECT_FALSE(crane::ExecutionFlowEnabled());
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());
}

TEST_F(ExecutionFlowRuntimeTest,
       DisabledFlowNeverStartsHeartbeatWhenTracingChanges) {
  crane::InitializeExecutionFlow(false, 1, "craned", "node0", true);
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::ReconcileExecutionFlowWithTracing();

  EXPECT_FALSE(crane::ExecutionFlowEnabled());
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());
}

TEST_F(ExecutionFlowRuntimeTest,
       EnabledTracingStartsHeartbeatAtInitialization) {
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 1, "craned", "node0", true);

  EXPECT_TRUE(crane::ExecutionFlowEnabled());
  EXPECT_TRUE(crane::ExecutionFlowHeartbeatRunning());
}

TEST_F(ExecutionFlowRuntimeTest,
       PointOnlyProcessDoesNotStartPipelineHeartbeat) {
  crane::g_tracing_enabled.store(true, std::memory_order_release);
  crane::InitializeExecutionFlow(true, 1, "supervisor", "node0:1:0", false);

  EXPECT_TRUE(crane::ExecutionFlowEnabled());
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());

  crane::g_tracing_enabled.store(false, std::memory_order_release);
  crane::ReconcileExecutionFlowWithTracing();
  crane::g_tracing_enabled.store(true, std::memory_order_release);
  crane::ReconcileExecutionFlowWithTracing();
  EXPECT_FALSE(crane::ExecutionFlowHeartbeatRunning());
}

TEST_F(ExecutionFlowRuntimeTest, ShutdownWaitsForInFlightPoint) {
  auto exporter_state = std::make_shared<BlockingExporterState>();
  auto& tracer_manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(tracer_manager.Initialize(
      "ExecutionFlowRuntimeTest",
      std::make_unique<BlockingExporter>(exporter_state)));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 1, "cranectld", "ctld", false);

  auto context = crane::FlowContext::Create("0123456789abcdef0123456789abcdef",
                                            std::string_view{});
  ASSERT_TRUE(context.has_value());
  context->Job(1);
  std::thread emitter_thread(
      [context] { crane::FlowEmitter::JobAccepted(context); });
  {
    std::unique_lock lock{exporter_state->mutex};
    const bool export_started = exporter_state->cv.wait_for(
        lock, std::chrono::seconds{5},
        [&] { return exporter_state->export_started; });
    if (!export_started) {
      exporter_state->release_export = true;
      lock.unlock();
      exporter_state->cv.notify_all();
      emitter_thread.join();
      tracer_manager.Shutdown();
      FAIL() << "flow point was not exported within the deadline";
    }
  }

  std::atomic<bool> shutdown_returned{false};
  std::thread shutdown_thread([&] {
    crane::ShutdownExecutionFlow();
    shutdown_returned.store(true, std::memory_order_release);
  });
  while (crane::ExecutionFlowEnabled()) std::this_thread::yield();
  EXPECT_FALSE(shutdown_returned.load(std::memory_order_acquire));

  {
    std::lock_guard lock{exporter_state->mutex};
    exporter_state->release_export = true;
  }
  exporter_state->cv.notify_all();
  emitter_thread.join();
  shutdown_thread.join();
  EXPECT_TRUE(shutdown_returned.load(std::memory_order_acquire));
  tracer_manager.Shutdown();
}

TEST_F(ExecutionFlowRuntimeTest,
       EmitterRejectsMissingAttributesAndWrongProducer) {
  auto exporter_state = std::make_shared<RecordingExporterState>();
  auto& tracer_manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(tracer_manager.Initialize(
      "ExecutionFlowRuntimeTest",
      std::make_unique<RecordingExporter>(exporter_state)));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 1, "cranectld", "ctld", false);

  auto missing_job = crane::FlowContext::Create(
      "0123456789abcdef0123456789abcdef", std::string_view{});
  ASSERT_TRUE(missing_job.has_value());
  crane::FlowEmitter::JobAccepted(missing_job);

  crane::InitializeExecutionFlow(true, 1, "craned", "node0", false);
  auto wrong_producer = crane::FlowContext::Create(
      "1123456789abcdef0123456789abcdef", std::string_view{});
  ASSERT_TRUE(wrong_producer.has_value());
  wrong_producer->Job(2);
  crane::FlowEmitter::JobAccepted(wrong_producer);

  std::vector<std::string> producers;
  {
    std::lock_guard lock{exporter_state->mutex};
    producers = exporter_state->producers;
  }
  tracer_manager.Shutdown();
  EXPECT_TRUE(producers.empty());
}

TEST_F(ExecutionFlowRuntimeTest, ContextSnapshotsProducerIdentity) {
  auto exporter_state = std::make_shared<RecordingExporterState>();
  auto& tracer_manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(tracer_manager.Initialize(
      "ExecutionFlowRuntimeTest",
      std::make_unique<RecordingExporter>(exporter_state)));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 1, "cranectld", "ctld", false);

  auto context = crane::FlowContext::Create("0123456789abcdef0123456789abcdef",
                                            std::string_view{});
  ASSERT_TRUE(context.has_value());
  context->Job(1);

  crane::InitializeExecutionFlow(true, 1, "craned", "node0", false);
  crane::FlowEmitter::JobAccepted(context);

  std::vector<std::string> producers;
  {
    std::lock_guard lock{exporter_state->mutex};
    producers = exporter_state->producers;
  }
  tracer_manager.Shutdown();
  ASSERT_EQ(producers.size(), 1);
  EXPECT_EQ(producers.front(), "cranectld");
}

TEST_F(ExecutionFlowRuntimeTest, EverySemanticEmitterMatchesCanonicalCatalog) {
  auto exporter_state = std::make_shared<RecordingExporterState>();
  auto& tracer_manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(tracer_manager.Initialize(
      "ExecutionFlowRuntimeTest",
      std::make_unique<RecordingExporter>(exporter_state)));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);

  auto job_context = [] {
    auto context = crane::FlowContext::Create(
        "0123456789abcdef0123456789abcdef", std::string_view{});
    if (context) context->Job(1);
    return context;
  };
  auto step_context = [&job_context] {
    auto context = job_context();
    if (context) context->Step(2);
    return context;
  };

#  define EXPECT_SEMANTIC(context_factory, point, operation, outcome, call)   \
    do {                                                                      \
      std::size_t before;                                                     \
      {                                                                       \
        std::lock_guard lock{exporter_state->mutex};                          \
        before = exporter_state->points.size();                               \
      }                                                                       \
      auto context = context_factory();                                       \
      ASSERT_TRUE(context.has_value());                                       \
      call;                                                                   \
      ExpectSemanticPoint(exporter_state, before, point, operation, outcome); \
    } while (false)

  crane::InitializeExecutionFlow(true, 1, "cranectld", "ctld", false);
  EXPECT_SEMANTIC(job_context, crane::FlowPoint::kCtldJobAccepted,
                  crane::FlowOperation::kSubmit, crane::FlowOutcome::kAccepted,
                  crane::FlowEmitter::JobAccepted(std::move(context)));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobAllocated,
      crane::FlowOperation::kAllocate, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::JobAllocated(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobAllocationPersisted,
      crane::FlowOperation::kEmbeddedDbCommit, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::JobAllocationPersisted(std::move(context)));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobAllocRpcResult,
      crane::FlowOperation::kAllocJobs, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::JobAllocRpcSucceeded(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobAllocRpcResult,
      crane::FlowOperation::kAllocJobs, crane::FlowOutcome::kRpcFailure,
      crane::FlowEmitter::JobAllocRpcFailed(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobAllocRpcResult,
      crane::FlowOperation::kAllocJobs, crane::FlowOutcome::kCranedDown,
      crane::FlowEmitter::JobAllocCranedDown(std::move(context), "node0"));
  EXPECT_SEMANTIC(job_context, crane::FlowPoint::kCtldJobEmbeddedPersisted,
                  crane::FlowOperation::kEmbeddedDbFinalize,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::JobEmbeddedPersisted(std::move(context)));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobFreeResult,
      crane::FlowOperation::kFreeJobs, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::JobFreeResult(std::move(context), "node0", true));
  EXPECT_SEMANTIC(job_context, crane::FlowPoint::kCtldJobMongoPersisted,
                  crane::FlowOperation::kMongodbFinalize,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::JobMongoPersisted(std::move(context)));
  EXPECT_SEMANTIC(job_context, crane::FlowPoint::kCtldJobResourcesReleased,
                  crane::FlowOperation::kReleaseResources,
                  crane::FlowOutcome::kCompleted,
                  crane::FlowEmitter::JobResourcesReleased(std::move(context)));
  EXPECT_SEMANTIC(job_context, crane::FlowPoint::kCtldJobTerminal,
                  crane::FlowOperation::kJobTerminal,
                  crane::FlowOutcome::kPersisted,
                  crane::FlowEmitter::JobTerminal(std::move(context), 3));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobUnsupported,
      crane::FlowOperation::kSubmit, crane::FlowOutcome::kUnsupported,
      crane::FlowEmitter::JobUnsupportedAtSubmit(
          std::move(context), crane::FlowUnsupportedReason::kNonBatchJob));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobUnsupported,
      crane::FlowOperation::kCancelPendingJob, crane::FlowOutcome::kUnsupported,
      crane::FlowEmitter::JobUnsupportedAtPendingCancel(
          std::move(context), crane::FlowUnsupportedReason::kNonBatchJob));
  EXPECT_SEMANTIC(job_context, crane::FlowPoint::kCtldJobUnsupported,
                  crane::FlowOperation::kSubmitCommonStep,
                  crane::FlowOutcome::kUnsupported,
                  crane::FlowEmitter::JobUnsupportedAtStepSubmit(
                      std::move(context),
                      crane::FlowUnsupportedReason::kExtraCommonStep, 2));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCtldJobUnsupported,
      crane::FlowOperation::kRequeue, crane::FlowOutcome::kUnsupported,
      crane::FlowEmitter::JobUnsupportedAtRequeue(
          std::move(context), crane::FlowUnsupportedReason::kRequeueAttempt));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStatusReceived,
      crane::FlowOperation::kStatusChange, std::nullopt,
      crane::FlowEmitter::StatusReceived(std::move(context), "node0", 3));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStatusApplied,
      crane::FlowOperation::kStateMachine, crane::FlowOutcome::kApplied,
      crane::FlowEmitter::StatusApplied(std::move(context), "node0", 3));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCtldStepAllConfigured,
                  crane::FlowOperation::kDaemonConfigure,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::DaemonAllConfigured(std::move(context)));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCtldStepAllConfigured,
                  crane::FlowOperation::kStepConfigure,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::StepAllConfigured(std::move(context)));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCtldStepAllCompleting,
                  crane::FlowOperation::kDaemonCleanup,
                  crane::FlowOutcome::kRequested,
                  crane::FlowEmitter::DaemonAllCompleting(std::move(context)));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCtldStepAllCompleting,
                  crane::FlowOperation::kStepCleanup,
                  crane::FlowOutcome::kRequested,
                  crane::FlowEmitter::StepAllCompleting(std::move(context)));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCtldStepAllTerminal,
                  crane::FlowOperation::kDaemonTerminal, std::nullopt,
                  crane::FlowEmitter::DaemonAllTerminal(std::move(context), 3));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCtldStepAllTerminal,
                  crane::FlowOperation::kStepTerminal, std::nullopt,
                  crane::FlowEmitter::StepAllTerminal(std::move(context), 3));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepAllocRpcResult,
      crane::FlowOperation::kAllocSteps, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::StepAllocRpcSucceeded(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepAllocRpcResult,
      crane::FlowOperation::kAllocSteps, crane::FlowOutcome::kRpcFailure,
      crane::FlowEmitter::StepAllocRpcFailed(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepAllocRpcResult,
      crane::FlowOperation::kAllocSteps, crane::FlowOutcome::kCranedDown,
      crane::FlowEmitter::StepAllocCranedDown(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepExecuteRequested,
      crane::FlowOperation::kExecuteSteps, crane::FlowOutcome::kRequested,
      crane::FlowEmitter::StepExecuteRequested(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepExecuteRpcResult,
      crane::FlowOperation::kExecuteSteps, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::StepExecuteRpcSucceeded(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepExecuteRpcResult,
      crane::FlowOperation::kExecuteSteps, crane::FlowOutcome::kRejected,
      crane::FlowEmitter::StepExecuteRpcRejected(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepExecuteRpcResult,
      crane::FlowOperation::kExecuteSteps, crane::FlowOutcome::kRpcFailure,
      crane::FlowEmitter::StepExecuteRpcFailed(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepExecuteRpcResult,
      crane::FlowOperation::kExecuteSteps, crane::FlowOutcome::kCranedDown,
      crane::FlowEmitter::StepExecuteCranedDown(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepFreeRpcResult,
      crane::FlowOperation::kFreeSteps, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::StepFreeRpcSucceeded(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepFreeRpcResult,
      crane::FlowOperation::kFreeSteps, crane::FlowOutcome::kRpcFailure,
      crane::FlowEmitter::StepFreeRpcFailed(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCtldStepFreeRpcResult,
      crane::FlowOperation::kFreeSteps, crane::FlowOutcome::kCranedDown,
      crane::FlowEmitter::StepFreeCranedDown(std::move(context), "node0"));

  crane::InitializeExecutionFlow(true, 1, "craned", "node0", false);
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedStatusSendResult,
                  crane::FlowOperation::kSendStatusChange,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::StatusSendResult(std::move(context),
                                                       "node0", 3, true));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCranedJobInstalled,
      crane::FlowOperation::kInstallJob, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::CranedJobInstalled(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      job_context, crane::FlowPoint::kCranedJobRemoved,
      crane::FlowOperation::kRemoveJob, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::CranedJobRemoved(std::move(context), "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedStepInstallAccepted,
                  crane::FlowOperation::kInstallStep,
                  crane::FlowOutcome::kAccepted,
                  crane::FlowEmitter::CranedStepInstallAccepted(
                      std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCranedStepInstalled,
      crane::FlowOperation::kInstallStep, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::CranedStepInstalled(std::move(context), "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedStepExecuteAccepted,
                  crane::FlowOperation::kExecuteStep,
                  crane::FlowOutcome::kAccepted,
                  crane::FlowEmitter::CranedStepExecuteAccepted(
                      std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCranedStepFreeAccepted,
      crane::FlowOperation::kFreeStep, crane::FlowOutcome::kAccepted,
      crane::FlowEmitter::CranedStepFreeAccepted(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCranedStepCleanupStarted,
      crane::FlowOperation::kJobCleanup, std::nullopt,
      crane::FlowEmitter::CranedJobCleanupStarted(std::move(context), "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedStepCleanupFinished,
                  crane::FlowOperation::kJobCleanup,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::CranedJobCleanupFinished(
                      std::move(context), "node0", true));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedStepCleanupStarted,
                  crane::FlowOperation::kStepCleanup, std::nullopt,
                  crane::FlowEmitter::CranedStepCleanupStarted(
                      std::move(context), "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedStepCleanupFinished,
                  crane::FlowOperation::kStepCleanup,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::CranedStepCleanupFinished(
                      std::move(context), "node0", true));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCranedSupervisorForked,
      crane::FlowOperation::kForkSupervisor, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::CranedSupervisorForked(std::move(context), "node0"));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kCranedSupervisorReady,
      crane::FlowOperation::kInitializeSupervisor, crane::FlowOutcome::kSuccess,
      crane::FlowEmitter::CranedSupervisorReady(std::move(context), "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kCranedSupervisorExitObserved,
                  crane::FlowOperation::kObserveSupervisorExit,
                  crane::FlowOutcome::kExited,
                  crane::FlowEmitter::CranedSupervisorExitObserved(
                      std::move(context), "node0"));

  crane::InitializeExecutionFlow(true, 1, "supervisor", "node0", false);
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorStepInitialized,
                  crane::FlowOperation::kInitialize,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::SupervisorStepInitialized(
                      std::move(context), "node0", 3, true));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorStepExecuteStarted,
                  crane::FlowOperation::kExecuteStep,
                  crane::FlowOutcome::kStarted,
                  crane::FlowEmitter::SupervisorStepExecuteStarted(
                      std::move(context), "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorTaskPrepared,
                  crane::FlowOperation::kPrepareTask,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::SupervisorTaskPrepared(std::move(context),
                                                             4, "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorTaskSpawned,
                  crane::FlowOperation::kSpawnTask,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::SupervisorTaskSpawned(std::move(context),
                                                            4, "node0"));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorTaskExitObserved,
                  crane::FlowOperation::kObserveTaskExit,
                  crane::FlowOutcome::kExit,
                  crane::FlowEmitter::SupervisorTaskExitObserved(
                      std::move(context), 4, "node0", false, false));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorTaskFinalized,
                  crane::FlowOperation::kFinalizeTask,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::SupervisorTaskFinalized(
                      std::move(context), 4, "node0", 3, true));
  EXPECT_SEMANTIC(step_context,
                  crane::FlowPoint::kSupervisorStepAllTasksFinalized,
                  crane::FlowOperation::kFinalizeStep, std::nullopt,
                  crane::FlowEmitter::SupervisorStepAllTasksFinalized(
                      std::move(context), "node0", 3));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kSupervisorStepCompletingEnqueued,
      crane::FlowOperation::kEnqueueStatus, crane::FlowOutcome::kQueued,
      crane::FlowEmitter::SupervisorStepCompletingQueued(std::move(context),
                                                         "node0", 3));
  EXPECT_SEMANTIC(step_context,
                  crane::FlowPoint::kSupervisorStepCompletingEnqueued,
                  crane::FlowOperation::kEnqueueStatus,
                  crane::FlowOutcome::kPreStartFailure,
                  crane::FlowEmitter::SupervisorStepCompletingPreStartFailure(
                      std::move(context), "node0", 3));
  EXPECT_SEMANTIC(
      step_context, crane::FlowPoint::kSupervisorStepCompletingEnqueued,
      crane::FlowOperation::kEnqueueStatus, crane::FlowOutcome::kEarlyCancel,
      crane::FlowEmitter::SupervisorStepCompletingEarlyCancel(
          std::move(context), "node0", 3));
  EXPECT_SEMANTIC(step_context,
                  crane::FlowPoint::kSupervisorStepShutdownReceived,
                  crane::FlowOperation::kShutdownSupervisor, std::nullopt,
                  crane::FlowEmitter::SupervisorStepShutdownReceived(
                      std::move(context), "node0", 3));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorStepEpilogFinished,
                  crane::FlowOperation::kSupervisorEpilog,
                  crane::FlowOutcome::kSuccess,
                  crane::FlowEmitter::SupervisorStepEpilogFinished(
                      std::move(context), "node0", true, true));
  EXPECT_SEMANTIC(step_context, crane::FlowPoint::kSupervisorStepExiting,
                  crane::FlowOperation::kExitSupervisor, std::nullopt,
                  crane::FlowEmitter::SupervisorStepExiting(std::move(context),
                                                            "node0", 3));

#  undef EXPECT_SEMANTIC
  tracer_manager.Shutdown();
}

TEST_F(ExecutionFlowRuntimeTest, DaemonAndCommonCleanupUseCanonicalStepPoints) {
  auto exporter_state = std::make_shared<RecordingExporterState>();
  auto& tracer_manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(tracer_manager.Initialize(
      "ExecutionFlowRuntimeTest",
      std::make_unique<RecordingExporter>(exporter_state)));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 1, "craned", "node0", false);

  auto context = crane::FlowContext::Create("0123456789abcdef0123456789abcdef",
                                            std::string_view{});
  ASSERT_TRUE(context.has_value());
  context->Job(1).Step(0);
  crane::FlowEmitter::CranedJobCleanupStarted(context, "node0");
  crane::FlowEmitter::CranedJobCleanupFinished(context, "node0", true);
  crane::FlowEmitter::CranedStepCleanupStarted(context, "node0");
  crane::FlowEmitter::CranedStepCleanupFinished(context, "node0", true);

  std::vector<std::string> names;
  std::vector<std::string> operations;
  std::vector<int64_t> step_ids;
  {
    std::lock_guard lock{exporter_state->mutex};
    names = exporter_state->names;
    operations = exporter_state->operations;
    step_ids = exporter_state->step_ids;
  }
  tracer_manager.Shutdown();

  ASSERT_EQ(names.size(), 4);
  EXPECT_EQ(names[0], "flow/v1/craned/step/cleanup_started");
  EXPECT_EQ(names[1], "flow/v1/craned/step/cleanup_finished");
  EXPECT_EQ(names[2], "flow/v1/craned/step/cleanup_started");
  EXPECT_EQ(names[3], "flow/v1/craned/step/cleanup_finished");
  EXPECT_EQ(operations,
            (std::vector<std::string>{"job-cleanup", "job-cleanup",
                                      "step-cleanup", "step-cleanup"}));
  EXPECT_EQ(step_ids, (std::vector<int64_t>{0, 0, 0, 0}));
}

TEST(ExecutionFlowTest, AcceptsOnlyLowercaseCorrelationId) {
  auto flow_id = crane::FlowId::Parse("0123456789abcdef0123456789abcdef");
  ASSERT_TRUE(flow_id.has_value());
  EXPECT_EQ(flow_id->Value(), "0123456789abcdef0123456789abcdef");
  EXPECT_FALSE(
      crane::FlowId::Parse("0123456789ABCDEF0123456789abcdef").has_value());
  EXPECT_FALSE(
      crane::FlowId::Parse("0123456789abcdef0123456789abcde").has_value());
  EXPECT_FALSE(
      crane::FlowId::Parse("0123456789abcdef0123456789abcdeg").has_value());
}

TEST(ExecutionFlowTest, ServiceInstanceSeparatesProcessLifetimes) {
  EXPECT_EQ(crane::MakeExecutionFlowServiceInstance("craned01", 42, 123456),
            "craned01#pid=42:start=123456");
  EXPECT_NE(crane::MakeExecutionFlowServiceInstance("craned01", 42, 123456),
            crane::MakeExecutionFlowServiceInstance("craned01", 43, 123456));
  EXPECT_NE(crane::MakeExecutionFlowServiceInstance("craned01", 42, 123456),
            crane::MakeExecutionFlowServiceInstance("craned01", 42, 123457));
}

TEST(ExecutionFlowTest, ClassifiesUnsupportedBatchContractBranches) {
  EXPECT_FALSE(
      crane::ExecutionFlowJobUnsupportedReason(true, false, false, true, 0)
          .has_value());
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(false, false, false, true, 0),
      crane::FlowUnsupportedReason::kNonBatchJob);
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(false, true, false, true, 0),
      crane::FlowUnsupportedReason::kContainerJob);
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, true, true, 0),
      crane::FlowUnsupportedReason::kArrayJob);
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, false, false, 0),
      crane::FlowUnsupportedReason::kRequeueEnabled);
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, false, true, 1),
      crane::FlowUnsupportedReason::kRequeueAttempt);
}

TEST(ExecutionFlowTest, GeneratedCatalogUsesCanonicalWireNames) {
  EXPECT_EQ(crane::FlowPointName(crane::FlowPoint::kCtldJobAccepted),
            "ctld/job/accepted");
  EXPECT_EQ(crane::FlowPointProducer(crane::FlowPoint::kCranedStepInstalled),
            "craned");
  EXPECT_EQ(crane::FlowOperationName(crane::FlowOperation::kExecuteSteps),
            "execute-steps");
  EXPECT_EQ(crane::FlowOutcomeName(crane::FlowOutcome::kCleanupFailure),
            "cleanup-failure");
  EXPECT_EQ(crane::kExecutionFlowSchemaName, "flow/v1");
  EXPECT_EQ(crane::kExecutionFlowSchemaVersion, "v1");
  EXPECT_EQ(crane::kExecutionFlowWirePrefix, "flow/v1/");
  EXPECT_EQ(crane::kExecutionFlowStorageMeasurement, "execution_flow_points");
  EXPECT_EQ(crane::kExecutionFlowHeartbeatPoint, "flow/v1/pipeline/heartbeat");
  EXPECT_EQ(crane::kExecutionFlowPipelineFaultPoint, "flow/v1/pipeline/fault");
  EXPECT_EQ(crane::kExecutionFlowSchemaSha256.size(), 64);
  EXPECT_EQ(crane::FlowEnvelopeAttributeName(
                crane::FlowEnvelopeAttribute::kEventSequence),
            "event_sequence");
  EXPECT_EQ(crane::FlowEnvelopeAttributeWireType(
                crane::FlowEnvelopeAttribute::kEventSequence),
            crane::FlowWireType::kInt64);
  EXPECT_EQ(
      crane::FlowEnvelopeAttributeName(crane::FlowEnvelopeAttribute::kFlowId),
      "flow_id");
  EXPECT_EQ(crane::FlowEnvelopeAttributeWireType(
                crane::FlowEnvelopeAttribute::kFlowId),
            crane::FlowWireType::kString);
  EXPECT_EQ(crane::FlowEnvelopeAttributeRequirement(
                crane::FlowEnvelopeAttribute::kFlowId),
            crane::FlowEnvelopeRequirement::kBusiness);
  EXPECT_EQ(crane::FlowEnvelopeAttributeRequirement(
                crane::FlowEnvelopeAttribute::kProducer),
            crane::FlowEnvelopeRequirement::kAlways);
  EXPECT_EQ(crane::FlowEnvelopeAttributeMissingReason(
                crane::FlowEnvelopeAttribute::kFlowId),
            "invalid_flow_id");
  EXPECT_EQ(crane::FlowAttributeName(crane::FlowAttribute::kOperation),
            "operation");
  EXPECT_EQ(crane::FlowAttributeWireType(crane::FlowAttribute::kOperation),
            crane::FlowWireType::kEnum);
}

TEST(ExecutionFlowTest, GeneratedCatalogDefinesRequiredAttributeContract) {
  for (std::underlying_type_t<crane::FlowPoint> value = 0;
       value < static_cast<std::underlying_type_t<crane::FlowPoint>>(
                   crane::FlowPoint::kCount);
       ++value) {
    const auto point = static_cast<crane::FlowPoint>(value);
    EXPECT_FALSE(crane::FlowPointName(point).empty());
    EXPECT_FALSE(crane::FlowPointProducer(point).empty());
    EXPECT_NE(crane::FlowPointRequiredAttributes(point), 0);
  }

  const auto accepted = crane::FlowPoint::kCtldJobAccepted;
  EXPECT_TRUE(crane::FlowPointRequires(accepted, crane::FlowAttribute::kJobId));
  EXPECT_TRUE(
      crane::FlowPointRequires(accepted, crane::FlowAttribute::kOperation));
  EXPECT_TRUE(
      crane::FlowPointRequires(accepted, crane::FlowAttribute::kOutcome));
  EXPECT_FALSE(
      crane::FlowPointRequires(accepted, crane::FlowAttribute::kStepId));
  EXPECT_FALSE(
      crane::FlowPointRequires(accepted, crane::FlowAttribute::kNodeId));
}

TEST(ExecutionFlowTest, ExtractsOnlyValidatedEnvironmentValue) {
  std::unordered_map<std::string, std::string> environment{
      {std::string{crane::kExecutionFlowIdEnvironmentVariable},
       "abcdef0123456789abcdef0123456789"}};
  auto flow_id = crane::ExecutionFlowIdFromEnvironment(environment);
  ASSERT_TRUE(flow_id.has_value());
  EXPECT_EQ(flow_id->Value(), "abcdef0123456789abcdef0123456789");

  environment[std::string{crane::kExecutionFlowIdEnvironmentVariable}] =
      "ABCDEF0123456789ABCDEF0123456789";
  EXPECT_FALSE(crane::ExecutionFlowIdFromEnvironment(environment).has_value());
}
#endif
