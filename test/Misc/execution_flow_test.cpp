#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "crane/ExecutionFlow.h"
#include "crane/TracerManager.h"
#include "gtest/gtest.h"

#ifdef CRANE_ENABLE_EXECUTION_FLOW
TEST(ExecutionFlowTest, FlowNamesAreCoreAtBasicTraceLevel) {
  EXPECT_EQ(crane::ClassifyTraceSpanName("flow/v1/ctld/job/accepted"),
            crane::TraceSpanClass::Core);
  EXPECT_TRUE(crane::TraceLevelAllowsSpan(
      crane::TraceLevel::Basic, "flow/v1/supervisor/task/spawned", false));
}
#else
TEST(ExecutionFlowTest, CompiledOutFlowNamesAreNotClassified) {
  EXPECT_EQ(crane::ClassifyTraceSpanName("flow/v1/ctld/job/accepted"),
            crane::TraceSpanClass::Other);
  EXPECT_FALSE(crane::TraceLevelAllowsSpan(
      crane::TraceLevel::Basic, "flow/v1/supervisor/task/spawned", false));
}
#endif

TEST(ExecutionFlowTest, DisabledPointDoesNotEvaluateArguments) {
#ifdef CRANE_ENABLE_EXECUTION_FLOW
  crane::ShutdownExecutionFlow();
  crane::ApplyRuntimeTraceConfig(false, crane::TraceLevel::Basic);
#endif
  int evaluations = 0;
  CRANE_FLOW_POINT((++evaluations, "ctld/test"),
                   (++evaluations, std::string(32, 'a')),
                   (++evaluations, std::string{}),
                   CRANE_FLOW_SET_ATTR("value", ++evaluations););
  EXPECT_EQ(evaluations, 0);
}

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
  auto& tracer_manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(tracer_manager.Initialize("ExecutionFlowRuntimeTest"));
  crane::ApplyRuntimeTraceConfig(true, crane::TraceLevel::Basic);
  crane::InitializeExecutionFlow(true, 1, "cranectld", "ctld", false);

  auto point = std::make_unique<crane::ExecutionFlowPoint>(
      "ctld/job/accepted", "0123456789abcdef0123456789abcdef");
  ASSERT_TRUE(point->IsActive());

  std::atomic<bool> shutdown_returned{false};
  std::thread shutdown_thread([&] {
    crane::ShutdownExecutionFlow();
    shutdown_returned.store(true, std::memory_order_release);
  });
  while (crane::ExecutionFlowEnabled()) std::this_thread::yield();
  EXPECT_FALSE(shutdown_returned.load(std::memory_order_acquire));

  point.reset();
  shutdown_thread.join();
  EXPECT_TRUE(shutdown_returned.load(std::memory_order_acquire));
  tracer_manager.Shutdown();
}

TEST(ExecutionFlowTest, AcceptsOnlyLowercaseCorrelationId) {
  EXPECT_TRUE(
      crane::IsValidExecutionFlowId("0123456789abcdef0123456789abcdef"));
  EXPECT_FALSE(
      crane::IsValidExecutionFlowId("0123456789ABCDEF0123456789abcdef"));
  EXPECT_FALSE(
      crane::IsValidExecutionFlowId("0123456789abcdef0123456789abcde"));
  EXPECT_FALSE(
      crane::IsValidExecutionFlowId("0123456789abcdef0123456789abcdeg"));

  EXPECT_EQ(crane::ParseExecutionFlowId("0123456789abcdef0123456789abcdef"),
            "0123456789abcdef0123456789abcdef");
}

TEST(ExecutionFlowTest, ServiceInstanceSeparatesProcessLifetimes) {
  EXPECT_EQ(crane::MakeExecutionFlowServiceInstance("craned01", 42, 123456),
            "craned01#pid=42:start=123456");
  EXPECT_NE(crane::MakeExecutionFlowServiceInstance("craned01", 42, 123456),
            crane::MakeExecutionFlowServiceInstance("craned01", 43, 123456));
  EXPECT_NE(crane::MakeExecutionFlowServiceInstance("craned01", 42, 123456),
            crane::MakeExecutionFlowServiceInstance("craned01", 42, 123457));
}

TEST(ExecutionFlowTest, AllowsOnlyBoundedLifecycleAttributes) {
  for (const auto key : {"job_id", "step_id", "task_id", "attempt", "node_id",
                         "operation", "outcome", "status", "reason_code"}) {
    EXPECT_TRUE(crane::IsAllowedExecutionFlowAttribute(key)) << key;
  }

  for (const auto key :
       {"command", "environment", "path", "error", "exit_code", "task_count",
        "node_count", "job_type", "requeue_count"}) {
    EXPECT_FALSE(crane::IsAllowedExecutionFlowAttribute(key)) << key;
  }
}

TEST(ExecutionFlowTest, ClassifiesUnsupportedBatchContractBranches) {
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, false, true, 0),
      "");
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(false, false, false, true, 0),
      "non-batch-job");
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(false, true, false, true, 0),
      "container-job");
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, true, true, 0),
      "array-job");
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, false, false, 0),
      "requeue-enabled");
  EXPECT_EQ(
      crane::ExecutionFlowJobUnsupportedReason(true, false, false, true, 1),
      "requeue-attempt");
}

TEST(ExecutionFlowTest, ExtractsOnlyValidatedEnvironmentValue) {
  std::unordered_map<std::string, std::string> environment{
      {std::string{crane::kExecutionFlowIdEnvironmentVariable},
       "abcdef0123456789abcdef0123456789"}};
  EXPECT_EQ(crane::ExecutionFlowIdFromEnvironment(environment),
            "abcdef0123456789abcdef0123456789");

  environment[std::string{crane::kExecutionFlowIdEnvironmentVariable}] =
      "ABCDEF0123456789ABCDEF0123456789";
  EXPECT_TRUE(crane::ExecutionFlowIdFromEnvironment(environment).empty());
}
#endif
