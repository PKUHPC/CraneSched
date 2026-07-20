#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "crane/TracerManager.h"
#include "crane/Tracing.h"
#include "gtest/gtest.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"

namespace {

class CountingExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit CountingExporter(std::shared_ptr<std::atomic<int>> counter)
      : counter_(std::move(counter)) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override {
    return std::make_unique<opentelemetry::sdk::trace::SpanData>();
  }

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override {
    counter_->fetch_add(static_cast<int>(spans.size()),
                        std::memory_order_relaxed);
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool Shutdown(std::chrono::microseconds) noexcept override { return true; }
  bool ForceFlush(std::chrono::microseconds) noexcept override { return true; }

 private:
  std::shared_ptr<std::atomic<int>> counter_;
};

}  // namespace

TEST(TracerManagerTest, InitializeAndExportSpan) {
  auto counter = std::make_shared<std::atomic<int>>(0);
  auto exporter = std::make_unique<CountingExporter>(counter);

  auto& manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(manager.Initialize("TracerManagerTest", std::move(exporter)));
  crane::g_tracing_enabled.store(true, std::memory_order_release);

  {
    crane::ScopedSpan span(
        "test-span", manager.GetTracerSafe());
    span.SetAttribute("key", "value");
  }

  // BatchSpanProcessor exports asynchronously; give it time to flush
  std::this_thread::sleep_for(std::chrono::seconds(6));

  manager.Shutdown();

  EXPECT_GE(counter->load(std::memory_order_relaxed), 1);
}

TEST(TracerManagerTest, SerializeDeserializeTraceParent) {
  auto counter = std::make_shared<std::atomic<int>>(0);
  auto exporter = std::make_unique<CountingExporter>(counter);

  auto& manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(manager.Initialize("SerializeTest", std::move(exporter)));
  crane::g_tracing_enabled.store(true, std::memory_order_release);

  std::string traceparent;
  {
    crane::ScopedSpan root("root-span", manager.GetTracerSafe());
    ASSERT_TRUE(root.IsActive());
    traceparent = crane::SerializeTraceParent(root.GetContext());
  }

  // Verify format: "00-{32hex}-{16hex}-{2hex}" = 55 chars
  ASSERT_EQ(traceparent.size(), 55u);
  EXPECT_EQ(traceparent.substr(0, 3), "00-");
  EXPECT_EQ(traceparent[35], '-');
  EXPECT_EQ(traceparent[52], '-');

  // Deserialize and verify it produces a valid context
  auto ctx = crane::DeserializeTraceParent(traceparent);
  EXPECT_TRUE(ctx.IsValid());
  EXPECT_TRUE(ctx.IsRemote());

  // Create a child span from the deserialized context
  {
    crane::ScopedSpan child("child-span", manager.GetTracerSafe(), ctx);
    EXPECT_TRUE(child.IsActive());
    // Child should share the same trace_id
    auto child_tp = crane::SerializeTraceParent(child.GetContext());
    EXPECT_EQ(child_tp.substr(3, 32), traceparent.substr(3, 32));
  }

  // Invalid inputs should return invalid context
  EXPECT_FALSE(crane::DeserializeTraceParent("").IsValid());
  EXPECT_FALSE(crane::DeserializeTraceParent("too-short").IsValid());

  std::this_thread::sleep_for(std::chrono::seconds(6));
  manager.Shutdown();
}

TEST(TracerManagerTest, ServiceName) {
  auto& manager = crane::TracerManager::GetInstance();
  auto counter = std::make_shared<std::atomic<int>>(0);
  ASSERT_TRUE(
      manager.Initialize("MyService", std::make_unique<CountingExporter>(counter)));
  EXPECT_EQ(manager.ServiceName(), "MyService");
  manager.Shutdown();
}
