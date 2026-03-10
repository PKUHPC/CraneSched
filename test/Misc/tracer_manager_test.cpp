#include <atomic>
#include <chrono>
#include <memory>

#include "gtest/gtest.h"
#include "crane/TracerManager.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"

namespace {

class CountingExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit CountingExporter(std::shared_ptr<std::atomic<int>> counter)
      : counter_(std::move(counter)) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable()
      noexcept override {
    return std::make_unique<opentelemetry::sdk::trace::SpanData>();
  }

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override {
    counter_->fetch_add(static_cast<int>(spans.size()), std::memory_order_relaxed);
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool Shutdown(std::chrono::microseconds /* timeout */) noexcept override {
    return true;
  }

  bool ForceFlush(std::chrono::microseconds /* timeout */) noexcept override {
    return true;
  }

 private:
  std::shared_ptr<std::atomic<int>> counter_;
};

}  // namespace

TEST(TracerManagerTest, InitializeAndExportSpan) {
  auto counter = std::make_shared<std::atomic<int>>(0);
  auto exporter = std::make_unique<CountingExporter>(counter);

  auto& manager = crane::TracerManager::GetInstance();
  ASSERT_TRUE(manager.Initialize("TracerManagerTest", std::move(exporter)));

  auto span = manager.CreateSpan("test-span");
  ASSERT_NE(span, nullptr);
  span->End();

  manager.Shutdown();

  EXPECT_GE(counter->load(std::memory_order_relaxed), 1);
}
