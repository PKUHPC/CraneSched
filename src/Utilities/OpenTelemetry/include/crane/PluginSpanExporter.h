#pragma once

#ifdef CRANE_ENABLE_TRACING

#  include <memory>
#  include <mutex>
#  include <string>
#  include <vector>

#  include "opentelemetry/sdk/trace/exporter.h"

namespace crane {

struct PluginSpanConfig {
  std::string measurement = "crane_spans";
};

class PluginSpanExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit PluginSpanExporter(const PluginSpanConfig& config);
  ~PluginSpanExporter() override;

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override;

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override;

  bool Shutdown(std::chrono::microseconds timeout =
                    std::chrono::microseconds::max()) noexcept override;

 private:
  PluginSpanConfig config_;
  bool is_shutdown_ = false;

  void SendData(const std::string& data);
};

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
