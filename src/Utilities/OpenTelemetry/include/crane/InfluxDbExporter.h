#pragma once

#ifdef CRANE_ENABLE_TRACING

#  include <memory>
#  include <mutex>
#  include <string>
#  include <vector>

#  include "opentelemetry/sdk/trace/exporter.h"

namespace crane {

struct InfluxDbConfig {
  std::string url;
  std::string token;
  std::string bucket;
  std::string org;
  std::string measurement = "crane_spans";
};

class InfluxDbExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit InfluxDbExporter(const InfluxDbConfig& config);
  ~InfluxDbExporter() override;

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override;

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override;

  bool Shutdown(std::chrono::microseconds timeout =
                    std::chrono::microseconds::max()) noexcept override;

 private:
  InfluxDbConfig config_;
  bool is_shutdown_ = false;

  void SendData(const std::string& data);
};

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
