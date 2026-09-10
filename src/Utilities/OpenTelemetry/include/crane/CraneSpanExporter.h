/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSpanExporter: Converts OpenTelemetry spans to protobuf SpanInfo
 * and sends them to the Plugin Daemon via PluginClient.
 */

#pragma once

#ifdef CRANE_ENABLE_TRACING

#  include <chrono>

#  include "crane/PluginClient.h"
#  include "opentelemetry/sdk/trace/exporter.h"

namespace crane {

class CraneSpanExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit CraneSpanExporter(plugin::PluginClient& client) : client_(client) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override;

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override;

  bool Shutdown(std::chrono::microseconds timeout) noexcept override;
  bool ForceFlush(std::chrono::microseconds timeout) noexcept override;

 private:
  plugin::PluginClient& client_;
};

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
