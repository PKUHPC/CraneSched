/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSpanExporter: Converts OpenTelemetry spans to protobuf SpanInfo
 * and sends them to the Plugin Daemon via PluginClient.
 */

#pragma once

#ifdef CRANE_ENABLE_TRACING

#include "crane/PluginClient.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "protos/Plugin.pb.h"

namespace crane {

class CraneSpanExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  explicit CraneSpanExporter(plugin::PluginClient& client)
      : client_(client) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable>
  MakeRecordable() noexcept override;

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
          spans) noexcept override;

  bool Shutdown(std::chrono::microseconds) noexcept override { return true; }
  bool ForceFlush(std::chrono::microseconds) noexcept override { return true; }

 private:
  plugin::PluginClient& client_;

  static std::string HexFromTraceId(
      const opentelemetry::trace::TraceId& id);
  static std::string HexFromSpanId(
      const opentelemetry::trace::SpanId& id);
  static std::string AttributeToString(
      const opentelemetry::sdk::common::OwnedAttributeValue& value);
  static void SetTimestamp(
      google::protobuf::Timestamp* ts,
      opentelemetry::common::SystemTimestamp time);
  static crane::grpc::plugin::SpanInfo ConvertSpan(
      const opentelemetry::sdk::trace::SpanData& span);
};

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
