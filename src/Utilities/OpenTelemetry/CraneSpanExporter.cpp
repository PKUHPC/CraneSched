/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#ifdef CRANE_ENABLE_TRACING

#  include "crane/CraneSpanExporter.h"

#  include "crane/Logger.h"

namespace crane {

namespace trace_sdk = opentelemetry::sdk::trace;

std::unique_ptr<trace_sdk::Recordable>
CraneSpanExporter::MakeRecordable() noexcept {
  return std::make_unique<trace_sdk::SpanData>();
}

opentelemetry::sdk::common::ExportResult CraneSpanExporter::Export(
    const opentelemetry::nostd::span<std::unique_ptr<trace_sdk::Recordable>>&
        spans) noexcept {
  CRANE_TRACE("CraneSpanExporter exporting {} spans", spans.size());

  std::vector<crane::grpc::plugin::SpanInfo> infos;
  infos.reserve(spans.size());
  for (auto& recordable : spans) {
    infos.emplace_back(
        ConvertSpan(static_cast<const trace_sdk::SpanData&>(*recordable)));
  }

  client_.TraceHookAsync(std::move(infos));
  return opentelemetry::sdk::common::ExportResult::kSuccess;
}

// ---- helpers ----

std::string CraneSpanExporter::HexFromTraceId(
    const opentelemetry::trace::TraceId& id) {
  char buf[32];
  id.ToLowerBase16(buf);
  return {buf, 32};
}

std::string CraneSpanExporter::HexFromSpanId(
    const opentelemetry::trace::SpanId& id) {
  char buf[16];
  id.ToLowerBase16(buf);
  return {buf, 16};
}

std::string CraneSpanExporter::AttributeToString(
    const opentelemetry::sdk::common::OwnedAttributeValue& value) {
  return std::visit(
      [](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, bool>)
          return arg ? "true" : "false";
        else if constexpr (std::is_arithmetic_v<T>)
          return std::to_string(arg);
        else if constexpr (std::is_same_v<T, std::string>)
          return arg;
        else if constexpr (std::is_same_v<T, const char*>)
          return arg;
        else
          return {};
      },
      value);
}

void CraneSpanExporter::SetTimestamp(
    google::protobuf::Timestamp* ts,
    opentelemetry::common::SystemTimestamp time) {
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                time.time_since_epoch())
                .count();
  ts->set_seconds(ns / 1000000000);
  ts->set_nanos(static_cast<int32_t>(ns % 1000000000));
}

crane::grpc::plugin::SpanInfo CraneSpanExporter::ConvertSpan(
    const trace_sdk::SpanData& span) {
  crane::grpc::plugin::SpanInfo info;

  info.set_trace_id(HexFromTraceId(span.GetTraceId()));
  info.set_span_id(HexFromSpanId(span.GetSpanId()));
  info.set_parent_span_id(HexFromSpanId(span.GetParentSpanId()));
  info.set_name(std::string(span.GetName()));

  SetTimestamp(info.mutable_start_time(), span.GetStartTime());

  auto end_time = opentelemetry::common::SystemTimestamp(
      span.GetStartTime().time_since_epoch() + span.GetDuration());
  SetTimestamp(info.mutable_end_time(), end_time);

  for (const auto& [key, value] : span.GetAttributes()) {
    (*info.mutable_attributes())[key] = AttributeToString(value);
  }

  return info;
}

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
