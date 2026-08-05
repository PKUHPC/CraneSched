/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#ifdef CRANE_ENABLE_TRACING

#  include "TraceSpanExport.h"

#  include <chrono>
#  include <string>
#  include <type_traits>
#  include <variant>

#  include "crane/TracerManager.h"

namespace crane::otel::detail {
namespace {

std::string HexFromTraceId(const opentelemetry::trace::TraceId& id) {
  char buffer[32];
  id.ToLowerBase16(buffer);
  return {buffer, 32};
}

std::string HexFromSpanId(const opentelemetry::trace::SpanId& id) {
  char buffer[16];
  id.ToLowerBase16(buffer);
  return {buffer, 16};
}

std::string AttributeToString(
    const opentelemetry::sdk::common::OwnedAttributeValue& value) {
  return std::visit(
      [](auto&& argument) -> std::string {
        using Value = std::decay_t<decltype(argument)>;
        if constexpr (std::is_same_v<Value, bool>)
          return argument ? "true" : "false";
        else if constexpr (std::is_arithmetic_v<Value>)
          return std::to_string(argument);
        else if constexpr (std::is_same_v<Value, std::string>)
          return argument;
        else if constexpr (std::is_same_v<Value, const char*>)
          return argument;
        else
          return {};
      },
      value);
}

void SetTimestamp(google::protobuf::Timestamp* timestamp,
                  opentelemetry::common::SystemTimestamp time) {
  const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               time.time_since_epoch())
                               .count();
  timestamp->set_seconds(nanoseconds / 1000000000);
  timestamp->set_nanos(static_cast<int32_t>(nanoseconds % 1000000000));
}

TraceSpanClass ResolveSpanClass(
    const opentelemetry::sdk::trace::SpanData& span) {
  TraceSpanClass span_class = ClassifyTraceSpanName(span.GetName());
  const auto explicit_class =
      span.GetAttributes().find(std::string{kSpanClassAttribute});
  if (explicit_class == span.GetAttributes().end()) return span_class;

  const auto encoded = AttributeToString(explicit_class->second);
  if (encoded == "1") return TraceSpanClass::Core;
  if (encoded == "2") return TraceSpanClass::Detailed;
  return span_class;
}

crane::grpc::plugin::SpanInfo ConvertSpan(
    const opentelemetry::sdk::trace::SpanData& span) {
  crane::grpc::plugin::SpanInfo info;
  info.set_trace_id(HexFromTraceId(span.GetTraceId()));
  info.set_span_id(HexFromSpanId(span.GetSpanId()));
  info.set_parent_span_id(HexFromSpanId(span.GetParentSpanId()));
  info.set_name(std::string{span.GetName()});
  SetTimestamp(info.mutable_start_time(), span.GetStartTime());

  const auto end_time = opentelemetry::common::SystemTimestamp(
      span.GetStartTime().time_since_epoch() + span.GetDuration());
  SetTimestamp(info.mutable_end_time(), end_time);

  for (const auto& [key, value] : span.GetAttributes()) {
    if (key == kSpanClassAttribute) continue;
    (*info.mutable_attributes())[key] = AttributeToString(value);
  }

  const auto status_code = span.GetStatus();
  if (status_code == opentelemetry::trace::StatusCode::kOk)
    info.set_status(crane::grpc::plugin::SPAN_STATUS_OK);
  else if (status_code == opentelemetry::trace::StatusCode::kError)
    info.set_status(crane::grpc::plugin::SPAN_STATUS_ERROR);

  info.set_service_name(TracerManager::GetInstance().ServiceName());
  return info;
}

}  // namespace

void MarkSpanClass(opentelemetry::trace::Span& span,
                   TraceSpanClass span_class) {
  span.SetAttribute(kSpanClassAttribute.data(),
                    static_cast<int64_t>(span_class));
}

std::optional<crane::grpc::plugin::SpanInfo> PrepareSpanForExport(
    const opentelemetry::sdk::trace::SpanData& span) {
  const bool is_error =
      span.GetStatus() == opentelemetry::trace::StatusCode::kError;
  if (!ShouldExportTraceSpan(ResolveSpanClass(span), is_error))
    return std::nullopt;
  return ConvertSpan(span);
}

}  // namespace crane::otel::detail

#endif  // CRANE_ENABLE_TRACING
