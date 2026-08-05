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

#  include "TraceSpanExport.h"
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
    const auto& span = static_cast<const trace_sdk::SpanData&>(*recordable);
    auto info = otel::detail::PrepareSpanForExport(span);
    if (info) infos.emplace_back(std::move(*info));
  }

  if (infos.empty()) return opentelemetry::sdk::common::ExportResult::kSuccess;
  client_.TraceHookAsync(std::move(infos));
  return opentelemetry::sdk::common::ExportResult::kSuccess;
}

bool CraneSpanExporter::Shutdown(std::chrono::microseconds timeout) noexcept {
  return ForceFlush(timeout);
}

bool CraneSpanExporter::ForceFlush(std::chrono::microseconds timeout) noexcept {
  return client_.DrainTraceHooks(timeout);
}

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
