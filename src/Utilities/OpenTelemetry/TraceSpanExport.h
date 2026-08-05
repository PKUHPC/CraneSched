/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#ifdef CRANE_ENABLE_TRACING

#  include <optional>
#  include <string_view>

#  include "crane/Tracing.h"
#  include "opentelemetry/sdk/trace/span_data.h"
#  include "opentelemetry/trace/span.h"
#  include "protos/Plugin.pb.h"

namespace crane::otel::detail {

inline constexpr std::string_view kSpanClassAttribute =
    "crane.internal.trace_span_class";

void MarkSpanClass(opentelemetry::trace::Span& span, TraceSpanClass span_class);

std::optional<crane::grpc::plugin::SpanInfo> PrepareSpanForExport(
    const opentelemetry::sdk::trace::SpanData& span);

}  // namespace crane::otel::detail

#endif  // CRANE_ENABLE_TRACING
