/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#ifdef CRANE_ENABLE_TRACING
#  include "crane/PluginSpanExporter.h"
#  include "opentelemetry/sdk/trace/processor.h"
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"
#  include "opentelemetry/sdk/trace/tracer_provider.h"
#  include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#  include "opentelemetry/trace/provider.h"
#  include "opentelemetry/trace/span.h"
#  include "opentelemetry/trace/tracer.h"
#endif

namespace crane {

class TracerManager;

class TraceGuard {
 public:
  TraceGuard() = default;
  ~TraceGuard();

  TraceGuard(TraceGuard&& other) noexcept;
  TraceGuard& operator=(TraceGuard&& other) noexcept;

  TraceGuard(const TraceGuard&) = delete;
  TraceGuard& operator=(const TraceGuard&) = delete;

  template <typename T>
  void SetAttribute(const std::string& key, const T& value) {
#ifdef CRANE_ENABLE_TRACING
    if (span_) {
      span_->SetAttribute(key, value);
    }
#endif
  }

  void AddEvent(const std::string& event_name);

 private:
#ifdef CRANE_ENABLE_TRACING
  explicit TraceGuard(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span);

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  bool moved_ = false;
#endif
  friend class TracerManager;
};

class TracerManager {
 public:
  static TracerManager& GetInstance();

  bool Initialize(const std::string& output_file_path,
                  const std::string& service_name);

#ifdef CRANE_ENABLE_TRACING
  bool Initialize(const PluginSpanConfig& influx_config,
                  const std::string& service_name);
#endif

  void Shutdown();

  TraceGuard StartSpan(const std::string& span_name);

  template <typename T>
  void SetAttribute(const std::string& key, const T& value) {
#ifdef CRANE_ENABLE_TRACING
    SetAttributeImpl(key, value);
#endif
  }

  void AddEvent(const std::string& event_name);

  void EndSpan(const std::string& status = "OK");

#ifdef CRANE_ENABLE_TRACING
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>
      tracer_provider_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
  std::shared_ptr<std::ostream> output_stream_;
#endif
  std::string service_name_;
  bool initialized_ = false;
};

}  // namespace crane
