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

#include "crane/TracerManager.h"

#ifdef CRANE_ENABLE_TRACING

#  include "crane/PluginSpanExporter.h"
#  include "opentelemetry/sdk/resource/resource.h"
#  include "opentelemetry/sdk/resource/semantic_conventions.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"
#  include "opentelemetry/sdk/trace/tracer_provider.h"
#endif

namespace crane {

TraceGuard::~TraceGuard() {
#ifdef CRANE_ENABLE_TRACING
  if (!moved_ && span_) {
    span_->End();
  }
#endif
}

TraceGuard::TraceGuard(TraceGuard&& other) noexcept {
  *this = std::move(other);
}

TraceGuard& TraceGuard::operator=(TraceGuard&& other) noexcept {
#ifdef CRANE_ENABLE_TRACING
  if (this != &other) {
    if (!moved_ && span_) {
      span_->End();
    }
    span_ = std::move(other.span_);
    moved_ = other.moved_;
    other.moved_ = true;
  }
#endif
  return *this;
}

#ifdef CRANE_ENABLE_TRACING
TraceGuard::TraceGuard(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span)
    : span_(span) {}
#endif

void TraceGuard::AddEvent(const std::string& event_name) {
  // If tracing is disabled, this is a no-op
#ifdef CRANE_ENABLE_TRACING
  if (span_) {
    span_->AddEvent(event_name);
  }
#endif
}

TracerManager& TracerManager::GetInstance() {
  static TracerManager instance;
  return instance;
}

#ifdef CRANE_ENABLE_TRACING
bool TracerManager::Initialize(const PluginSpanConfig& influx_config,
                               const std::string& service_name) {
  namespace trace_api = opentelemetry::trace;
  namespace trace_sdk = opentelemetry::sdk::trace;
  namespace resource = opentelemetry::sdk::resource;

  service_name_ = service_name;

  auto exporter = std::make_unique<PluginSpanExporter>(influx_config);

  auto processor =
      trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));

  auto resource_attributes = resource::ResourceAttributes{
      {resource::SemanticConventions::kServiceName, service_name_}};
  auto resource_ptr = resource::Resource::Create(resource_attributes);

  auto provider = std::make_shared<trace_sdk::TracerProvider>(
      std::move(processor), resource_ptr);
  tracer_provider_ = provider;

  trace_api::Provider::SetTracerProvider(tracer_provider_);

  tracer_ = tracer_provider_->GetTracer(service_name_);

  initialized_ = true;
  return true;
}
#endif

void TracerManager::Shutdown() {
#ifdef CRANE_ENABLE_TRACING
  if (tracer_provider_) {
    static_cast<opentelemetry::sdk::trace::TracerProvider*>(
        tracer_provider_.get())
        ->Shutdown();
  }
  initialized_ = false;
#endif
}

TraceGuard TracerManager::StartSpan(const std::string& span_name) {
#ifdef CRANE_ENABLE_TRACING
  if (!tracer_) {
    return TraceGuard(
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(nullptr));
  }
  return TraceGuard(tracer_->StartSpan(span_name));
#else
  return TraceGuard();
#endif
}

#ifdef CRANE_ENABLE_TRACING
void TracerManager::SetAttributeImpl(
    const std::string& key,
    const opentelemetry::common::AttributeValue& value) {
  // Deprecated. Use TraceGuard::SetAttribute.
}
#endif

}  // namespace crane
