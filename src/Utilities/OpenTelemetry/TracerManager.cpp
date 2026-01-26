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

#include <vector>

#ifdef CRANE_ENABLE_TRACING
#  include "opentelemetry/sdk/resource/resource.h"
#  include "opentelemetry/sdk/resource/semantic_conventions.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"
#  include "opentelemetry/sdk/trace/tracer_provider.h"
#  include "opentelemetry/trace/span.h"
#  include "opentelemetry/trace/span_context.h"
#  include "opentelemetry/trace/span_id.h"
#  include "opentelemetry/trace/trace_id.h"
#endif

namespace crane {

namespace _internal {
#ifdef CRANE_ENABLE_TRACING
thread_local opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
    g_current_span;
#endif
}  // namespace _internal

TracerManager& TracerManager::GetInstance() {
  static TracerManager instance;
  return instance;
}

bool TracerManager::Initialize(
    const std::string& service_name,
    std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> extra_exporter) {
#ifdef CRANE_ENABLE_TRACING
  namespace trace_api = opentelemetry::trace;
  namespace trace_sdk = opentelemetry::sdk::trace;
  namespace resource = opentelemetry::sdk::resource;

  service_name_ = service_name;

  auto resource_attributes = resource::ResourceAttributes{
      {resource::SemanticConventions::kServiceName, service_name_}};
  auto resource_ptr = resource::Resource::Create(resource_attributes);

  std::shared_ptr<trace_sdk::TracerProvider> provider;

  if (extra_exporter) {
    auto processor = trace_sdk::SimpleSpanProcessorFactory::Create(
        std::move(extra_exporter));
    provider = std::make_shared<trace_sdk::TracerProvider>(std::move(processor),
                                                           resource_ptr);
  } else {
    // If no exporter is provided, we still create a provider but it won't
    // export anything. Assuming TracerProvider has a constructor that accepts
    // just resource or we pass null processor? To be safe and compatible with
    // typical usage where we expect output, we might want to warn. For now,
    // let's assume we can create it with empty processors (not available in all
    // versions directly via constructor maybe?) Let's create a provider with no
    // processor if possible. If headers allow:
    // TracerProvider(std::vector<std::unique_ptr<SpanProcessor>>&& processors,
    // ...)
    std::vector<std::unique_ptr<trace_sdk::SpanProcessor>> processors;
    provider = std::make_shared<trace_sdk::TracerProvider>(
        std::move(processors), resource_ptr);
  }

  tracer_provider_ = provider;

  trace_api::Provider::SetTracerProvider(tracer_provider_);

  tracer_ = tracer_provider_->GetTracer(service_name_);

  initialized_ = true;
  return true;
#else
  return false;
#endif
}

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

#ifdef CRANE_ENABLE_TRACING
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateSpan(const std::string& span_name) {
  if (!tracer_) {
    return opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(
        nullptr);
  }

  // Check if there is an active parent span in the current thread
  if (_internal::g_current_span &&
      _internal::g_current_span->GetContext().IsValid()) {
    opentelemetry::trace::StartSpanOptions options;
    options.parent = _internal::g_current_span->GetContext();
    return tracer_->StartSpan(span_name, options);
  }

  auto span = tracer_->StartSpan(span_name);
  return span;
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateRootSpan(const std::string& span_name) {
  if (!tracer_) {
    return opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(
        nullptr);
  }

  auto span = tracer_->StartSpan(span_name);
  return span;
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateChildSpan(
    const std::string& span_name,
    const opentelemetry::trace::SpanContext& parent_context) {
  if (!tracer_) {
    return opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(
        nullptr);
  }

  opentelemetry::trace::StartSpanOptions options;
  options.parent = parent_context;
  return tracer_->StartSpan(span_name, options);
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
TracerManager::GetTracer() {
  return tracer_;
}
#endif

}  // namespace crane
