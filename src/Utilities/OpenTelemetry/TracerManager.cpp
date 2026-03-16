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
#  include "crane/Tracing.h"
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

TracerManager& TracerManager::GetInstance() {
  static TracerManager instance;
  return instance;
}

bool TracerManager::Initialize(const std::string& service_name) {
#ifdef CRANE_ENABLE_TRACING
  return Initialize(service_name, nullptr);
#else
  service_name_ = service_name;
  initialized_ = false;
  return false;
#endif
}

#ifdef CRANE_ENABLE_TRACING
bool TracerManager::Initialize(
    const std::string& service_name,
    std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> extra_exporter) {
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
    std::vector<std::unique_ptr<trace_sdk::SpanProcessor>> processors;
    provider = std::make_shared<trace_sdk::TracerProvider>(
        std::move(processors), resource_ptr);
  }

  tracer_provider_ = provider;

  trace_api::Provider::SetTracerProvider(tracer_provider_);

  tracer_ = tracer_provider_->GetTracer(service_name_);

  initialized_ = true;
  return true;
}
#endif

void TracerManager::Shutdown() {
#ifdef CRANE_ENABLE_TRACING
  g_tracing_enabled.store(false, std::memory_order_release);
  if (tracer_provider_) {
    static_cast<opentelemetry::sdk::trace::TracerProvider*>(
        tracer_provider_.get())
        ->Shutdown();
  }
#endif
  initialized_ = false;
}

#ifdef CRANE_ENABLE_TRACING
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateSpan(const std::string& span_name) {
  if (!tracer_) {
    return {nullptr};
  }

  return tracer_->StartSpan(span_name);
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateRootSpan(const std::string& span_name) {
  if (!tracer_) {
    return {nullptr};
  }

  auto span = tracer_->StartSpan(span_name);
  return span;
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateChildSpan(
    const std::string& span_name,
    const opentelemetry::trace::SpanContext& parent_context) {
  if (!tracer_) {
    return {nullptr};
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
