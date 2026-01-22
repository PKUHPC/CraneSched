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
#  include <fstream>

#  include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#  include "opentelemetry/sdk/resource/resource.h"
#  include "opentelemetry/sdk/resource/semantic_conventions.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"
#  include "opentelemetry/sdk/trace/tracer_provider.h"
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

bool TracerManager::Initialize(const std::string& output_file_path,
                               const std::string& service_name) {
#ifdef CRANE_ENABLE_TRACING
  namespace trace_api = opentelemetry::trace;
  namespace trace_sdk = opentelemetry::sdk::trace;
  namespace resource = opentelemetry::sdk::resource;

  service_name_ = service_name;

  output_stream_ = std::make_shared<std::ofstream>(
      output_file_path, std::ios::out | std::ios::app);
  if (!static_cast<std::ofstream*>(output_stream_.get())->is_open()) {
    return false;
  }

  auto exporter =
      opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create(
          *output_stream_);

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

  return tracer_->StartSpan(span_name);
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
TracerManager::CreateRootSpan(const std::string& span_name) {
  if (!tracer_) {
    return opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(
        nullptr);
  }
  return tracer_->StartSpan(span_name);
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
