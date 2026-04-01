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
#  include <thread>

#  include "crane/Tracing.h"
#  include "opentelemetry/sdk/resource/resource.h"
#  include "opentelemetry/sdk/resource/semantic_conventions.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#  include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#  include "opentelemetry/sdk/trace/tracer_provider.h"
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
  namespace trace_sdk = opentelemetry::sdk::trace;
  namespace resource = opentelemetry::sdk::resource;

  service_name_ = service_name;

  auto resource_attributes = resource::ResourceAttributes{
      {resource::SemanticConventions::kServiceName, service_name_}};
  auto resource_ptr = resource::Resource::Create(resource_attributes);

  std::shared_ptr<trace_sdk::TracerProvider> provider;

  if (extra_exporter) {
    trace_sdk::BatchSpanProcessorOptions batch_opts;
    batch_opts.max_queue_size = 2048;
    batch_opts.schedule_delay_millis = std::chrono::milliseconds(5000);
    batch_opts.max_export_batch_size = 512;

    auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
        std::move(extra_exporter), batch_opts);
    provider = std::make_shared<trace_sdk::TracerProvider>(std::move(processor),
                                                           resource_ptr);
  } else {
    std::vector<std::unique_ptr<trace_sdk::SpanProcessor>> processors;
    provider = std::make_shared<trace_sdk::TracerProvider>(
        std::move(processors), resource_ptr);
  }

  tracer_provider_ = provider;
  tracer_ = tracer_provider_->GetTracer(service_name_);

  initialized_ = true;
  return true;
}
#endif

void TracerManager::Shutdown() {
#ifdef CRANE_ENABLE_TRACING
  // Step 1: Prevent new spans from being created
  g_tracing_enabled.store(false, std::memory_order_release);

  // Step 2: Short pause to let in-flight ScopedSpan constructors finish
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Step 3: Flush and shutdown the provider
  if (tracer_provider_) {
    auto* sdk_provider =
        static_cast<opentelemetry::sdk::trace::TracerProvider*>(
            tracer_provider_.get());
    sdk_provider->ForceFlush(std::chrono::milliseconds(5000));
    sdk_provider->Shutdown();
  }

  // Step 4: Clear references
  tracer_.reset();
  tracer_provider_.reset();
#endif
  initialized_ = false;
}

}  // namespace crane
