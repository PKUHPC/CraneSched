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
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"
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

  auto resource_attributes = resource::ResourceAttributes{
      {resource::SemanticConventions::kServiceName, service_name}};
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

  auto tracer = provider->GetTracer(service_name);
  {
    std::lock_guard lock{tracer_mutex_};
    service_name_ = service_name;
    tracer_provider_ = std::move(provider);
    tracer_ = std::move(tracer);
    initialized_ = true;
  }
  return true;
}
#endif

void TracerManager::Shutdown() {
#ifdef CRANE_ENABLE_TRACING
  // Step 1: Prevent new spans from being created
  g_tracing_enabled.store(false, std::memory_order_release);

  // Step 2: Short pause to let in-flight ScopedSpan constructors finish
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>
      tracer_provider;
  {
    std::lock_guard lock{tracer_mutex_};
    tracer_provider = std::move(tracer_provider_);
    tracer_.reset();
    initialized_ = false;
  }

  // Step 3: Flush and shutdown the provider through a private snapshot. New
  // readers already observe an empty tracer, while existing snapshots keep
  // their provider alive until they finish.
  if (tracer_provider) {
    auto* sdk_provider =
        static_cast<opentelemetry::sdk::trace::TracerProvider*>(
            tracer_provider.get());
    sdk_provider->ForceFlush(std::chrono::milliseconds(5000));
    sdk_provider->Shutdown();
  }
#else
  initialized_ = false;
#endif
}

}  // namespace crane
