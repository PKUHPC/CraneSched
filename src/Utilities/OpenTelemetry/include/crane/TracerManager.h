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

#include <string>

#ifdef CRANE_ENABLE_TRACING
#  include "opentelemetry/sdk/trace/exporter.h"
#  include "opentelemetry/sdk/trace/tracer_provider.h"
#  include "opentelemetry/trace/provider.h"
#  include "opentelemetry/trace/tracer.h"
#endif

namespace crane {

class TracerManager {
 public:
  static TracerManager& GetInstance();

  bool Initialize(const std::string& service_name);

#ifdef CRANE_ENABLE_TRACING
  bool Initialize(
      const std::string& service_name,
      std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> extra_exporter);

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
  GetTracerSafe() {
    return tracer_;
  }
#endif

  const std::string& ServiceName() const { return service_name_; }

  void Shutdown();

  TracerManager(const TracerManager&) = delete;
  TracerManager& operator=(const TracerManager&) = delete;

 private:
  TracerManager() = default;
  ~TracerManager() = default;

#ifdef CRANE_ENABLE_TRACING
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>
      tracer_provider_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
#endif
  std::string service_name_;
  bool initialized_ = false;
};

}  // namespace crane
