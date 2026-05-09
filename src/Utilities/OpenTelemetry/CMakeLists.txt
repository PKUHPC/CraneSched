# OpenTelemetry tracing library for CraneSched

if(CRANE_ENABLE_TRACING)
  add_library(crane_tracer STATIC TracerManager.cpp CraneSpanExporter.cpp)
  target_include_directories(crane_tracer PUBLIC
                             ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_compile_definitions(crane_tracer PUBLIC CRANE_ENABLE_TRACING)
  target_link_libraries(
    crane_tracer
    PUBLIC opentelemetry-cpp::trace
           opentelemetry-cpp::ostream_span_exporter
           opentelemetry-cpp::resources
    PRIVATE Utility_PluginClient
    )
  message(STATUS "OpenTelemetry tracing enabled (CRANE_ENABLE_TRACING=ON)")
else()
  # INTERFACE library: no compilation, only provides include path
  # so that Tracing.h (with no-op stubs) is always findable
  add_library(crane_tracer INTERFACE)
  target_include_directories(crane_tracer INTERFACE
                             ${CMAKE_CURRENT_SOURCE_DIR}/include)
  message(STATUS "OpenTelemetry tracing disabled (CRANE_ENABLE_TRACING=OFF)")
endif()

install(DIRECTORY include/ DESTINATION include)
