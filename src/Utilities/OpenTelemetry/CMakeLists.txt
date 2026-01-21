# OpenTelemetry tracing library for CraneSched

add_library(crane_tracer STATIC TracerManager.cpp PluginSpanExporter.cpp)

target_include_directories(crane_tracer PUBLIC
                           ${CMAKE_CURRENT_SOURCE_DIR}/include)

// Force enable tracing for all builds
if(TRUE) 
  target_compile_definitions(crane_tracer PUBLIC CRANE_ENABLE_TRACING)
  
  target_link_libraries(
    crane_tracer
    PUBLIC opentelemetry-cpp::trace
           opentelemetry-cpp::ostream_span_exporter
           opentelemetry-cpp::resources
           Utility_PluginClient)
  
  message(STATUS "OpenTelemetry tracing enabled (Force enable) - output to file and influxdb")
else()
  message(STATUS "OpenTelemetry tracing disabled (Release build)")
endif()

install(DIRECTORY include/ DESTINATION include)
