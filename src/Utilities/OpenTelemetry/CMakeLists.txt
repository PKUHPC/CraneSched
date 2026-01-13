# OpenTelemetry tracing library for CraneSched

add_library(crane_tracer STATIC TracerManager.cpp InfluxDbExporter.cpp)

target_include_directories(crane_tracer PUBLIC
                           ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(crane_tracer PUBLIC CRANE_ENABLE_TRACING)
  
  target_link_libraries(
    crane_tracer
    PUBLIC opentelemetry-cpp::trace
           opentelemetry-cpp::ostream_span_exporter
           opentelemetry-cpp::resources
           libcurl)
  
  message(STATUS "OpenTelemetry tracing enabled (Debug build) - output to file and influxdb")
else()
  message(STATUS "OpenTelemetry tracing disabled (Release build)")
endif()

install(DIRECTORY include/ DESTINATION include)
