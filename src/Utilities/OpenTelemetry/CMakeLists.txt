# OpenTelemetry tracing library for CraneSched

add_library(crane_tracer STATIC TracerManager.cpp)

target_include_directories(crane_tracer PUBLIC
                           ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(CRANE_ENABLE_TRACING)
  target_compile_definitions(crane_tracer PUBLIC CRANE_ENABLE_TEST)
  
  target_link_libraries(
    crane_tracer
    PUBLIC opentelemetry-cpp::trace
           opentelemetry-cpp::ostream_span_exporter
           opentelemetry-cpp::resources)
  
  message(STATUS "OpenTelemetry tracing enabled (CRANE_ENABLE_TRACING=ON)")
else()
  message(STATUS "OpenTelemetry tracing disabled (CRANE_ENABLE_TRACING=OFF)")
endif()

install(DIRECTORY include/ DESTINATION include)
