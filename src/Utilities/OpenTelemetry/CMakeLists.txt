# OpenTelemetry tracing library for CraneSched

add_library(crane_tracer STATIC TracerManager.cpp)

target_include_directories(crane_tracer PUBLIC
                           ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(CMAKE_TEST)
  target_compile_definitions(crane_tracer PUBLIC CRANE_ENABLE_TEST)
  
  target_link_libraries(
    crane_tracer
    PUBLIC opentelemetry-cpp::trace
           opentelemetry-cpp::ostream_span_exporter
           opentelemetry-cpp::resources)
  
  message(STATUS "OpenTelemetry tracing enabled (CMAKE_TEST=ON)")
else()
  message(STATUS "OpenTelemetry tracing disabled (CMAKE_TEST=OFF)")
endif()

install(DIRECTORY include/ DESTINATION include)
