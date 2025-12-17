include(FetchContent)

FetchContent_Declare(
  opentelemetry-cpp
  GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-cpp.git
  GIT_TAG v1.16.1
)

set(WITH_OTLP_GRPC OFF CACHE BOOL "" FORCE)
set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)
set(WITH_STL ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(opentelemetry-cpp)

if(TARGET opentelemetry_trace)
  add_library(opentelemetry-cpp::trace ALIAS opentelemetry_trace)
endif()

if(TARGET opentelemetry_exporter_ostream_span)
  add_library(opentelemetry-cpp::ostream_span_exporter ALIAS opentelemetry_exporter_ostream_span)
endif()

if(TARGET opentelemetry_resources)
  add_library(opentelemetry-cpp::resources ALIAS opentelemetry_resources)
endif()
