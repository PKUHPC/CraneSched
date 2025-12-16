include(FetchContent)

FetchContent_Declare(
  opentelemetry-cpp
  GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-cpp.git
  GIT_TAG v1.16.1
)

set(WITH_OTLP_GRPC ON CACHE BOOL "" FORCE)
set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(opentelemetry-cpp)
