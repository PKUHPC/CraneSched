include(FetchContent)
FetchContent_Declare(
        grpc
        GIT_REPOSITORY https://github.com/grpc/grpc
        GIT_TAG v1.49.0
)
set(FETCHCONTENT_QUIET OFF)
set(ABSL_PROPAGATE_CXX_STD ON)
FetchContent_MakeAvailable(grpc)
FetchContent_GetProperties(grpc)

set_property(GLOBAL PROPERTY grpc_SOURCE_DIR "${grpc_SOURCE_DIR}")
