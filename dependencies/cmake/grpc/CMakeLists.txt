include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(ABSL_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/abseil-cpp-20220623.1.tar.gz")
    set(C_ARES_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/c-ares-1.18.1.tar.gz")
    set(PROTOBUF_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/protobuf-cpp-3.21.8.tar.gz")
    set(RE2_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/re2-2022-06-01.tar.gz")
    set(GRPC_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/grpc-1.51.0.tar.gz")
else ()
    set(ABSL_SRC_URL "https://github.com/abseil/abseil-cpp/releases/download/20240116.2/abseil-cpp-20240116.2.tar.gz")
    set(C_ARES_SRC_URL "https://github.com/c-ares/c-ares/releases/download/cares-1_18_1/c-ares-1.18.1.tar.gz")
    set(PROTOBUF_SRC_URL "https://github.com/protocolbuffers/protobuf/releases/download/v27.2/protobuf-27.2.tar.gz")
    set(RE2_SRC_URL "https://github.com/google/re2/archive/refs/tags/2022-06-01.tar.gz")
    set(GRPC_SRC_URL "https://github.com/grpc/grpc/archive/refs/tags/v1.67.0.tar.gz")
endif ()

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
FetchContent_Declare(absl
        OVERRIDE_FIND_PACKAGE

        URL ${ABSL_SRC_URL}
        URL_HASH SHA256=733726b8c3a6d39a4120d7e45ea8b41a434cdacde401cba500f14236c49b39dc
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(absl)
FetchContent_GetProperties(absl)
find_package(absl REQUIRED CONFIG)

if (BUILD_SHARED_LIBS)
    set(CARES_SHARED ON)
    set(CARES_STATIC OFF)
else ()
    set(CARES_SHARED OFF)
    set(CARES_STATIC ON)
endif ()

FetchContent_Declare(c-ares
        OVERRIDE_FIND_PACKAGE

        URL ${C_ARES_SRC_URL}
        URL_HASH SHA256=1a7d52a8a84a9fbffb1be9133c0f6e17217d91ea5a6fa61f6b4729cda78ebbcf
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(c-ares)
FetchContent_GetProperties(c-ares)
find_package(c-ares REQUIRED CONFIG)

set(protobuf_BUILD_TESTS OFF)
set(protobuf_WITH_ZLIB OFF)
set(protobuf_MSVC_STATIC_RUNTIME OFF)
set(protobuf_USE_EXTERNAL_GTEST ON)
if (BUILD_SHARED_LIBS)
    set(protobuf_BUILD_SHARED_LIBS ON)
else ()
    set(protobuf_BUILD_SHARED_LIBS OFF)
endif ()
FetchContent_Declare(protobuf
        OVERRIDE_FIND_PACKAGE

        URL ${PROTOBUF_SRC_URL}
        URL_HASH SHA256=e4ff2aeb767da6f4f52485c2e72468960ddfe5262483879ef6ad552e52757a77
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(protobuf)
FetchContent_GetProperties(protobuf)
find_package(protobuf REQUIRED CONFIG)

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
FetchContent_Declare(re2
        OVERRIDE_FIND_PACKAGE

        URL ${RE2_SRC_URL}
        URL_HASH SHA256=f89c61410a072e5cbcf8c27e3a778da7d6fd2f2b5b1445cd4f4508bee946ab0f
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(re2)
FetchContent_GetProperties(re2)
find_package(re2 REQUIRED CONFIG)

# This version of zlib is kind of new.
# Disable it and use dynamic system version of zlib instead.
#
#FetchContent_Declare(ZLIB
#        OVERRIDE_FIND_PACKAGE
#
#        URL https://github.com/madler/zlib/releases/download/v1.2.13/zlib-1.2.13.tar.gz
#        URL_HASH SHA256=b3a24de97a8fdbc835b9833169501030b8977031bcb54b3b3ac13740f846ab30
#        INACTIVITY_TIMEOUT 5
#        )
#FetchContent_MakeAvailable(ZLIB)
#FetchContent_GetProperties(ZLIB)
find_package(ZLIB REQUIRED)
if (ZLIB_FOUND)
    message(STATUS "${ZLIB_LIBRARIES}")
endif ()


set(gRPC_INSTALL ON CACHE BOOL "" FORCE)
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(gRPC_PROTOBUF_PROVIDER package CACHE STRING "" FORCE)
set(gRPC_RE2_PROVIDER package CACHE STRING "" FORCE)
set(gRPC_ZLIB_PROVIDER package CACHE STRING "" FORCE)
set(gRPC_ABSL_PROVIDER package CACHE STRING "" FORCE)
set(gRPC_CARES_PROVIDER package CACHE STRING "" FORCE)
set(gRPC_SSL_PROVIDER package CACHE STRING "" FORCE)

FetchContent_Declare(grpc
        OVERRIDE_FIND_PACKAGE

        URL ${GRPC_SRC_URL}
        URL_HASH SHA256=af0638f73e4452e22e295f8b3f452518234254104713a08497f3d3aaa76733ad
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(grpc)
FetchContent_GetProperties(grpc)

set_property(GLOBAL PROPERTY protobuf_SOURCE_DIR "${protobuf_SOURCE_DIR}")
set_property(GLOBAL PROPERTY _PROTOBUF_LIBS_PATH "${protobuf_BINARY_DIR}")