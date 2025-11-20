include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(ABSL_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/abseil-cpp-20220623.1.tar.gz")
    set(C_ARES_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/c-ares-1.18.1.tar.gz")
    set(PROTOBUF_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/protobuf-cpp-3.21.8.tar.gz")
    set(RE2_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/re2-2022-06-01.tar.gz")
    set(GRPC_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/grpc-1.51.0.tar.gz")
else ()
    set(ABSL_SRC_URL "https://github.com/abseil/abseil-cpp/releases/download/20250127.1/abseil-cpp-20250127.1.tar.gz")
    set(C_ARES_SRC_URL "https://github.com/c-ares/c-ares/releases/download/v1.34.4/c-ares-1.34.4.tar.gz")
    set(PROTOBUF_SRC_URL "https://github.com/protocolbuffers/protobuf/releases/download/v33.1/protobuf-33.1.tar.gz")
    set(RE2_SRC_URL "https://github.com/google/re2/releases/download/2024-07-02/re2-2024-07-02.tar.gz")
    set(GRPC_SRC_URL "https://github.com/grpc/grpc/archive/refs/tags/v1.76.0.tar.gz")
endif ()

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
FetchContent_Declare(absl
        OVERRIDE_FIND_PACKAGE

        URL ${ABSL_SRC_URL}
        URL_HASH SHA256=B396401FD29E2E679CACE77867481D388C807671DC2ACC602A0259EEB79B7811
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
        URL_HASH SHA256=FA38DBED659EE4CC5A32DF5E27DEDA575FA6852C79A72BA1AF85DE35A6AE222F
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
        URL_HASH SHA256=FDA132CB0C86400381C0AF1FE98BD0F775CB566CB247CDCC105E344E00ACC30E
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(protobuf)
FetchContent_GetProperties(protobuf)
find_package(protobuf REQUIRED CONFIG)

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
set(RE2_BUILD_TESTING OFF)
FetchContent_Declare(re2
        OVERRIDE_FIND_PACKAGE

        URL ${RE2_SRC_URL}
        URL_HASH SHA256=EB2DF807C781601C14A260A507A5BB4509BE1EE626024CB45ACBD57CB9D4032B
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
        URL_HASH SHA256=0AF37B800953130B47C075B56683EE60BDC3EDA3C37FC6004193F5B569758204
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(grpc)
FetchContent_GetProperties(grpc)

set_property(GLOBAL PROPERTY protobuf_SOURCE_DIR "${protobuf_SOURCE_DIR}")
set_property(GLOBAL PROPERTY _PROTOBUF_LIBS_PATH "${protobuf_BINARY_DIR}")