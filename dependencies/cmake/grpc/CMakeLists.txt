include(FetchContent)

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
FetchContent_Declare(absl
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.1.tar.gz
        URL_HASH SHA256=91ac87d30cc6d79f9ab974c51874a704de9c2647c40f6932597329a282217ba8
        )
FetchContent_MakeAvailable(absl)
FetchContent_GetProperties(absl)
find_package(absl REQUIRED CONFIG)

set(CARES_SHARED OFF)
set(CARES_STATIC ON)
set(CARES_STATIC_PIC ON)
FetchContent_Declare(c-ares
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/c-ares/c-ares/releases/download/cares-1_18_1/c-ares-1.18.1.tar.gz
        URL_HASH SHA256=1a7d52a8a84a9fbffb1be9133c0f6e17217d91ea5a6fa61f6b4729cda78ebbcf
        )
FetchContent_MakeAvailable(c-ares)
FetchContent_GetProperties(c-ares)
find_package(c-ares REQUIRED CONFIG)

set(protobuf_BUILD_TESTS OFF)
set(protobuf_WITH_ZLIB OFF)
set(protobuf_MSVC_STATIC_RUNTIME OFF)
set(protobuf_USE_EXTERNAL_GTEST ON)
FetchContent_Declare(protobuf
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protobuf-cpp-3.21.8.tar.gz
        URL_HASH SHA256=f6251f2d00aad41b34c1dfa3d752713cb1bb1b7020108168a4deaa206ba8ed42
        )
FetchContent_MakeAvailable(protobuf)
FetchContent_GetProperties(protobuf)
find_package(protobuf REQUIRED CONFIG)

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
FetchContent_Declare(re2
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/google/re2/archive/refs/tags/2022-06-01.tar.gz
        URL_HASH SHA256=f89c61410a072e5cbcf8c27e3a778da7d6fd2f2b5b1445cd4f4508bee946ab0f

        CMAKE_ARGS
        )
FetchContent_MakeAvailable(re2)
FetchContent_GetProperties(re2)
find_package(re2 REQUIRED CONFIG)

FetchContent_Declare(zlib
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/madler/zlib/releases/download/v1.2.13/zlib-1.2.13.tar.gz
        URL_HASH SHA256=b3a24de97a8fdbc835b9833169501030b8977031bcb54b3b3ac13740f846ab30
        )
FetchContent_MakeAvailable(zlib)
FetchContent_GetProperties(zlib)
find_package(zlib REQUIRED CONFIG)


set(gRPC_INSTALL ON)
set(gRPC_BUILD_TESTS OFF)
set(gRPC_PROTOBUF_PROVIDER package)
set(gRPC_RE2_PROVIDER package)
set(gRPC_ZLIB_PROVIDER package)
set(gRPC_ABSL_PROVIDER package)
set(gRPC_CARES_PROVIDER package)
set(gRPC_SSL_PROVIDER package)

FetchContent_Declare(grpc
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/grpc/grpc/archive/refs/tags/v1.50.0.tar.gz
        URL_HASH SHA256=76900ab068da86378395a8e125b5cc43dfae671e09ff6462ddfef18676e2165a
        )
set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(grpc)
FetchContent_GetProperties(grpc)

set_property(GLOBAL PROPERTY protobuf_SOURCE_DIR "${protobuf_SOURCE_DIR}")
