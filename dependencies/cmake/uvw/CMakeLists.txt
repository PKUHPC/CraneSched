include(FetchContent)

set(BUILD_UVW_LIBS ON)
if (BUILD_SHARED_LIBS)
    set(BUILD_UVW_SHARED_LIB ON)
else ()
    set(BUILD_UVW_SHARED_LIB OFF)
endif ()

set(USE_LIBCPP OFF)
set(BUILD_TESTING OFF)

if (CRANE_USE_GITEE_SOURCE)
    set(UVW_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/v3.4.0_libuv_v1.48.tar.gz")
else ()
    set(UVW_SRC_URL "https://github.com/skypjack/uvw/archive/refs/tags/v3.4.0_libuv_v1.48.tar.gz")
endif ()

FetchContent_Declare(
        uvw
        URL ${UVW_SRC_URL}
        URL_HASH SHA256=C16600573871A5FEEB524234B378AB832C8971B2A68D030C6BD0E3077D416ADE
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(uvw)