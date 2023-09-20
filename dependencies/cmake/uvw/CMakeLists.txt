include(FetchContent)

set(BUILD_UVW_LIBS ON)
set(USE_LIBCPP OFF)
set(BUILD_TESTING OFF)

if (CRANE_USE_GITEE_SOURCE)
    FetchContent_Declare(uvw
            URL https://gitee.com/zenglingbo/crane-sched-deps/raw/master/uvw-3.2.0_libuv_v1.46.tar.gz
            URL_HASH SHA256=bd5aed741765950074b1ea2507291dce81e528abdf56c406991ad4a27d8d1714
            INACTIVITY_TIMEOUT 5

            PATCH_COMMAND patch --forward -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/uvw.diff || true
            )
else ()
    FetchContent_Declare(uvw
            URL https://github.com/skypjack/uvw/archive/refs/tags/v3.2.0_libuv_v1.46.tar.gz
            URL_HASH SHA256=bd5aed741765950074b1ea2507291dce81e528abdf56c406991ad4a27d8d1714
            INACTIVITY_TIMEOUT 5
            )
endif ()

FetchContent_MakeAvailable(uvw)