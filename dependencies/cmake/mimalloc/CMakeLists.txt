include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(MMLC_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/blob/master/mimalloc-v1.8.2.zip")
else ()
    set(MMLC_SRC_URL "https://github.com/microsoft/mimalloc/archive/refs/tags/v1.8.2.zip")
endif ()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
FetchContent_Declare(mimalloc
        OVERRIDE_FIND_PACKAGE
        URL ${MMLC_SRC_URL}
        URL_HASH SHA256=4a9d1d319609889c1f82a2a5f3d520c0061ccc6958a089bb33b398ce595d11d7
        INACTIVITY_TIMEOUT 5
)
FetchContent_MakeAvailable(mimalloc)

find_package(mimalloc REQUIRED CONFIG)


