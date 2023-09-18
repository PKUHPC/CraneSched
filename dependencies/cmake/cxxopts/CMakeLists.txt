include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(CXXOPTS_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/cxxopts-3.0.0.tar.gz")
else ()
    set(CXXOPTS_SRC_URL "https://github.com/jarro2783/cxxopts/archive/refs/tags/v3.0.0.tar.gz")
endif ()

FetchContent_Declare(cxxopts
        URL ${CXXOPTS_SRC_URL}
        URL_HASH SHA256=36f41fa2a46b3c1466613b63f3fa73dc24d912bc90d667147f1e43215a8c6d00
        INACTIVITY_TIMEOUT 5
        )

set(CXXOPTS_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CXXOPTS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CXXOPTS_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(CXXOPTS_ENABLE_WARNINGS OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(cxxopts)