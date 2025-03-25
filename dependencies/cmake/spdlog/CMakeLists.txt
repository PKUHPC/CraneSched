include(FetchContent)

if (BUILD_SHARED_LIBS)
    set(SPDLOG_BUILD_SHARED ON)
else ()
    set(SPDLOG_BUILD_SHARED OFF)
endif ()

if (CRANE_USE_GITEE_SOURCE)
    set(SPDLOG_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/spdlog-v1.13.0.tar.gz")
else ()
    set(SPDLOG_SRC_URL "https://github.com/gabime/spdlog/archive/refs/tags/v1.15.1.tar.gz")
endif ()

FetchContent_Declare(spdlog
        URL ${SPDLOG_SRC_URL}
        URL_HASH SHA256=25C843860F039A1600F232C6EB9E01E6627F7D030A2AE5E232BDD3C9205D26CC
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(spdlog)