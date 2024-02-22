include(FetchContent)

if (BUILD_SHARED_LIBS)
    set(SPDLOG_BUILD_SHARED ON)
else ()
    set(SPDLOG_BUILD_SHARED OFF)
endif ()

if (CRANE_USE_GITEE_SOURCE)
    set(SPDLOG_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/spdlog-v1.13.0.tar.gz")
else ()
    set(SPDLOG_SRC_URL "https://github.com/gabime/spdlog/archive/refs/tags/v1.13.0.tar.gz")
endif ()

FetchContent_Declare(spdlog
        URL ${SPDLOG_SRC_URL}
        URL_HASH SHA256=534F2EE1A4DCBEB22249856EDFB2BE76A1CF4F708A20B0AC2ED090EE24CFDBC9
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(spdlog)