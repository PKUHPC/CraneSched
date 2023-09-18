include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(SPDLOG_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/spdlog-1.10.0.tar.gz")
else ()
    set(SPDLOG_SRC_URL "https://github.com/gabime/spdlog/archive/refs/tags/v1.10.0.tar.gz")
endif ()

FetchContent_Declare(spdlog
        URL ${SPDLOG_SRC_URL}
        URL_HASH SHA256=697f91700237dbae2326b90469be32b876b2b44888302afbc7aceb68bcfe8224
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(spdlog)