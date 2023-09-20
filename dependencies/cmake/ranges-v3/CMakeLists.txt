include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(RANGES_V3_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/range-v3-0.12.0.tar.gz")
else ()
    set(RANGES_V3_SRC_URL "https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz")
endif ()

FetchContent_Declare(ranges_v3_repo
        URL ${RANGES_V3_SRC_URL}
        URL_HASH SHA256=015adb2300a98edfceaf0725beec3337f542af4915cec4d0b89fa0886f4ba9cb
        INACTIVITY_TIMEOUT 5
        )

FetchContent_MakeAvailable(ranges_v3_repo)
