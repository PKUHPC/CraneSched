include(FetchContent)

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

if (CRANE_USE_GITEE_SOURCE)
    set(FMT_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/fmt-9.1.0.zip")
else ()
    set(FMT_SRC_URL "https://github.com/fmtlib/fmt/releases/download/9.1.0/fmt-9.1.0.zip")
endif ()

FetchContent_Declare(fmt
        URL ${FMT_SRC_URL}
        URL_HASH SHA256=cceb4cb9366e18a5742128cb3524ce5f50e88b476f1e54737a47ffdf4df4c996
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(fmt)