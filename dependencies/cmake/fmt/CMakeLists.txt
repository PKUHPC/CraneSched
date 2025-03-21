include(FetchContent)

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

if (CRANE_USE_GITEE_SOURCE)
    set(FMT_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/fmt-9.1.0.zip")
else ()
    set(FMT_SRC_URL "https://github.com/fmtlib/fmt/releases/download/11.1.4/fmt-11.1.4.zip")
endif ()

FetchContent_Declare(fmt
        URL ${FMT_SRC_URL}
        URL_HASH SHA256=49B039601196E1A765E81C5C9A05A61ED3D33F23B3961323D7322E4FE213D3E6
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(fmt)