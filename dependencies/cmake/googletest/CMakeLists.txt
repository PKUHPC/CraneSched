include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(GTEST_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/googletest-release-1.12.1.tar.gz")
else ()
    set(GTEST_SRC_URL "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz")
endif ()

FetchContent_Declare(GTest
        OVERRIDE_FIND_PACKAGE

        URL ${GTEST_SRC_URL}
        URL_HASH SHA256=81964fe578e9bd7c94dfdb09c8e4d6e6759e19967e397dbea48d1c10e45d0df2
        INACTIVITY_TIMEOUT 5
        )

FetchContent_MakeAvailable(GTest)