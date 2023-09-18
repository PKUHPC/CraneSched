include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(BS_THREAD_POOL_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/thread-pool-3.1.0.tar.gz")
else ()
    set(BS_THREAD_POOL_SRC_URL "https://github.com/bshoshany/thread-pool/archive/refs/tags/v3.1.0.tar.gz")
endif ()

FetchContent_Declare(bs_thread_pool_src
        URL ${BS_THREAD_POOL_SRC_URL}
        URL_HASH SHA256=c8b70ee63822ee21f25c201806e464100686b1faca5aeabeee96378d2a2c3be8
        INACTIVITY_TIMEOUT 5
        )

FetchContent_GetProperties(bs_thread_pool_src)
if (NOT bs_thread_pool_src_POPULATED)
    FetchContent_Populate(bs_thread_pool_src)

    add_library(bs_thread_pool INTERFACE)
    target_include_directories(bs_thread_pool INTERFACE ${bs_thread_pool_src_SOURCE_DIR})
    target_link_libraries(bs_thread_pool INTERFACE Threads::Threads)

    message(STATUS "BSThreadPool Installed. Dir: ${bs_thread_pool_src_SOURCE_DIR}")
endif ()