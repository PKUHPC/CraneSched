include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(BS_THREAD_POOL_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/thread-pool-3.1.0.tar.gz")
#    MESSAGE(FATAL_ERROR "Thread pool library haven't been synchronized to gitee. Set CRANE_USE_GITEE_SOURCE to OFF.")
else ()
    set(BS_THREAD_POOL_SRC_URL "https://github.com/bshoshany/thread-pool/archive/refs/tags/v4.0.1.tar.gz")
endif ()

FetchContent_Declare(bs_thread_pool_src
        URL ${BS_THREAD_POOL_SRC_URL}
        URL_HASH SHA256=34d25503e17e58198613d9306313f8358cfaaa9320f2b7694dc599b90a1858a7
        INACTIVITY_TIMEOUT 5
)

FetchContent_GetProperties(bs_thread_pool_src)
if (NOT bs_thread_pool_src_POPULATED)
    FetchContent_Populate(bs_thread_pool_src)

    add_library(bs_thread_pool INTERFACE)
    target_include_directories(bs_thread_pool INTERFACE ${bs_thread_pool_src_SOURCE_DIR}/include)
    target_link_libraries(bs_thread_pool INTERFACE Threads::Threads)

    message(STATUS "BSThreadPool Installed. Dir: ${bs_thread_pool_src_SOURCE_DIR}")
endif ()