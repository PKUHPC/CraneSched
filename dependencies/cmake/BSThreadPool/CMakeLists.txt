include(FetchContent)
FetchContent_Declare(bs_thread_pool_src
        GIT_REPOSITORY https://github.com/bshoshany/thread-pool
        GIT_TAG v3.1.0)

FetchContent_GetProperties(bs_thread_pool_src)
if (NOT bs_thread_pool_src_POPULATED)
    FetchContent_Populate(bs_thread_pool_src)

    add_library(bs_thread_pool INTERFACE)
    target_include_directories(bs_thread_pool INTERFACE ${bs_thread_pool_src_SOURCE_DIR})
    target_link_libraries(bs_thread_pool INTERFACE Threads::Threads)

    message(STATUS "BSThreadPool Installed. Dir: ${bs_thread_pool_src_SOURCE_DIR}")
endif ()