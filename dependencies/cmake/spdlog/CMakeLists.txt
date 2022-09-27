include(FetchContent)

FetchContent_Declare(spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG v1.10.0)
FetchContent_MakeAvailable(spdlog)

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    target_compile_definitions(spdlog PUBLIC -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
endif ()