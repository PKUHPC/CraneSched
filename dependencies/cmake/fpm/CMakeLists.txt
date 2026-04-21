include(FetchContent)

set(BUILD_ACCURACY OFF)
set(BUILD_BENCHMARK OFF)
set(BUILD_TESTS OFF)
FetchContent_Declare(fpm
        GIT_REPOSITORY https://github.com/MikeLankamp/fpm
        GIT_TAG b46537fe9697e1a598ac8a26f8ae43d8b286ac3f
        GIT_SHALLOW TRUE
        SYSTEM
)
FetchContent_MakeAvailable(fpm)
