include(FetchContent)

set(BUILD_ACCURACY OFF)
set(BUILD_BENCHMARK OFF)
set(BUILD_TESTS OFF)
FetchContent_Declare(fpm
        GIT_REPOSITORY https://github.com/MikeLankamp/fpm
        GIT_TAG aca475058b762e81a61495473a74fc8e48915fc6
        GIT_SHALLOW TRUE
        SYSTEM
)
FetchContent_MakeAvailable(fpm)
