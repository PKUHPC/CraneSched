include(FetchContent)

set(BUILD_UVW_LIBS ON)
if (BUILD_SHARED_LIBS)
    set(BUILD_UVW_SHARED_LIB ON)
else ()
    set(BUILD_UVW_SHARED_LIB OFF)
endif ()

set(USE_LIBCPP OFF)
set(BUILD_TESTING OFF)

FetchContent_Declare(
        uvw
        GIT_REPOSITORY https://github.com/skypjack/uvw.git
        GIT_TAG d32ddd2
)
FetchContent_MakeAvailable(uvw)