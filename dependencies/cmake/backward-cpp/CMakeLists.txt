include(FetchContent)

if (BUILD_SHARED_LIBS)
    set(BACKWARD_SHARED ON)
else ()
    set(BACKWARD_SHARED OFF)
endif ()

FetchContent_Declare(backward
        GIT_REPOSITORY https://github.com/bombela/backward-cpp
        GIT_TAG 0bfd0a07a61551413ccd2ab9a9099af3bad40681  # or a version tag, such as v1.6
        GIT_SHALLOW TRUE
        SYSTEM          # optional, the Backward include directory will be treated as system directory
)
FetchContent_MakeAvailable(backward)