include(FetchContent)

FetchContent_Declare(
        libuv
        GIT_REPOSITORY https://github.com/libuv/libuv.git
        GIT_TAG v1.44.2
        GIT_SHALLOW 1
)

FetchContent_GetProperties(libuv)

if (NOT libuv_POPULATED)
    FetchContent_Populate(libuv)
    add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()

add_library(uv::uv-static ALIAS uv_a)
set_target_properties(uv_a PROPERTIES POSITION_INDEPENDENT_CODE 1)