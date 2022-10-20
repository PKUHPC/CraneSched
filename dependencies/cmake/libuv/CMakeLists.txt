include(FetchContent)

FetchContent_Declare(libuv
        URL https://github.com/libuv/libuv/archive/refs/tags/v1.44.2.tar.gz
        URL_HASH SHA256=e6e2ba8b4c349a4182a33370bb9be5e23c51b32efb9b9e209d0e8556b73a48da
        INACTIVITY_TIMEOUT 5
        )

FetchContent_GetProperties(libuv)

if (NOT libuv_POPULATED)
    FetchContent_Populate(libuv)
    add_subdirectory(${libuv_SOURCE_DIR} ${libuv_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()

add_library(uv::uv-static ALIAS uv_a)
set_target_properties(uv_a PROPERTIES POSITION_INDEPENDENT_CODE 1)