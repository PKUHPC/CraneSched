include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(JSON_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/json-3.11.2.tar.gz")
else ()
    set(JSON_SRC_URL "https://github.com/nlohmann/json/archive/refs/tags/v3.11.2.tar.gz")
endif ()

FetchContent_Declare(json
        URL ${JSON_SRC_URL}
        URL_HASH SHA256=d69f9deb6a75e2580465c6c4c5111b89c4dc2fa94e3a85fcd2ffcd9a143d9273
        INACTIVITY_TIMEOUT 5
        )

FetchContent_GetProperties(json)
if (NOT json_POPULATED)
    FetchContent_Populate(json)
    add_subdirectory(${json_SOURCE_DIR} ${json_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()