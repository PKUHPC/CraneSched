include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(JSON_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/json-3.11.2.tar.gz")
else ()
    set(JSON_SRC_URL "https://github.com/nlohmann/json/archive/refs/tags/v3.11.2.tar.gz")
endif ()

set(JWT_CPP_SRC_URL "https://github.com/Thalhammer/jwt-cpp/archive/refs/tags/v0.7.0.tar.gz")

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

set(nlohmann_json_DIR  "${json_BINARY_DIR}")

fetchcontent_declare(jwt-cpp
        URL ${JWT_CPP_SRC_URL}
        URL_HASH SHA256=b9eb270e3ba8221e4b2bc38723c9a1cb4fa6c241a42908b9a334daff31137406
        INACTIVITY_TIMEOUT 5
        )
set(JWT_BUILD_EXAMPLES OFF CACHE BOOL "disable building examples" FORCE)

fetchcontent_makeavailable(jwt-cpp)