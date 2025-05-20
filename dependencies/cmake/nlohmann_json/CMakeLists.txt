include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(JSON_SRC_URL
        "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/json-3.11.2.tar.gz")
else()
    set(JSON_SRC_URL
        "https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz")
endif()

FetchContent_Declare(json
        URL                ${JSON_SRC_URL}
        URL_HASH           SHA256=42F6E95CAD6EC532FD372391373363B62A14AF6D771056DBFC86160E6DFFF7AA
        INACTIVITY_TIMEOUT 5
)

if(POLICY CMP0169)
    cmake_policy(SET CMP0169 NEW)
endif()

FetchContent_MakeAvailable(json)