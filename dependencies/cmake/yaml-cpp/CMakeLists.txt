cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(YAML_CPP_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/yaml-cpp-yaml-cpp-0.7.0.tar.gz")
else ()
    set(YAML_CPP_SRC_URL "https://github.com/jbeder/yaml-cpp/archive/refs/tags/yaml-cpp-0.7.0.tar.gz")
endif ()

FetchContent_Declare(yaml_cpp
        URL ${YAML_CPP_SRC_URL}
        URL_HASH SHA256=43e6a9fcb146ad871515f0d0873947e5d497a1c9c60c58cb102a97b47208b7c3
        INACTIVITY_TIMEOUT 5
        )

FetchContent_GetProperties(yaml_cpp)
if (NOT yaml_cpp_POPULATED)
    FetchContent_Populate(yaml_cpp)
    add_subdirectory(${yaml_cpp_SOURCE_DIR} ${yaml_cpp_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()