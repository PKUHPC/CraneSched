cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)
if (BUILD_SHARED_LIBS)
    set(YAML_BUILD_SHARED_LIBS ON)
else ()
    set(YAML_BUILD_SHARED_LIBS OFF)
endif ()

set(YAML_CPP_FORMAT_SOURCE OFF)

if (CRANE_USE_GITEE_SOURCE)
    set(YAML_CPP_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/yaml-cpp-0.8.0.tar.gz")
else ()
    set(YAML_CPP_SRC_URL "https://github.com/jbeder/yaml-cpp/archive/refs/tags/0.8.0.tar.gz")
endif ()

FetchContent_Declare(yaml_cpp
        URL ${YAML_CPP_SRC_URL}
        URL_HASH SHA256=FBE74BBDCEE21D656715688706DA3C8BECFD946D92CD44705CC6098BB23B3A16
        INACTIVITY_TIMEOUT 5
        )

FetchContent_GetProperties(yaml_cpp)
if (NOT yaml_cpp_POPULATED)
    FetchContent_Populate(yaml_cpp)
    add_subdirectory(${yaml_cpp_SOURCE_DIR} ${yaml_cpp_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()