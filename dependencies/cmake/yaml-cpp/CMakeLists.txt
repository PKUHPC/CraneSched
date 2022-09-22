cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)
FetchContent_Declare(yaml_cpp
        GIT_REPOSITORY https://github.com/jbeder/yaml-cpp
        GIT_TAG yaml-cpp-0.7.0)

FetchContent_GetProperties(yaml_cpp)
if (NOT yaml_cpp_POPULATED)
    FetchContent_Populate(yaml_cpp)
    add_subdirectory(${yaml_cpp_SOURCE_DIR} ${yaml_cpp_BINARY_DIR} EXCLUDE_FROM_ALL)
endif ()