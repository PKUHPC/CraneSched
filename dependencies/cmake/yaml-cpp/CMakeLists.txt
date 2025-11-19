cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)
if (BUILD_SHARED_LIBS)
    set(YAML_BUILD_SHARED_LIBS ON)
else ()
    set(YAML_BUILD_SHARED_LIBS OFF)
endif ()

set(YAML_CPP_FORMAT_SOURCE OFF)

FetchContent_Declare(
        yaml-cpp
        GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
        GIT_TAG a83cd31548b19d50f3f983b069dceb4f4d50756d
        GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(yaml-cpp)