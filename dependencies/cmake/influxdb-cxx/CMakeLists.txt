cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)

# 获取influxdb-cpp头文件库
FetchContent_Declare(
    influxdb_cpp_header
    GIT_REPOSITORY https://github.com/orca-zhang/influxdb-cpp.git
    GIT_TAG master  # 或者指定具体的commit hash以确保稳定性
)

FetchContent_GetProperties(influxdb_cpp_header)
if(NOT influxdb_cpp_header_POPULATED)
    FetchContent_Populate(influxdb_cpp_header)
    
    # 创建一个INTERFACE库
    add_library(influxdb_cpp INTERFACE)
    target_include_directories(influxdb_cpp INTERFACE 
        ${influxdb_cpp_header_SOURCE_DIR}
    )
endif() 