# Create a static library for common components
add_library(craned_common STATIC
    CranedPreCompiledHeader.h
    DeviceManager.h
    DeviceManager.cpp
    CgroupManager.h
    CgroupManager.cpp
)

# Set the precompiled header for the common library
target_precompile_headers(craned_common PRIVATE CranedPreCompiledHeader.h)

# Include directories for the common library
target_include_directories(craned_common 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${LIBCGROUP_PATH}/include/
)

# Link dependencies for the common library
target_link_libraries(craned_common
    PUBLIC
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>
        concurrentqueue
        
        ${LIBCGROUP_BUILD_PRODUCTS}
        
        Utility_PublicHeader
        Utility_PluginClient
        
        crane_proto_lib
        
        bs_thread_pool
        
        cxxopts
        Threads::Threads
        ${LIBUTIL_LIBRARY}
        nlohmann_json::nlohmann_json
        
        absl::flat_hash_map
        absl::synchronization
        
        uvw
        yaml-cpp
        subprocess
        
        Backward::Interface
        
        $<$<BOOL:${CRANE_ENABLE_BPF}>:BPF::BPF>
        $<$<BOOL:${ENABLE_BPF}>:bpf>
)

# Compile definitions
if (CRANE_ENABLE_CGROUP_V2)
    target_compile_definitions(craned_common PUBLIC -DCRANE_ENABLE_CGROUP_V2)
endif ()

if (CRANE_ENABLE_BPF)
    target_compile_definitions(craned_common PUBLIC -DCRANE_ENABLE_BPF)
endif ()

# Dependencies
add_dependencies(craned_common libcgroup)
if (CRANE_ENABLE_BPF)
    add_dependencies(craned_common crane_ebpf)
endif ()

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned_common PUBLIC stdc++fs)
endif ()
