add_executable(craned
        CtldClient.h
        CtldClient.cpp
        CgroupManager.h
        CgroupManager.cpp
        CforedClient.h
        CforedClient.cpp
        TaskManager.h
        TaskManager.cpp
        CranedServer.h
        CranedServer.cpp
        CranedPublicDefs.h
        Craned.cpp

        CranedPreCompiledHeader.h
        DeviceManager.cpp
        DeviceManager.h)

target_precompile_headers(craned PRIVATE CranedPreCompiledHeader.h)
add_dependencies(craned libcgroup)
target_include_directories(
        craned PRIVATE ${LIBCGROUP_PATH}/include/
)



if(ENABLE_BPF)
        add_dependencies(craned cgroup_dev_bpf_object)
        add_compile_definitions(craned PRIVATE CRANE_ENABLE_BPF)
endif()
if(CRANE_USE_MIMALLOC)
        add_compile_definitions(craned PRIVATE MI_OVERRIDE=ON)
        message("Override malloc with mimalloc in craned")
endif()
target_link_libraries(craned
        concurrentqueue

        ${LIBCGROUP_BUILD_PRODUCTS}

        Utility_PublicHeader
        Utility_PluginClient

        crane_proto_lib

        dev_event_core
        dev_event_pthreads

        bs_thread_pool

        cxxopts
        Threads::Threads
        nlohmann_json::nlohmann_json

        absl::flat_hash_map
        absl::synchronization

        uvw

        yaml-cpp
        result

        Backward::Interface

        $<$<BOOL:${ENABLE_BPF}>:bpf>
        
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:mimalloc>
)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned stdc++fs)
endif ()