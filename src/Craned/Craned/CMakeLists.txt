add_executable(craned
        CtldClient.h
        CtldClient.cpp
        CgroupManager.h
        CgroupManager.cpp
        JobManager.h
        JobManager.cpp
        CranedServer.h
        CranedServer.cpp
        CranedPublicDefs.h
        Craned.cpp

        CranedPreCompiledHeader.h
        DeviceManager.cpp
        DeviceManager.h
        SupervisorKeeper.cpp
        SupervisorKeeper.h)

target_precompile_headers(craned PRIVATE CranedPreCompiledHeader.h)
add_dependencies(craned libcgroup)
target_include_directories(
        craned PRIVATE ${LIBCGROUP_PATH}/include/
)


if (ENABLE_BPF)
    add_dependencies(craned cgroup_dev_bpf_object)
    target_compile_definitions(craned PRIVATE CRANE_ENABLE_BPF)
endif ()

target_link_libraries(craned
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

        Backward::Interface

        $<$<BOOL:${ENABLE_BPF}>:bpf>

)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned stdc++fs)
endif ()