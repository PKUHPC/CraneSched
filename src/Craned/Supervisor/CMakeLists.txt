add_executable(supervisor
        CranedClient.cpp
        CranedClient.h
        CforedClient.cpp
        CforedClient.h
        SupervisorServer.h
        SupervisorServer.cpp
        SupervisorPublicDefs.h
        Supervisor.cpp
        SupervisorPreCompiledHeader.h
        TaskManager.cpp
        TaskManager.h
)

target_precompile_headers(supervisor PRIVATE SupervisorPreCompiledHeader.h)

add_dependencies(supervisor libcgroup)
target_include_directories(
        supervisor PRIVATE ${LIBCGROUP_PATH}/include/
)

if (ENABLE_BPF)
    add_dependencies(supervisor cgroup_dev_bpf_object)
    target_compile_definitions(supervisor PRIVATE CRANE_ENABLE_BPF)
endif ()

target_link_libraries(supervisor
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>

        ${LIBCGROUP_BUILD_PRODUCTS}

        Utility_PublicHeader
        Utility_PluginClient

        crane_proto_lib

        bs_thread_pool

        cxxopts
        Threads::Threads
        ${LIBUTIL_LIBRARY}

        absl::flat_hash_map
        absl::synchronization

        uvw

        yaml-cpp

        Backward::Interface

        $<$<BOOL:${ENABLE_BPF}>:bpf>

)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(supervisor stdc++fs)
endif ()