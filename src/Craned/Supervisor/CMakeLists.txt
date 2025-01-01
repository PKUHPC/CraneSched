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

target_link_libraries(supervisor
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>

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

        Backward::Interface

        $<$<BOOL:${ENABLE_BPF}>:bpf>

)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(supervisor stdc++fs)
endif ()