add_executable(csupervisor
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

target_precompile_headers(csupervisor PRIVATE SupervisorPreCompiledHeader.h)

target_link_libraries(csupervisor
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

        nlohmann_json::nlohmann_json
        
        uvw
        subprocess

        Backward::Interface

        $<$<BOOL:${ENABLE_BPF}>:bpf>

)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(csupervisor stdc++fs)
endif ()