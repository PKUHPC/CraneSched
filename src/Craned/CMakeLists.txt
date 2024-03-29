add_executable(craned
        CtldClient.h
        CtldClient.cpp
        ResourceAllocators.h
        ResourceAllocators.cpp
        TaskManager.h
        TaskManager.cpp
        CranedServer.h
        CranedServer.cpp
        CranedPublicDefs.h
        Craned.cpp

        CranedPreCompiledHeader.h)

target_precompile_headers(craned PRIVATE CranedPreCompiledHeader.h)

if ((NOT ${CRANE_ADDRESS_SANITIZER}) OR (NOT ${CRANE_THREAD_SANITIZER}))
    target_link_libraries(craned PRIVATE mimalloc)
endif() 

target_link_libraries(craned PRIVATE
        spdlog::spdlog
        concurrentqueue

        Utility_AnonymousPipe
        Utility_cgroup
        Utility_PublicHeader

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
)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned stdc++fs)
endif ()