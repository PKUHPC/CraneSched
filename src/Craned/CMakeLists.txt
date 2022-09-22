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
        )

target_link_libraries(craned
        spdlog::spdlog
        concurrentqueue

        Utility_AnonymousPipe
        Utility_cgroup
        Utility_PublicHeader

        crane_proto_lib
        Boost::boost
        Boost::filesystem

        libevent::core
        libevent::pthreads

        bs_thread_pool

        cxxopts
        Threads::Threads
        nlohmann_json::nlohmann_json

        absl::flat_hash_map
        absl::synchronization

        yaml-cpp
        )

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned stdc++fs)
endif ()