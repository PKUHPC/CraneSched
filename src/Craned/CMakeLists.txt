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

        CranedPreCompiledHeader.h)

target_precompile_headers(craned PRIVATE CranedPreCompiledHeader.h)

target_link_libraries(craned
        concurrentqueue

        PkgConfig::libcgroup
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