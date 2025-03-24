add_executable(cranectld
        CtldPublicDefs.h
        CtldGrpcServer.h
        CtldGrpcServer.cpp
        DbClient.h
        DbClient.cpp
        TaskScheduler.h
        TaskScheduler.cpp
        CranedKeeper.h
        CranedKeeper.cpp
        CranedMetaContainer.h
        CranedMetaContainer.cpp
        AccountManager.h
        AccountManager.cpp
        AccountMetaContainer.h
        AccountMetaContainer.cpp
        EmbeddedDbClient.cpp
        EmbeddedDbClient.h

        Security/VaultClient.cpp
        Security/VaultClient.h

        CraneCtld.cpp

        CtldPreCompiledHeader.h
        CraneStateMachine.cpp
        CraneStateMachine.h
        RaftServerStuff.cpp
        RaftServerStuff.h
)

target_precompile_headers(cranectld PRIVATE CtldPreCompiledHeader.h)

target_include_directories(cranectld PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(cranectld PRIVATE
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>

        spdlog::spdlog
        concurrentqueue

        Utility_PublicHeader
        Utility_PluginClient
        Utility_NuRaft
        Utility_EmbeddedDb

        uvw

        cxxopts
        Threads::Threads

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        phmap
        curl
        vault
        nlohmann_json

        crane_proto_lib

        bs_thread_pool

        yaml-cpp
        mongocxx_static

        range-v3::range-v3

        Backward::Interface
)

if (CRANE_RAFT_SERVER)
    target_compile_definitions(cranectld PRIVATE CRANE_WITH_RAFT)
endif ()


# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(cranectld PRIVATE stdc++fs)
endif ()
