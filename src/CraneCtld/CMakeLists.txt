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
        CraneCtld.cpp

        CtldPreCompiledHeader.h)

target_precompile_headers(cranectld PRIVATE CtldPreCompiledHeader.h)

target_link_libraries(cranectld PRIVATE
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>

        spdlog::spdlog
        concurrentqueue

        Utility_PublicHeader
        Utility_PluginClient

        uvw

        cxxopts
        Threads::Threads

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        phmap

        crane_proto_lib

        bs_thread_pool

        yaml-cpp
        mongocxx_static

        range-v3::range-v3
        result

        Backward::Interface
)

if (ENABLE_BERKELEY_DB AND BERKELEYDB_FOUND)
    target_compile_definitions(cranectld PRIVATE CRANE_HAVE_BERKELEY_DB)
    target_include_directories(cranectld PRIVATE ${BERKELEY_DB_INCLUDE_DIR})
    target_link_libraries(cranectld PRIVATE ${BERKELEY_DB_CXX_LIBRARIES})
endif ()

if (ENABLE_UNQLITE)
    target_compile_definitions(cranectld PRIVATE CRANE_HAVE_UNQLITE)
    target_link_libraries(cranectld PRIVATE unqlite)
endif ()


# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(cranectld PRIVATE stdc++fs)
endif ()
