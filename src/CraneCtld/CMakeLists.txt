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
        CraneCtld.cpp
        )

target_link_libraries(cranectld
        spdlog::spdlog
        concurrentqueue

        Utility_AnonymousPipe
        Utility_cgroup
        Utility_PublicHeader
        Utility_TimerSet

        Boost::boost
        Boost::thread
        Boost::filesystem

        libevent::core
        libevent::pthreads

        cxxopts
        Threads::Threads

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        crane_proto_lib

        yaml-cpp
        mariadbclient
        mongo::mongocxx_static
        )

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(cranectld stdc++fs)
endif ()
