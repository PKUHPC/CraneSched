list(APPEND CRANECTLD_TEST_SRC
        NodeSelectionAlgoTest.cpp
        server_keep_alive_test.cpp
        XdNodeKeeper_test.cpp
        XdNodeMetaContainer_test.cpp)

# Source files of CraneCtld without 'main' symbol
list(APPEND CRANECTLD_SRC_NO_MAIN
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPublicDefs.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldGrpcServer.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldGrpcServer.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/DbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/DbClient.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/TaskScheduler.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/TaskScheduler.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CranedKeeper.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CranedKeeper.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CranedMetaContainer.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CranedMetaContainer.cpp)

# Source files of Craned without 'main' symbol
list(APPEND CRANED_SRC_NO_MAIN
        ${PROJECT_SOURCE_DIR}/src/Craned/CtldClient.h
        ${PROJECT_SOURCE_DIR}/src/Craned/CtldClient.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/ResourceAllocators.h
        ${PROJECT_SOURCE_DIR}/src/Craned/ResourceAllocators.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/TaskManager.h
        ${PROJECT_SOURCE_DIR}/src/Craned/TaskManager.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/CranedServer.h
        ${PROJECT_SOURCE_DIR}/src/Craned/CranedServer.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/CranedPublicDefs.h)

add_executable(cranectld_test
        ${CRANECTLD_TEST_SRC}
        ${CRANECTLD_SRC_NO_MAIN}
        ${CRANED_SRC_NO_MAIN}
        )
target_include_directories(cranectld_test PUBLIC
        ${CMAKE_SOURCE_DIR}/src/CraneCtld
        ${CMAKE_SOURCE_DIR}/src/Craned
        )

target_link_libraries(cranectld_test
        GTest::gtest
        GTest::gmock
        GTest::gmock_main

        concurrentqueue
        Threads::Threads
        libevent::core
        cxxopts
        Boost::boost
        Boost::thread
        Boost::fiber

        mariadbclient
        mongo::mongocxx_static
        bs_thread_pool
        yaml-cpp

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        crane_proto_lib

        test_proto
        shared_test_impl_lib
        )
