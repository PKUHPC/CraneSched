add_executable(craned_test
        ${CMAKE_SOURCE_DIR}/src/Craned/TaskManager.cpp
        ${CMAKE_SOURCE_DIR}/src/Craned/ResourceAllocators.cpp
        ${CMAKE_SOURCE_DIR}/src/Craned/CtldClient.cpp
        TaskManager_test.cpp)
target_link_libraries(craned_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        spdlog::spdlog
        libevent::core
        libevent::pthreads
        cxxopts
        bs_thread_pool

        Threads::Threads

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        crane_proto_lib

        test_proto
        shared_test_impl_lib
        )
