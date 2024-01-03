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
        event_core_static
        event_pthreads_static
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

include(GoogleTest)
gtest_discover_tests(craned_test)