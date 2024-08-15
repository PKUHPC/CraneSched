add_executable(utility_test
        dedicated_resource_test.cpp)
target_link_libraries(utility_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        Threads::Threads
        event_core_static
        cxxopts
        absl::synchronization

        Utility_PublicHeader
        crane_proto_lib

        shared_test_impl_lib
        )
