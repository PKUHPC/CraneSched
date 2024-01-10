add_executable(utility_test cgroup_limit_test.cpp network_function_test.cpp PublicHeader_test.cpp)
target_link_libraries(utility_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
        Threads::Threads
        event_core_static
        cxxopts
        absl::synchronization

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        crane_proto_lib

        test_proto
        shared_test_impl_lib
        )
