add_executable(utility_test TimerSet_test.cpp cgroup_limit_test.cpp network_function_test.cpp PublicHeader_test.cpp)
target_link_libraries(utility_test
        GTest::gtest
        GTest::gtest_main
        concurrentqueue
		Boost::boost
		Threads::Threads
		event_core
		cxxopts
        absl::synchronization

        Utility_cgroup
        Utility_PublicHeader
        Utility_AnonymousPipe
        Utility_TimerSet
        crane_proto_lib

        test_proto
        shared_test_impl_lib
        )
