include(GoogleTest)

add_executable(group_resolver_test GroupResolverTest.cpp)
target_link_libraries(group_resolver_test
        GTest::gtest
        GTest::gtest_main
        craned_common)
gtest_discover_tests(group_resolver_test)
