include(GoogleTest)

add_executable(execution_groups_test ExecutionGroupsTest.cpp)
target_link_libraries(execution_groups_test
        GTest::gtest
        GTest::gtest_main
        craned_common)
gtest_discover_tests(execution_groups_test)

add_executable(container_identity_test ContainerIdentityTest.cpp)
target_link_libraries(container_identity_test
        GTest::gtest
        GTest::gtest_main
        craned_common)
gtest_discover_tests(container_identity_test)
