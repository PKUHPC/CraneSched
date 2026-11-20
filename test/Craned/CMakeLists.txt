include(GoogleTest)

add_executable(execution_groups_test ExecutionGroupsTest.cpp)
target_link_libraries(execution_groups_test
        GTest::gtest
        GTest::gtest_main
        Utility_PublicHeader
        Utility_PasswordEntry)
gtest_discover_tests(execution_groups_test)

add_executable(oom_monitor_test OomMonitorTest.cpp)
target_link_libraries(oom_monitor_test PRIVATE
        GTest::gtest GTest::gtest_main craned_common)
gtest_discover_tests(oom_monitor_test)

add_executable(task_manager_test
        TaskManagerTest.cpp
        OomCgroupIntegrationTest.cpp
        ../../src/Craned/Supervisor/CranedClient.cpp
        ../../src/Craned/Supervisor/CforedClient.cpp
        ../../src/Craned/Supervisor/SupervisorServer.cpp)
target_link_libraries(task_manager_test
        GTest::gtest
        GTest::gtest_main
        craned_common
        Utility_CriClient
        Utility_BindFs
        Utility_Pmix)
gtest_discover_tests(task_manager_test)
