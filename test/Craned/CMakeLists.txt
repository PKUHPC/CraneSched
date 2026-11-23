include(GoogleTest)

add_executable(execution_groups_test ExecutionGroupsTest.cpp)
target_link_libraries(execution_groups_test
        GTest::gtest
        GTest::gtest_main
        Utility_PublicHeader
        Utility_PasswordEntry)
gtest_discover_tests(execution_groups_test)

add_executable(task_manager_test
        TaskManagerTest.cpp
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

# Exercise the production completion polling and config parser. The tests
# include JobManager.cpp and Craned.cpp; link the remaining Craned sources.
get_target_property(job_manager_sources craned SOURCES)
list(FILTER job_manager_sources EXCLUDE REGEX "(^|/)(Craned|JobManager)\\.cpp$")
list(TRANSFORM job_manager_sources PREPEND "${PROJECT_SOURCE_DIR}/src/Craned/Core/")
get_target_property(job_manager_libraries craned LINK_LIBRARIES)
add_executable(job_manager_test
        JobManagerTest.cpp
        CranedConfigTest.cpp
        ${job_manager_sources})
target_link_libraries(job_manager_test PRIVATE
        GTest::gtest GTest::gtest_main ${job_manager_libraries})
gtest_discover_tests(job_manager_test)
