include(GoogleTest)

add_executable(execution_groups_test ExecutionGroupsTest.cpp)
target_link_libraries(execution_groups_test
        GTest::gtest
        GTest::gtest_main
        Utility_PublicHeader
        Utility_PasswordEntry)
gtest_discover_tests(execution_groups_test)

add_executable(container_identity_test
        ContainerIdentityTest.cpp
        TaskEnvironmentTest.cpp
        ../../src/Craned/Supervisor/CranedClient.cpp
        ../../src/Craned/Supervisor/CforedClient.cpp
        ../../src/Craned/Supervisor/SupervisorServer.cpp)
target_link_libraries(container_identity_test
        GTest::gtest
        GTest::gtest_main
        craned_common
        Utility_CriClient
        Utility_BindFs
        Utility_Pmix)
gtest_discover_tests(container_identity_test)

add_executable(job_manager_lifecycle_test
        JobManagerLifecycleTest.cpp
        ../../src/Craned/Core/JobManager.cpp
        ../../src/Craned/Core/StepInstance.cpp
        ../../src/Craned/Core/SupervisorStub.cpp)
target_include_directories(job_manager_lifecycle_test PRIVATE
        ../../src/Craned/Core)
target_link_libraries(job_manager_lifecycle_test
        GTest::gtest
        GTest::gtest_main
        craned_common
        Utility_CriClient)
gtest_discover_tests(job_manager_lifecycle_test PROPERTIES TIMEOUT 30)
