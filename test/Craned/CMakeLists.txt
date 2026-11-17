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
        TaskManagerLifecycleTest.cpp
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
        CtldStepCleanupAssertions.cpp
        ../../src/Craned/Core/JobManager.cpp
        ../../src/Craned/Core/StepInstance.cpp
        ../../src/Craned/Core/SupervisorStub.cpp
        ../../src/CraneCtld/CtldPublicDefs.cpp
        ../../src/CraneCtld/Database/EmbeddedDbClient.cpp
        ../../src/CraneCtld/Database/LegacyEmbeddedDbClient.cpp
        ../CraneCtld/CtldTestStubs.cpp)
target_include_directories(job_manager_lifecycle_test PRIVATE
        ../../src/Craned/Core
        ../../src/CraneCtld)
target_link_libraries(job_manager_lifecycle_test
        GTest::gtest
        GTest::gtest_main
        craned_common
        Utility_CriClient
        Utility_PrologEpilogExecutor
        phmap
        cxxopts
        range-v3::range-v3
        absl::btree
        unqlite)
if (ENABLE_UNQLITE)
    target_compile_definitions(job_manager_lifecycle_test PRIVATE CRANE_HAVE_UNQLITE)
endif ()
gtest_discover_tests(job_manager_lifecycle_test PROPERTIES TIMEOUT 30)
