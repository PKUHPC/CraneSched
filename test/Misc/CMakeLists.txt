# Add Linker flag for c++ 17 filesystem library in this folder.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    link_libraries(stdc++fs)
endif ()

include(GoogleTest)

add_executable(misc_mongodb_test mongodb_test.cpp)
target_link_libraries(misc_mongodb_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
        mongocxx_static
        spdlog::spdlog
        absl::btree
        Utility_PublicHeader
)
target_include_directories(misc_mongodb_test PRIVATE ${CMAKE_SOURCE_DIR}/src/CraneCtld)
gtest_discover_tests(misc_mongodb_test)

add_executable(misc_unqlite_test unqlite_test.cpp)
target_link_libraries(misc_unqlite_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
        spdlog::spdlog
        unqlite
        test_proto
)
gtest_discover_tests(misc_unqlite_test)

add_executable(misc_cgroup_v2_fs_test cgroup_v2_fs_test.cpp)
target_link_libraries(misc_cgroup_v2_fs_test
        GTest::gtest GTest::gtest_main
        craned_common
)
gtest_discover_tests(misc_cgroup_v2_fs_test)

add_executable(misc_task_lifecycle_test task_lifecycle_test.cpp)
target_include_directories(misc_task_lifecycle_test
        PRIVATE ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor)
target_link_libraries(misc_task_lifecycle_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
)
gtest_discover_tests(misc_task_lifecycle_test)

if (CRANE_ENABLE_TRACING)
    add_executable(misc_execution_flow_test execution_flow_test.cpp)
    target_link_libraries(misc_execution_flow_test
            GTest::gtest GTest::gtest_main
            crane_tracer
    )
    gtest_discover_tests(misc_execution_flow_test)

    add_executable(misc_tracer_manager_test tracer_manager_test.cpp)
    target_link_libraries(misc_tracer_manager_test
            GTest::gtest GTest::gtest_main
            crane_tracer
    )
    gtest_discover_tests(misc_tracer_manager_test)
endif ()
