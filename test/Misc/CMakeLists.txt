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

# Compile the production cleanup implementation twice so the same behavioral
# contract runs with execution-flow instrumentation both compiled in and out.
# Linking craned_common supplies the implementation's normal usage requirements;
# the direct object is selected before the archive, so its CgroupManager symbols
# are the ones exercised by each test executable.
function(add_cgroup_cleanup_contract_variant variant flow_enabled)
    set(impl_target "cgroup_cleanup_contract_${variant}_impl")
    set(test_target "misc_cgroup_cleanup_contract_${variant}_test")

    add_library(${impl_target} OBJECT EXCLUDE_FROM_ALL
            "${PROJECT_SOURCE_DIR}/src/Craned/Common/CgroupManager.cpp")
    target_link_libraries(${impl_target} PRIVATE craned_common)

    add_executable(${test_target}
            cgroup_cleanup_contract_test.cpp
            $<TARGET_OBJECTS:${impl_target}>)
    target_link_libraries(${test_target}
            GTest::gtest GTest::gtest_main
            craned_common)

    foreach(target IN ITEMS ${impl_target} ${test_target})
        target_compile_options(${target} PRIVATE
                $<$<COMPILE_LANG_AND_ID:CXX,GNU,Clang>:-UCRANE_ENABLE_EXECUTION_FLOW>)
        if (flow_enabled)
            target_compile_options(${target} PRIVATE
                    $<$<COMPILE_LANG_AND_ID:CXX,GNU,Clang>:-DCRANE_ENABLE_EXECUTION_FLOW>)
        endif ()
        target_compile_definitions(${target} PRIVATE
                CRANE_CGROUP_CLEANUP_FLOW_EXPECTED=$<BOOL:${flow_enabled}>)
    endforeach()

    gtest_discover_tests(${test_target}
            TEST_PREFIX "execution_flow_cleanup.${variant}."
            PROPERTIES LABELS execution-flow-cleanup-matrix)
endfunction()

add_cgroup_cleanup_contract_variant(flow_on TRUE)
add_cgroup_cleanup_contract_variant(flow_off FALSE)

add_executable(misc_task_lifecycle_test task_lifecycle_test.cpp)
target_include_directories(misc_task_lifecycle_test
        PRIVATE ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor)
target_link_libraries(misc_task_lifecycle_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
        concurrentqueue
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

if (CRANE_ENABLE_EXECUTION_FLOW)
    add_test(
            NAME misc_execution_flow_schema_generator_test
            COMMAND "${Python3_EXECUTABLE}"
                    "${PROJECT_SOURCE_DIR}/scripts/test_generate_execution_flow_schema.py"
    )
    set_tests_properties(
            misc_execution_flow_schema_generator_test
            PROPERTIES WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    )
endif ()
