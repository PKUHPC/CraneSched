# Add Linker flag for c++ 17 filesystem library in this folder.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    link_libraries(stdc++fs)
endif ()

include(GoogleTest)

add_executable(misc_unqlite_test unqlite_test.cpp)
target_link_libraries(misc_unqlite_test
        GTest::gtest GTest::gtest_main
        unqlite
        ${_PROTOBUF_LIBPROTOBUF}
)
gtest_discover_tests(misc_unqlite_test)

add_executable(misc_cgroup_v2_fs_test cgroup_v2_fs_test.cpp)
target_link_libraries(misc_cgroup_v2_fs_test
        GTest::gtest GTest::gtest_main
        craned_common
)
gtest_discover_tests(misc_cgroup_v2_fs_test)

if (CRANE_ENABLE_TRACING)
    add_executable(misc_tracer_manager_test tracer_manager_test.cpp)
    target_link_libraries(misc_tracer_manager_test
            GTest::gtest GTest::gtest_main
            crane_tracer
    )
    gtest_discover_tests(misc_tracer_manager_test)
endif ()
