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
