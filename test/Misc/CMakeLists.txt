# Add Linker flag for c++ 17 filesystem library in this folder.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    link_libraries(stdc++fs)
endif ()

include(GoogleTest)

add_executable(misc_grpc_example_test grpc_example_test.cpp)
target_link_libraries(misc_grpc_example_test
        GTest::gtest GTest::gtest_main
        concurrentqueue
        shared_test_impl_lib)
gtest_discover_tests(misc_grpc_example_test)

add_executable(misc_libevent_example_test libevent_example_test.cpp)
target_link_libraries(misc_libevent_example_test
        GTest::gtest GTest::gtest_main
        Utility_AnonymousPipe
        event_core
        event_pthreads
        Threads::Threads
        spdlog::spdlog
        )
gtest_discover_tests(misc_libevent_example_test)

add_executable(misc_libuv_test libuv_test.cpp)
target_link_libraries(misc_libuv_test
        GTest::gtest GTest::gtest_main
        uv::uv-static
        Threads::Threads
        )
gtest_discover_tests(misc_libuv_test)

add_executable(misc_Misc_test Misc_test.cpp)
target_link_libraries(misc_Misc_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
        absl::btree
        absl::synchronization
        spdlog::spdlog
        )
gtest_discover_tests(misc_Misc_test)


add_executable(misc_mariadb_c_test mariadb_connector_c_test.cpp)
target_link_libraries(misc_mariadb_c_test
        GTest::gtest GTest::gtest_main
        Boost::boost
        Threads::Threads
        absl::strings
        mariadbclient
        Utility_PublicHeader
        spdlog::spdlog
        )
target_include_directories(misc_mariadb_c_test PRIVATE ${CMAKE_SOURCE_DIR}/src/CraneCtld)
gtest_discover_tests(misc_mariadb_c_test)

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

add_executable(misc_yaml_test yaml_cpp_test.cpp)
target_link_libraries(misc_yaml_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
        spdlog::spdlog
        yaml-cpp
        )
gtest_discover_tests(misc_yaml_test)

add_executable(misc_unqlite_test unqlite_test.cpp)
target_link_libraries(misc_unqlite_test
        GTest::gtest GTest::gtest_main
        Threads::Threads
        spdlog::spdlog
        unqlite
        test_proto
        )
gtest_discover_tests(misc_unqlite_test)

add_executable(misc_libaio_test libaio_test.cpp)
target_include_directories(misc_libaio_test PUBLIC ${LIBAIO_INCLUDE_DIRS})
target_link_libraries(misc_libaio_test PUBLIC
        GTest::gtest GTest::gtest_main
        Threads::Threads
        spdlog::spdlog
        test_proto
        ${LIBAIO_LIBRARIES}
        )
gtest_discover_tests(misc_libaio_test)
