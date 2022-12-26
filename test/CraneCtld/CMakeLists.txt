# Add Linker flag for c++ 17 filesystem library in this folder.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    link_libraries(stdc++fs)
endif ()

include(GoogleTest)

add_executable(server_keep_alive_test
        server_keep_alive_test.cpp
        )
target_link_libraries(server_keep_alive_test
        GTest::gtest GTest::gtest_main

        Utility_PublicHeader

        test_proto
        shared_test_impl_lib
        )
gtest_discover_tests(server_keep_alive_test)

add_executable(embedded_db_client_test
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPublicDefs.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/EmbeddedDbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/EmbeddedDbClient.cpp

        EmbeddedDbClientTest.cpp
        )
target_link_libraries(embedded_db_client_test
        GTest::gtest GTest::gtest_main

        crane_proto_lib

        Utility_PublicHeader

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        unqlite
        )
target_include_directories(embedded_db_client_test PUBLIC ${PROJECT_SOURCE_DIR}/src/CraneCtld)
gtest_discover_tests(embedded_db_client_test)