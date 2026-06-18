# Add Linker flag for c++ 17 filesystem library in this folder.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    link_libraries(stdc++fs)
endif ()

include(GoogleTest)

add_executable(embedded_db_client_test
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPublicDefs.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/EmbeddedDbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/EmbeddedDbClient.cpp

        EmbeddedDbClientTest.cpp
        )
target_link_libraries(embedded_db_client_test
        GTest::gtest GTest::gtest_main

        crane_proto_lib

        Utility_PublicHeader
        Utility_PasswordEntry
        crane_tracer

        concurrentqueue

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        phmap
        nlohmann_json
        range-v3::range-v3
        uvw

        unqlite
        Backward::Interface
        )
if (ENABLE_UNQLITE)
    target_compile_definitions(embedded_db_client_test PRIVATE CRANE_HAVE_UNQLITE)
endif ()
if (ENABLE_ROCKSDB)
    target_sources(embedded_db_client_test PRIVATE
            ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/RocksDbEmbeddedStore.h
            ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/RocksDbEmbeddedStore.cpp)
    target_compile_definitions(embedded_db_client_test PRIVATE CRANE_HAVE_ROCKSDB)
    if (TARGET rocksdb)
        target_link_libraries(embedded_db_client_test rocksdb)
    elseif (TARGET rocksdb-shared)
        target_link_libraries(embedded_db_client_test rocksdb-shared)
    endif ()
endif ()
target_include_directories(embedded_db_client_test PUBLIC
        ${PROJECT_SOURCE_DIR}/src/CraneCtld
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database)
gtest_discover_tests(embedded_db_client_test)
