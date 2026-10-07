# Add Linker flag for c++ 17 filesystem library in this folder.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    link_libraries(stdc++fs)
endif ()

include(GoogleTest)

# Exercise the real RPC implementation without adding a production test interface.
get_target_property(ctld_query_sources cranectld SOURCES)
get_target_property(ctld_query_libraries cranectld LINK_LIBRARIES)
get_target_property(ctld_query_definitions cranectld COMPILE_DEFINITIONS)
get_target_property(ctld_query_includes cranectld INCLUDE_DIRECTORIES)
list(FILTER ctld_query_sources EXCLUDE REGEX "(^|/)(CraneCtld|CtldGrpcServer)\\.cpp$")
list(TRANSFORM ctld_query_sources PREPEND "${PROJECT_SOURCE_DIR}/src/CraneCtld/")
add_library(ctld_query_test_support STATIC ${ctld_query_sources})
target_include_directories(ctld_query_test_support PUBLIC
        ${ctld_query_includes})
target_compile_definitions(ctld_query_test_support PUBLIC
        ${ctld_query_definitions})
target_link_libraries(ctld_query_test_support PUBLIC
        ${ctld_query_libraries})
target_precompile_headers(ctld_query_test_support PRIVATE
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPreCompiledHeader.h)

add_executable(job_query_privacy_test
        JobQueryPrivacyTest.cpp)
target_link_libraries(job_query_privacy_test PRIVATE
        GTest::gtest GTest::gtest_main ctld_query_test_support)
gtest_discover_tests(job_query_privacy_test)

add_executable(job_query_rpc_test
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/RpcService/CtldGrpcServer.cpp
        JobQueryRpcTest.cpp)
target_link_libraries(job_query_rpc_test PRIVATE
        GTest::gtest ctld_query_test_support)
# Replace only read dependencies, using the Linux GCC/Clang ABI for the selected
# standard library. Share each symbol with the test declarations to prevent drift.
set(job_query_wrap_names
        JOB_QUERY_WRAP_RAM
        JOB_QUERY_WRAP_HISTORY
        JOB_QUERY_WRAP_STEPS
        JOB_QUERY_WRAP_ADMIN
        JOB_QUERY_WRAP_CERTIFICATE
        JOB_QUERY_WRAP_QOS
        JOB_QUERY_WRAP_USERS
        JOB_QUERY_WRAP_WCKEYS
        JOB_QUERY_WRAP_ACCOUNTS)
if (CRANE_USE_LIBCXX)
    set(job_query_wrap_symbols
        _ZN4Ctld12JobScheduler14QueryJobsInRamEPKN5crane4grpc20QueryJobsInfoRequestEPNSt3__113unordered_mapIjNS2_7JobInfoENS6_4hashIjEENS6_8equal_toIjEENS6_9allocatorINS6_4pairIKjS8_EEEEEEm
        _ZN4Ctld13MongodbClient15FetchJobRecordsEPKN5crane4grpc20QueryJobsInfoRequestEPNSt3__113unordered_mapIjNS2_7JobInfoENS6_4hashIjEENS6_8equal_toIjEENS6_9allocatorINS6_4pairIKjS8_EEEEEEm
        _ZN4Ctld13MongodbClient19FetchJobStepRecordsEPKN5crane4grpc20QueryJobsInfoRequestEPNSt3__113unordered_mapIjNS2_7JobInfoENS6_4hashIjEENS6_8equal_toIjEENS6_9allocatorINS6_4pairIKjS8_EEEEEE
        _ZN4Ctld14AccountManager15CheckUidIsAdminEj
        _ZN4Ctld8Security11VaultClient13IsCertAllowedERKNSt3__112basic_stringIcNS2_11char_traitsIcEENS2_9allocatorIcEEEE
        _ZN4Ctld13MongodbClient12SelectAllQosEPNSt3__14listINS_3QosENS1_9allocatorIS3_EEEE
        _ZN4Ctld13MongodbClient13SelectAllUserEPNSt3__14listINS_4UserENS1_9allocatorIS3_EEEE
        _ZN4Ctld13MongodbClient14SelectAllWckeyEPNSt3__14listINS_5WckeyENS1_9allocatorIS3_EEEE
        _ZN4Ctld13MongodbClient16SelectAllAccountEPNSt3__14listINS_7AccountENS1_9allocatorIS3_EEEE)
else ()
    set(job_query_wrap_symbols
        _ZN4Ctld12JobScheduler14QueryJobsInRamEPKN5crane4grpc20QueryJobsInfoRequestEPSt13unordered_mapIjNS2_7JobInfoESt4hashIjESt8equal_toIjESaISt4pairIKjS7_EEEm
        _ZN4Ctld13MongodbClient15FetchJobRecordsEPKN5crane4grpc20QueryJobsInfoRequestEPSt13unordered_mapIjNS2_7JobInfoESt4hashIjESt8equal_toIjESaISt4pairIKjS7_EEEm
        _ZN4Ctld13MongodbClient19FetchJobStepRecordsEPKN5crane4grpc20QueryJobsInfoRequestEPSt13unordered_mapIjNS2_7JobInfoESt4hashIjESt8equal_toIjESaISt4pairIKjS7_EEE
        _ZN4Ctld14AccountManager15CheckUidIsAdminB5cxx11Ej
        _ZN4Ctld8Security11VaultClient13IsCertAllowedERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
        _ZN4Ctld13MongodbClient12SelectAllQosEPNSt7__cxx114listINS_3QosESaIS3_EEE
        _ZN4Ctld13MongodbClient13SelectAllUserEPNSt7__cxx114listINS_4UserESaIS3_EEE
        _ZN4Ctld13MongodbClient14SelectAllWckeyEPNSt7__cxx114listINS_5WckeyESaIS3_EEE
        _ZN4Ctld13MongodbClient16SelectAllAccountEPNSt7__cxx114listINS_7AccountESaIS3_EEE)
endif ()
foreach(name symbol IN ZIP_LISTS job_query_wrap_names job_query_wrap_symbols)
    target_compile_definitions(job_query_rpc_test PRIVATE "${name}=\"__wrap_${symbol}\"")
    target_link_options(job_query_rpc_test PRIVATE "LINKER:--wrap=${symbol}")
endforeach()
# Wrapping must happen before calls to the read dependencies can be inlined.
set_target_properties(ctld_query_test_support job_query_rpc_test
        PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE)
gtest_discover_tests(job_query_rpc_test)

add_executable(embedded_db_client_test
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPublicDefs.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/EmbeddedDbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/EmbeddedDbClient.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/LegacyEmbeddedDbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/LegacyEmbeddedDbClient.cpp

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

add_executable(ctld_public_defs_test
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPublicDefs.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/CtldPublicDefs.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/EmbeddedDbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/EmbeddedDbClient.cpp
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/LegacyEmbeddedDbClient.h
        ${PROJECT_SOURCE_DIR}/src/CraneCtld/Database/LegacyEmbeddedDbClient.cpp

        CtldPublicDefsTest.cpp
        CtldTestStubs.cpp
        )
target_link_libraries(ctld_public_defs_test
        GTest::gtest GTest::gtest_main

        spdlog::spdlog
        concurrentqueue
        crane_proto_lib

        Utility_PublicHeader
        Utility_PasswordEntry
        Utility_PrologEpilogExecutor
        crane_tracer
        uvw
        cxxopts
        Threads::Threads

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        phmap
        nlohmann_json
        unqlite
        range-v3::range-v3
        bs_thread_pool
        yaml-cpp
        Backward::Interface
        )
target_include_directories(ctld_public_defs_test PUBLIC ${PROJECT_SOURCE_DIR}/src/CraneCtld)
if (ENABLE_UNQLITE)
    target_compile_definitions(ctld_public_defs_test PRIVATE CRANE_HAVE_UNQLITE)
endif ()
if (ENABLE_BERKELEY_DB AND BERKELEYDB_FOUND)
    target_compile_definitions(ctld_public_defs_test PRIVATE CRANE_HAVE_BERKELEY_DB)
    target_include_directories(ctld_public_defs_test PRIVATE ${BERKELEY_DB_INCLUDE_DIR})
    target_link_libraries(ctld_public_defs_test ${BERKELEY_DB_CXX_LIBRARIES})
endif ()
gtest_discover_tests(ctld_public_defs_test)
