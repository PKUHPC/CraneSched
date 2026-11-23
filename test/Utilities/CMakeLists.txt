include(GoogleTest)

add_executable(dedicated_resource_test
        DedicatedResourceTest.cpp)
target_link_libraries(dedicated_resource_test
        GTest::gtest
        GTest::gtest_main
        Utility_PublicHeader
        )
gtest_discover_tests(dedicated_resource_test)

add_executable(utilities_test
        AccountingTimeTest.cpp
        UtilitiesTest.cpp)
target_link_libraries(utilities_test
        GTest::gtest
        GTest::gtest_main
        absl::strings

        Utility_PublicHeader
        )
gtest_discover_tests(utilities_test)

add_executable(prolog_epilog_test PrologEpilogTest.cpp)
target_link_libraries(prolog_epilog_test
        GTest::gtest_main
        Utility_PrologEpilogExecutor)
gtest_discover_tests(prolog_epilog_test PROPERTIES TIMEOUT 15)
