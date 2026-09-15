include(GoogleTest)

add_executable(dedicated_resource_test
        dedicated_resource_test.cpp)
target_link_libraries(dedicated_resource_test
        GTest::gtest
        GTest::gtest_main
        Utility_PublicHeader
        )
gtest_discover_tests(dedicated_resource_test)

add_executable(public_header_test
        AccountingTime_test.cpp
        PublicHeader_test.cpp)
target_link_libraries(public_header_test
        GTest::gtest
        GTest::gtest_main
        absl::strings

        Utility_PublicHeader
        )
gtest_discover_tests(public_header_test)
