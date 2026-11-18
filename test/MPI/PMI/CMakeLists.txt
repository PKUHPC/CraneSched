include(GoogleTest)
add_executable(pmi2_test_client Client.c)
target_link_libraries(pmi2_test_client PRIVATE crane_pmi2)
add_executable(pmi1_test_client Pmi1Client.c)
add_executable(pmi1_api_test_client Pmi1ApiClient.c)
target_link_libraries(pmi1_api_test_client PRIVATE crane_pmi1)
add_executable(pmi2_server_test ServerTest.cpp)
target_link_libraries(pmi2_server_test PRIVATE Utility_Mpi GTest::gtest_main)
target_compile_definitions(pmi2_server_test PRIVATE
        PMI2_TEST_CLIENT="$<TARGET_FILE:pmi2_test_client>"
        PMI1_TEST_CLIENT="$<TARGET_FILE:pmi1_test_client>"
        PMI1_API_TEST_CLIENT="$<TARGET_FILE:pmi1_api_test_client>")
add_dependencies(pmi2_server_test pmi2_test_client pmi1_test_client pmi1_api_test_client)
gtest_discover_tests(pmi2_server_test PROPERTIES TIMEOUT 20)

add_executable(pmi2_client_test ClientTest.cpp)
target_link_libraries(pmi2_client_test PRIVATE crane_pmi2_internal GTest::gtest_main)
gtest_discover_tests(pmi2_client_test PROPERTIES TIMEOUT 20)

add_executable(pmi1_client_test Pmi1ClientTest.cpp)
target_link_libraries(pmi1_client_test PRIVATE crane_pmi1_internal crane_pmi1 GTest::gtest_main)
gtest_discover_tests(pmi1_client_test PROPERTIES TIMEOUT 20)
