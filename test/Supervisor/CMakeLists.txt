include(GoogleTest)

add_executable(supervisor_exit_test
        SupervisorExitTest.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor/TaskManager.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor/CforedClient.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor/CranedClient.cpp
        ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor/SupervisorServer.cpp
)
target_include_directories(supervisor_exit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/src/Craned/Supervisor
)
target_precompile_headers(supervisor_exit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/src/Craned/Common/PreCompiledHeader.h
)
add_dependencies(supervisor_exit_test Utility_PasswordEntry_SubidCompat)
target_link_libraries(supervisor_exit_test PRIVATE
        GTest::gtest GTest::gtest_main
        craned_common
        Utility_CriClient
        Utility_BindFs
        crane_tracer
        Utility_Pmix
)
gtest_discover_tests(supervisor_exit_test PROPERTIES TIMEOUT 10)
