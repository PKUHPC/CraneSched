find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_test(
        NAME query_next_job_ids_test
        COMMAND ${Python3_EXECUTABLE} -B
                ${CMAKE_SOURCE_DIR}/test/query_next_job_ids_test.py)

add_subdirectory(CraneCtld)
add_subdirectory(Utilities)
add_subdirectory(Misc)
