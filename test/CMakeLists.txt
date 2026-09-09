add_subdirectory(CraneCtld)
add_subdirectory(Craned)
add_subdirectory(Utilities)
add_subdirectory(Misc)
# Tests are intentionally excluded from the default build.  Keep one explicit
# aggregate target so CI can build every registered unit-test executable
# without duplicating the target list in an external script.
set(CRANE_UNIT_TEST_TARGETS
    embedded_db_client_test
    ctld_public_defs_test
    dedicated_resource_test
    public_header_test
    misc_unqlite_test
    misc_cgroup_v2_fs_test
)
if (TARGET misc_tracer_manager_test)
    list(APPEND CRANE_UNIT_TEST_TARGETS misc_tracer_manager_test)
endif ()
add_custom_target(crane_unit_tests DEPENDS ${CRANE_UNIT_TEST_TARGETS})
