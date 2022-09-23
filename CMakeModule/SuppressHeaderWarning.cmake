include(${CMAKE_SOURCE_DIR}/CMakeModule/GetAllTargets.cmake)

# suppress all warning caused by including library headers in third party libraries.
function(suppress_header_warning)
    get_all_targets(all_targets)
    foreach (tgt ${all_targets})
        get_target_property(target_type ${tgt} TYPE)
        if (target_type STREQUAL "STATIC_LIBRARY" OR target_type STREQUAL "SHARED_LIBRARY")
            get_property(tgt_include_dirs TARGET ${tgt} PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
            set_property(TARGET ${tgt} PROPERTY INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${tgt_include_dirs}")
        endif ()
    endforeach ()
endfunction()