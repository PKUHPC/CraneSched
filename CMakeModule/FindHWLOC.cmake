# ~~~
# - Try to find hwloc
#
# Once done this will define
#
#  HWLOC_FOUND        - System has hwloc
#  HWLOC_INCLUDE_DIRS - The hwloc include directories
#  HWLOC_LIBRARIES    - The libraries needed to use hwloc
# ~~~

find_package(PkgConfig QUIET)
pkg_check_modules(PC_HWLOC hwloc)

message(STATUS "PkgConfig: HWLOC INCLUDE_DIR:${PC_HWLOC_INCLUDE_DIRS}, LIB_DIR:${PC_HWLOC_LIBRARY_DIRS}, Ver: ${PC_HWLOC_VERSION}")

find_path(
        HWLOC_INCLUDE_DIRS
        NAMES hwloc.h
        HINTS ${PC_HWLOC_INCLUDE_DIRS}
)

find_library(
        HWLOC_LIBRARIES
        NAMES hwloc
        HINTS ${PC_HWLOC_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
        HWLOC
        REQUIRED_VARS HWLOC_LIBRARIES HWLOC_INCLUDE_DIRS
        VERSION_VAR PC_HWLOC_VERSION
        FAIL_MESSAGE "Error occurred when searching for hwloc"
)

if (HWLOC_FOUND AND NOT TARGET HWLOC::hwloc)
    add_library(HWLOC::hwloc INTERFACE IMPORTED)
    set_target_properties(HWLOC::hwloc PROPERTIES
            INTERFACE_LINK_LIBRARIES "${HWLOC_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${HWLOC_INCLUDE_DIRS}"
    )
endif ()