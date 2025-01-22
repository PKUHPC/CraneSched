# ~~~
# - Try to find libbpf
#
# Once done this will define
#
#  BPF_FOUND        - System has libbpf
#  BPF_INCLUDE_DIRS - The libbpf include directories
#  BPF_LIBRARIES    - The libraries needed to use libbpf
# ~~~

find_package(PkgConfig QUIET)
pkg_check_modules(PC_LIBBPF libbpf)

message(STATUS "PkgConfig: LIBBPF INCLUDE_DIR:${PC_LIBBPF_INCLUDE_DIRS}, LIB_DIR:${PC_LIBBPF_LIBRARY_DIRS}, Ver: ${PC_LIBBPF_VERSION}")

find_path(
        BPF_INCLUDE_DIRS
        NAMES bpf/bpf.h
        HINTS ${PC_LIBBPF_INCLUDE_DIRS})

find_library(
        BPF_LIBRARIES
        NAMES bpf
        HINTS ${PC_LIBBPF_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
        BPF
        REQUIRED_VARS BPF_LIBRARIES BPF_INCLUDE_DIRS
        VERSION_VAR PC_LIBBPF_VERSION
        FAIL_MESSAGE "Error occurred when searching libbpf")

if (BPF_FOUND AND NOT TARGET BPF::BPF)
    add_library(BPF::BPF INTERFACE IMPORTED)
    set_target_properties(BPF::BPF PROPERTIES INTERFACE_LINK_LIBRARIES "${BPF_LIBRARIES}" INTERFACE_INCLUDE_DIRECTORIES
            "${BPF_INCLUDE_DIRS}")
endif ()
