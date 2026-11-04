# Locate an installed PMIx SDK. PMIx_ROOT and CMAKE_PREFIX_PATH take precedence
# over pkg-config hints, which also cover distribution-specific multiarch paths.
# This module does not enable product features or select a PMIx transport.
find_package(PkgConfig QUIET)
if (PkgConfig_FOUND)
    pkg_check_modules(PC_PMIx QUIET pmix)
endif ()

# Re-evaluate discovery on each configure so changing the prefix cannot retain
# a header or library from a previous installation in CMakeCache.txt.
if (PMIx_ROOT OR NOT "$ENV{PMIx_ROOT}" STREQUAL "")
    find_path(_PMIx_INCLUDE_DIR
        NAMES pmix.h
        PATHS ${PMIx_ROOT} "$ENV{PMIx_ROOT}"
        PATH_SUFFIXES include include/pmix
        NO_DEFAULT_PATH
        NO_CACHE
    )
else ()
    find_path(_PMIx_INCLUDE_DIR
        NAMES pmix.h
        HINTS ${PC_PMIx_INCLUDE_DIRS}
        PATH_SUFFIXES pmix
        NO_CACHE
    )
endif ()
get_filename_component(PMIX_INCLUDE_DIR "${_PMIx_INCLUDE_DIR}" ABSOLUTE)
if (NOT _PMIx_INCLUDE_DIR)
    set(PMIX_INCLUDE_DIR "PMIX_INCLUDE_DIR-NOTFOUND")
endif ()

set(PMIX_LIBRARY "PMIX_LIBRARY-NOTFOUND")
set(PMIx_VERSION "")
if (PMIX_INCLUDE_DIR)
    # Pair the library with the selected headers instead of falling through to
    # a different system PMIx installation. Ubuntu uses <triplet>/pmix2/include.
    string(REGEX REPLACE "/include(/pmix)?$" "" _PMIx_PREFIX "${PMIX_INCLUDE_DIR}")
    find_library(_PMIx_LIBRARY
        NAMES pmix
        PATHS "${_PMIx_PREFIX}/lib" "${_PMIx_PREFIX}/lib64"
              "${_PMIx_PREFIX}/lib/${CMAKE_LIBRARY_ARCHITECTURE}"
        NO_DEFAULT_PATH
        NO_CACHE
    )
    set(PMIX_LIBRARY "${_PMIx_LIBRARY}")

    if (EXISTS "${PMIX_INCLUDE_DIR}/pmix_version.h")
        file(READ "${PMIX_INCLUDE_DIR}/pmix_version.h" _PMIx_VERSION_HEADER)
        foreach (_part MAJOR MINOR RELEASE)
            string(REGEX MATCH "#[ \t]*define[ \t]+PMIX_VERSION_${_part}[ \t]+([0-9]+)"
                   _match "${_PMIx_VERSION_HEADER}")
            set(_PMIx_${_part} "${CMAKE_MATCH_1}")
        endforeach ()
        if (NOT "${_PMIx_MAJOR}" STREQUAL "" AND
            NOT "${_PMIx_MINOR}" STREQUAL "" AND
            NOT "${_PMIx_RELEASE}" STREQUAL "")
            set(PMIx_VERSION "${_PMIx_MAJOR}.${_PMIx_MINOR}.${_PMIx_RELEASE}")
        endif ()
    endif ()
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PMIx
    REQUIRED_VARS PMIX_INCLUDE_DIR PMIX_LIBRARY PMIx_VERSION
    VERSION_VAR PMIx_VERSION
    REASON_FAILURE_MESSAGE
        "Install a compatible PMIx development package, set PMIx_ROOT for a custom prefix, or use -DCRANE_ENABLE_PMIX=OFF."
)

if (PMIx_FOUND AND NOT TARGET PMIx::pmix)
    add_library(PMIx::pmix UNKNOWN IMPORTED)
    set_target_properties(PMIx::pmix PROPERTIES
        IMPORTED_LOCATION "${PMIX_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${PMIX_INCLUDE_DIR}"
    )
    message(STATUS "PMIx ${PMIx_VERSION}: include=${PMIX_INCLUDE_DIR}; library=${PMIX_LIBRARY}")
endif ()

set(PMIX_INCLUDE_DIRS "${PMIX_INCLUDE_DIR}")
set(PMIX_LIBRARIES "${PMIX_LIBRARY}")
# Preserve the actual selection in build provenance without reusing it for
# discovery on the next configure.
set(PMIX_INCLUDE_DIR "${PMIX_INCLUDE_DIR}" CACHE PATH "Selected PMIx headers" FORCE)
set(PMIX_LIBRARY "${PMIX_LIBRARY}" CACHE FILEPATH "Selected PMIx library" FORCE)
set(PMIx_VERSION "${PMIx_VERSION}" CACHE STRING "Selected PMIx version" FORCE)
mark_as_advanced(PMIX_INCLUDE_DIR PMIX_LIBRARY PMIx_VERSION)
