# - Try to find the Subid libraries (part of shadow-utils)
# Once done this will define
#
#  Subid_FOUND - system has subid
#  SUBID_INCLUDE_DIR - the subid include directory
#  SUBID_LIBRARIES - libsubid library

if (SUBID_INCLUDE_DIR AND SUBID_LIBRARY)
    # Already in cache, be silent
    set(Subid_FIND_QUIETLY TRUE)
endif (SUBID_INCLUDE_DIR AND SUBID_LIBRARY)

# First try pkg-config approach
find_package(PkgConfig QUIET)
if (PKG_CONFIG_FOUND)
    pkg_check_modules(PC_SUBID QUIET libsubid)
    if (PC_SUBID_FOUND)
        set(SUBID_VERSION ${PC_SUBID_VERSION})
        set(SUBID_DEFINITIONS ${PC_SUBID_CFLAGS_OTHER})
    endif()
endif()

# Find include directory
find_path(SUBID_INCLUDE_DIR 
    NAMES shadow/subid.h subid.h
    HINTS ${PC_SUBID_INCLUDEDIR} ${PC_SUBID_INCLUDE_DIRS}
    PATH_SUFFIXES shadow
)

# Find library
find_library(SUBID_LIBRARY 
    NAMES subid
    HINTS ${PC_SUBID_LIBDIR} ${PC_SUBID_LIBRARY_DIRS}
)

# Handle results
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Subid
    FOUND_VAR Subid_FOUND
    REQUIRED_VARS SUBID_LIBRARY SUBID_INCLUDE_DIR
    VERSION_VAR SUBID_VERSION
)

if (Subid_FOUND)
    set(SUBID_LIBRARIES ${SUBID_LIBRARY})
    set(SUBID_INCLUDE_DIRS ${SUBID_INCLUDE_DIR})
    
    # Create imported target for modern CMake usage
    if (NOT TARGET Subid::subid)
        add_library(Subid::subid UNKNOWN IMPORTED)
        set_target_properties(Subid::subid PROPERTIES
            IMPORTED_LOCATION "${SUBID_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${SUBID_INCLUDE_DIR}"
        )
        if (SUBID_DEFINITIONS)
            set_target_properties(Subid::subid PROPERTIES
                INTERFACE_COMPILE_DEFINITIONS "${SUBID_DEFINITIONS}"
            )
        endif()
    endif()
endif()

mark_as_advanced(SUBID_INCLUDE_DIR SUBID_LIBRARY SUBID_VERSION)