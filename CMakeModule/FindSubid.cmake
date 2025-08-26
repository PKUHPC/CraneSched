# FindSubid.cmake
# Exports:
#   Subid_FOUND
#   SUBID_INCLUDE_DIR
#   SUBID_LIBRARY
#   Subid::subid  (IMPORTED STATIC/SHARED)

if (SUBID_INCLUDE_DIR AND SUBID_LIBRARY)
  set(Subid_FIND_QUIETLY TRUE)
endif()

find_package(PkgConfig QUIET)
if (PKG_CONFIG_FOUND)
  pkg_check_modules(PC_SUBID QUIET libsubid)
  if (PC_SUBID_FOUND)
    set(SUBID_VERSION ${PC_SUBID_VERSION})
  endif()
endif()

find_path(SUBID_INCLUDE_DIR
  NAMES subid.h
  HINTS ${PC_SUBID_INCLUDEDIR} ${PC_SUBID_INCLUDE_DIRS}
  PATH_SUFFIXES shadow
)

find_library(SUBID_LIBRARY
  NAMES subid
  HINTS ${PC_SUBID_LIBDIR} ${PC_SUBID_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Subid
  FOUND_VAR Subid_FOUND
  REQUIRED_VARS SUBID_LIBRARY SUBID_INCLUDE_DIR
  VERSION_VAR SUBID_VERSION
)

if (Subid_FOUND AND NOT TARGET Subid::subid)
  get_filename_component(_ext "${SUBID_LIBRARY}" EXT)
  if (_ext STREQUAL ".a")
    set(_kind STATIC)
  else()
    set(_kind SHARED)
  endif()

  add_library(Subid::subid ${_kind} IMPORTED GLOBAL)
  set_target_properties(Subid::subid PROPERTIES
    IMPORTED_LOCATION             "${SUBID_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${SUBID_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(SUBID_INCLUDE_DIR SUBID_LIBRARY SUBID_VERSION)
