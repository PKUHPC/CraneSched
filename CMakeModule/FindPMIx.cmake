if(NOT DEFINED WITH_PMIX OR "${WITH_PMIX}" STREQUAL "")
    set(PMIx_FOUND FALSE)
    set(UCX_FOUND FALSE)
    message("WITH_PMIX not set, PMIx support will be disabled.")
    return()
endif()

find_path(PMIX_INCLUDE_DIR pmix.h PATHS "${WITH_PMIX}/include" NO_DEFAULT_PATH)
find_library(PMIX_LIBRARY pmix PATHS "${WITH_PMIX}/lib" NO_DEFAULT_PATH)

set(PMIX_INCLUDE_DIRS ${PMIX_INCLUDE_DIR})
set(PMIX_LIBRARIES ${PMIX_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PMIx
    REQUIRED_VARS PMIX_INCLUDE_DIR PMIX_LIBRARY
)

if(PMIx_FOUND)
    message(STATUS "PMIx found! include: ${PMIX_INCLUDE_DIR}, library: ${PMIX_LIBRARY}")
else()
    message(FATAL_ERROR "PMIx NOT found!")
endif()

if(PMIx_FOUND AND NOT TARGET PMIx::pmix)
    add_library(PMIx::pmix SHARED IMPORTED)
    set_target_properties(PMIx::pmix PROPERTIES
        IMPORTED_LOCATION "${PMIX_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${PMIX_INCLUDE_DIR}"
    )
endif()

add_definitions(-DHAVE_PMIX)

find_package(PkgConfig REQUIRED)
pkg_check_modules(UCX REQUIRED IMPORTED_TARGET ucx)
if (UCX_FOUND)
    message(STATUS "UCX found! include: ${UCX_INCLUDE_DIRS}, library: ${UCX_LIBRARIES}")
    add_definitions(-DHAVE_UCX)
endif()
