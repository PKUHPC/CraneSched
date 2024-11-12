if (${CRANE_USE_SYSTEM_LIBCGROUP})
    message(STATUS "Using libcgroup from system.")

    find_package(PkgConfig REQUIRED)
    pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

    if (libcgroup_FOUND)
        message(STATUS "Found libcgroup ${libcgroup_VERSION} in system using pkg-config.")
        if(CRANE_ENABLE_CGROUP_V2 AND libcgroup_VERSION VERSION_LESS "3.1.0")
            message(FATAL_ERROR "libcgroup version must be higher than 3.1.0 when CRANE_ENABLE_CGROUP_V2 is set.")
        endif()
    else()
        message(FATAL_ERROR "libcgroup in system is not found using pkg-config.")
    endif()

    add_library(libcgroup INTERFACE)
    target_link_libraries(libcgroup INTERFACE PkgConfig::libcgroup)
    target_include_directories(libcgroup INTERFACE ${libcgroup_INCLUDE_DIRS})

    # libcgroup in system is a shared library
    set(LIBCGROUP_BUILD_PRODUCTS ${libcgroup_LIBRARY_DIRS}/libcgroup.so CACHE STRING "Path to libcgroup library" FORCE)
    message(STATUS "Using libcgroup from system at ${LIBCGROUP_BUILD_PRODUCTS}.")
    return()
endif ()

message(STATUS "Building libcgroup from source.")

include(FetchContent)
include(ExternalProject)

FetchContent_Declare(
        libcgroup
        URL https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
        URL_HASH SHA256=976ec4b1e03c0498308cfd28f1b256b40858f636abc8d1f9db24f0a7ea9e1258
)
FetchContent_GetProperties(libcgroup)
if (NOT libcgroup_POPULATED)
    FetchContent_Populate(libcgroup)
endif ()

if (BUILD_SHARED_LIBS)
    set(LIBCGROUP_BUILD_PRODUCTS ${libcgroup_BINARY_DIR}/lib/libcgroup.so CACHE STRING "Path to libcgroup library" FORCE)
else ()
    set(LIBCGROUP_BUILD_PRODUCTS ${libcgroup_BINARY_DIR}/lib/libcgroup.a CACHE STRING "Path to libcgroup library" FORCE)
endif ()
set(LIBCGROUP_PATH "${libcgroup_BINARY_DIR}" CACHE STRING "Path to libcgroup dependency" FORCE)

ExternalProject_Add(
        libcgroup
        SOURCE_DIR ${libcgroup_SOURCE_DIR}
        CONFIGURE_COMMAND ./configure --prefix=${libcgroup_BINARY_DIR}/
        BUILD_COMMAND make -j
        INSTALL_COMMAND make install
        BUILD_IN_SOURCE 1
        BUILD_BYPRODUCTS ${LIBCGROUP_BUILD_PRODUCTS} ${libcgroup_BINARY_DIR}/include/
)