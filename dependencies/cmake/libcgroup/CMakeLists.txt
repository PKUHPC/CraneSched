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