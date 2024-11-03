include(FetchContent)
include(ExternalProject)

set(LIBCGROUP_PATH "${CMAKE_BINARY_DIR}/_deps/libcgroup" CACHE STRING "Path to libcgroup dependency" FORCE)
set(LIBCGROUP_BUILD_PRODUCTS ${LIBCGROUP_PATH}/install/lib/libcgroup.a)
ExternalProject_Add(
        libcgroup-extern
        PREFIX ${LIBCGROUP_PATH}
        URL https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
        URL_HASH SHA256=976ec4b1e03c0498308cfd28f1b256b40858f636abc8d1f9db24f0a7ea9e1258
        CONFIGURE_COMMAND ./configure --prefix=${LIBCGROUP_PATH}/install
        BUILD_COMMAND make -j
        INSTALL_COMMAND make install
        BUILD_IN_SOURCE 1
        BUILD_BYPRODUCTS ${LIBCGROUP_BUILD_PRODUCTS}
)