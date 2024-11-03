include(FetchContent)
include(ExternalProject)

find_package(PkgConfig REQUIRED)
pkg_check_modules(SYSTEMD REQUIRED libsystemd)

include_directories(${SYSTEMD_INCLUDE_DIRS})
link_directories(${SYSTEMD_LIBRARY_DIRS})

set(LIBCGROUP_PATH "${CMAKE_BINARY_DIR}/_deps/libcgroup" CACHE STRING "Path to libcgroup dependency" FORCE)
ExternalProject_Add(
        libcgroup-extern
        PREFIX ${LIBCGROUP_PATH}
        URL https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
        URL_HASH SHA256=976ec4b1e03c0498308cfd28f1b256b40858f636abc8d1f9db24f0a7ea9e1258
        CONFIGURE_COMMAND ./configure --prefix=${LIBCGROUP_PATH}/install
        BUILD_COMMAND make -j
        INSTALL_COMMAND make install
        BUILD_IN_SOURCE 1
)
add_library(libcgroup STATIC IMPORTED)
set_target_properties(libcgroup PROPERTIES IMPORTED_LOCATION ${LIBCGROUP_PATH}/install/lib/libcgroup.a)
set_target_properties(libcgroup PROPERTIES EXCLUDE_FROM_ALL FALSE)
add_dependencies(libcgroup libcgroup-extern)
#target_link_directories(libcgroup PUBLIC
#        ${LIBCGROUP_PATH}/install/lib/libcgroup.a
#)
#target_include_directories(libcgroup PUBLIC
#        ${LIBCGROUP_PATH}/install/include/
#)

#FetchContent_Declare(
#        libcgroup
#        EXCLUDE_FROM_ALL FALSE
#        OVERRIDE_FIND_PACKAGE
#        CONFIGURE_COMMAND ./configure
#        BUILD_COMMAND make
#        URL https://github.com/libcgroup/libcgroup/releases/download/v3.1.0/libcgroup-3.1.0.tar.gz
#        URL_HASH SHA256=976ec4b1e03c0498308cfd28f1b256b40858f636abc8d1f9db24f0a7ea9e1258
#)
#FetchContent_MakeAvailable(libcgroup)

#set(CMAKE_FIND_DEBUG_MODE TRUE)
#set(PKG_CONFIG_PATH "${libcgroup_BINARY_DIR}:$ENV{PKG_CONFIG_PATH}")
#pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)
#set(CMAKE_FIND_DEBUG_MODE FALSE)

#add_library(libcgroup INTERFACE)
#add_dependencies(libcgroup extern_libcgroup)
#target_link_libraries(libcgroup INTERFACE
#        ${LIBCGROUP_PATH}/install/include
#)
#target_include_directories(libcgroup INTERFACE
#        ${LIBCGROUP_PATH}/install/lib
#)