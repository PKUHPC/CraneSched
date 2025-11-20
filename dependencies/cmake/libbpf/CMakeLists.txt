if (NOT CRANE_ENABLE_BPF)
    return()
endif ()

find_package(PkgConfig REQUIRED)

set(LIBBPF_INCLUDE_DIR "" CACHE STRING "Path to libbpf headers" FORCE)
set(LIBBPF_BUILD_PRODUCTS "" CACHE STRING "Path to libbpf library" FORCE)
set(LIBBPF_EXTRA_LIBS "" CACHE STRING "Extra libraries required by libbpf" FORCE)

if (CRANE_USE_SYSTEM_LIBBPF)
    message(STATUS "Using libbpf from system.")

    pkg_check_modules(PC_LIBBPF REQUIRED libbpf>=1.4.6)

    message(STATUS "PkgConfig: LIBBPF INCLUDE_DIR:${PC_LIBBPF_INCLUDE_DIRS}, LIB_DIR:${PC_LIBBPF_LIBRARY_DIRS}, Ver: ${PC_LIBBPF_VERSION}")

    find_path(LIBBPF_INCLUDE_DIRS
            NAMES bpf/bpf.h
            HINTS ${PC_LIBBPF_INCLUDE_DIRS})

    find_library(LIBBPF_LIBRARIES
            NAMES bpf
            HINTS ${PC_LIBBPF_LIBRARY_DIRS})

    if (NOT LIBBPF_INCLUDE_DIRS OR NOT LIBBPF_LIBRARIES)
        message(FATAL_ERROR "Error occurred when searching libbpf")
    endif ()

    if (PC_LIBBPF_LIBRARIES)
        set(_LIBBPF_LINK_LIBS ${PC_LIBBPF_LIBRARIES})
    else ()
        set(_LIBBPF_LINK_LIBS ${LIBBPF_LIBRARIES})
    endif ()

    set(LIBBPF_INCLUDE_DIR ${LIBBPF_INCLUDE_DIRS} CACHE STRING "Path to libbpf headers" FORCE)
    set(LIBBPF_BUILD_PRODUCTS "${_LIBBPF_LINK_LIBS}" CACHE STRING "Path to libbpf library" FORCE)
    if (PC_LIBBPF_LDFLAGS_OTHER)
        set(LIBBPF_EXTRA_LIBS "${PC_LIBBPF_LDFLAGS_OTHER}" CACHE STRING "Extra libraries required by libbpf" FORCE)
    else ()
        set(LIBBPF_EXTRA_LIBS "" CACHE STRING "Extra libraries required by libbpf" FORCE)
    endif ()
    unset(_LIBBPF_LINK_LIBS)
    return()
endif ()

message(STATUS "Building libbpf from source.")

include(FetchContent)
include(ExternalProject)

set(LIBBPF_VERSION 1.6.2)
set(LIBBPF_URL "https://github.com/libbpf/libbpf/archive/refs/tags/v${LIBBPF_VERSION}.tar.gz")
set(LIBBPF_URL_SHA256 16f31349c70764cba8e0fad3725cc9f52f6cf952554326aa0229daaa21ef4fbd)

FetchContent_Declare(
        libbpf
        URL ${LIBBPF_URL}
        URL_HASH SHA256=${LIBBPF_URL_SHA256}
)
FetchContent_GetProperties(libbpf)
if (NOT libbpf_POPULATED)
    FetchContent_Populate(libbpf)
endif ()

set(LIBBPF_PREFIX ${libbpf_BINARY_DIR})
set(LIBBPF_INCLUDE_DIR ${LIBBPF_PREFIX}/include CACHE STRING "Path to libbpf headers" FORCE)
set(LIBBPF_LIBRARY_DIR ${LIBBPF_PREFIX}/lib)

set(LIBBPF_BUILD_ARGS)
if (NOT BUILD_SHARED_LIBS)
    list(APPEND LIBBPF_BUILD_ARGS BUILD_STATIC_ONLY=1)
endif ()

if (BUILD_SHARED_LIBS)
    set(LIBBPF_BUILD_PRODUCTS ${LIBBPF_LIBRARY_DIR}/libbpf.so CACHE STRING "Path to libbpf library" FORCE)
else ()
    set(LIBBPF_BUILD_PRODUCTS ${LIBBPF_LIBRARY_DIR}/libbpf.a CACHE STRING "Path to libbpf library" FORCE)
endif ()

pkg_check_modules(libelf REQUIRED libelf)
find_package(ZLIB REQUIRED)

set(LIBBPF_EXTRA_LIBS "${libelf_LIBRARIES};${ZLIB_LIBRARIES}" CACHE STRING "Extra libraries required by libbpf" FORCE)

ExternalProject_Add(
        libbpf_ep
        SOURCE_DIR ${libbpf_SOURCE_DIR}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND make -C src ${LIBBPF_BUILD_ARGS}
        INSTALL_COMMAND make -C src PREFIX=${LIBBPF_PREFIX} LIBDIR=${LIBBPF_LIBRARY_DIR} INCLUDEDIR=${LIBBPF_INCLUDE_DIR} install
        BUILD_IN_SOURCE 1
        BUILD_BYPRODUCTS ${LIBBPF_BUILD_PRODUCTS} ${LIBBPF_INCLUDE_DIR}/bpf/bpf.h
)
