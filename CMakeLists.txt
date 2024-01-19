cmake_minimum_required(VERSION 3.24)

set(CMAKE_POLICY_DEFAULT_CMP0075 NEW)

# Let cmake apply IPO flags for all compilers and do not output warnings.
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

# When the option() command sees a normal variable of the given name,
# the NEW behavior for this policy is to do nothing when a normal variable of the same name exists.
# The normal variable is not removed. The cache entry is not created or updated and is ignored if it exists.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# Set a default build type if none was specified
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Debug' as none was specified.")
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
            "MinSizeRel" "RelWithDebInfo")
endif ()

project(Crane C CXX)

# Options start here ----------------------------------------------------------------------------

option(ENABLE_BERKELEY_DB "Enable Berkeley DB as the embedded db backend" OFF)

option(ENABLE_UNQLITE "Enable Berkeley DB as the embedded db backend" ON)

option(CRANE_ENABLE_TESTS "Enable test targets" OFF)

option(CRANE_FORCE_COLORED_OUTPUT "Always produce ANSI-colored output (GNU/Clang only)." TRUE)

option(CRANE_NATIVE_ARCH_OPT "Enable -march=native compile option" ON)

option(CRANE_USE_GITEE_SOURCE "Enable the Gitee repository as the download source" ON)

option(CRANE_FULL_DYNAMIC "Enable dynamic libs" OFF)

option(CRANE_ADDRESS_SANITIZER "Enable address sanitizer" OFF)

option(CRANE_THREAD_SANITIZER "Enable thread sanitizer" OFF)
# Options end here -------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 20)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CRANE_ENABLE_TESTS ON)
    add_compile_definitions(CRANE_LOG_LEVEL=CRANE_LOG_LEVEL_TRACE)
else ()
    add_compile_definitions(CRANE_LOG_LEVEL=CRANE_LOG_LEVEL_INFO)
endif ()

# Set colorized output when ninja build system is used.
if (${CRANE_FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        message(STATUS "colorized output for gcc is enabled")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        message(STATUS "colorized output for clang is enabled")
        add_compile_options(-fcolor-diagnostics)
    endif ()
endif ()

if (${CRANE_NATIVE_ARCH_OPT})
    message(STATUS "-march=native enabled")
    add_compile_options(-march=native)
endif ()

#need to install devtoolset-11-libasan-devel.x86_64 devtoolset-11-libtsan-devel.x86_64
if (${CRANE_ADDRESS_SANITIZER})
    message(STATUS "address_sanitizer is enabled")
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)

    if (NOT ${CRANE_FULL_DYNAMIC})
        if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            add_compile_options(-static-libsan)
        else ()
            add_compile_options(-static-libasan)
        endif ()
    endif ()
endif ()

if (${CRANE_THREAD_SANITIZER})
    message(STATUS "thread_sanitizer is enabled")
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)

    if (NOT ${CRANE_FULL_DYNAMIC})
        if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            add_compile_options(-static-libsan)
        else ()
            add_compile_options(-static-libtsan)
        endif ()
    endif ()
endif ()

# Check LTO support
include(CheckIPOSupported)
check_ipo_supported(RESULT supported OUTPUT error)
if (supported)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_BUILD_TYPE STREQUAL "Release")
        # IPO/LTO is disabled in g++ under Debug mode since it's quite slow.
        message(STATUS "IPO / LTO enabled")
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif ()
else ()
    message(STATUS "IPO / LTO not supported: <${error}>")
endif ()

find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    message(STATUS "ccache found. Use ccache to launch compilers.")
    set_property(GLOBAL PROPERTY CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set_property(GLOBAL PROPERTY CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif ()

if (${CRANE_FULL_DYNAMIC})
    set(CMAKE_SKIP_BUILD_RPATH FALSE)
    set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
else ()
    # static link on c++ libs
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
endif ()

# If Clang is used, select lld as the linker.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=lld")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
endif ()

message("CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS}")
message("CMAKE_SHARED_LINKER_FLAGS ${CMAKE_SHARED_LINKER_FLAGS}")

if (CMAKE_SIZEOF_VOID_P EQUAL 8)
    # search lib64 directory
    set(FIND_LIBRARY_USE_LIB64_PATHS TRUE)
endif ()

# Add pre-included libraries files.
set(DEPENDENCIES_PRE_INSTALLED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/pre_installed)
add_subdirectory(${DEPENDENCIES_PRE_INSTALLED_DIR})


find_package(Threads REQUIRED)

# New in version cmake3.24:
# Set ZLIB_USE_STATIC_LIBS to ON to look for static libraries. Default is OFF.
# We set this directory variable here to OFF to make all find_package(ZLIB) in
# in this project to use dynamic zlib library file.
set(ZLIB_USE_STATIC_LIBS OFF)

# Some content are downloaded and built inside cmake folder.
# This line must be place before any find_package() command.
# Independently built projects are installed to ${DEPENDENCIES_ONLINE_DIR}
#
# Since find_package needs to be in the top scope, we append the paths of installed
# projects at top-level CMakeLists.txt
#
# EXCLUDE_FROM_ALL excludes all dependencies from being installed
add_subdirectory(dependencies/cmake EXCLUDE_FROM_ALL)
add_subdirectory(dependencies/CPM EXCLUDE_FROM_ALL)

# Notify CMake that we have module files to find packages/libs.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModule/")

if (ENABLE_BERKELEY_DB)
    message("Enable berkeley db as one of embedded db backend.")
    find_package(BerkeleyDB REQUIRED)
    if (BERKELEYDB_FOUND)
        message("Berkeley DB found. Include: ${BERKELEY_DB_INCLUDE_DIR}; Libs: ${BERKELEY_DB_CXX_LIBRARIES}")
    else ()
        message(FATAL_ERROR "Berkeley DB was not found.")
    endif ()
endif ()

if (ENABLE_UNQLITE)
    message("Enable Unqlite as one of embedded db backend.")
endif ()

if (NOT (ENABLE_BERKELEY_DB AND BERKELEYDB_FOUND) AND NOT ENABLE_UNQLITE)
    message(FATAL_ERROR "At least one of Berkeley DB and Unqlite should be enabled.")
endif ()

find_package(PAM REQUIRED)
if (PAM_FOUND)
    message(STATUS "PAM library found. Include: ${PAM_INCLUDE_DIR}; Libs: ${PAM_LIBRARIES}")
else ()
    message(FATAL_ERROR "PAM library was not found.")
endif ()

find_package(LibAIO REQUIRED)
if (LibAIO_FOUND)
    message(STATUS "LibAIO found. Include: ${LIBAIO_INCLUDE_DIRS}; Libs: ${LIBAIO_LIBRARIES}")
else ()
    message(FATAL_ERROR "LibAIO was not found.")
endif ()

# Needed by grpc
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_GRPCPP grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

find_package(PkgConfig REQUIRED)
pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

# @formatter:off
add_definitions(-DCRANE_BUILD_DIRECTORY=\("${CMAKE_BINARY_DIR}"\))
# @formatter:on

# Proto
add_subdirectory(protos)

# Source Code
add_subdirectory(src)

# Tests
if (CRANE_ENABLE_TESTS)
    enable_testing()
    # Test source files may include lots of warnings and errors.
    # Exclude it from 'all' target
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif ()

# install binaries
install(TARGETS cranectld craned
        EXPORT crane
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)


# Install configuration files

set(BINDIR_VAR ${CMAKE_INSTALL_PREFIX}/bin) # This variable will replace @BINDIR_VAR@ in *.in file
configure_file(./etc/cranectld.service.in ${PROJECT_BINARY_DIR}/etc/cranectld.service)
configure_file(./etc/craned.service.in ${PROJECT_BINARY_DIR}/etc/craned.service)

install(FILES
        ${PROJECT_BINARY_DIR}/etc/cranectld.service
        ${PROJECT_BINARY_DIR}/etc/craned.service
        DESTINATION /etc/systemd/system/)

install(FILES etc/config.yaml
        DESTINATION /etc/crane
        RENAME config_example.yaml)

install(FILES etc/database.yaml
        DESTINATION /etc/crane
        RENAME database_example.yaml)
