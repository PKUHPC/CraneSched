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

option(CRANE_USE_GITEE_SOURCE "Enable the Gitee repository as the download source" OFF)

option(CRANE_FULL_DYNAMIC "Enable dynamic libs" OFF)

option(CRANE_ADDRESS_SANITIZER "Enable address sanitizer" OFF)

option(CRANE_THREAD_SANITIZER "Enable thread sanitizer" OFF)

option(CRANE_MIN_LOG_LEVEL "Set the minimal log level (INFO/DEBUG/TRACE)" OFF)
# Options end here -------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 20)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CRANE_ENABLE_TESTS ON)
endif ()

# Set the minimal log level based on the build type if it has not been explicitly set by the user
if (CRANE_MIN_LOG_LEVEL STREQUAL "OFF")
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CRANE_MIN_LOG_LEVEL "TRACE")
    else ()
        set(CRANE_MIN_LOG_LEVEL "DEBUG")
    endif ()
endif ()

# Validate the user-provided log level
if (NOT CRANE_MIN_LOG_LEVEL MATCHES "INFO|DEBUG|TRACE")
    message(FATAL_ERROR "Invalid log level: ${CRANE_MIN_LOG_LEVEL}. Must be INFO, DEBUG, or TRACE.")
endif ()

add_compile_definitions(CRANE_LOG_LEVEL=CRANE_LOG_LEVEL_${CRANE_MIN_LOG_LEVEL})
message(STATUS "Minimal log level is set to ${CRANE_MIN_LOG_LEVEL}")

# Generate version text and building timestamp
if (EXISTS "${CMAKE_SOURCE_DIR}/VERSION")
    # For a release version, the version file should be provided
    file(READ "${CMAKE_SOURCE_DIR}/VERSION" VERSION_CONTENT)
else ()
    # Otherwise, use git hash as the version
    message(WARNING "No VERSION file found. Use git hash as the version string.")
    execute_process(
            COMMAND git rev-parse --short HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE VERSION_CONTENT
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif ()

if ("${VERSION_CONTENT}" STREQUAL "")
    # if not in git repository, fall back to "Unknown"
    set(VERSION_CONTENT "Unknown")
endif ()

string(TIMESTAMP BUILD_TIMESTAMP "%a, %d %b %Y %H:%M:%S %z")

message(STATUS "Version: ${VERSION_CONTENT}")
message(STATUS "Building Time: ${BUILD_TIMESTAMP}")
add_compile_definitions(CRANE_BUILD_TIMESTAMP="${BUILD_TIMESTAMP}")
add_compile_definitions(CRANE_VERSION_STRING="${VERSION_CONTENT}")

# Set colorized output when ninja build system is used.
if (${CRANE_FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        message(STATUS "Colorized output for gcc is enabled")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        message(STATUS "Colorized output for clang is enabled")
        add_compile_options(-fcolor-diagnostics)
    endif ()
endif ()

if (${CRANE_NATIVE_ARCH_OPT})
    message(STATUS "-march=native enabled")
    add_compile_options(-march=native)
endif ()

# In CentOS 7, the following packages are required to enable sanitizers:
# 1. devtoolset-11-libasan-devel.x86_64
# 2. devtoolset-11-libtsan-devel.x86_64
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

# If Clang is used, select lld as the linker.
# Under clang, if lld is not selected as the linker and LLVMGold is not installed,
# CheckIpoSupported will failed.
# Normally we should pass "-fuse-ld=lld" to linker flags,
# but CheckIpoSupported will only use CMAKE_<LANG>_FLAGS to compile test source files.
# If we add "-fuse-ld=lld" to CMAKE_<LANG>_FLAGS, clang will give the warning:
# argument unused during compilation: '-fuse-ld=lld'.
# To solve this, we first set the linker flag in CMAKE_<LANG>_FLAGS and then move it
# to linker flags.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(RAW_CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    set(RAW_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fuse-ld=lld")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fuse-ld=lld")

    message("CMAKE_C_FLAGS ${CMAKE_C_FLAGS}")
    message("CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")
endif ()

# Check LTO support
include(CheckIPOSupported)
check_ipo_supported(RESULT supported OUTPUT error)

# If Clang is used, select lld as the linker.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_C_FLAGS "${RAW_CMAKE_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${RAW_CMAKE_CXX_FLAGS}")
    message("CMAKE_C_FLAGS ${CMAKE_C_FLAGS}")
    message("CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")

    unset(RAW_CMAKE_C_FLAGS)
    unset(RAW_CMAKE_CXX_FLAGS)

    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=lld")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
    message("CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS}")
    message("CMAKE_SHARED_LINKER_FLAGS ${CMAKE_SHARED_LINKER_FLAGS}")
endif ()

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
#pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

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

# Add group and components
set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)
set(CPACK_RPM_COMPONENT_INSTALL ON)

cpack_add_component(cranedc
        DISPLAY_NAME "craned"
        DESCRIPTION "craned component"
        GROUP craned)
cpack_add_component(cranectldc
        DISPLAY_NAME "cranectld"
        DESCRIPTION "cranectld component"
        GROUP cranectld)

cpack_add_component_group(craned)
cpack_add_component_group(cranectld)
set(CPACK_COMPONENTS_ALL cranedc cranectldc)

configure_file(${CMAKE_SOURCE_DIR}/etc/cranectld.service.in ${CMAKE_BINARY_DIR}/etc/cranectld.service)
configure_file(${CMAKE_SOURCE_DIR}/etc/craned.service.in ${CMAKE_BINARY_DIR}/etc/craned.service)

# Install binaries
install(TARGETS craned
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
        COMPONENT cranedc
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_WRITE GROUP_EXECUTE WORLD_READ WORLD_WRITE WORLD_EXECUTE
)

install(TARGETS cranectld
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
        COMPONENT cranectldc
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_WRITE GROUP_EXECUTE WORLD_READ WORLD_WRITE WORLD_EXECUTE
)

install(FILES
        ${CMAKE_BINARY_DIR}/etc/cranectld.service
        DESTINATION /etc/systemd/system/
        COMPONENT cranectldc)

install(FILES
        ${CMAKE_BINARY_DIR}/etc/craned.service
        DESTINATION /etc/systemd/system/
        COMPONENT cranedc)

# Install configuration files
install(FILES ${CMAKE_SOURCE_DIR}/etc/config.yaml
        DESTINATION /etc/crane/
        COMPONENT cranectldc)

install(FILES ${CMAKE_SOURCE_DIR}/etc/config.yaml
        DESTINATION /etc/crane/
        COMPONENT cranedc)

install(FILES ${CMAKE_SOURCE_DIR}/etc/database.yaml
        DESTINATION /etc/crane/
        COMPONENT cranectldc)

install(FILES ${CMAKE_SOURCE_DIR}/etc/database.yaml
        DESTINATION /etc/crane/
        COMPONENT cranedc)

# Install pam config
install(TARGETS pam_crane
        DESTINATION /usr/lib64/security/
        COMPONENT cranedc)

set(CPACK_GENERATOR "RPM;DEB")
set(CPACK_PACKAGE_NAME "CraneSched")
set(CPACK_PACKAGE_VENDOR "PKUHPC")
set(CPACK_PACKAGE_CONTACT "CraneSched <service@csjstt.com>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "CraneSched package")
set(CPACK_PACKAGE_DESCRIPTION "CraneSched package")
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_VERSION_MAJOR "1")
set(CPACK_PACKAGE_VERSION_MINOR "0")
set(CPACK_PACKAGE_VERSION_PATCH "0")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_SOURCE_DIR}/README.md")
# not support install dir specification
set(CPACK_SET_DESTDIR OFF)
set(CPACK_SOURCE_TGZ_COMPRESSION TRUE)

#RPM
set(CPACK_RPM_PACKAGE_GROUP "PKUHPC")
set(CPACK_RPM_PACKAGE_LICENSE "AGPL v3")
set(CPACK_RPM_PACKAGE_URL "https://github.com/PKUHPC/CraneSched")
set(CPACK_RPM_PACKAGE_DESCRIPTION "CraneSched package")
#DEB
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "PKUHPC")

set(CPACK_DEB_COMPONENT_INSTALL ON)

# pam autoconf for deb package not supported yet
include(CPack)