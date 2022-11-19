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

set(PREVIOUS_BUILD_TYPE_FILE ${CMAKE_BINARY_DIR}/PreviousBuildType.txt)
if (NOT EXISTS ${PREVIOUS_BUILD_TYPE_FILE})
    message(STATUS "No previous build found. Write build type to ${PREVIOUS_BUILD_TYPE_FILE}")
    file(WRITE ${PREVIOUS_BUILD_TYPE_FILE} "${CMAKE_BUILD_TYPE}")
    set(RECONFIGURATION_NEEDED TRUE)
else ()
    file(READ ${PREVIOUS_BUILD_TYPE_FILE} PREVIOUS_BUILD_TYPE)
    if (CMAKE_BUILD_TYPE STREQUAL PREVIOUS_BUILD_TYPE)
        message(STATUS "Previous build type found: ${PREVIOUS_BUILD_TYPE}. Build type is not changed. No reconfiguration for third-party libraries is needed.")
        set(RECONFIGURATION_NEEDED FALSE)
    else ()
        message(STATUS "Previous build type found: ${PREVIOUS_BUILD_TYPE}. Build type is changed to ${CMAKE_BUILD_TYPE}. Reconfiguration for third-party libraries is needed.")
        set(RECONFIGURATION_NEEDED TRUE)
        file(WRITE ${PREVIOUS_BUILD_TYPE_FILE} "${CMAKE_BUILD_TYPE}")
    endif ()
endif ()


project(Crane C CXX)

# Options start here ----------------------------------------------------------------------------

option(CRANE_ENABLE_TESTS "Enable test targets" OFF)

option(CRANE_FORCE_COLORED_OUTPUT "Always produce ANSI-colored output (GNU/Clang only)." TRUE)

# Options end here -------------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 20)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CRANE_ENABLE_TESTS ON)
    add_compile_definitions(CRANE_LOG_LEVEL=CRANE_LOG_LEVEL_TRACE)
endif ()

# Set colorized output when ninja build system is used.
if (${CRANE_FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
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

# If Clang is used, select lld as the linker.
set(CLANG_LLD_LINKER_FLAGS "-fuse-ld=lld")
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Linker flags passed to manually built third party libraries
    set(CRANE_LINKER_FLAGS_INIT "-DCMAKE_SHARED_LINKER_FLAGS_INIT=${CLANG_LLD_LINKER_FLAGS}")

    # static link on c++ libs
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ ${CLANG_LLD_LINKER_FLAGS}")
    # ld doesn't recognize clang object files with IPO / LTO enabled. Use lld here
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++ ${CLANG_LLD_LINKER_FLAGS}")
else ()
    # Linker flags passed to manually built third party libraries
    set(CRANE_LINKER_FLAGS_INIT)

    # static link on c++ libs
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++")
endif ()

if (CMAKE_SIZEOF_VOID_P EQUAL 8)
    # search lib64 directory
    set(FIND_LIBRARY_USE_LIB64_PATHS TRUE)
endif ()

# The following two variables are used in dependency/cmake files.
set(DEPENDENCIES_PRE_INSTALLED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/pre_installed)
set(DEPENDENCIES_ONLINE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/online)


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
add_subdirectory(dependencies/cmake)

find_package(mongocxx REQUIRED CONFIG)

# Link libevent as a static lib
set(LIBEVENT_STATIC_LINK ON)
list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/libevent")
find_package(Libevent REQUIRED)

# Notify CMake that we have module files to find packages/libs.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModule/")

find_package(PAM REQUIRED)

# Check if we found PAM.
if (NOT PAM_FOUND)
    message(FATAL_ERROR "PAM library was not found.")
endif ()

# Find third-party libraries
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.67 REQUIRED COMPONENTS thread system filesystem)
add_definitions(-DBoost_MINOR_VERSION=${Boost_MINOR_VERSION})

# Needed by grpc
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_GRPCPP grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

find_package(PkgConfig REQUIRED)
pkg_check_modules(libcgroup REQUIRED IMPORTED_TARGET libcgroup>=0.41)

# concurrent queue has no cmake config files. Add it manually as a header-only library.
add_library(concurrentqueue INTERFACE)
target_include_directories(concurrentqueue INTERFACE ${DEPENDENCIES_PRE_INSTALLED_DIR}/concurrentqueue/include)

add_definitions(-DCRANE_BUILD_DIRECTORY=\("${CMAKE_BINARY_DIR}"\))

# Proto
add_subdirectory(protos)

# Source Code
add_subdirectory(src)

# Tests
if (CRANE_ENABLE_TESTS)
    enable_testing()
    add_subdirectory(test)
endif ()

# install script
install(TARGETS cranectld craned
        EXPORT ${PROJECT_NAME}Targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)

set(bindir ${CMAKE_INSTALL_PREFIX}/bin)
configure_file(./etc/cranectld.service.in ${PROJECT_BINARY_DIR}/etc/cranectld.service)
configure_file(./etc/craned.service.in ${PROJECT_BINARY_DIR}/etc/craned.service)

install(FILES ${PROJECT_BINARY_DIR}/etc/cranectld.service ${PROJECT_BINARY_DIR}/etc/craned.service DESTINATION /etc/systemd/system/)
install(FILES ./etc/config.yaml.example DESTINATION /etc/crane/)
install(FILES README.md DESTINATION share/doc/crane/)
