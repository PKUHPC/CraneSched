cmake_minimum_required(VERSION 3.16)

# Let cmake apply IPO flags for all compilers and do not output warnings.
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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
    set(CRANE_LINKER_FLAGS_INIT "\"\"")

    # static link on c++ libs
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++")
endif ()

# Set colorized output when ninja build system is used.
option(FORCE_COLORED_OUTPUT "Always produce ANSI-colored output (GNU/Clang only)." TRUE)
if (${FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    endif ()
endif ()

# The following two variables are used in dependency/cmake files.
set(DEPENDENCIES_PRE_INSTALLED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/pre_installed)
set(DEPENDENCIES_ONLINE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/online)


find_package(Threads REQUIRED)

# Some content are downloaded and built inside cmake folder.
# This line must be place before any find_package() command.
# Independently built projects are installed to ${DEPENDENCIES_ONLINE_DIR}
#
# Since find_package needs to be in the top scope, we append the paths of installed
# projects at top-level CMakeLists.txt
add_subdirectory(dependencies/cmake)

list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/mongo-c-driver")
list(APPEND CMAKE_PREFIX_PATH "${DEPENDENCIES_ONLINE_DIR}/mongo-cxx-driver")
find_package(mongocxx REQUIRED)

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
find_package(Boost 1.67 REQUIRED COMPONENTS thread fiber system filesystem)
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

# Proto
add_subdirectory(protos)

# Source Code
add_subdirectory(src)

# Tests
add_subdirectory(test)

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