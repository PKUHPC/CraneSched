if (CRANE_USE_SYSTEM_LUA)
    message(STATUS "Using Lua from system.")

    find_package(Lua REQUIRED)

    add_library(crane_lua INTERFACE)
    target_include_directories(crane_lua SYSTEM INTERFACE ${LUA_INCLUDE_DIR})
    target_link_libraries(crane_lua INTERFACE ${LUA_LIBRARIES})
    return()
endif ()

message(STATUS "Building Lua from source.")

include(FetchContent)

# Lua has no upstream CMake build. Fetch the audited release archive and build
# the same core and standard-library objects as Lua's Linux Makefile.
if (POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif ()

set(CRANE_LUA_VERSION 5.4.8)
set(CRANE_LUA_URL
        "https://www.lua.org/ftp/lua-${CRANE_LUA_VERSION}.tar.gz")
set(CRANE_LUA_SHA256
        4f18ddae154e793e46eeab727c59ef1c0c0c2b744e7b94219710d76f530629ae)

FetchContent_Declare(crane_lua
        URL ${CRANE_LUA_URL}
        URL_HASH SHA256=${CRANE_LUA_SHA256}
        INACTIVITY_TIMEOUT 5)
FetchContent_GetProperties(crane_lua)
if (NOT crane_lua_POPULATED)
    FetchContent_Populate(crane_lua)
endif ()

set(CRANE_LUA_SOURCE_DIR "${crane_lua_SOURCE_DIR}/src")
set(CRANE_LUA_SOURCES
        ${CRANE_LUA_SOURCE_DIR}/lapi.c
        ${CRANE_LUA_SOURCE_DIR}/lauxlib.c
        ${CRANE_LUA_SOURCE_DIR}/lbaselib.c
        ${CRANE_LUA_SOURCE_DIR}/lcode.c
        ${CRANE_LUA_SOURCE_DIR}/lcorolib.c
        ${CRANE_LUA_SOURCE_DIR}/lctype.c
        ${CRANE_LUA_SOURCE_DIR}/ldblib.c
        ${CRANE_LUA_SOURCE_DIR}/ldebug.c
        ${CRANE_LUA_SOURCE_DIR}/ldo.c
        ${CRANE_LUA_SOURCE_DIR}/ldump.c
        ${CRANE_LUA_SOURCE_DIR}/lfunc.c
        ${CRANE_LUA_SOURCE_DIR}/lgc.c
        ${CRANE_LUA_SOURCE_DIR}/linit.c
        ${CRANE_LUA_SOURCE_DIR}/liolib.c
        ${CRANE_LUA_SOURCE_DIR}/llex.c
        ${CRANE_LUA_SOURCE_DIR}/lmathlib.c
        ${CRANE_LUA_SOURCE_DIR}/lmem.c
        ${CRANE_LUA_SOURCE_DIR}/loadlib.c
        ${CRANE_LUA_SOURCE_DIR}/lobject.c
        ${CRANE_LUA_SOURCE_DIR}/lopcodes.c
        ${CRANE_LUA_SOURCE_DIR}/loslib.c
        ${CRANE_LUA_SOURCE_DIR}/lparser.c
        ${CRANE_LUA_SOURCE_DIR}/lstate.c
        ${CRANE_LUA_SOURCE_DIR}/lstring.c
        ${CRANE_LUA_SOURCE_DIR}/lstrlib.c
        ${CRANE_LUA_SOURCE_DIR}/ltable.c
        ${CRANE_LUA_SOURCE_DIR}/ltablib.c
        ${CRANE_LUA_SOURCE_DIR}/ltm.c
        ${CRANE_LUA_SOURCE_DIR}/lundump.c
        ${CRANE_LUA_SOURCE_DIR}/lutf8lib.c
        ${CRANE_LUA_SOURCE_DIR}/lvm.c
        ${CRANE_LUA_SOURCE_DIR}/lzio.c)

add_library(crane_lua ${CRANE_LUA_SOURCES})
target_compile_definitions(crane_lua PRIVATE LUA_USE_LINUX)
target_include_directories(crane_lua SYSTEM PUBLIC ${CRANE_LUA_SOURCE_DIR})

if (UNIX)
    target_link_libraries(crane_lua PUBLIC m ${CMAKE_DL_LIBS})
endif ()
