include(FetchContent)

FetchContent_Declare(lz4_repo
        URL https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz
        INACTIVITY_TIMEOUT 5
        )
FetchContent_GetProperties(lz4_repo)
if (NOT lz4_repo_POPULATED)
    FetchContent_Populate(lz4_repo)
endif ()

set(LZ4_BUNDLED_MODE ON CACHE BOOL "" FORCE)
set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
add_subdirectory("${lz4_repo_SOURCE_DIR}/build/cmake"
        "${lz4_repo_BINARY_DIR}" EXCLUDE_FROM_ALL)

set(CRANE_LZ4_LIBRARY
        "${lz4_repo_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX}")
if (NOT TARGET lz4::lz4)
    add_library(lz4::lz4 UNKNOWN IMPORTED GLOBAL)
    set_target_properties(lz4::lz4 PROPERTIES
            IMPORTED_LOCATION "${CRANE_LZ4_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${lz4_repo_SOURCE_DIR}/lib")
endif ()

set(lz4_INCLUDE_DIRS "${lz4_repo_SOURCE_DIR}/lib" CACHE PATH "" FORCE)
set(lz4_LIBRARIES "${CRANE_LZ4_LIBRARY}" CACHE FILEPATH "" FORCE)

if (CRANE_USE_GITEE_SOURCE)
    set(ROCKSDB_SRC_URL "https://gitee.com/mirrors/rocksdb/repository/archive/v11.1.1.tar.gz")
else ()
    set(ROCKSDB_SRC_URL "https://github.com/facebook/rocksdb/archive/refs/tags/v11.1.1.tar.gz")
endif ()

set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
set(WITH_LZ4 ON CACHE BOOL "" FORCE)
set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)
set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(rocksdb_repo
        URL ${ROCKSDB_SRC_URL}
        INACTIVITY_TIMEOUT 5
        )

FetchContent_MakeAvailable(rocksdb_repo)

foreach (rocksdb_target rocksdb rocksdb-shared)
    if (TARGET ${rocksdb_target} AND TARGET lz4_static)
        add_dependencies(${rocksdb_target} lz4_static)
    endif ()
endforeach ()
