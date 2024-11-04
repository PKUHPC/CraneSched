include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    set(UNQLITE_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/unqlite-9c4cd308e139a21a20247d13af77fde8a2556503.tar.gz")
else ()
    set(UNQLITE_SRC_URL "https://github.com/symisc/unqlite/archive/9c4cd308e139a21a20247d13af77fde8a2556503.tar.gz")
endif ()

FetchContent_Declare(unqlite_repo
        URL ${UNQLITE_SRC_URL}
        URL_HASH SHA256=2918635c5e14156f7f7b836427f964d2a34809c71f36b29cce974287f39f41ca
        INACTIVITY_TIMEOUT 5
        )

SET(ENABLE_THREADS ON)

# Unqlite will pollute `CMAKE_INSTALL_PREFIX`. Set this to OFF to avoid it.
set(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT OFF)
FetchContent_MakeAvailable(unqlite_repo)

target_include_directories(unqlite PUBLIC ${unqlite_repo_SOURCE_DIR})