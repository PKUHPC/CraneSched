include(FetchContent)

FetchContent_Declare(unqlite_repo
        URL https://github.com/symisc/unqlite/archive/9c4cd308e139a21a20247d13af77fde8a2556503.tar.gz
        URL_HASH SHA256=2918635c5e14156f7f7b836427f964d2a34809c71f36b29cce974287f39f41ca
        )

SET(ENABLE_THREADS ON CACHE BOOL "unqlite multi-threading support" FORCE)
FetchContent_MakeAvailable(unqlite_repo)

target_include_directories(unqlite PUBLIC ${unqlite_repo_SOURCE_DIR})