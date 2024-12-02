include(FetchContent)

set(HASHMAP_SRC_URL "https://github.com/greg7mdp/parallel-hashmap/archive/refs/tags/v1.4.1.tar.gz")

FetchContent_Declare(parallel-hashmap
        URL ${HASHMAP_SRC_URL}
        URL_HASH SHA256=949874f4207b8735422438b23b884fb1f4b926689bb5eebff38cc4d357d09cd2
        INACTIVITY_TIMEOUT 5
        )
        
FetchContent_MakeAvailable(parallel-hashmap)