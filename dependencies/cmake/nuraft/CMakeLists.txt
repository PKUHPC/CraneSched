include(FetchContent)

set(NURAFT_SRC_URL "https://github.com/eBay/NuRaft.git")

FetchContent_Declare(nuraft
        GIT_REPOSITORY ${NURAFT_SRC_URL}
        GIT_TAG master
        GIT_SHALLOW TRUE
        SYSTEM
        GIT_SUBMODULES "asio"
)
FetchContent_MakeAvailable(nuraft)
FetchContent_GetProperties(nuraft)