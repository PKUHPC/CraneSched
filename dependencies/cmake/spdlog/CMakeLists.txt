include(FetchContent)

FetchContent_Declare(spdlog
        URL https://github.com/gabime/spdlog/archive/refs/tags/v1.10.0.tar.gz
        URL_HASH SHA256=697f91700237dbae2326b90469be32b876b2b44888302afbc7aceb68bcfe8224
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(spdlog)