include(FetchContent)

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

FetchContent_Declare(fmt
        URL https://github.com/fmtlib/fmt/releases/download/9.1.0/fmt-9.1.0.zip
        URL_HASH SHA256=cceb4cb9366e18a5742128cb3524ce5f50e88b476f1e54737a47ffdf4df4c996
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(fmt)