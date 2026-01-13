include(FetchContent)

FetchContent_Declare(
        sol2
        GIT_REPOSITORY https://github.com/ThePhD/sol2.git
        GIT_TAG        v3.5.0
)

FetchContent_MakeAvailable(sol2)