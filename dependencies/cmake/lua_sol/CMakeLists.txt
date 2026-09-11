include(FetchContent)

FetchContent_Declare(
        sol2
        GIT_REPOSITORY https://github.com/ThePhD/sol2.git
        GIT_TAG        v3.5.0
        GIT_SHALLOW    TRUE
        GIT_SUBMODULES ""
)

FetchContent_MakeAvailable(sol2)
