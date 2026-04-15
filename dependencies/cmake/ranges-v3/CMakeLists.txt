include(FetchContent)

FetchContent_Declare(ranges_v3_repo
        GIT_REPOSITORY  https://github.com/ericniebler/range-v3.git
        GIT_TAG         108f93c279c8f9cec175dac361084983d0176e99
        GIT_SHALLOW     TRUE
)

FetchContent_MakeAvailable(ranges_v3_repo)
