include(FetchContent)

FetchContent_Declare(ranges_v3_repo
        GIT_REPOSITORY  https://github.com/ericniebler/range-v3.git
        GIT_TAG         ca1388fb9da8e69314dda222dc7b139ca84e092f
        GIT_SHALLOW     TRUE
)

FetchContent_MakeAvailable(ranges_v3_repo)
