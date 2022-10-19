include(FetchContent)
FetchContent_Declare(GTest
        OVERRIDE_FIND_PACKAGE

        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(GTest)