include(FetchContent)
FetchContent_Declare(ranges_v3_repo
        URL https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz
        URL_HASH SHA256=015adb2300a98edfceaf0725beec3337f542af4915cec4d0b89fa0886f4ba9cb
        INACTIVITY_TIMEOUT 5
        )

FetchContent_MakeAvailable(ranges_v3_repo)
