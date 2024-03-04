include(FetchContent)

set(FPM_SRC_URL "https://github.com/MikeLankamp/fpm/archive/refs/tags/v1.1.0.zip")

FetchContent_Declare(fpm
        URL ${FPM_SRC_URL}
        URL_HASH SHA256=0e68e697bb23bb77bf61e7ff8277f372aa9daf887bfe534654b12a8b7bc807ce
        INACTIVITY_TIMEOUT 5
)

FetchContent_GetProperties(fpm)
if(NOT fpm_POPULATED)
    FetchContent_Populate(fpm)
    add_library(fpm INTERFACE)
    target_include_directories(fpm INTERFACE ${fpm_SOURCE_DIR}/include)
endif()
