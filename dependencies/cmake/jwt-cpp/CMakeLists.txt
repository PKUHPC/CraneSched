include(FetchContent)


set(JWT_CPP_SRC_URL "https://github.com/Thalhammer/jwt-cpp/archive/refs/tags/v0.7.0.tar.gz")

fetchcontent_declare(jwt-cpp
        URL ${JWT_CPP_SRC_URL}
        URL_HASH SHA256=b9eb270e3ba8221e4b2bc38723c9a1cb4fa6c241a42908b9a334daff31137406
        INACTIVITY_TIMEOUT 5
        )
set(JWT_BUILD_EXAMPLES OFF CACHE BOOL "disable building examples" FORCE)

fetchcontent_makeavailable(jwt-cpp)