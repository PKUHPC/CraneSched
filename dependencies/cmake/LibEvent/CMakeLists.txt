include(FetchContent)

set(EVENT__LIBRARY_TYPE STATIC)
set(EVENT__DISABLE_TESTS ON)
set(EVENT__DISABLE_BENCHMARK ON)

FetchContent_Declare(libevent_repo
        URL https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz
        URL_HASH SHA256=92e6de1be9ec176428fd2367677e61ceffc2ee1cb119035037a27d346b0403bb
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(libevent_repo)