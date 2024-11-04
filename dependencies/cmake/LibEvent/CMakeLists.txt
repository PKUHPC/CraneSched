include(FetchContent)

if (BUILD_SHARED_LIBS)
    set(EVENT__LIBRARY_TYPE "SHARED")
else ()
    set(EVENT__LIBRARY_TYPE "STATIC")
endif ()
message(STATUS "LibEvent ${EVENT__LIBRARY_TYPE}")
set(EVENT__DISABLE_TESTS ON)
set(EVENT__DISABLE_BENCHMARK ON)
set(EVENT__DISABLE_REGRESS ON)
set(EVENT__DISABLE_SAMPLES ON)

# Disable as we don't use network functions in libevent
set(EVENT__DISABLE_OPENSSL ON)
set(EVENT__DISABLE_MBEDTLS ON)

if (CRANE_USE_GITEE_SOURCE)
    set(LIBEVENT_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/libevent-2.1.12-stable.tar.gz")
else ()
    set(LIBEVENT_SRC_URL "https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz")
endif ()

FetchContent_Declare(libevent_repo
        URL ${LIBEVENT_SRC_URL}
        URL_HASH SHA256=92e6de1be9ec176428fd2367677e61ceffc2ee1cb119035037a27d346b0403bb
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(libevent_repo)

if (BUILD_SHARED_LIBS)
    add_library(dev_event_core ALIAS event_core_shared)
    add_library(dev_event_pthreads ALIAS event_pthreads_shared)
else ()
    add_library(dev_event_core ALIAS event_core)
    add_library(dev_event_pthreads ALIAS event_pthreads)
endif ()