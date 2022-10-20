include(FetchContent)

FetchContent_Declare(libevent_repo
        URL https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz
        URL_HASH SHA256=92e6de1be9ec176428fd2367677e61ceffc2ee1cb119035037a27d346b0403bb
        INACTIVITY_TIMEOUT 5
        )

FetchContent_GetProperties(libevent_repo)
if (NOT libevent_repo_POPULATED)
    FetchContent_Populate(libevent_repo)
    message(STATUS "LibEvent source dir: ${libevent_repo_SOURCE_DIR}")

    if (NOT EXISTS ${libevent_repo_SOURCE_DIR}/cmake-build/build.ninja
            OR NOT EXISTS ${libevent_repo_SOURCE_DIR}/cmake-build/CMakeCache.txt
            OR RECONFIGURATION_NEEDED)
        execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory "${libevent_repo_SOURCE_DIR}/cmake-build")
        execute_process(
                COMMAND ${CMAKE_COMMAND} -Wno-dev -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_ONLINE_DIR}/libevent -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} ${CRANE_LINKER_FLAGS_INIT} -G Ninja -DEVENT__LIBRARY_TYPE=STATIC -DEVENT__DISABLE_TESTS=ON -DEVENT__DISABLE_BENCHMARK=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ..
                WORKING_DIRECTORY ${libevent_repo_SOURCE_DIR}/cmake-build
        )
    endif ()
    execute_process(
            COMMAND ninja install
            WORKING_DIRECTORY ${libevent_repo_SOURCE_DIR}/cmake-build
    )
endif ()

