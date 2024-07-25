include(FetchContent)
include(ExternalProject)

# download libcgroup 
FetchContent_Declare(
  libcgroup
  GIT_REPOSITORY https://github.com/libcgroup/libcgroup.git
  GIT_TAG        v3.1.0
)
FetchContent_MakeAvailable(libcgroup)

# configure and build libcgroup
ExternalProject_Add(
  libcgroup_project
  SOURCE_DIR ${libcgroup_SOURCE_DIR}
  CONFIGURE_COMMAND pwd && autoreconf -ivf && ./configure --enable-static --disable-shared --prefix=<INSTALL_DIR>
  BUILD_COMMAND make
  INSTALL_COMMAND make install
  BUILD_IN_SOURCE 1
)

# get install path
ExternalProject_Get_Property(libcgroup_project install_dir)

include_directories(${install_dir}/include)

link_directories(${install_dir}/lib)

find_package(PkgConfig REQUIRED)
pkg_check_modules(SYSTEMD REQUIRED libsystemd)

include_directories(${SYSTEMD_INCLUDE_DIRS})
link_directories(${SYSTEMD_LIBRARY_DIRS})
add_executable(craned
        CtldClient.h
        CtldClient.cpp
        CgroupManager.h
        CgroupManager.cpp
        CforedClient.h
        CforedClient.cpp
        TaskManager.h
        TaskManager.cpp
        CranedServer.h
        CranedServer.cpp
        CranedPublicDefs.h
        Craned.cpp

        CranedPreCompiledHeader.h)

target_precompile_headers(craned PRIVATE CranedPreCompiledHeader.h)

target_link_libraries(craned
        concurrentqueue

        #${LIBCGROUP_LIBRARIES}
        # cgroup
        ${install_dir}/lib/libcgroup.a
        ${SYSTEMD_LIBRARIES}
        Utility_PublicHeader

        crane_proto_lib

        dev_event_core
        dev_event_pthreads

        bs_thread_pool

        cxxopts
        Threads::Threads
        nlohmann_json::nlohmann_json

        absl::flat_hash_map
        absl::synchronization

        uvw

        yaml-cpp
        result

        Backward::Interface
)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned stdc++fs)
endif ()