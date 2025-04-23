add_executable(craned
        CtldClient.h
        CtldClient.cpp
        CgroupManager.h
        CgroupManager.cpp
        CforedClient.h
        CforedClient.cpp
        TaskManager.h
        TaskManager.cpp
        DeviceManager.cpp
        DeviceManager.h
        CranedServer.h
        CranedServer.cpp
        CranedClient.h
        CranedClient.cpp
        CranedPublicDefs.h
        Craned.cpp

        Pmix/Pmix.cpp
        Pmix/Pmix.h
        Pmix/PmixState.cpp
        Pmix/PmixState.h
        Pmix/PmixColl.cpp
        Pmix/PmixColl.h
        Pmix/PmixCollRing.cpp
        Pmix/PmixCollTree.cpp
        Pmix/ReverseTree.cpp
        Pmix/ReverseTree.h

        CranedPreCompiledHeader.h
)

target_precompile_headers(craned PRIVATE CranedPreCompiledHeader.h)
add_dependencies(craned libcgroup)
target_include_directories(
        craned PRIVATE ${LIBCGROUP_PATH}/include/
                       ${CMAKE_CURRENT_SOURCE_DIR}
)

if (CRANE_ENABLE_CGROUP_V2)
    target_compile_definitions(craned PRIVATE -DCRANE_ENABLE_CGROUP_V2)
endif ()

if (CRANE_ENABLE_BPF)
    add_dependencies(craned crane_ebpf)
    target_compile_definitions(craned PRIVATE -DCRANE_ENABLE_BPF)
endif ()

target_link_libraries(craned
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>
        concurrentqueue

        ${LIBCGROUP_BUILD_PRODUCTS}

        Utility_PublicHeader
        Utility_PluginClient

        crane_proto_lib

        bs_thread_pool

        cxxopts
        Threads::Threads
        ${LIBUTIL_LIBRARY}
        nlohmann_json::nlohmann_json

        absl::flat_hash_map
        absl::synchronization

        uvw
        subprocess

        libpmix
        fmt::fmt
        phmap

        yaml-cpp

        Backward::Interface

        $<$<BOOL:${CRANE_ENABLE_BPF}>:BPF::BPF>
)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(craned stdc++fs)
endif ()