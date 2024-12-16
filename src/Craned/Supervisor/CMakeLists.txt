add_executable(supervisor
        SupervisorPublicDefs.h
        Supervisor.cpp
        SupervisorPreCompiledHeader.h
)

target_precompile_headers(supervisor PRIVATE SupervisorPreCompiledHeader.h)

add_dependencies(supervisor libcgroup)
target_include_directories(
        supervisor PRIVATE ${LIBCGROUP_PATH}/include/
)

target_link_libraries(supervisor
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>

        ${LIBCGROUP_BUILD_PRODUCTS}

        Utility_PublicHeader
        Utility_PluginClient

        crane_proto_lib

        bs_thread_pool

        cxxopts
        Threads::Threads
        ${LIBUTIL_LIBRARY}

        absl::flat_hash_map
        absl::synchronization

        uvw

        yaml-cpp

        Backward::Interface

        $<$<BOOL:${ENABLE_BPF}>:bpf>

)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(supervisor stdc++fs)
endif ()