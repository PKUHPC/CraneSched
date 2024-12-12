add_executable(cranestepd
        CraneStepdPublicDefs.h
        CraneStepd.cpp
        CraneStepdPreCompiledHeader.h
)

target_precompile_headers(cranestepd PRIVATE CraneStepdPreCompiledHeader.h)

add_dependencies(cranestepd libcgroup)
target_include_directories(
        cranestepd PRIVATE ${LIBCGROUP_PATH}/include/
)

target_link_libraries(cranestepd
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
    target_link_libraries(cranestepd stdc++fs)
endif ()