# Create a library from the RpcService sources
add_library(RpcService
    CtldSecureServer.h
    CtldSecureServer.cpp
    CtldPlainServer.h
    CtldPlainServer.cpp
    CtldForCranedServer.h
    CtldForCranedServer.cpp
    CtldForCforedServer.h
    CtldForCforedServer.cpp
    CranedKeeper.h
    CranedKeeper.cpp
)

target_include_directories(RpcService PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src/CraneCtld
)

target_link_libraries(RpcService PUBLIC
        spdlog::spdlog
        concurrentqueue

        Utility_PublicHeader
        Utility_PluginClient
        Utility_VaultClient

        uvw

        cxxopts
        Threads::Threads

        absl::btree
        absl::synchronization
        absl::flat_hash_map

        crane_proto_lib

        bs_thread_pool

        yaml-cpp
        mongocxx_static

        range-v3::range-v3

        Backward::Interface
)

