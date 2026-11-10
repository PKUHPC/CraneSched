add_library(Utility_Pmix
        Callbacks.cpp
        Collective/CollectiveManager.cpp
        Collective/ReverseTree.cpp
        Collective/RingCollective.cpp
        Collective/TreeCollective.cpp
        Modex/ModexManager.cpp
        Runtime.cpp
        Server.cpp
        Namespace.cpp
        Operations.cpp
        Transport/PmixGrpcClient.cpp
        Transport/PmixGrpcServer.cpp
        Transport/PmixUcxClient.cpp
        Transport/PmixUcxServer.cpp
)
target_include_directories(Utility_Pmix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(Utility_Pmix PUBLIC Utility_MpiControl concurrentqueue spdlog::spdlog crane_proto_lib fmt::fmt Utility_PublicHeader phmap uvw)
if (CRANE_ENABLE_PMIX)
    target_compile_definitions(Utility_Pmix PUBLIC HAVE_PMIX)
    target_link_libraries(Utility_Pmix PUBLIC PMIx::pmix)
endif ()
if (CRANE_ENABLE_UCX)
    target_compile_definitions(Utility_Pmix PUBLIC HAVE_UCX)
    target_link_libraries(Utility_Pmix PUBLIC PkgConfig::UCX)
endif ()
