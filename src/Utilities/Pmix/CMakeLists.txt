add_library(Utility_Pmix
        Pmix.cpp
        Pmix.h
        
        PmixConn/PmixASyncServer.h
        PmixConn/PmixClient.h
        PmixConn/PmixGrpcServer.h
        PmixConn/PmixGrpcServer.cpp
        PmixConn/PmixUcxServer.h
        PmixConn/PmixUcxServer.cpp
        PmixConn/PmixGrpcClient.h
        PmixConn/PmixGrpcClient.cpp
        PmixConn/PmixUcxClient.h
        PmixConn/PmixUcxClient.cpp

        PmixColl/PmixColl.h
        PmixColl/PmixCollRing.h
        PmixColl/PmixCollRing.cpp
        PmixColl/PmixCollTree.h
        PmixColl/PmixCollTree.cpp
        
        CranedClient.h
        CranedClient.cpp
        PmixCallbacks.h
        PmixCallbacks.cpp
        PmixCommon.h
        PmixDModex.h
        PmixDModex.cpp
        PmixState.h
        PmixState.cpp
        ReverseTree.h
        ReverseTree.cpp)

target_include_directories(Utility_Pmix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Utility_Pmix PUBLIC
        concurrentqueue
        spdlog::spdlog
        crane_proto_lib
        fmt::fmt
        Utility_PublicHeader
        phmap
        uvw
)

if (CRANE_ENABLE_PMIX)
    target_compile_definitions(Utility_Pmix PUBLIC HAVE_PMIX)
    target_link_libraries(Utility_Pmix PUBLIC PMIx::pmix)
endif ()

if (CRANE_ENABLE_UCX)
    target_compile_definitions(Utility_Pmix PUBLIC HAVE_UCX)
    target_link_libraries(Utility_Pmix PUBLIC PkgConfig::UCX)
endif ()
