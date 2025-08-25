add_library(Utility_Pmix
        Pmix.cpp
        Pmix.h
        PmixASyncServer.h
        PmixASyncServer.cpp
        PmixClient.h
        PmixClient.cpp
        CranedClient.h
        CranedClient.cpp
        PmixColl.h
        PmixColl.cpp
        PmixCollRing.cpp
        PmixCollTree.cpp
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
        libpmix
        phmap
        uvw
)