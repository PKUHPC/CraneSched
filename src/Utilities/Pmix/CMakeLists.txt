add_library(Utility_Pmix
        Pmix.cpp
        Pmix.h
        PmixColl.cpp
        PmixColl.h
        PmixCollRing.cpp
        PmixCollTree.cpp
        PmixState.h
        PmixState.cpp
        ReverseTree.h
        ReverseTree.cpp)

target_include_directories(Utility_Pmix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(Utility_Pmix PUBLIC
        concurrentqueue

        spdlog::spdlog

        crane_proto_lib

        Utility_PublicHeader

        fmt::fmt

        libpmix

        phmap
)
