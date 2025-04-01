add_library(Utility_Pmix
        PmixComment.h
        Pmix.cpp
        Pmix.h)

target_include_directories(Utility_Pmix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(Utility_Pmix PUBLIC
        concurrentqueue
        spdlog::spdlog
        crane_proto_lib
        Utility_PublicHeader
        fmt::fmt
)
