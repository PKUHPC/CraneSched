add_library(Utility_CriClient
        CriClient.cpp
        include/crane/CriClient.h)
target_include_directories(Utility_CriClient PUBLIC include)
target_link_libraries(Utility_CriClient PUBLIC
        concurrentqueue
        spdlog::spdlog
        cri_proto_lib
        crane_proto_lib
        Utility_PublicHeader
)
