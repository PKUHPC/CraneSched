add_library(Utility_PluginClient
        PluginClient.cpp
        include/crane/PluginClient.h)
target_include_directories(Utility_PluginClient PUBLIC include)
target_link_libraries(Utility_PluginClient PUBLIC
        concurrentqueue
        spdlog::spdlog
        crane_proto_lib
        Utility_PublicHeader
)
