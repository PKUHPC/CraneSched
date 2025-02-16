add_subdirectory(AnonymousPipe)
add_subdirectory(FileLogger)
add_subdirectory(PluginClient)
add_subdirectory(PublicHeader)

if (CRANE_RAFT_SERVER)
    add_subdirectory(NuRaft)
endif ()