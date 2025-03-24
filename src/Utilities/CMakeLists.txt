add_subdirectory(AnonymousPipe)
add_subdirectory(FileLogger)
add_subdirectory(PluginClient)
add_subdirectory(PublicHeader)
add_subdirectory(EmbeddedDb)

if (CRANE_RAFT_SERVER)
    add_subdirectory(NuRaft)
endif ()