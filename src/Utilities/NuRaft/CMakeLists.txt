add_library(Utility_NuRaft NuRaftLogStore.cpp
        include/crane/NuRaftLogStore.h
        include/crane/NuRaftStateManager.h
        include/crane/NuRaftLoggerWrapper.h)
target_include_directories(Utility_NuRaft PUBLIC include)
target_link_libraries(Utility_NuRaft PUBLIC NuRaft::static_lib Utility_EmbeddedDb)