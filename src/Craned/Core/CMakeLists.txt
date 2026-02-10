add_executable(craned
        CtldClient.h
        CtldClient.cpp
        JobManager.h
        JobManager.cpp
        SupervisorKeeper.cpp
        SupervisorKeeper.h
        CranedServer.h
        CranedServer.cpp
        CranedForPamServer.h
        CranedForPamServer.cpp
        Craned.cpp
)

# Link with the common static library
target_link_libraries(craned
    PRIVATE
        craned_common
        Utility_CriClient
        crane_tracer
)