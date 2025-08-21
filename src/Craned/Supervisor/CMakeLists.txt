add_executable(csupervisor
        CranedClient.cpp
        CranedClient.h
        CforedClient.cpp
        CforedClient.h
        SupervisorServer.h
        SupervisorServer.cpp
        Supervisor.cpp
        TaskManager.cpp
        TaskManager.h
)

# Link with the common static library
target_link_libraries(csupervisor
    PRIVATE
        craned_common
        Utility_CriClient
        cri_proto_lib
)