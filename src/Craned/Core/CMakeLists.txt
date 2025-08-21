add_executable(craned
        CtldClient.h
        CtldClient.cpp
        CgroupManager.h
        CgroupManager.cpp
        JobManager.h
        JobManager.cpp
        DeviceManager.cpp
        DeviceManager.h
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
        ${LIBCGROUP_BUILD_PRODUCTS}
)