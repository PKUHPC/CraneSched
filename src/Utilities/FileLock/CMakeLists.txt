add_library(Utility_FileLock INTERFACE
        include/crane/FileLock.h)
target_include_directories(Utility_FileLock INTERFACE include)
target_link_libraries(Utility_FileLock INTERFACE Utility_PublicHeader)
