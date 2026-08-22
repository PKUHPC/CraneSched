add_library(Utility_PrologEpilogExecutor
        PrologEpilogExecutor.cpp
        include/crane/PrologEpilogExecutor.h)

target_include_directories(Utility_PrologEpilogExecutor PUBLIC include)

target_link_libraries(Utility_PrologEpilogExecutor PUBLIC
        Utility_PublicHeader
        uvw
        Threads::Threads)
