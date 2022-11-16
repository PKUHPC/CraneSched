add_library(Utility_PublicHeader
        String.cpp Network.cpp FdFunctions.cpp PublicHeader.cpp
        include/crane/String.h
        include/crane/Network.h
        include/crane/FdFunctions.h
        include/crane/PublicHeader.h
        include/crane/BoostInterprocessBarrier.h
        include/crane/Lock.h
        include/crane/Pointer.h
        include/crane/Logger.h
        )
target_include_directories(Utility_PublicHeader PUBLIC include)
target_link_libraries(Utility_PublicHeader PUBLIC spdlog::spdlog crane_proto_lib)

add_library(Utility_PublicHeaderNoLogger
        include/crane/PublicHeader.h
        PublicHeader.cpp
        )
target_include_directories(Utility_PublicHeaderNoLogger PUBLIC include)
target_link_libraries(Utility_PublicHeaderNoLogger PUBLIC crane_proto_lib)
