add_library(Utility_PublicHeader
        String.cpp Network.cpp FdFunctions.cpp PublicHeader.cpp
        include/crane/String.h
        include/crane/Network.h
        include/crane/FdFunctions.h
        include/crane/PublicHeader.h
        include/crane/BoostInterprocessBarrier.h
        include/crane/Lock.h
        include/crane/Pointer.h
        )
target_include_directories(Utility_PublicHeader PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} include)
target_link_libraries(Utility_PublicHeader PUBLIC spdlog::spdlog crane_proto_lib)