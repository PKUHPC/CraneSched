#add_library(Utility_Pmix
#        ../../Craned/Pmix/Pmix.cpp
#        ../../Craned/Pmix/Pmix.h
#        ../../Craned/Pmix/PmixColl.cpp
#        ../../Craned/Pmix/PmixColl.h
#        ../../Craned/Pmix/PmixCollRing.cpp
#        ../../Craned/Pmix/PmixCollTree.cpp
#        ../../Craned/Pmix/PmixState.h
#        ../../Craned/Pmix/PmixState.cpp
#        ../../Craned/Pmix/ReverseTree.h
#        ../../Craned/Pmix/ReverseTree.cpp)
#
#target_include_directories(Utility_Pmix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
#
#target_link_libraries(Utility_Pmix PUBLIC
#        concurrentqueue
#
##        spdlog::spdlog
#
#        crane_proto_lib
#
#        Utility_PublicHeader
#
#        fmt::fmt
#
#        libpmix
#
#        phmap
#)
