add_library(Utility_TimerSet TimerSet.cpp include/crane/TimerSet.h)
target_include_directories(Utility_TimerSet PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} include)
target_link_libraries(Utility_TimerSet PUBLIC
        libevent::core
        libevent::pthreads

        concurrentqueue

        Utility_PublicHeader
        )
