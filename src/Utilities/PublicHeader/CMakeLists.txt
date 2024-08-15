add_library(Utility_PublicHeader
        String.cpp Network.cpp OS.cpp PublicHeader.cpp Logger.cpp
        include/crane/String.h
        include/crane/Network.h
        include/crane/OS.h
        include/crane/PublicHeader.h
        include/crane/Lock.h
        include/crane/Pointer.h
        include/crane/Logger.h
        include/crane/PasswordEntry.h
        include/crane/AtomicHashMap.h
        GrpcHelper.cpp
        include/crane/GrpcHelper.h)
target_include_directories(Utility_PublicHeader PUBLIC include)
target_link_libraries(Utility_PublicHeader PUBLIC
        spdlog::spdlog
        crane_proto_lib
        fpm
)

# This trimmed version is used for PAM module
# since -fPIC flag should be enabled for a dynamic library
# and we do not expect the performance to be affected by it.
add_library(Utility_PublicHeaderNoLogger
        include/crane/PublicHeader.h
        PublicHeader.cpp
)
target_include_directories(Utility_PublicHeaderNoLogger PUBLIC include)
target_link_libraries(Utility_PublicHeaderNoLogger PUBLIC crane_proto_lib fpm)