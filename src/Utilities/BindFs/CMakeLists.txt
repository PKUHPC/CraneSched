add_library(Utility_BindFs
        BindFs.cpp
        include/crane/BindFs.h)
target_include_directories(Utility_BindFs PUBLIC include)
target_link_libraries(Utility_BindFs PUBLIC
        subprocess
        Utility_PublicHeader
        Utility_PasswordEntry
        Utility_FileLock
        nlohmann_json
)
