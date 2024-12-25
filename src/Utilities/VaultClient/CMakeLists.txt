add_library(Utility_VaultClient
        VaultClient.cpp
        include/crane/VaultClient.h)
target_include_directories(Utility_VaultClient PUBLIC include)
target_link_libraries(Utility_VaultClient PUBLIC
        curl
        vault
        nlohmann_json
        Utility_PublicHeader
)
