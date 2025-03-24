add_library(VaultClientWrapper
        VaultClientWrapper.h
        VaultClientWrapper.cpp)

target_include_directories(VaultClientWrapper PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(VaultClientWrapper PUBLIC
        curl
        vault
        nlohmann_json
        phmap
        Utility_PublicHeader
)