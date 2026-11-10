add_subdirectory(Protocol)
add_library(Utility_Pmi Server/Server.cpp Server/Connection.cpp Server/Collective.cpp Server/Topology.cpp
        Protocol/Handshake.cpp Protocol/Pmi1.cpp Protocol/Pmi2.cpp Protocol/Reply.cpp Runtime.cpp)
target_include_directories(Utility_Pmi PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(Utility_Pmi PUBLIC Utility_MpiControl Utility_PublicHeader crane_proto_lib fmt::fmt crane_pmi_wire)
add_subdirectory(Client)
