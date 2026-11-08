add_library(Utility_Pmi2 Pmi2.cpp)
target_include_directories(Utility_Pmi2 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../Mpi)
target_link_libraries(Utility_Pmi2 PUBLIC Utility_PublicHeader crane_proto_lib fmt::fmt)
