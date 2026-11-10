add_library(crane_pmi_wire STATIC Pmi1Codec.cpp Pmi2Codec.cpp)
set_target_properties(crane_pmi_wire PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(crane_pmi_wire PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
