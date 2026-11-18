add_library(crane_pmi_transport STATIC Transport.cpp)
set_target_properties(crane_pmi_transport PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(crane_pmi_transport PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(crane_pmi_transport PUBLIC crane_pmi_wire)

add_library(crane_pmi2_internal STATIC Pmi2Session.cpp)
set_target_properties(crane_pmi2_internal PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(crane_pmi2_internal
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/crane/pmi2
        ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(crane_pmi2_internal PUBLIC crane_pmi_transport PRIVATE Threads::Threads)

add_library(crane_pmi2 SHARED
        Pmi2Client.cpp
)

target_include_directories(crane_pmi2
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/crane/pmi2
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(crane_pmi2 PRIVATE crane_pmi2_internal Threads::Threads)
set_target_properties(crane_pmi2 PROPERTIES OUTPUT_NAME pmi2 VERSION 0.0.0 SOVERSION 0)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_options(crane_pmi2 PRIVATE
            "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map")
endif ()

install(TARGETS crane_pmi2 LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/crane/pmi2
        COMPONENT cranedc)
install(FILES include/crane/pmi2/pmi2.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/crane/pmi2 COMPONENT cranedc)

add_library(crane_pmi1_internal STATIC Pmi1Session.cpp)
set_target_properties(crane_pmi1_internal PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(crane_pmi1_internal PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(crane_pmi1_internal PUBLIC crane_pmi_transport PRIVATE Threads::Threads)
add_library(crane_pmi1 SHARED Pmi1Client.cpp)
target_include_directories(crane_pmi1 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/crane/pmi1)
target_link_libraries(crane_pmi1 PRIVATE crane_pmi1_internal Threads::Threads)
set_target_properties(crane_pmi1 PROPERTIES OUTPUT_NAME pmi VERSION 0.0.0 SOVERSION 0)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_options(crane_pmi1 PRIVATE
            "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/pmi1-exports.map")
endif ()
install(TARGETS crane_pmi1 LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/crane/pmi1
        COMPONENT cranedc)
install(FILES include/crane/pmi1/pmi.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/crane/pmi1 COMPONENT cranedc)
