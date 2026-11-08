set(PMI2_INTERNAL_SOURCES
        Pmi2Codec.cpp
        Pmi2Transport.cpp
        Pmi2Session.cpp)

add_library(crane_pmi2_internal STATIC ${PMI2_INTERNAL_SOURCES}
        Pmi2Codec.h Pmi2Transport.h Pmi2Session.h)
set_target_properties(crane_pmi2_internal PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(crane_pmi2_internal
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/crane/pmi2
        ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(crane_pmi2_internal PRIVATE Threads::Threads)

add_library(crane_pmi2 SHARED
        Pmi2Client.cpp
        Pmi2Client.h
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
