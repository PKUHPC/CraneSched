add_library(crane_pmi2 SHARED pmi2_api.c pmi2_util.c)
target_include_directories(crane_pmi2 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/slurm
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(crane_pmi2 PRIVATE Threads::Threads)
set_target_properties(crane_pmi2 PROPERTIES OUTPUT_NAME pmi2 VERSION 0.0.0 SOVERSION 0)
if (UNIX AND NOT APPLE)
    target_link_options(crane_pmi2 PRIVATE "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map")
endif ()
include(GNUInstallDirs)
install(TARGETS crane_pmi2 LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/crane/pmi2
        COMPONENT cranedc)
install(FILES slurm/pmi2.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/crane/pmi2
        COMPONENT cranedc)
install(FILES COPYRIGHT NOTICE DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/crane-pmi2
        COMPONENT cranedc)
