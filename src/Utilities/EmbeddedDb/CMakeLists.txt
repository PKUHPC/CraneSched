add_library(Utility_EmbeddedDb EmbeddedDb.cpp include/crane/EmbeddedDb.h)
target_include_directories(Utility_EmbeddedDb PUBLIC include)
target_link_libraries(Utility_EmbeddedDb PUBLIC Utility_PublicHeader)

if (ENABLE_BERKELEY_DB AND BERKELEYDB_FOUND)
    target_compile_definitions(Utility_EmbeddedDb PUBLIC CRANE_HAVE_BERKELEY_DB)
    target_include_directories(Utility_EmbeddedDb PUBLIC ${BERKELEY_DB_INCLUDE_DIR})
    target_link_libraries(Utility_EmbeddedDb PUBLIC ${BERKELEY_DB_CXX_LIBRARIES})
endif ()

if (ENABLE_UNQLITE)
    target_compile_definitions(Utility_EmbeddedDb PUBLIC CRANE_HAVE_UNQLITE)
    target_link_libraries(Utility_EmbeddedDb PUBLIC unqlite)
endif ()