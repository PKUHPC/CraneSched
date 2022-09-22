include(FetchContent)
FetchContent_Declare(mariadb_connector_c_repo
        GIT_REPOSITORY https://github.com/mariadb-corporation/mariadb-connector-c
        GIT_TAG v3.2.6)

SET(INSTALL_PLUGINDIR ${CMAKE_BINARY_DIR})
FetchContent_GetProperties(mariadb_connector_c_repo)

if (NOT mariadb_connector_c_repo_POPULATED)
    FetchContent_Populate(mariadb_connector_c_repo)
    add_subdirectory(${mariadb_connector_c_repo_SOURCE_DIR} ${mariadb_connector_c_repo_BINARY_DIR} EXCLUDE_FROM_ALL)
    set_target_properties(mariadbclient PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${mariadb_connector_c_repo_BINARY_DIR}/include;${mariadb_connector_c_repo_SOURCE_DIR}/include")
endif ()