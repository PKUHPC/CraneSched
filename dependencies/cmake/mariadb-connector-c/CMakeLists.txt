include(FetchContent)
FetchContent_Declare(mariadb_connector_c_repo
        URL https://github.com/mariadb-corporation/mariadb-connector-c/archive/refs/tags/v3.3.2.zip
        URL_HASH SHA256=9d3a76aefec32ff75e62a311095557d1718547dbd9de6963feda1e4874a65ba5
        INACTIVITY_TIMEOUT 5
        )

SET(INSTALL_PLUGINDIR ${CMAKE_BINARY_DIR})
FetchContent_GetProperties(mariadb_connector_c_repo)

if (NOT mariadb_connector_c_repo_POPULATED)
    FetchContent_Populate(mariadb_connector_c_repo)
    add_subdirectory(${mariadb_connector_c_repo_SOURCE_DIR} ${mariadb_connector_c_repo_BINARY_DIR} EXCLUDE_FROM_ALL)
    set_target_properties(mariadbclient PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${mariadb_connector_c_repo_BINARY_DIR}/include;${mariadb_connector_c_repo_SOURCE_DIR}/include")
endif ()