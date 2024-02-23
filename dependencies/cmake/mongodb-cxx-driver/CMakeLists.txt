include(FetchContent)

if (BUILD_SHARED_LIBS)
    set(ENABLE_SHARED ON)
else ()
    set(ENABLE_STATIC ON)
endif ()
set(ENABLE_TESTS OFF)
set(ENABLE_MONGODB_AWS_AUTH OFF)
set(BUILD_TESTING OFF)
set(ENABLE_UNINSTALL OFF)
set(ENABLE_HTML_DOCS OFF)
set(ENABLE_EXAMPLES OFF)
set(ENABLE_AUTOMATIC_INIT_AND_CLEANUP OFF)

if (CRANE_USE_GITEE_SOURCE)
    set(MONGO_C_DRIVER_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/mongo-c-driver-1.26.0.tar.gz")
    set(MONGO_CXX_DRIVER_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/mongo-cxx-driver-r3.9.0.tar.gz")
else ()
    set(MONGO_C_DRIVER_SRC_URL "https://github.com/mongodb/mongo-c-driver/archive/refs/tags/1.26.0.tar.gz")
    set(MONGO_CXX_DRIVER_SRC_URL "https://codeload.github.com/mongodb/mongo-cxx-driver/tar.gz/refs/tags/r3.9.0")
endif ()

FetchContent_Declare(mongo_c_driver_repo
        OVERRIDE_FIND_PACKAGE

        URL ${MONGO_C_DRIVER_SRC_URL}
        URL_HASH SHA256=9F6D5615EF189F63D590A68BD414395A2AF3DAC03F9189696EA73E8C230C2DF0
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(mongo_c_driver_repo)

set(BUILD_SHARED_LIBS OFF)
set(BUILD_SHARED_AND_STATIC_LIBS OFF)
set(BUILD_SHARED_LIBS_WITH_STATIC_MONGOC OFF)
set(ENABLE_TESTS OFF)
FetchContent_Declare(mongocxx
        OVERRIDE_FIND_PACKAGE

        URL ${MONGO_CXX_DRIVER_SRC_URL}
        URL_HASH SHA256=458BD58AED3F1A8D4B3347DB0B6F0B122BBAC05BD5343C60DB8F25B4F2681D59
        INACTIVITY_TIMEOUT 5
        )
FetchContent_GetProperties(mongocxx)

if(NOT mongocxx_POPULATED)
  FetchContent_Populate(mongocxx)

  # Specify the version number manually as there's a bug in mongocxx repo
  set(VERSION_FILE ${mongocxx_SOURCE_DIR}/build/VERSION_CURRENT)
  file(WRITE ${VERSION_FILE} "3.9.0")
  
  set(MONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX OFF)
  add_subdirectory(${mongocxx_SOURCE_DIR} ${mongocxx_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()