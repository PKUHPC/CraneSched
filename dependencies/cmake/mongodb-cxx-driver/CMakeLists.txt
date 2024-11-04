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
    set(MONGO_C_DRIVER_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/mongo-c-driver-1.28.0.tar.gz")
    set(MONGO_CXX_DRIVER_SRC_URL "https://gitee.com/zenglingbo/crane-sched-deps/raw/master/mongo-cxx-driver-r3.11.0.tar.gz")
else ()
    set(MONGO_C_DRIVER_SRC_URL "https://github.com/mongodb/mongo-c-driver/releases/download/1.28.0/mongo-c-driver-1.28.0.tar.gz")
    set(MONGO_CXX_DRIVER_SRC_URL "https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.11.0/mongo-cxx-driver-r3.11.0.tar.gz")
endif ()

FetchContent_Declare(mongo_c_driver_repo
        OVERRIDE_FIND_PACKAGE

        URL ${MONGO_C_DRIVER_SRC_URL}
        URL_HASH SHA256=BEC90C85361B4A9F89D80243440D53F39B9E706B1FE4BF901C4D3FFD6F50FBDB
        INACTIVITY_TIMEOUT 5
        )
FetchContent_MakeAvailable(mongo_c_driver_repo)

set(BUILD_SHARED_LIBS OFF)
set(BUILD_SHARED_AND_STATIC_LIBS OFF)
set(BUILD_SHARED_LIBS_WITH_STATIC_MONGOC OFF)
set(ENABLE_TESTS OFF)
set(MONGOCXX_OVERRIDE_DEFAULT_INSTALL_PREFIX OFF)
FetchContent_Declare(mongocxx
        OVERRIDE_FIND_PACKAGE

        URL ${MONGO_CXX_DRIVER_SRC_URL}
        URL_HASH SHA256=CB4263229D769EC44AA66563E2DD2D70C6384C85D93840A52FE26B15436C54F1
        INACTIVITY_TIMEOUT 5
        )

if(NOT mongocxx_POPULATED)
  FetchContent_Populate(mongocxx)
  add_subdirectory(${mongocxx_SOURCE_DIR} ${mongocxx_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()