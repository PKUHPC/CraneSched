include(FetchContent)

set(ENABLE_STATIC ON)
set(ENABLE_TESTS OFF)
set(ENABLE_MONGODB_AWS_AUTH OFF)
set(BUILD_TESTING OFF)
set(ENABLE_UNINSTALL OFF)
set(ENABLE_HTML_DOCS OFF)
set(ENABLE_EXAMPLES OFF)
set(ENABLE_AUTOMATIC_INIT_AND_CLEANUP OFF)

FetchContent_Declare(mongo_c_driver_repo
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/mongodb/mongo-c-driver/releases/download/1.23.0/mongo-c-driver-1.23.0.tar.gz
        URL_HASH SHA256=2b91d6a9c1a80ec82c5643676e44f1a9edf3849c7f25d490e1b5587eb408ad93
        PATCH_COMMAND patch -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/mongo-c-driver.diff
        )
FetchContent_MakeAvailable(mongo_c_driver_repo)

set(BUILD_SHARED_LIBS OFF)
set(BUILD_SHARED_AND_STATIC_LIBS OFF)
set(BUILD_SHARED_LIBS_WITH_STATIC_MONGOC OFF)
set(ENABLE_TESTS OFF)
FetchContent_Declare(mongocxx
        OVERRIDE_FIND_PACKAGE

        URL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.7.0/mongo-cxx-driver-r3.7.0.tar.gz
        URL_HASH SHA256=fb2da11178db728f63147fe4b0c7509eb49b1b02c5cb55f9bee5f927e451a0c7
        )
FetchContent_MakeAvailable(mongocxx)

find_package(mongocxx REQUIRED CONFIG)
