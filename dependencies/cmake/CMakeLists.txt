set(FETCHCONTENT_QUIET OFF)

# Suppress all warning on all third party libraries
add_compile_options(-w)

add_subdirectory(BSThreadPool)
add_subdirectory(mariadb-connector-c)
add_subdirectory(nlohmann_json)
add_subdirectory(yaml-cpp)
add_subdirectory(fmt)
add_subdirectory(googletest)
add_subdirectory(spdlog)
add_subdirectory(LibEvent)
add_subdirectory(cxxopts)
add_subdirectory(grpc)
add_subdirectory(libuv)
add_subdirectory(unqlite)
add_subdirectory(mongodb-cxx-driver)
add_subdirectory(ranges-v3)

include(${CMAKE_SOURCE_DIR}/CMakeModule/SuppressHeaderWarning.cmake)
suppress_header_warning()
