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

# Slowest. Place it at last.
add_subdirectory(mongodb-cxx-driver)
