# Raw fetch_content managed packages
set(FETCHCONTENT_QUIET OFF)

# Suppress all warning on all third party libraries
add_compile_options(-w)

if (${CRANE_FULL_DYNAMIC})
    set(BUILD_SHARED_LIBS ON)
else ()
    set(BUILD_SHARED_LIBS OFF)
endif ()

add_subdirectory(BSThreadPool)
add_subdirectory(nlohmann_json)
add_subdirectory(yaml-cpp)
add_subdirectory(fmt)
add_subdirectory(googletest)
add_subdirectory(spdlog)
add_subdirectory(LibEvent)
add_subdirectory(cxxopts)
add_subdirectory(grpc)
add_subdirectory(libcgroup)
add_subdirectory(uvw)
add_subdirectory(unqlite)
add_subdirectory(mongodb-cxx-driver)
add_subdirectory(ranges-v3)
add_subdirectory(backward-cpp)
add_subdirectory(fpm)
add_subdirectory(mimalloc)

#add_subdirectory(mariadb-connector-c)

include(${CMAKE_SOURCE_DIR}/CMakeModule/SuppressHeaderWarning.cmake)
suppress_header_warning()
