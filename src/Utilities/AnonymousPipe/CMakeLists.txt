add_library(Utility_AnonymousPipe INTERFACE)
target_include_directories(Utility_AnonymousPipe INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Utility_AnonymousPipe INTERFACE PkgConfig::libcgroup Boost::boost spdlog::spdlog)
