add_library(Utility_cgroup cgroup.linux.h cgroup.linux.cpp)
target_include_directories(Utility_cgroup PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(Utility_cgroup
        PRIVATE
        PkgConfig::libcgroup
        spdlog::spdlog
        Boost::boost
        absl::synchronization
        absl::flat_hash_map
        Utility_PublicHeader)
