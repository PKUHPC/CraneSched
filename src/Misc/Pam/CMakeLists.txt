add_library(pam_crane SHARED
        Pam.cpp
        PamUtil.h
        PamUtil.cpp
)

# remove "lib" prefix
set_target_properties(pam_crane PROPERTIES PREFIX "")

target_include_directories(pam_crane PUBLIC ${PAM_INCLUDE_DIR})



target_link_libraries(pam_crane PUBLIC
        $<$<BOOL:${CRANE_USE_MIMALLOC}>:dev_mimalloc>
        ${PAM_LIBRARIES}
        Utility_PublicHeaderNoLogger
        crane_proto_lib
        fmt::fmt
        yaml-cpp
)

# Linker flag for c++ 17 filesystem library
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_link_libraries(pam_crane PUBLIC stdc++fs)
endif ()
