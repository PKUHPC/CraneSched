# Suppress all warning on all third party libraries
add_compile_options(-w)

add_subdirectory(concurrentqueue)
add_subdirectory(pevents-1.22.11)

include(${CMAKE_SOURCE_DIR}/CMakeModule/SuppressHeaderWarning.cmake)
suppress_header_warning()
