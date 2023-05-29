# concurrent queue has no cmake config files. Add it manually as a header-only library.
add_library(concurrentqueue INTERFACE)
target_include_directories(concurrentqueue INTERFACE include)
