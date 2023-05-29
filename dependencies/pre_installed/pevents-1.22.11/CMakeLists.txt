# concurrent queue has no cmake config files. Add it manually as a header-only library.
add_library(pevents STATIC include/pevents/pevents.cpp include/pevents/pevents.h)
target_include_directories(pevents PUBLIC include)
