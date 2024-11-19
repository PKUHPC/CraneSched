include(FetchContent)
FetchContent_Declare(
    mimalloc
    GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
    GIT_TAG v2.1.7 
    GIT_SHALLOW TRUE
)
# if MI_OVERRIDE is ON then operator new and delete will be override not only malloc and free  
# https://github.com/microsoft/mimalloc/issues/535
set(MI_OVERRIDE ON)
set(MI_BUILD_TESTS OFF)
if (BUILD_SHARED_LIBS)
    set(MI_BUILD_SHARED ON)
else ()
    set(MI_BUILD_SHARED OFF)
endif()

if(CRANE_ADDRESS_SANITIZER)
    set(MI_TRACK_ASAN ON)
    message(STATUS "Set ASAN enable in mimalloc")
endif()

FetchContent_MakeAvailable(mimalloc)

if (BUILD_SHARED_LIBS)
    add_library(dev_mimalloc ALIAS mimalloc) 
else ()
    # when building with static library ,make sure that link it as the first object file.
    # https://github.com/microsoft/mimalloc?tab=readme-ov-file#static-override
    add_library(dev_mimalloc ALIAS mimalloc-static) 
endif()
