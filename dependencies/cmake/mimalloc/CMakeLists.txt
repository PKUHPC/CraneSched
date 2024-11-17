include(FetchContent)
FetchContent_Declare(
    mimalloc
    GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
    GIT_TAG v2.1.7 
    GIT_SHALLOW TRUE
)
# if MI_OVERRIDE is ON then operator new and delete will be override not only malloc and free  
# https://github.com/microsoft/mimalloc/issues/535
set(MI_OVERRIDE ON CACHE BOOL "Enable mimalloc overriding malloc/free/new/delete" FORCE)
set(MI_BUILD_TESTS OFF CACHE BOOL "Disable tests in mimalloc" FORCE)
if (BUILD_SHARED_LIBS)
    set(MI_BUILD_SHARED ON CACHE BOOL "Build mimalloc as shared library" FORCE)
else ()
    set(MI_BUILD_SHARED OFF CACHE BOOL "Build mimalloc as static library" FORCE)
endif()

if(CRANE_ADDRESS_SANITIZER)
    set(MI_TRACK_ASAN ON CACHE BOOL "Enable ASAN in mimalloc" FORCE)
    message(STATUS "Set ASAN enable in mimalloc")
endif()

FetchContent_MakeAvailable(mimalloc)

if (BUILD_SHARED_LIBS)
    add_library(dev_mimalloc ALIAS mimalloc)  # 动态库别名
    message(STATUS "Using mimalloc shared library")
else ()
    add_library(dev_mimalloc ALIAS mimalloc-static)  # 静态库别名
    message(STATUS "Using mimalloc static library")
endif()
