include(FetchContent)
FetchContent_Declare(
    mimalloc
    GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
    GIT_TAG v2.1.7 
    GIT_SHALLOW TRUE
)
# if MI_OVERRIDE is ON then operator new and delete will be override not only malloc and free  
# https://github.com/microsoft/mimalloc/issues/535

if(CRANE_ADDRESS_SANITIZER AND NOT CRANE_THREAD_SANITIZER)
    set(MI_TRACK_ASAN ON CACHE BOOL "Enable ASAN in mimalloc" FORCE)
    message(STATUS "Set ASAN enable in mimalloc")
endif()

FetchContent_MakeAvailable(mimalloc)
