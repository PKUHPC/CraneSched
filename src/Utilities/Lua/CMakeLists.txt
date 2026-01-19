add_library(Utility_Lua
        Lua.cpp
        include/crane/Lua.h)

target_include_directories(Utility_Lua PUBLIC include)

target_link_libraries(Utility_Lua PUBLIC
        spdlog::spdlog
        sol2
        Utility_PublicHeader
        bs_thread_pool
        $<$<BOOL:${LUA_FOUND}>:${LUA_LIBRARIES}>
)

if (${LUA_FOUND})
    target_include_directories(Utility_Lua PRIVATE ${LUA_INCLUDE_DIR})
endif ()
