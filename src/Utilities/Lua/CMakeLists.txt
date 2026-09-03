add_library(Utility_Lua
        Lua.cpp
        include/crane/Lua.h)

target_include_directories(Utility_Lua PUBLIC include)

target_link_libraries(Utility_Lua PUBLIC
        spdlog::spdlog
        sol2
        Utility_PublicHeader
        bs_thread_pool
        $<$<BOOL:${CRANE_ENABLE_LUA}>:crane_lua>
)
