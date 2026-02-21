add_library(Utility_PasswordEntry_SubidCompat OBJECT SubidCompat.cpp)
target_link_libraries(Utility_PasswordEntry_SubidCompat PRIVATE Subid::subid)

add_library(Utility_PasswordEntry INTERFACE
        include/crane/PasswordEntry.h)
target_include_directories(Utility_PasswordEntry INTERFACE include)
target_link_libraries(Utility_PasswordEntry INTERFACE
        Utility_PublicHeader
        Utility_FileLock
        Subid::subid
        $<TARGET_OBJECTS:Utility_PasswordEntry_SubidCompat>
)
