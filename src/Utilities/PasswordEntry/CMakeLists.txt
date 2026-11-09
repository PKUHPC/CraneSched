include(CheckLibraryExists)

check_library_exists(c reallocarray "" CRANE_HAVE_REALLOCARRAY)
set(UTILITY_PASSWORD_ENTRY_COMPAT_SOURCES SubidCompat.cpp)
if (NOT CRANE_HAVE_REALLOCARRAY)
    list(APPEND UTILITY_PASSWORD_ENTRY_COMPAT_SOURCES ReallocarrayCompat.cpp)
endif ()

add_library(Utility_PasswordEntry_SubidCompat OBJECT
        ${UTILITY_PASSWORD_ENTRY_COMPAT_SOURCES})
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
