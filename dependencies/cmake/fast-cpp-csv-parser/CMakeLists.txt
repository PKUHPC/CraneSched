cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    # TODO: replace with gitee source
else ()
    set(CSV_PARSER_URL "https://raw.githubusercontent.com/ben-strasser/fast-cpp-csv-parser/master/csv.h")
endif ()

FetchContent_Declare(
    fast_cpp_csv_parser
    URL ${CSV_PARSER_URL}
    DOWNLOAD_NO_EXTRACT TRUE
    INACTIVITY_TIMEOUT 5
)

FetchContent_GetProperties(fast_cpp_csv_parser)
if(NOT fast_cpp_csv_parser_POPULATED)
    FetchContent_Populate(fast_cpp_csv_parser)
    
    add_library(fast_cpp_csv_parser INTERFACE)
    target_include_directories(fast_cpp_csv_parser INTERFACE 
        ${fast_cpp_csv_parser_SOURCE_DIR}
    )
endif() 