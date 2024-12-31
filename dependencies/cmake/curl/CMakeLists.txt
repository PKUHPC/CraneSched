cmake_policy(SET CMP0069 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)

include(FetchContent)

if (CRANE_USE_GITEE_SOURCE)
    # TODO: replace with gitee source
else ()
    set(CURL_URL "https://github.com/curl/curl/releases/download/curl-8_5_0/curl-8.5.0.tar.gz")
endif ()

FetchContent_Declare(
    curl
    URL ${CURL_URL}
    URL_HASH SHA256=05fc17ff25b793a437a0906e0484b82172a9f4de02be5ed447e0cab8c3475add
)

FetchContent_GetProperties(curl)
if(NOT curl_POPULATED)
    set(BUILD_SHARED_LIBS OFF)
    set(BUILD_TESTING OFF)
    set(HTTP_ONLY ON)
    set(ENABLE_MANUAL OFF)
    set(BUILD_CURL_EXE OFF)
    set(CURL_DISABLE_LDAP ON)
    set(CURL_DISABLE_LDAPS ON)
    set(CURL_DISABLE_TELNET ON)
    set(CURL_DISABLE_DICT ON)
    set(CURL_DISABLE_FILE ON)
    set(CURL_DISABLE_TFTP ON)
    set(CURL_DISABLE_RTSP ON)
    set(CURL_DISABLE_POP3 ON)
    set(CURL_DISABLE_IMAP ON)
    set(CURL_DISABLE_SMTP ON)
    set(CURL_DISABLE_GOPHER ON)
    
    FetchContent_Populate(curl)
    add_subdirectory(${curl_SOURCE_DIR} ${curl_BINARY_DIR})
    
    # add_library(CURL::libcurl ALIAS libcurl)
endif() 