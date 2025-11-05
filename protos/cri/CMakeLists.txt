# Generate CRI proto files
set(GENERATED_FILE_PATH ${CMAKE_SOURCE_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_FILE_PATH}/protos/cri)

file(READ "VERSION" CRI_VERSION)
string(STRIP "${CRI_VERSION}" CRI_VERSION)
message(STATUS "CRI proto file version (hash): ${CRI_VERSION}")

include(${CMAKE_SOURCE_DIR}/CMakeModule/ProtobufGenerate.cmake)

set(cri_proto_src api.proto)
PROTOBUF_GENERATE_GRPC_CPP(CriProtoCxxSources CriProtoCxxHeaders ${GENERATED_FILE_PATH}/protos/cri
        ${protobuf_SOURCE_DIR}/src ${cri_proto_src})

get_property(protobuf_SOURCE_DIR GLOBAL PROPERTY protobuf_SOURCE_DIR)

message(STATUS "Generated CRI proto: ${CriProtoCxxSources} ${CriProtoCxxHeaders}")

add_library(cri_proto_lib STATIC
        ${CriProtoCxxSources} ${CriProtoCxxHeaders})
target_link_libraries(cri_proto_lib
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
target_include_directories(cri_proto_lib PUBLIC ${GENERATED_FILE_PATH}/protos)
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/../../generated/protos/cri/*")

target_link_libraries(cri_proto_lib absl::base absl::synchronization)
set_property(TARGET cri_proto_lib PROPERTY POSITION_INDEPENDENT_CODE ON)
