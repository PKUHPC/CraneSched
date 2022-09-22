# Generate proto files
set(proto_src PublicDefs.proto Crane.proto CraneSubprocess.proto)
set(GENERATED_FILE_PATH ${CMAKE_SOURCE_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_FILE_PATH})

include(${CMAKE_SOURCE_DIR}/CMakeModule/ProtobufGenerate.cmake)
PROTOBUF_GENERATE_GRPC_CPP(ProtoCxxSources ProtoCxxHeaders ${GENERATED_FILE_PATH}/protos
        ${grpc_SOURCE_DIR}/third_party/protobuf/src ${proto_src})

get_property(grpc_SOURCE_DIR GLOBAL PROPERTY grpc_SOURCE_DIR)
message(STATUS "Generated proto: ${ProtoCxxSources} ${ProtoCxxHeaders}")
message(STATUS "_PROTOBUF_PROTOC: ${_PROTOBUF_PROTOC} ${grpc_SOURCE_DIR}")

add_library(crane_proto_lib STATIC
        ${ProtoCxxSources} ${ProtoCxxHeaders})
target_link_libraries(crane_proto_lib
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
target_include_directories(crane_proto_lib PUBLIC ${GENERATED_FILE_PATH})
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/generated/*")

target_link_libraries(crane_proto_lib absl::base absl::synchronization)
set_property(TARGET crane_proto_lib PROPERTY POSITION_INDEPENDENT_CODE ON)