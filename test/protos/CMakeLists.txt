set(test_proto_src math.proto greeter.proto)

# The path used for #include
set(GENERATED_TEST_PATH ${CMAKE_SOURCE_DIR}/generated/test)

# The path where proto cpp/h are generated
set(GENERATED_TEST_PROTO_PATH ${GENERATED_TEST_PATH}/protos)

file(MAKE_DIRECTORY ${GENERATED_TEST_PROTO_PATH})
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES
        "${CMAKE_SOURCE_DIR}/generated/test/*")

include(${CMAKE_SOURCE_DIR}/CMakeModule/ProtobufGenerate.cmake)
PROTOBUF_GENERATE_GRPC_CPP(TestProtoCxxSources TestProtoCxxHeaders
        ${GENERATED_TEST_PROTO_PATH}
        ${grpc_SOURCE_DIR}/third_party/protobuf/src
        ${test_proto_src})

add_library(test_proto STATIC
        ${TestProtoCxxSources} ${TestProtoCxxHeaders})
target_link_libraries(test_proto
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
target_link_libraries(test_proto absl::base absl::synchronization)

target_include_directories(test_proto PUBLIC ${GENERATED_TEST_PATH})