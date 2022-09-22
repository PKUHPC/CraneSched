function(PROTOBUF_GENERATE_GRPC_CPP SRCS HDRS OUTDIR SYSTEM_PROTO_DIR)
    if (NOT ARGN)
        message(SEND_ERROR "Error: PROTOBUF_GENERATE_GRPC_CPP() called without any proto files")
        return()
    endif ()

    set(_protobuf_include_path)

    set(${SRCS})
    set(${HDRS})
    foreach (FIL ${ARGN})
        get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
        get_filename_component(FIL_WE ${FIL} NAME_WE)

        list(APPEND ${SRCS}
                "${OUTDIR}/${FIL_WE}.pb.cc"
                "${OUTDIR}/${FIL_WE}.grpc.pb.cc")
        list(APPEND ${HDRS}
                "${OUTDIR}/${FIL_WE}.pb.h"
                "${OUTDIR}/${FIL_WE}.grpc.pb.h")

        add_custom_command(
                OUTPUT "${OUTDIR}/${FIL_WE}.grpc.pb.cc" "${OUTDIR}/${FIL_WE}.grpc.pb.h"
                COMMAND ${_PROTOBUF_PROTOC}
                ARGS --grpc_out "${OUTDIR}" --cpp_out "${OUTDIR}" -I ${CMAKE_CURRENT_SOURCE_DIR}
                -I ${SYSTEM_PROTO_DIR}
                --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
                ${ABS_FIL}
                DEPENDS ${ABS_FIL}
                COMMENT "Running gRPC C++ protocol buffer compiler on ${FIL}"
        )
    endforeach ()

    set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
    set(${SRCS} ${${SRCS}} PARENT_SCOPE)
    set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

