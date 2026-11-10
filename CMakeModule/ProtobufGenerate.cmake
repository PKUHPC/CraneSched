function(PROTOBUF_GENERATE_GRPC_CPP SRCS HDRS OUTDIR SYSTEM_PROTO_DIR)
    if (NOT ARGN)
        message(SEND_ERROR "Error: PROTOBUF_GENERATE_GRPC_CPP() called without any proto files")
        return()
    endif ()

    set(_protobuf_include_path)

    set(${SRCS})
    set(${HDRS})
    if (${CRANE_FULL_DYNAMIC})
        get_property(_PROTOBUF_LIBS_PATH GLOBAL PROPERTY _PROTOBUF_LIBS_PATH)
    endif ()
    foreach (FIL ${ARGN})
        get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
        file(RELATIVE_PATH REL_FIL "${CMAKE_CURRENT_SOURCE_DIR}" "${ABS_FIL}")
        string(REGEX REPLACE "\\.[^.]+$" "" FIL_WE "${REL_FIL}")
        get_filename_component(GENERATED_DIR "${OUTDIR}/${FIL_WE}" DIRECTORY)
        file(MAKE_DIRECTORY "${GENERATED_DIR}")

        list(APPEND ${SRCS}
                "${OUTDIR}/${FIL_WE}.pb.cc"
                "${OUTDIR}/${FIL_WE}.grpc.pb.cc")
        list(APPEND ${HDRS}
                "${OUTDIR}/${FIL_WE}.pb.h"
                "${OUTDIR}/${FIL_WE}.grpc.pb.h")

        add_custom_command(
                OUTPUT "${OUTDIR}/${FIL_WE}.pb.cc" "${OUTDIR}/${FIL_WE}.pb.h"
                       "${OUTDIR}/${FIL_WE}.grpc.pb.cc" "${OUTDIR}/${FIL_WE}.grpc.pb.h"
                COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${_PROTOBUF_LIBS_PATH}:$LD_LIBRARY_PATH"  ${_PROTOBUF_PROTOC}
                ARGS --grpc_out "${OUTDIR}" --cpp_out "${OUTDIR}" -I ${CMAKE_CURRENT_SOURCE_DIR}
                -I ${SYSTEM_PROTO_DIR}
                --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
                --dependency_out "${OUTDIR}/${FIL_WE}.d"
                ${ABS_FIL}
                DEPENDS ${ABS_FIL}
                DEPFILE "${OUTDIR}/${FIL_WE}.d"
                COMMENT "Running gRPC C++ protocol buffer compiler on ${FIL}"
        )
    endforeach ()

    set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
    set(${SRCS} ${${SRCS}} PARENT_SCOPE)
    set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

