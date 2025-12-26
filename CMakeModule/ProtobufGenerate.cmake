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
        get_filename_component(FIL_WE ${FIL} NAME_WE)

        # Check if the proto file contains service definitions
        # Use CMake's file(STRINGS) for cross-platform compatibility
        # Pattern matches: optional whitespace, 'service', required whitespace, service name, optional whitespace, opening brace
        set(SERVICE_REGEX "^[[:space:]]*service[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\\{")
        file(STRINGS "${ABS_FIL}" PROTO_SERVICE_LINES REGEX "${SERVICE_REGEX}")
        if (PROTO_SERVICE_LINES)
            set(HAS_GRPC_SERVICE TRUE)
        else ()
            set(HAS_GRPC_SERVICE FALSE)
        endif ()

        list(APPEND ${SRCS}
                "${OUTDIR}/${FIL_WE}.pb.cc")
        list(APPEND ${HDRS}
                "${OUTDIR}/${FIL_WE}.pb.h")

        # Always generate .pb.{cc,h} files with --cpp_out
        set(PROTO_OUTPUTS "${OUTDIR}/${FIL_WE}.pb.cc" "${OUTDIR}/${FIL_WE}.pb.h")

        # Build protoc command arguments based on whether the proto has services
        set(PROTOC_ARGS --cpp_out "${OUTDIR}" -I ${CMAKE_CURRENT_SOURCE_DIR} -I ${SYSTEM_PROTO_DIR})

        # Only add .grpc.pb.{cc,h} if the proto file has service definitions
        if (HAS_GRPC_SERVICE)
            list(APPEND ${SRCS} "${OUTDIR}/${FIL_WE}.grpc.pb.cc")
            list(APPEND ${HDRS} "${OUTDIR}/${FIL_WE}.grpc.pb.h")
            list(APPEND PROTO_OUTPUTS "${OUTDIR}/${FIL_WE}.grpc.pb.cc" "${OUTDIR}/${FIL_WE}.grpc.pb.h")
            list(APPEND PROTOC_ARGS --grpc_out "${OUTDIR}" --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}")
            set(COMMENT_TEXT "Running gRPC C++ protocol buffer compiler on ${FIL}")
        else ()
            set(COMMENT_TEXT "Running C++ protocol buffer compiler on ${FIL}")
        endif ()

        add_custom_command(
                OUTPUT ${PROTO_OUTPUTS}
                COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${_PROTOBUF_LIBS_PATH}:$LD_LIBRARY_PATH"  ${_PROTOBUF_PROTOC}
                ARGS ${PROTOC_ARGS} ${ABS_FIL}
                DEPENDS ${ABS_FIL}
                COMMENT "${COMMENT_TEXT}"
        )
    endforeach ()

    set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
    set(${SRCS} ${${SRCS}} PARENT_SCOPE)
    set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()

