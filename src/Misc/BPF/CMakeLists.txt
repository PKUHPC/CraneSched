set(BPF_SOURCE_FILE "${CMAKE_CURRENT_SOURCE_DIR}/cgroup_dev_bpf.c")
set(BPF_OBJECT_FILE "${CMAKE_CURRENT_BINARY_DIR}/cgroup_dev_bpf.o")

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    set(BPF_ARCH "__TARGET_ARCH_x86")
    list(APPEND BPF_COMPILE_OPTIONS -D__x86_64__)
elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(BPF_ARCH "__TARGET_ARCH_arm64")
elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm")
    set(BPF_ARCH "__TARGET_ARCH_arm")
else ()
    message(FATAL_ERROR "Unsupported architecture: ${CMAKE_SYSTEM_PROCESSOR}")
endif ()

# If default compiler is gcc, use clang to compile BPF program
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(BPF_COMPILER ${CLANG_EXECUTABLE})
else ()
    set(BPF_COMPILER ${CMAKE_C_COMPILER})
endif ()

# always use -g for BPF CO-RE
list(APPEND BPF_COMPILE_OPTIONS -O2 -g)

add_custom_command(
        OUTPUT ${BPF_OBJECT_FILE}
        COMMAND ${BPF_COMPILER} -target bpf ${BPF_COMPILE_OPTIONS} -D ${BPF_ARCH} -c ${BPF_SOURCE_FILE} -o ${BPF_OBJECT_FILE}
        DEPENDS ${BPF_SOURCE_FILE}
        COMMENT "Building BPF object: ${BPF_OBJECT_FILE}"
)

add_custom_target(crane_ebpf ALL
        DEPENDS ${BPF_OBJECT_FILE}
)