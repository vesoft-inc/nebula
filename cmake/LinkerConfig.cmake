execute_process(
    COMMAND
        ld --version
    COMMAND
        head -1
    COMMAND
        cut -d " " -f2
    OUTPUT_VARIABLE default_linker_type
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if ("${default_linker_type}" STREQUAL "ld")
    set(default_linker_type "bfd")
endif()

if (NOT DEFINED NEBULA_USE_LINKER)
    set(NEBULA_USE_LINKER ${default_linker_type})
endif()

message(STATUS "NEBULA_USE_LINKER: ${NEBULA_USE_LINKER}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${NEBULA_USE_LINKER}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${NEBULA_USE_LINKER}")
