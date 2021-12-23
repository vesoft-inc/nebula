set(NEBULA_USE_LINKER
  "bfd"
  CACHE STRING "Linker to be used")
set(USER_LINKER_OPTION_VALUES "lld" "gold" "bfd")
set_property(CACHE NEBULA_USE_LINKER PROPERTY STRINGS ${USER_LINKER_OPTION_VALUES})
list(
  FIND
  USER_LINKER_OPTION_VALUES
  ${NEBULA_USE_LINKER}
  USER_LINKER_OPTION_INDEX)

if(${USER_LINKER_OPTION_INDEX} EQUAL -1)
  message(
    STATUS
    "Using custom linker: '${NEBULA_USE_LINKER}', explicitly supported entries are ${USER_LINKER_OPTION_VALUES}")
endif()

print_config(NEBULA_USE_LINKER)

nebula_add_exe_linker_flag(-fuse-ld=${NEBULA_USE_LINKER})
nebula_add_shared_linker_flag(-fuse-ld=${NEBULA_USE_LINKER})
nebula_add_exe_linker_flag(-static-libstdc++)
nebula_add_exe_linker_flag(-static-libgcc)
nebula_add_exe_linker_flag(-no-pie)
nebula_add_exe_linker_flag(-rdynamic)

if(NOT ${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    add_definitions(-D_FORTIFY_SOURCE=2)
else()
    # The mips need to add it when build Debug to lift the usual restrictions on the size of the global offset table.
    if (${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "mips64")
        add_compile_options(-mxgot)
    endif()

    if (NOT ${NEBULA_USE_LINKER} STREQUAL "gold" AND NOT ENABLE_GDB_SCRIPT_SECTION)
        # `gold' linker is buggy for `--gc-sections', disabled for it
        # `gc-sections' will discard the `.debug_gdb_scripts' section if enabled
        add_compile_options(-ffunction-sections)
        add_compile_options(-fdata-sections)
        nebula_add_exe_linker_flag(-Wl,--gc-sections)
    endif()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # The mips not supported
    if (NOT ${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "mips64")
        if(ENABLE_COMPRESSED_DEBUG_INFO)
            nebula_add_exe_linker_flag(-Wl,--compress-debug-sections=zlib)
        else()
            nebula_remove_exe_linker_flag(-Wl,--compress-debug-sections=zlib)
        endif()
    endif()
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        add_compile_options(-fno-limit-debug-info)
    endif()
endif()
