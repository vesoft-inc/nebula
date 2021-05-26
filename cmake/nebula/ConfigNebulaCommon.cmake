# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

macro(config_nebula_common)
    message(">>>> Configuring Nebula Common <<<<")
    cmake_parse_arguments(
        common
        ""
        "SOURCE_DIR;BUILD_DIR"
        ""
        ${ARGN}
    )
    set(common_source_dir ${common_SOURCE_DIR})
    set(common_build_dir ${common_BUILD_DIR})
    if(NOT EXISTS ${common_build_dir})
        file(MAKE_DIRECTORY ${common_build_dir})
    endif()

    execute_process(
        COMMAND
            ${CMAKE_COMMAND}
                -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DNEBULA_THIRDPARTY_ROOT=${NEBULA_THIRDPARTY_ROOT}
                -DNEBULA_OTHER_ROOT=${NEBULA_OTHER_ROOT}
                -DENABLE_JEMALLOC=${ENABLE_JEMALLOC}
                -DENABLE_TESTING=OFF
                -DENABLE_CCACHE=${ENABLE_CCACHE}
                -DENABLE_ASAN=${ENABLE_ASAN}
                -DENABLE_UBSAN=${ENABLE_UBSAN}
                -DENABLE_FRAME_POINTER=${ENABLE_FRAME_POINTER}
                -DENABLE_PIC=${ENABLE_PIC}
                -DENABLE_COMPRESSED_DEBUG_INFO=${ENABLE_COMPRESSED_DEBUG_INFO}
                -DNEBULA_USE_LINKER=${NEBULA_USE_LINKER}
                -DENABLE_GDB_SCRIPT_SECTION=${ENABLE_GDB_SCRIPT_SECTION}
                -DDISABLE_CXX11_ABI=${DISABLE_CXX11_ABI}
                -DNEBULA_SERVER_GIT_INFO_SHA=${GIT_INFO_SHA}
                -DNEBULA_BUILD_VERSION=${NEBULA_BUILD_VERSION}
                ${common_source_dir}
        WORKING_DIRECTORY ${common_build_dir}
        RESULT_VARIABLE cmake_status
    )

    if(NOT ${cmake_result} EQUAL 0)
        message(FATAL_ERROR "Failed to configure nebula-common")
    endif()

    add_custom_target(
        common_project ALL
        COMMAND +${CMAKE_COMMAND} --build ${common_build_dir}
    )

    add_custom_target(
        clean-common
        COMMAND $(MAKE) clean
        WORKING_DIRECTORY ${common_build_dir}
    )

    set(nebula-common_DIR ${common_build_dir})
    find_package(nebula-common REQUIRED)
    include_directories(AFTER ${common_source_dir}/src)
    include_directories(AFTER ${common_build_dir}/src)
    message(">>>> Configuring Nebula Common done <<<<")
endmacro()
