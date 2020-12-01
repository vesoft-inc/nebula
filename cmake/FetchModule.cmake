# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

find_package(Git)
macro(nebula_fetch_module)
    cmake_parse_arguments(
        module                      # <prefix>
        ""                          # <options>
        "URL;TAG;UPDATE;NAME"       # <one_value_args>
        ""                          # <multi_value_args>
        ${ARGN}
    )
    set(module_dir ${CMAKE_SOURCE_DIR}/modules/${module_NAME})
    if(NOT EXISTS ${module_dir}/.git)
        message(STATUS "Cloning from ${module_URL}:${module_TAG}")
        execute_process(
            COMMAND
                ${GIT_EXECUTABLE} clone
                    --single-branch
                    --branch ${module_TAG}
                    ${module_URL} ${module_dir}
            RESULT_VARIABLE fetch_status
            ERROR_VARIABLE ERROR_MESSAGE
        )
        if(NOT ${fetch_status} EQUAL 0)
            message(FATAL_ERROR "Cloning failed")
        endif()
    elseif(${module_UPDATE})
        message(STATUS "Updating from ${module_URL}")
        execute_process(
            COMMAND ${GIT_EXECUTABLE} pull
            WORKING_DIRECTORY ${module_dir}
            RESULT_VARIABLE fetch_status
        )
        if(NOT ${fetch_status} EQUAL 0)
            message(FATAL_ERROR "Updating failed")
        endif()
    endif()
endmacro()
