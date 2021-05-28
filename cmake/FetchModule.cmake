# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

find_package(Git)
macro(nebula_fetch_module)
    cmake_parse_arguments(
        module                               # <prefix>
        ""                                   # <options>
        "URL;TAG;UPDATE;NAME;CHECKOUT"       # <one_value_args>
        ""                                   # <multi_value_args>
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
    else()
        if (${module_CHECKOUT})
            execute_process(
                COMMAND ${GIT_EXECUTABLE} rev-parse --abbrev-ref HEAD
                WORKING_DIRECTORY ${module_dir}
                OUTPUT_VARIABLE branch_name
            )
            string(REPLACE "\n" "" branch_name "${branch_name}")
            if(NOT ${branch_name} STREQUAL ${module_TAG})
                message(STATUS "The branch of ${module_NAME} is ${branch_name}, need to change to ${module_TAG}")
                execute_process(
                    COMMAND ${GIT_EXECUTABLE} remote set-branches origin --add ${module_TAG}
                    WORKING_DIRECTORY ${module_dir}
                )
                execute_process(
                    COMMAND ${GIT_EXECUTABLE} config remote.origin.fetch
                    WORKING_DIRECTORY ${module_dir}
                )
                execute_process(
                    COMMAND ${GIT_EXECUTABLE} fetch
                    WORKING_DIRECTORY ${module_dir}
                )
                execute_process(
                    COMMAND ${GIT_EXECUTABLE} checkout ${module_TAG}
                    WORKING_DIRECTORY ${module_dir}
                    RESULT_VARIABLE checkout_status
                    ERROR_VARIABLE error_msg
                )
                if(NOT ${checkout_status} EQUAL 0)
                    message(FATAL_ERROR "Checkout to branch ${module_TAG} failed: ${error_msg}, error_code: ${checkout_status}")
                endif()
            endif()
        endif()
        if(${module_UPDATE})
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
    endif()
endmacro()
