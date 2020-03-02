# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

macro(add_dependent_project base name repo tag)

# Clone or update the repo
if(EXISTS ${base}/${name}/.git)
    message(STATUS "Updating from the repo \"" ${repo} "\"")
    execute_process(
        COMMAND ${GIT_EXECUTABLE} pull --depth=1
        WORKING_DIRECTORY ${base}/${name}
        RESULT_VARIABLE clone_result
    )
else()
    message(STATUS "Cloning from the repo \"" ${repo} "\"")
    execute_process(
        COMMAND
            ${GIT_EXECUTABLE} clone --depth 1 --progress --branch ${tag} ${repo} ${base}/${name}
        RESULT_VARIABLE clone_result
    )
endif()

if(NOT ${clone_result} EQUAL 0)
    message(
        FATAL_ERROR
        "Cannot clone the repo from \""
        ${repo}
        "\" (branch \""
        ${tag}
        "\"): \""
        ${clone_result}
        "\"")
else()
    message(STATUS "Updated the repo from \"" ${repo} "\" (branch \"" ${tag} "\")")
endif()

# Configure the repo
execute_process(
    COMMAND
        ${CMAKE_COMMAND}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DNEBULA_THIRDPARTY_ROOT=${NEBULA_THIRDPARTY_ROOT}
        -DNEBULA_OTHER_ROOT=${NEBULA_OTHER_ROOT}
        -DENABLE_JEMALLOC=${ENABLE_JEMALLOC}
        -DENABLE_NATIVE=${ENABLE_NATIVE}
        -DENABLE_TESTING=false
        -DENABLE_CCACHE=${ENABLE_CCACHE}
        -DENABLE_ASAN=${ENABLE_ASAN}
        -DENABLE_UBSAN=${ENABLE_UBSAN}
        .
    WORKING_DIRECTORY ${base}/${name}
    RESULT_VARIABLE cmake_result
)
if(NOT ${cmake_result} EQUAL 0)
    message(FATAL_ERROR "Failed to configure the dependent project \"" ${name} "\"")
endif()

# Add a custom target to build the project
add_custom_target(
    ${name}_project ALL
    COMMAND make -j
    WORKING_DIRECTORY ${base}/${name}
)

endmacro()
