# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#

if (ENABLE_CLANG_TIDY)
    if (${CMAKE_VERSION} VERSION_LESS "3.6.0")
        message(FATAL_ERROR "clang-tidy requires CMake version at least 3.6.")
    endif()

    find_program (CLANG_TIDY_PATH NAMES "clang-tidy")
    if (CLANG_TIDY_PATH)
        message(STATUS "Using clang-tidy: ${CLANG_TIDY_PATH}.")
        set (CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY_PATH} --config=)
    else ()
        message(STATUS "clang-tidy is not found.")
    endif ()
endif ()
