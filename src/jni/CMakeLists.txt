# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER               -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER             -- Specify the compiler for C++ language
#
#   NEBULA_HOME                    -- Specify the root directory for nebula project
#   NEBULA_THIRDPARTY_ROOT         -- Specify the third-party root dir.
#   ENABLE_TESTING                 -- Build unit test
#
cmake_minimum_required(VERSION 3.0.0)

project("Nebula Graph codec" C CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ENABLE_TESTING "Whether to compile unit test ON or OFF" OFF)

message(STATUS "CMAKE_CURRENT_BINARY_DIR:" ${CMAKE_CURRENT_BINARY_DIR})
message(STATUS "CMAKE_CURRENT_SOURCE_DIR:" ${CMAKE_CURRENT_SOURCE_DIR})
message(STATUS "NEBULA_HOME:" ${NEBULA_HOME})
message(STATUS "NEBULA_THIRDPARTY_ROOT:" ${NEBULA_THIRDPARTY_ROOT})

# locate jni header
include_directories($ENV{JAVA_HOME}/include
                    $ENV{JAVA_HOME}/include/linux)
include_directories(AFTER ${NEBULA_HOME}/src)
include_directories(AFTER ${NEBULA_HOME}/src/common)
include_directories(AFTER ${NEBULA_HOME}/src/jni/src)

include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/include)
link_directories(
    ${NEBULA_THIRDPARTY_ROOT}/lib
    ${NEBULA_THIRDPARTY_ROOT}/lib64
)

if (ENABLE_TESTING)
    enable_testing()
endif()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

add_compile_options(-fPIC)
add_compile_options(-Wall)
add_compile_options(-Werror)
add_compile_options(-Wunused-parameter)


add_subdirectory(src)

