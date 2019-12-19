# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
# NEBULA_CLIENT_LIB_INCLUDE_DIR     -- The dir of include file  cpp client 
# NEBULA_CLIENT_LIB_LIB_DIR         -- The dir of so file  cpp client 
#
#

cmake_minimum_required(VERSION 3.0.0)

project("Nebula Cliet test" C CXX)

if (NEBULA_CLIENT_LIB_INCLUDE_DIR)
    include_directories(${NEBULA_CLIENT_LIB_INCLUDE_DIR})
endif()

if (NEBULA_CLIENT_LIB_LIB_DIR)
    link_directories(${NEBULA_CLIENT_LIB_LIB_DIR})
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

add_executable(
    nebula_client_example
    NebulaClientExample.cpp
)

target_link_libraries(
    nebula_client_example
    nebulaClient
    glog
    bz2
    pthread
)

