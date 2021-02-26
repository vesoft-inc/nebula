# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# CMakeLists.txt file contains a set of directives and instructions describing
# the project's source files and targets (executable, library, or both).
#
# The building can be controlled by defining the following variables on the
# <cmake> command line.
#
# ENABLE_PACK_ONE         -- Package to one or multi packages
#

# CMake version check
cmake_minimum_required(VERSION 3.5.0)

# Set the project name
project("Nebula Package")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_BINARY_DIR}/../../modules/common/cmake")

message(STATUS "CMAKE_MODULE_PATH = ${CMAKE_MODULE_PATH}")

if(NOT NEBULA_STORAGE_SOURCE_DIR)
    set(NEBULA_STORAGE_SOURCE_DIR ${CMAKE_BINARY_DIR}/../../modules/storage)
endif()

if(NOT NEBULA_STORAGE_BINARY_DIR)
    set(NEBULA_STORAGE_BINARY_DIR ${CMAKE_BINARY_DIR}/../modules/storage)
endif()

if(NOT NEBULA_SOURCE_DIR)
    set(NEBULA_SOURCE_DIR ${CMAKE_BINARY_DIR}/../../)
endif()

if(NOT NEBULA_BINARY_DIR)
    set(NEBULA_BINARY_DIR ${CMAKE_BINARY_DIR}/../)
endif()

message(STATUS "NEBULA_STORAGE_SOURCE_DIR : ${NEBULA_STORAGE_SOURCE_DIR}")
message(STATUS "NEBULA_STORAGE_BINARY_DIR : ${NEBULA_STORAGE_BINARY_DIR}")
message(STATUS "NEBULA_SOURCE_DIR         : ${NEBULA_SOURCE_DIR}")
message(STATUS "NEBULA_BINARY_DIR         : ${NEBULA_BINARY_DIR}")

install(
    FILES
        ${NEBULA_STORAGE_BINARY_DIR}/bin/nebula-storaged
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        storage
)

install(
    FILES
        ${NEBULA_STORAGE_BINARY_DIR}/bin/nebula-metad
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        meta
)

install(
    FILES
        ${NEBULA_BINARY_DIR}/bin/nebula-graphd
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        graph
)

install(
    FILES
        ${NEBULA_STORAGE_BINARY_DIR}/bin/db_dump
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)

install(
    FILES
        ${NEBULA_STORAGE_BINARY_DIR}/bin/meta_dump
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)

install(
    FILES
        ${NEBULA_STORAGE_BINARY_DIR}/bin/db_upgrader
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)

install(
    FILES
        ${NEBULA_STORAGE_SOURCE_DIR}/conf/nebula-storaged.conf.default
        ${NEBULA_STORAGE_SOURCE_DIR}/conf/nebula-storaged.conf.production
    PERMISSIONS
        OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        etc
    COMPONENT
        storage
)

install(
    FILES
        ${NEBULA_STORAGE_SOURCE_DIR}/conf/nebula-metad.conf.default
        ${NEBULA_STORAGE_SOURCE_DIR}/conf/nebula-metad.conf.production
    PERMISSIONS
        OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        etc
    COMPONENT
        meta
)

install(
    FILES
        ${NEBULA_SOURCE_DIR}/conf/nebula-graphd.conf.default
        ${NEBULA_SOURCE_DIR}/conf/nebula-graphd.conf.production
    PERMISSIONS
        OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        etc
    COMPONENT
        graph
)

install(
    FILES
        ${NEBULA_STORAGE_SOURCE_DIR}/scripts/nebula-storaged.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        storage
)

install(
    FILES
        ${NEBULA_STORAGE_SOURCE_DIR}/scripts/nebula-metad.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        meta
)

install(
    FILES
        ${NEBULA_SOURCE_DIR}/scripts/nebula-graphd.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        graph
)

install(
    FILES
        ${NEBULA_SOURCE_DIR}/scripts/nebula.service
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        common
)

install(
    FILES
        ${NEBULA_SOURCE_DIR}/scripts/utils.sh
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        scripts
    COMPONENT
        common
)

install(
    FILES
        ${NEBULA_SOURCE_DIR}/resources/gflags.json
    PERMISSIONS
        OWNER_WRITE OWNER_READ
        GROUP_READ
        WORLD_READ
    DESTINATION
        share/resources
    COMPONENT
        common
)

include(CPackage)
package(${ENABLE_PACK_ONE}
        "nebula-graph"
        "https://github.com/vesoft-inc/nebula-graph/releases"
        ${CMAKE_SOURCE_DIR})
