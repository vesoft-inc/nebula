# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER                -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER              -- Specify the compiler for C++ language
#
#   NEBULA_THIRDPARTY_ROOT          -- Specify the root directory for third-party
#   NEBULA_OTHER_ROOT               -- Specify the root directory for user build
#                                   -- Split with ":", exp: DIR:DIR
#
#   NEBULA_COMMON_REPO_URL          -- Git URL for the nebula-common repo
#   NEBULA_COMMON_REPO_TAG          -- Tag/branch of the nebula-common repo
#
#   NEBULA_STORAGE_REPO_URL         -- Git URL for the nebula-storage repo
#   NEBULA_STORAGE_REPO_TAG         -- Tag/branch of the nebula-storage repo
#   ENABLE_BUILD_STORAGE            -- Build storage repo
#
#   ENABLE_JEMALLOC                -- Link jemalloc into all executables
#   ENABLE_NATIVE                  -- Build native client
#   ENABLE_TESTING                 -- Build unit test
#   ENABLE_PACK_ONE                -- Package to one or multi packages
#
# CMake version check
cmake_minimum_required(VERSION 3.9.0)

# Set the project name
project("Nebula Graph" C CXX)

option(ENABLE_PACK_ONE "Whether to package into one" ON)
option(ENABLE_BUILD_STORAGE "Whether to build storage" OFF)
option(ENABLE_MODULE_UPDATE "Automatically update module" OFF)
option(ENABLE_VERBOSE_BISON "Enable Bison to report state" OFF)
option(ENABLE_MODULE_FORCE_CHECKOUT "Whether checkout branch of module to same as graph." ON)

add_definitions(-DNEBULA_HOME=${CMAKE_SOURCE_DIR})

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# Submodules
if("${NEBULA_COMMON_REPO_URL}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_URL "https://github.com/vesoft-inc/nebula-common.git")
endif()

if("${NEBULA_COMMON_REPO_TAG}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_TAG "master")
endif()

if("${NEBULA_STORAGE_REPO_URL}" STREQUAL "")
    SET(NEBULA_STORAGE_REPO_URL "https://github.com/vesoft-inc/nebula-storage.git")
endif()

if("${NEBULA_STORAGE_REPO_TAG}" STREQUAL "")
    SET(NEBULA_STORAGE_REPO_TAG "master")
endif()

include(FetchModule)
if(NOT NEBULA_COMMON_SOURCE_DIR)
    nebula_fetch_module(
        NAME
            common
        URL
            ${NEBULA_COMMON_REPO_URL}
        TAG
            ${NEBULA_COMMON_REPO_TAG}
        UPDATE
            ${ENABLE_MODULE_UPDATE}
        CHECKOUT
            ${ENABLE_MODULE_FORCE_CHECKOUT}
    )
    set(nebula_common_source_dir ${CMAKE_SOURCE_DIR}/modules/common)
    set(nebula_common_build_dir ${CMAKE_BINARY_DIR}/modules/common)
else()
    message(STATUS "NEBULA_COMMON_SOURCE_DIR: " ${NEBULA_COMMON_SOURCE_DIR})
    set(nebula_common_source_dir ${NEBULA_COMMON_SOURCE_DIR})
    if(NOT NEBULA_COMMON_BUILD_DIR)
        set(nebula_common_build_dir ${CMAKE_BINARY_DIR}/modules/common)
    else()
        set(nebula_common_build_dir ${NEBULA_COMMON_BUILD_DIR})
    endif()
endif()

list(APPEND CMAKE_MODULE_PATH ${nebula_common_source_dir}/cmake)
list(APPEND CMAKE_MODULE_PATH ${nebula_common_source_dir}/cmake/nebula)

include(PlatformCheck)
include(NebulaCMakeMacros)
include(GeneralCMakeOptions)
include(GeneralCMakeConfig)
include(GeneralCompilerConfig)
include(LinkerConfig)
include(CcacheConfig)
include(ThirdPartyConfig)
include(SanitizerConfig)
include(GitHooksConfig)
include(GitInfoConfig)
include(NebulaCustomTargets)
include(ConfigNebulaCommon)
config_nebula_common(
    SOURCE_DIR ${nebula_common_source_dir}
    BUILD_DIR ${nebula_common_build_dir}
)

if(ENABLE_BUILD_STORAGE)
    include(ConfigNebulaStorage)
    if(NOT NEBULA_STORAGE_SOURCE_DIR)
        nebula_fetch_module(
            NAME
                storage
            URL
                ${NEBULA_STORAGE_REPO_URL}
            TAG
                ${NEBULA_STORAGE_REPO_TAG}
            UPDATE
                ${ENABLE_MODULE_UPDATE}
            CHECKOUT
                ${ENABLE_MODULE_FORCE_CHECKOUT}
        )
        set(nebula_storage_source_dir ${CMAKE_SOURCE_DIR}/modules/storage)
        set(nebula_storage_build_dir ${CMAKE_BINARY_DIR}/modules/storage)
    else()
        message(STATUS "NEBULA_STORAGE_SOURCE_DIR: " ${NEBULA_STORAGE_SOURCE_DIR})
        set(nebula_storage_source_dir ${NEBULA_STORAGE_SOURCE_DIR})
        if(NOT NEBULA_STORAGE_BUILD_DIR)
            set(nebula_storage_build_dir ${CMAKE_BINARY_DIR}/modules/storage)
        else()
            set(nebula_storage_build_dir ${NEBULA_STORAGE_BUILD_DIR})
        endif()
    endif()
    config_nebula_storage(
        SOURCE_DIR ${nebula_storage_source_dir}
        BUILD_DIR ${nebula_storage_build_dir}
        COMMON_SOURCE_DIR ${nebula_common_source_dir}
        COMMON_BUILD_DIR ${nebula_common_build_dir}
    )
endif()

add_custom_target(
    clean-modules
    DEPENDS clean-common
)

if(TARGET clean-storage)
    add_dependencies(clean-modules clean-storage)
endif()

add_custom_target(
    install-all
    COMMAND $(MAKE) install
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

if(TARGET install-storage)
    add_dependencies(install-all install-storage)
endif()


if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

include_directories(AFTER ${CMAKE_SOURCE_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)

# For simplicity, we make all ordinary libraries depend on the compile-time generated files,
# including the precompiled header, a.k.a Base.h.gch, and thrift headers.
macro(nebula_add_library name type)
    add_library(${name} ${type} ${ARGN})
    if (PCHSupport_FOUND)
        add_dependencies(
            ${name}
            base_obj_gch
        )
    endif()
    add_dependencies(
        ${name}
        common_project
        parser_target
    )
endmacro()

nebula_add_subdirectory(src)
nebula_add_subdirectory(conf)
nebula_add_subdirectory(resources)
nebula_add_subdirectory(scripts)
