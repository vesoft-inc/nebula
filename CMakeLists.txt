# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

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
#   ENABLE_JEMALLOC                -- Link jemalloc into all executables
#   ENABLE_NATIVE                  -- Build native client
#   ENABLE_TESTING                 -- Build unit test
#   ENABLE_PACK_ONE                -- Package to one or multi packages

# CMake version check
cmake_minimum_required(VERSION 3.9.0)

# Set the project name
project("Nebula Graph" C CXX)

option(ENABLE_PACK_ONE "Whether to package into one" ON)
option(ENABLE_VERBOSE_BISON "Enable Bison to report state" OFF)
option(ENABLE_PACKAGE_TAR "Enable package artifacts to tar." OFF)

add_definitions(-DNEBULA_HOME=${CMAKE_SOURCE_DIR})

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/nebula)

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

add_custom_target(
    install-all
    COMMAND $(MAKE) install
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)


add_custom_target(
  clang-format
  COMMAND "find" "src/" "-type" "f" "\\(" "-iname" "\\*.h" "-o" "-iname" "\\*.cpp" "\\)" "|" "xargs" "clang-format" "-i"
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

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
        common_thrift_generator
        graph_thrift_generator
        storage_thrift_generator
        meta_thrift_generator
        raftex_thrift_generator
        # hbase_thrift_generator
        parser_target
        wkt_parser_target
    )
endmacro()

nebula_add_subdirectory(src)
nebula_add_subdirectory(conf)
nebula_add_subdirectory(resources)
nebula_add_subdirectory(scripts)

include(CPackage)
package(
  ${ENABLE_PACK_ONE}
  "nebula-graph"
  "https://github.com/vesoft-inc/nebula/releases"
  ${CMAKE_SOURCE_DIR}/package
)
