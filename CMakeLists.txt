# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

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
cmake_minimum_required(VERSION 3.21.0)
set(VCPKG_OVERLAY_PORTS "${CMAKE_CURRENT_LIST_DIR}/custom-ports")

# Set the project name
project("Nebula Graph" C CXX)

# Enable vcpkg manifest mode
if(NOT CMAKE_TOOLCHAIN_FILE)
    message(FATAL_ERROR "CMAKE_TOOLCHAIN_FILE must be set to use vcpkg manifest mode. "
                        "Please set it to: <vcpkg-root>/scripts/buildsystems/vcpkg.cmake")
endif()

# Prioritize vcpkg packages over system packages (e.g., Homebrew)
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)
set(CMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH FALSE)

option(ENABLE_PACK_ONE "Whether to package into one" ON)
option(ENABLE_VERBOSE_BISON "Enable Bison to report state" OFF)
option(ENABLE_CONSOLE_COMPILATION "Enable nebula-console compilation" OFF)
option(ENABLE_PACKAGE_TAR "Enable package artifacts to tar." OFF)
option(ENABLE_CREATE_GIT_HOOKS "Enable create git hooks." ON)
option(ENABLE_INCLUDE_WHAT_YOU_USE "Enable include-what-you-use find  nouse include files" OFF)

add_definitions(-DNEBULA_HOME=${CMAKE_SOURCE_DIR})

if(ENABLE_STANDALONE_VERSION)
    add_definitions(-DBUILD_STANDALONE)
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/nebula)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
include(PlatformCheck)
include(NebulaCMakeMacros)
include(ThirdPartyConfig)
include(GeneralCMakeOptions)
include(GeneralCMakeConfig)
include(GeneralCompilerConfig)
include(LinkerConfig)
include(SanitizerConfig)
include(GitInfoConfig)
include(NebulaCustomTargets)
include(IncludeWhatYouUse)
include(TimeTrace)
add_custom_target(
  clang-format
  COMMAND "find" "src/" "-type" "f" "\\(" "-iname" "\\*.h" "-o" "-iname" "\\*.cpp" "\\)" "|" "xargs" "clang-format" "-i"
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

if(ENABLE_MEMORY_TRACKER AND ENABLE_JEMALLOC AND NOT ENABLE_ASAN)
    add_definitions(-DENABLE_MEMORY_TRACKER)
    message(STATUS "MemoryTracker is ENABLED")
else()
    if(ENABLE_MEMORY_TRACKER)
        if(NOT ENABLE_JEMALLOC)
            message(WARNING "MemoryTracker requires ENABLE_JEMALLOC=ON, disabling MemoryTracker")
        elseif(ENABLE_ASAN)
            message(WARNING "MemoryTracker requires ENABLE_ASAN=OFF, disabling MemoryTracker")
        endif()
    endif()
    message(WARNING "MemoryTracker is DISABLED")
endif()

include_directories(AFTER ${CMAKE_SOURCE_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)

# For simplicity, we make all ordinary libraries depend on the compile-time generated files,
# including the precompiled header, a.k.a Base.h.gch, and thrift headers.

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
