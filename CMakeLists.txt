# Copyright (c) 2019 - present, VE Software Inc. All rights reserved
#
# This source code is licensed under Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory)
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER        -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER      -- Specify the compiler for C++ language
#   FLEX_EXECUTABLE         -- Specify the full path of flex executable
#   VGRAPH_GPERF_BIN_DIR    -- Specify the full path to the directory
#                              containing gperf binary
#
#   VGRAPH_KRB5_ROOT        -- Specify the root directory for KRB5
#   VGRAPH_LIBUNWIND_ROOT   -- Specify the root directory for libunwind
#   VGRAPH_OPENSSL_ROOT     -- Specify the root directory for openssl
#   VGRAPH_BOOST_ROOT       -- Specify the root directory for boost
#
cmake_minimum_required(VERSION 3.0.0)

project("vGraph" C CXX)

set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_VERBOSE_MAKEFILE TRUE)

enable_testing()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++")

# Possible values are Debug, Release, RelWithDebInfo, MinSizeRel
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" FORCE)
endif(NOT CMAKE_BUILD_TYPE)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "_build")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "_build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

# Set the project home dir
set(VGRAPH_HOME ${CMAKE_CURRENT_SOURCE_DIR})

# To include customized FindXXX.cmake modules
set(CMAKE_MODULE_PATH "${VGRAPH_HOME}/cmake" ${CMAKE_MODULE_PATH})

if(NOT ${VGRAPH_KRB5_ROOT} STREQUAL "")
    message(STATUS "Specified VGRAPH_KRB5_ROOT: " ${VGRAPH_KRB5_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${VGRAPH_KRB5_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${VGRAPH_KRB5_ROOT}/lib)
    list(APPEND CMAKE_PROGRAM_PATH ${VGRAPH_KRB5_ROOT}/bin)
endif()

if(NOT ${VGRAPH_LIBUNWIND_ROOT} STREQUAL "")
    message(STATUS "Specified VGRAPH_LIBUNWIND_ROOT: " ${VGRAPH_LIBUNWIND_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${VGRAPH_LIBUNWIND_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${VGRAPH_LIBUNWIND_ROOT}/lib)
endif()

if(NOT ${VGRAPH_OPENSSL_ROOT} STREQUAL "")
    message(STATUS "Specified VGRAPH_OPENSSL_ROOT: " ${VGRAPH_OPENSSL_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${VGRAPH_OPENSSL_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${VGRAPH_OPENSSL_ROOT}/lib)
endif()

if(NOT ${VGRAPH_BOOST_ROOT} STREQUAL "")
    message(STATUS "Specified VGRAPH_BOOST_ROOT: " ${VGRAPH_BOOST_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${VGRAPH_BOOST_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${VGRAPH_BOOST_ROOT}/lib)
endif()

if(NOT ${VGRAPH_GPERF_BIN_DIR} STREQUAL "")
    message(STATUS "Specified VGRAPH_GPERF_BIN_DIR: " ${VGRAPH_GPERF_BIN_DIR})
    list(APPEND CMAKE_PROGRAM_PATH ${VGRAPH_GPERF_BIN_DIR})
endif()

message(STATUS "CMAKE_INCLUDE_PATH: " ${CMAKE_INCLUDE_PATH})
message(STATUS "CMAKE_LIBRARY_PATH: " ${CMAKE_LIBRARY_PATH})
message(STATUS "CMAKE_PROGRAM_PATH: " ${CMAKE_PROGRAM_PATH})

find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Krb5 REQUIRED gssapi)
find_package(PCHSupport)
find_package(GPERF 2.8 REQUIRED)
find_package(Libunwind REQUIRED)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

add_compile_options(-Winvalid-pch)
add_compile_options(-Wall)
add_compile_options(-Werror)
add_compile_options(-Wno-error=sign-compare)
add_compile_options(-Wshadow)

include_directories(SYSTEM ${VGRAPH_HOME}/third-party/bzip2/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/double-conversion/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/fatal/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/fbthrift/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/folly/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/gflags/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/glog/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/googletest/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/jemalloc/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/libevent/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/mstch/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/proxygen/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/rocksdb/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/snappy/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/wangle/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/zlib/_install/include)
include_directories(SYSTEM ${VGRAPH_HOME}/third-party/zstd/_install/include)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
include_directories(SYSTEM ${KRB5_INCLUDE_DIRS})
include_directories(AFTER common)
include_directories(AFTER interface)
include_directories(AFTER ${VGRAPH_HOME})

link_directories(
    ${VGRAPH_HOME}/third-party/bzip2/_install/lib
    ${VGRAPH_HOME}/third-party/double-conversion/_install/lib
    ${VGRAPH_HOME}/third-party/fatal/_install/lib
    ${VGRAPH_HOME}/third-party/fbthrift/_install/lib
    ${VGRAPH_HOME}/third-party/folly/_install/lib
    ${VGRAPH_HOME}/third-party/gflags/_install/lib
    ${VGRAPH_HOME}/third-party/glog/_install/lib
    ${VGRAPH_HOME}/third-party/googletest/_install/lib
    ${VGRAPH_HOME}/third-party/jemalloc/_install/lib
    ${VGRAPH_HOME}/third-party/libevent/_install/lib
    ${VGRAPH_HOME}/third-party/mstch/_install/lib
    ${VGRAPH_HOME}/third-party/proxygen/_install/lib
    ${VGRAPH_HOME}/third-party/rocksdb/_install/lib64
    ${VGRAPH_HOME}/third-party/snappy/_install/lib
    ${VGRAPH_HOME}/third-party/wangle/_install/lib
    ${VGRAPH_HOME}/third-party/zlib/_install/lib
    ${VGRAPH_HOME}/third-party/zstd/_install/lib
    ${Boost_LIBRARY_DIRS}
    ${KRB5_LIBRARY_DIRS}
)

# All thrift libraries
set(THRIFT_LIBRARIES
    thriftcpp2
    thrift
    thriftprotocol
    async
    protocol
    transport
    concurrency
    security
    thriftfrozen2
    thrift-core
)

# All compression libraries
set(COMPRESSION_LIBRARIES bz2 snappy zstd z)

add_subdirectory(third-party)
add_subdirectory(interface)
add_subdirectory(common)
add_subdirectory(dataman)
add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(console)
add_subdirectory(raftex)
#add_subdirectory(storage)
add_subdirectory(parser)

add_dependencies(common third-party)
#add_dependencies(storage_engines common)

add_custom_target(
    clean-build
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS clean-interface clean-pch
)

add_custom_target(
    clean-all
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS clean-interface clean-pch clean-third-party
)

add_custom_target(
    distclean
    COMMAND "find" "." "-name" "CMakeFiles" "|" "xargs" "rm" "-fr"
    COMMAND "find" "." "-name" "CMakeCache.txt" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "cmake_install.cmake" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "CTestTestfile.cmake" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "Makefile" "|" "xargs" "rm" "-f"
    DEPENDS clean-all
)

