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
#   NEBULA_GPERF_BIN_DIR    -- Specify the full path to the directory
#                              containing gperf binary
#
#   NEBULA_KRB5_ROOT        -- Specify the root directory for KRB5
#   NEBULA_LIBUNWIND_ROOT   -- Specify the root directory for libunwind
#   NEBULA_OPENSSL_ROOT     -- Specify the root directory for openssl
#   NEBULA_BOOST_ROOT       -- Specify the root directory for boost
#
cmake_minimum_required(VERSION 3.0.0)

project("Nebula Graph" C CXX)

set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_VERBOSE_MAKEFILE TRUE)

enable_testing()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++")

if (NOT THIRD_PARTY_JOBS)
    set(THIRD_PARTY_JOBS 2)
endif(NOT THIRD_PARTY_JOBS)

# Possible values are Debug, Release, RelWithDebInfo, MinSizeRel
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif(NOT CMAKE_BUILD_TYPE)
message("== Build type is " ${CMAKE_BUILD_TYPE} " ==")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "_build")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "_build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

# Set the project home dir
set(NEBULA_HOME ${CMAKE_CURRENT_SOURCE_DIR})

# To include customized FindXXX.cmake modules
set(CMAKE_MODULE_PATH "${NEBULA_HOME}/cmake" ${CMAKE_MODULE_PATH})

if(NOT ${NEBULA_KRB5_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_KRB5_ROOT: " ${NEBULA_KRB5_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${NEBULA_KRB5_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${NEBULA_KRB5_ROOT}/lib)
    list(APPEND CMAKE_PROGRAM_PATH ${NEBULA_KRB5_ROOT}/bin)
endif()

if(NOT ${NEBULA_LIBUNWIND_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_LIBUNWIND_ROOT: " ${NEBULA_LIBUNWIND_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${NEBULA_LIBUNWIND_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${NEBULA_LIBUNWIND_ROOT}/lib)
endif()

if(NOT ${NEBULA_OPENSSL_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_OPENSSL_ROOT: " ${NEBULA_OPENSSL_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${NEBULA_OPENSSL_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${NEBULA_OPENSSL_ROOT}/lib)
endif()

if(NOT ${NEBULA_BOOST_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_BOOST_ROOT: " ${NEBULA_BOOST_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${NEBULA_BOOST_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${NEBULA_BOOST_ROOT}/lib)
endif()

if(NOT ${NEBULA_READLINE_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_READLINE_ROOT: " ${NEBULA_READLINE_ROOT})
    list(APPEND CMAKE_INCLUDE_PATH ${NEBULA_READLINE_ROOT}/include)
    list(APPEND CMAKE_LIBRARY_PATH ${NEBULA_READLINE_ROOT}/lib)
endif()

if(NOT ${NEBULA_GPERF_BIN_DIR} STREQUAL "")
    message(STATUS "Specified NEBULA_GPERF_BIN_DIR: " ${NEBULA_GPERF_BIN_DIR})
    list(APPEND CMAKE_PROGRAM_PATH ${NEBULA_GPERF_BIN_DIR})
endif()


option(asan "Whether to turn AddressSanitizer ON or OFF" OFF)

message(STATUS "ASAN: ${asan}")


message(STATUS "CMAKE_INCLUDE_PATH: " ${CMAKE_INCLUDE_PATH})
message(STATUS "CMAKE_LIBRARY_PATH: " ${CMAKE_LIBRARY_PATH})
message(STATUS "CMAKE_PROGRAM_PATH: " ${CMAKE_PROGRAM_PATH})

find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Krb5 REQUIRED gssapi)
find_package(GPERF 2.8 REQUIRED)
find_package(Libunwind REQUIRED)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)
find_package(Readline REQUIRED)
if(NOT asan)
    find_package(PCHSupport)
    add_compile_options(-Winvalid-pch)
endif()


add_compile_options(-Wall)
add_compile_options(-Werror)
add_compile_options(-Wunused-parameter)
add_compile_options(-Wshadow)

if(asan)
    add_compile_options(-fsanitize=address)
    add_compile_options(-g)
    add_compile_options(-fno-omit-frame-pointer)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

include_directories(SYSTEM ${NEBULA_HOME}/third-party/bzip2/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/double-conversion/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/fatal/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/fbthrift/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/folly/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/gflags/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/glog/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/googletest/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/jemalloc/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/libevent/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/mstch/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/proxygen/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/rocksdb/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/snappy/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/wangle/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/zlib/_install/include)
include_directories(SYSTEM ${NEBULA_HOME}/third-party/zstd/_install/include)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
include_directories(SYSTEM ${KRB5_INCLUDE_DIRS})
include_directories(SYSTEM ${FLEX_INCLUDE_DIRS})
include_directories(SYSTEM ${Readline_INCLUDE_DIR})
include_directories(AFTER ${NEBULA_HOME}/src)
include_directories(AFTER src/common)
include_directories(AFTER src/interface)

link_directories(
    ${NEBULA_HOME}/third-party/bzip2/_install/lib
    ${NEBULA_HOME}/third-party/double-conversion/_install/lib
    ${NEBULA_HOME}/third-party/fatal/_install/lib
    ${NEBULA_HOME}/third-party/fbthrift/_install/lib
    ${NEBULA_HOME}/third-party/folly/_install/lib
    ${NEBULA_HOME}/third-party/gflags/_install/lib
    ${NEBULA_HOME}/third-party/glog/_install/lib
    ${NEBULA_HOME}/third-party/googletest/_install/lib
    ${NEBULA_HOME}/third-party/jemalloc/_install/lib
    ${NEBULA_HOME}/third-party/libevent/_install/lib
    ${NEBULA_HOME}/third-party/mstch/_install/lib
    ${NEBULA_HOME}/third-party/proxygen/_install/lib
    ${NEBULA_HOME}/third-party/rocksdb/_install/lib64
    ${NEBULA_HOME}/third-party/snappy/_install/lib
    ${NEBULA_HOME}/third-party/wangle/_install/lib
    ${NEBULA_HOME}/third-party/zlib/_install/lib
    ${NEBULA_HOME}/third-party/zstd/_install/lib
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

set(ROCKSDB_LIBRARIES ${NEBULA_HOME}/third-party/rocksdb/_install/lib64/librocksdb.a)

# All compression libraries
set(COMPRESSION_LIBRARIES bz2 snappy zstd z)

add_subdirectory(third-party)
add_subdirectory(src)
add_subdirectory(etc)
add_subdirectory(scripts)

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

