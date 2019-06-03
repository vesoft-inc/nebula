# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER        -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER      -- Specify the compiler for C++ language

#   NEBULA_GPERF_BIN_DIR    -- Specify the full path to the directory
#                              containing gperf binary
#   NEBULA_FLEX_ROOT        -- Specify the root directory for flex
#   NEBULA_BISON_ROOT       -- Specify the root directory for bison
#   NEBULA_READLINE_ROOT    -- Specify the root directory for readline
#   NEBULA_NCURSES_ROOT     -- Specify the root directory for ncurses
#   NEBULA_KRB5_ROOT        -- Specify the root directory for KRB5
#   NEBULA_LIBUNWIND_ROOT   -- Specify the root directory for libunwind
#   NEBULA_OPENSSL_ROOT     -- Specify the root directory for openssl
#   NEBULA_BOOST_ROOT       -- Specify the root directory for boost
#
#   SKIP_JAVA_CLIENT        -- Skip building the java client
#   ENABLE_JEMALLOC         -- Link jemalloc into all executables
#   ENABLE_NATIVE           -- Build native client
#
cmake_minimum_required(VERSION 3.0.0)

project("Nebula Graph" C CXX)

set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_VERBOSE_MAKEFILE TRUE)

option(SKIP_JAVA_CLIENT "Whether to skip building the java client" OFF)
option(ENABLE_JEMALLOC "Whether to link jemalloc to all executables" ON)
option(ENABLE_NATIVE "Whether to build native client" OFF)

if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

enable_testing()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++")

# CMake macro to specify number of jobs to build third-party
if (NOT THIRD_PARTY_JOBS)
    set(THIRD_PARTY_JOBS 2)
endif(NOT THIRD_PARTY_JOBS)

# Possible values are Debug, Release, RelWithDebInfo, MinSizeRel
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif(NOT CMAKE_BUILD_TYPE)

message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}, "
               "options: Debug, Release, RelWithDebInfo, MinSizeRel")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "_build")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "_build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

# Set the project home dir
set(NEBULA_HOME ${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DNEBULA_HOME=${NEBULA_HOME})

# To include customized FindXXX.cmake modules
set(CMAKE_MODULE_PATH "${NEBULA_HOME}/cmake" ${CMAKE_MODULE_PATH})

if(NOT ${NEBULA_KRB5_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_KRB5_ROOT: " ${NEBULA_KRB5_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_KRB5_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_KRB5_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_KRB5_ROOT}/bin)
endif()

if(NOT ${NEBULA_LIBUNWIND_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_LIBUNWIND_ROOT: " ${NEBULA_LIBUNWIND_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_LIBUNWIND_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_LIBUNWIND_ROOT}/lib)
endif()

if(NOT ${NEBULA_OPENSSL_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_OPENSSL_ROOT: " ${NEBULA_OPENSSL_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_OPENSSL_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_OPENSSL_ROOT}/lib)
endif()

if(NOT ${NEBULA_BOOST_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_BOOST_ROOT: " ${NEBULA_BOOST_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_BOOST_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_BOOST_ROOT}/lib)
endif()

if(NOT ${NEBULA_READLINE_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_READLINE_ROOT: " ${NEBULA_READLINE_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_READLINE_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_READLINE_ROOT}/lib)
endif()

if(NOT ${NEBULA_NCURSES_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_NCURSES_ROOT: " ${NEBULA_NCURSES_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_NCURSES_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_NCURSES_ROOT}/lib)
endif()

if(NOT ${NEBULA_FLEX_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_FLEX_ROOT: " ${NEBULA_FLEX_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_FLEX_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_FLEX_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_FLEX_ROOT}/bin)
endif()

if(NOT ${NEBULA_BISON_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_BISON_ROOT: " ${NEBULA_BISON_ROOT})
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_BISON_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_BISON_ROOT}/bin)
endif()

if(NOT ${NEBULA_GPERF_BIN_DIR} STREQUAL "")
    message(STATUS "Specified NEBULA_GPERF_BIN_DIR: " ${NEBULA_GPERF_BIN_DIR})
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_GPERF_BIN_DIR})
endif()

option(asan "Whether to turn AddressSanitizer ON or OFF" OFF)

message(STATUS "ASAN: ${asan}")

string(REPLACE ";" ":" INCLUDE_PATH_STR "${CMAKE_INCLUDE_PATH}")
string(REPLACE ";" ":" LIBRARY_PATH_STR "${CMAKE_LIBRARY_PATH}")
string(REPLACE ";" ":" PROGRAM_PATH_STR "${CMAKE_PROGRAM_PATH}")
message(STATUS "CMAKE_INCLUDE_PATH: " ${INCLUDE_PATH_STR})
message(STATUS "CMAKE_LIBRARY_PATH: " ${LIBRARY_PATH_STR})
message(STATUS "CMAKE_PROGRAM_PATH: " ${PROGRAM_PATH_STR})

find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Krb5 REQUIRED gssapi)
find_package(GPERF 2.8 REQUIRED)
find_package(Libunwind REQUIRED)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)
find_package(Readline REQUIRED)
find_package(NCURSES REQUIRED)
find_package(LibLZMA MODULE)
if(NOT asan AND NOT ENABLE_NATIVE)
    find_package(PCHSupport)
    add_compile_options(-Winvalid-pch)
endif()

add_compile_options(-Wall)
add_compile_options(-Werror)
add_compile_options(-Wunused-parameter)
add_compile_options(-Wshadow)

# This requries GCC 5.1+
add_compile_options(-Wsuggest-override)

if(asan)
    add_definitions(-DBUILT_WITH_SANITIZER)
    add_compile_options(-fsanitize=address)
    add_compile_options(-g)
    add_compile_options(-fno-omit-frame-pointer)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

macro(nebula_add_test test_name)
    add_test(NAME ${test_name} COMMAND ${test_name})
    # e.g. cmake -DNEBULA_ASAN_PRELOAD=/path/to/libasan.so
    # or,  cmake -DNEBULA_ASAN_PRELOAD=`/path/to/gcc --print-file-name=libasan.so`
    if (NEBULA_ASAN_PRELOAD)
        set_property(TEST ${test_name} PROPERTY ENVIRONMENT LD_PRELOAD=${NEBULA_ASAN_PRELOAD})
    endif(NEBULA_ASAN_PRELOAD)
endmacro(nebula_add_test)

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
include_directories(SYSTEM ${NCURSES_INCLUDE_DIR})
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
    ${NEBULA_HOME}/third-party/rocksdb/_install/lib
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

set(ROCKSDB_LIBRARIES ${NEBULA_HOME}/third-party/rocksdb/_install/lib/librocksdb.a)

# All compression libraries
set(COMPRESSION_LIBRARIES bz2 snappy zstd z)
if (LIBLZMA_FOUND)
    include_directories(SYSTEM ${LIBLZMA_INCLUDE_DIRS})
    list(APPEND COMPRESSION_LIBRARIES ${LIBLZMA_LIBRARIES})
endif()

if (NOT ENABLE_JEMALLOC)
    set(JEMALLOC_LIB )
else()
    set(JEMALLOC_LIB jemalloc)
endif()

execute_process(
    COMMAND ldd --version
    COMMAND head -1
    COMMAND cut -d ")" -f 2
    COMMAND cut -d " " -f 2
    OUTPUT_VARIABLE GLIBC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "Glibc version is " ${GLIBC_VERSION})

if (GLIBC_VERSION VERSION_LESS "2.17")
    set(GETTIME_LIB rt)
else()
    set(GETTIME_LIB)
endif()

# A wrapper for target_link_libraries()
macro(nebula_link_libraries target)
    target_link_libraries(
        ${target}
        ${ARGN}
        folly
        glog
        gflags
        boost_context
        boost_system
        event
        double-conversion
        resolv
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${KRB5_LIBRARIES}
        ${COMPRESSION_LIBRARIES}
        ${JEMALLOC_LIB}
        ${LIBUNWIND_LIBRARIES}
        dl
        ${GETTIME_LIB}
        -pthread
    )
endmacro(nebula_link_libraries)

add_subdirectory(third-party)
add_subdirectory(src)
add_subdirectory(etc)
add_subdirectory(scripts)

add_custom_target(
    clean-build
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS clean-interface clean-pch
)

if (NOT SKIP_JAVA_CLIENT)
    set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch clean-third-party clean-java-client)
else()
    set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch clean-third-party)
endif()

add_custom_target(
    clean-all
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS ${NEBULA_CLEAN_ALL_DEPS}
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


IF(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git/")
    # Create the pre-commit hook every time we run cmake
    message(STATUS "Create the pre-commit hook")
    set(PRE_COMMIT_HOOK ${CMAKE_CURRENT_SOURCE_DIR}/.git/hooks/pre-commit)
    execute_process(
        COMMAND
        "rm" "-f" ${PRE_COMMIT_HOOK}
    )
    execute_process(
        COMMAND
        "ln" "-s" ${CMAKE_CURRENT_SOURCE_DIR}/.linters/cpp/hooks/pre-commit.sh ${PRE_COMMIT_HOOK}
        RESULT_VARIABLE retcode
    )
    IF(${retcode} EQUAL 0)
        MESSAGE(STATUS "Creating pre-commit hook done")
    ELSE()
        MESSAGE(FATAL_ERROR "Creating pre-commit hook failed: ${retcode}")
    ENDIF()
ELSE()
    MESSAGE(FATAL_ERROR "Currently, we don't support downloaded source code.\nPlease using git clone it and rebuild.")
ENDIF()
