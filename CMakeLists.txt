# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER               -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER             -- Specify the compiler for C++ language

#   NEBULA_GPERF_BIN_DIR           -- Specify the full path to the directory
#                                      containing gperf binary
#   NEBULA_FLEX_ROOT               -- Specify the root directory for flex
#   NEBULA_BISON_ROOT              -- Specify the root directory for bison
#   NEBULA_READLINE_ROOT           -- Specify the root directory for readline
#   NEBULA_NCURSES_ROOT            -- Specify the root directory for ncurses
#   NEBULA_KRB5_ROOT               -- Specify the root directory for KRB5
#   NEBULA_LIBUNWIND_ROOT          -- Specify the root directory for libunwind
#   NEBULA_OPENSSL_ROOT            -- Specify the root directory for openssl
#   NEBULA_BOOST_ROOT              -- Specify the root directory for boost
#   NEBULA_DOUBLE_CONVERSION_ROOT  -- Specify the root directory double-conversion
#   NEBULA_FATAL_ROOT              -- Specify the root directory fatal
#   NEBULA_FBTHRIFT_ROOT           -- Specify the root directory fbthrift
#   NEBULA_FOLLY_ROOT              -- Specify the root directory folly
#   NEBULA_GFLAGS_ROOT             -- Specify the root directory gflags
#   NEBULA_GLOG_ROOT               -- Specify the root directory glog
#   NEBULA_GOOGLETEST_ROOT         -- Specify the root directory googletest
#   NEBULA_JEMALLOC_ROOT           -- Specify the root directory jemalloc
#   NEBULA_LIBEVENT_ROOT           -- Specify the root directory libevent
#   NEBULA_MSTCH_ROOT              -- Specify the root directory mstch
#   NEBULA_PROXYGEN_ROOT           -- Specify the root directory proxygen
#   NEBULA_ROCKSDB_ROOT            -- Specify the root directory rocksdb
#   NEBULA_SNAPPY_ROOT             -- Specify the root directory snappy
#   NEBULA_WANGLE_ROOT             -- Specify the root directory wangle
#   NEBULA_ZLIB_ROOT               -- Specify the root directory zlib
#   NEBULA_ZSTD_ROOT               -- Specify the root directory zstd
#
#   SKIP_JAVA_CLIENT               -- Skip building the java client
#   ENABLE_JEMALLOC                -- Link jemalloc into all executables
#   ENABLE_NATIVE                  -- Build native client
#
cmake_minimum_required(VERSION 3.0.0)

project("Nebula Graph" C CXX)

set(CMAKE_SKIP_RPATH TRUE)

option(SKIP_JAVA_CLIENT "Whether to skip building the java client" OFF)
option(ENABLE_JEMALLOC "Whether to link jemalloc to all executables" ON)
option(ENABLE_NATIVE "Whether to build native client" OFF)
option(ENABLE_CCACHE "Whether to use ccache to speed up compiling" ON)
option(ENABLE_ASAN "Whether to turn AddressSanitizer ON or OFF" OFF)

message(STATUS "ASAN: ${ENABLE_ASAN}")


if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

enable_testing()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -no-pie")

# To detect if ccache available
find_program(ccache_program_found "ccache")
if (ENABLE_CCACHE AND ccache_program_found)
    message(STATUS "CCACHE: ON")
    if (NOT $ENV{CCACHE_DIR} STREQUAL "")
        message(STATUS "CCACHE_DIR: $ENV{CCACHE_DIR}")
    else()
        message(STATUS "CCACHE_DIR: $ENV{HOME}/.ccache")
    endif()
    set(CMAKE_CXX_COMPILER_LAUNCHER "ccache")
elseif (ENABLE_CCACHE)
    message(STATUS "CCACHE: enabled but not found")
    set(CMAKE_CXX_COMPILER_LAUNCHER)
else ()
    message(STATUS "CCACHE: OFF")
    set(CMAKE_CXX_COMPILER_LAUNCHER)
endif()

# Possible values are Debug, Release, RelWithDebInfo, MinSizeRel
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif(NOT CMAKE_BUILD_TYPE)

if(NOT ${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    message(STATUS "Set macro _FORTIFY_SOURCE=2")
    add_definitions(-D_FORTIFY_SOURCE=2)
endif()

message(STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE} "
               "(Options are: Debug, Release, RelWithDebInfo, MinSizeRel)")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "_build")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "_build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

# Set the project home dir
set(NEBULA_HOME ${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DNEBULA_HOME=${NEBULA_HOME})

find_package(Git)
if (GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
    execute_process(
        COMMAND
        ${GIT_EXECUTABLE} rev-parse --short HEAD
        OUTPUT_VARIABLE GIT_INFO_SHA
    )
endif()

if (GIT_INFO_SHA)
    string(REGEX REPLACE "[^0-9a-f]+" "" GIT_INFO_SHA "${GIT_INFO_SHA}")
    add_definitions(-DGIT_INFO_SHA=${GIT_INFO_SHA})
endif()

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

# third-party
if(NOT ${NEBULA_BZIP2_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_BZIP2_ROOT: " ${NEBULA_BZIP2_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_BZIP2_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_BZIP2_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_BZIP2_ROOT}/bin)
endif()

if(NOT ${NEBULA_DOUBLE_CONVERSION_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_DOUBLE_CONVERSION_ROOT: " ${NEBULA_DOUBLE_CONVERSION_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_DOUBLE_CONVERSION_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_DOUBLE_CONVERSION_ROOT}/lib)
endif()

if(NOT ${NEBULA_FATAL_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_FATAL_ROOT: " ${NEBULA_FATAL_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_FATAL_ROOT}/include)
endif()

if(NOT ${NEBULA_FBTHRIFT_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_FBTHRIFT_ROOT: " ${NEBULA_FBTHRIFT_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_FBTHRIFT_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_FBTHRIFT_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_FBTHRIFT_ROOT}/bin)
endif()

if(NOT ${NEBULA_SNAPSHOT_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_SNAPSHOT_ROOT: " ${NEBULA_SNAPSHOT_ROOT})
    set_property(GLOBAL PROPERTY java-fbthrift-jar ${NEBULA_SNAPSHOT_ROOT})
else()
    message(FATAL_ERROR "Please input the path of thrift-1.0-SNAPSHOT.jar")
endif()

if(NOT ${NEBULA_FOLLY_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_FOLLY_ROOT: " ${NEBULA_FOLLY_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_FOLLY_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_FOLLY_ROOT}/lib)
endif()

if(NOT ${NEBULA_GFLAGS_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_GFLAGS_ROOT: " ${NEBULA_GFLAGS_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_GFLAGS_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_GFLAGS_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_GFLAGS_ROOT}/bin)
endif()

if(NOT ${NEBULA_GLOG_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_GLOG_ROOT: " ${NEBULA_GLOG_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_GLOG_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_GLOG_ROOT}/lib)
endif()

if(NOT ${NEBULA_GOOGLETEST_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_GOOGLETEST_ROOT: " ${NEBULA_GOOGLETEST_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_GOOGLETEST_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_GOOGLETEST_ROOT}/lib)
endif()

if(NOT ${NEBULA_JEMALLOC_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_JEMALLOC_ROOT: " ${NEBULA_JEMALLOC_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_JEMALLOC_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_JEMALLOC_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_JEMALLOC_ROOT}/bin)
endif()

if(NOT ${NEBULA_LIBEVENT_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_LIBEVENT_ROOT: " ${NEBULA_LIBEVENT_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_LIBEVENT_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_LIBEVENT_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_LIBEVENT_ROOT}/bin)
endif()

if(NOT ${NEBULA_MSTCH_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_MSTCH_ROOT: " ${NEBULA_MSTCH_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_MSTCH_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_MSTCH_ROOT}/lib)
endif()

if(NOT ${NEBULA_PROXYGEN_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_PROXYGEN_ROOT: " ${NEBULA_PROXYGEN_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_PROXYGEN_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_PROXYGEN_ROOT}/lib)
endif()

if(NOT ${NEBULA_ROCKSDB_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_ROCKSDB_ROOT: " ${NEBULA_ROCKSDB_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_ROCKSDB_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_ROCKSDB_ROOT}/lib)
    set(ROCKSDB_LIBRARIES ${NEBULA_ROCKSDB_ROOT}/lib/librocksdb.a)
endif()

if(NOT ${NEBULA_SNAPPY_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_SNAPPY_ROOT: " ${NEBULA_SNAPPY_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_SNAPPY_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_SNAPPY_ROOT}/lib)
endif()

if(NOT ${NEBULA_WANGLE_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_WANGLE_ROOT: " ${NEBULA_WANGLE_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_WANGLE_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_WANGLE_ROOT}/lib)
endif()

if(NOT ${NEBULA_ZLIB_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_ZLIB_ROOT: " ${NEBULA_ZLIB_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_ZLIB_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_ZLIB_ROOT}/lib)
endif()

if(NOT ${NEBULA_ZSTD_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_ZSTD_ROOT: " ${NEBULA_ZSTD_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_ZSTD_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_ZSTD_ROOT}/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_ZSTD_ROOT}/bin)
endif()

option(asan "Whether to turn AddressSanitizer ON or OFF" OFF)

message(STATUS "ASAN: ${asan}")

string(REPLACE ";" ":" INCLUDE_PATH_STR "${CMAKE_INCLUDE_PATH}")
string(REPLACE ";" ":" LIBRARY_PATH_STR "${CMAKE_LIBRARY_PATH}")
string(REPLACE ";" ":" PROGRAM_PATH_STR "${CMAKE_PROGRAM_PATH}")
message(STATUS "CMAKE_INCLUDE_PATH: " ${INCLUDE_PATH_STR})
message(STATUS "CMAKE_LIBRARY_PATH: " ${LIBRARY_PATH_STR})
message(STATUS "CMAKE_PROGRAM_PATH: " ${PROGRAM_PATH_STR})

find_package(Bzip2 REQUIRED)
find_package(DoubleConversion REQUIRED)
find_package(Fatal REQUIRED)
find_package(Fbthrift REQUIRED)
find_package(Folly REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_package(Googletest REQUIRED)
find_package(Jemalloc REQUIRED)
find_package(Libevent REQUIRED)
find_package(Mstch REQUIRED)
find_package(Proxygen REQUIRED)
find_package(Rocksdb REQUIRED)
find_package(Snappy REQUIRED)
find_package(Wangle REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Zstd REQUIRED)
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Krb5 REQUIRED gssapi)
find_package(GPERF 2.8 REQUIRED)
find_package(Libunwind REQUIRED)
find_package(BISON 3.0.5 REQUIRED)
find_package(FLEX REQUIRED)
find_package(Readline REQUIRED)
find_package(NCURSES REQUIRED)
find_package(LibLZMA MODULE)
if(NOT ENABLE_ASAN AND NOT ENABLE_NATIVE)
    find_package(PCHSupport)
    add_compile_options(-Winvalid-pch)
endif()

add_compile_options(-Wall)
add_compile_options(-Werror)
add_compile_options(-Wunused-parameter)
add_compile_options(-Wshadow)

# This requries GCC 5.1+
add_compile_options(-Wsuggest-override)

if(ENABLE_ASAN)
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

macro(nebula_add_base_obj obj_name)
    add_dependencies(${obj_name} base_obj)
endmacro(nebula_add_base_obj)

include_directories(SYSTEM ${NEBULA_BZIP2_ROOT}/include)
include_directories(SYSTEM ${NEBULA_DOUBLE_CONVERSION_ROOT}/include)
include_directories(SYSTEM ${NEBULA_FATAL_ROOT}/include)
include_directories(SYSTEM ${NEBULA_FBTHRIFT_ROOT}/include)
include_directories(SYSTEM ${NEBULA_FOLLY_ROOT}/include)
include_directories(SYSTEM ${NEBULA_GFLAGS_ROOT}/include)
include_directories(SYSTEM ${NEBULA_GLOG_ROOT}/include)
include_directories(SYSTEM ${NEBULA_GOOGLETEST_ROOT}/include)
include_directories(SYSTEM ${NEBULA_JEMALLOC_ROOT}/include)
include_directories(SYSTEM ${NEBULA_LIBEVENT_ROOT}/include)
include_directories(SYSTEM ${NEBULA_MSTCH_ROOT}/include)
include_directories(SYSTEM ${NEBULA_PROXYGEN_ROOT}/include)
include_directories(SYSTEM ${NEBULA_ROCKSDB_ROOT}/include)
include_directories(SYSTEM ${NEBULA_SNAPPY_ROOT}/include)
include_directories(SYSTEM ${NEBULA_WANGLE_ROOT}/include)
include_directories(SYSTEM ${NEBULA_ZLIB_ROOT}/include)
include_directories(SYSTEM ${NEBULA_ZSTD_ROOT}/include)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
include_directories(SYSTEM ${KRB5_INCLUDE_DIRS})
include_directories(SYSTEM ${FLEX_INCLUDE_DIRS})
include_directories(SYSTEM ${NCURSES_INCLUDE_DIR})
include_directories(SYSTEM ${Readline_INCLUDE_DIR})
include_directories(AFTER ${NEBULA_HOME}/src)
include_directories(AFTER src/common)
include_directories(AFTER src/interface)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/interface)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/parser)

link_directories(
    ${NEBULA_BZIP2_ROOT}/lib
    ${NEBULA_DOUBLE_CONVERSION_ROOT}/lib
    ${NEBULA_FATAL_ROOT}/lib
    ${NEBULA_FBTHRIFT_ROOT}/lib
    ${NEBULA_FOLLY_ROOT}/lib
    ${NEBULA_GFLAGS_ROOT}/lib
    ${NEBULA_GLOG_ROOT}/lib
    ${NEBULA_GOOGLETEST_ROOT}/lib
    ${NEBULA_JEMALLOC_ROOT}/lib
    ${NEBULA_LIBEVENT_ROOT}/lib
    ${NEBULA_MSTCH_ROOT}/lib
    ${NEBULA_PROXYGEN_ROOT}/lib
    ${NEBULA_ROCKSDB_ROOT}/lib
    ${NEBULA_SNAPPY_ROOT}/lib
    ${NEBULA_WANGLE_ROOT}/lib
    ${NEBULA_ZLIB_ROOT}/lib
    ${NEBULA_ZSTD_ROOT}/lib
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

set(ROCKSDB_LIBRARIES ${Rocksdb_LIBRARY})

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

add_subdirectory(src)
add_subdirectory(etc)
add_subdirectory(scripts)
add_subdirectory(share)

add_custom_target(
    clean-build
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS clean-interface clean-pch
)

if (NOT SKIP_JAVA_CLIENT)
    set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch clean-java-client)
else()
    set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch )
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
