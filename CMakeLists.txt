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

#   NEBULA_THIRDPARTY_ROOT          -- Specify the root directory for third-party
#
#   SKIP_JAVA_CLIENT               -- Skip building the java client
#   ENABLE_JEMALLOC                -- Link jemalloc into all executables
#   ENABLE_NATIVE                  -- Build native client
#   ENABLE_TESTING                 -- Build unit test
#
cmake_minimum_required(VERSION 3.0.0)

project("Nebula Graph" C CXX)

set(CMAKE_SKIP_RPATH TRUE)

option(SKIP_JAVA_CLIENT "Whether to skip building the java client" ON)
option(ENABLE_JEMALLOC "Whether to link jemalloc to all executables" ON)
option(ENABLE_NATIVE "Whether to build native client" OFF)
option(ENABLE_CCACHE "Whether to use ccache to speed up compiling" ON)
option(ENABLE_ASAN "Whether to turn AddressSanitizer ON or OFF" OFF)
option(ENABLE_TESTING "Whether to turn unit test ON or OFF" ON)

message(STATUS "ASAN: ${ENABLE_ASAN}")
message(STATUS "ENABLE_TESTING: ${ENABLE_TESTING}")


if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

if (ENABLE_TESTING)
    enable_testing()
endif()

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

if (NEBULA_BUILD_VERSION)
    add_definitions(-DNEBULA_BUILD_VERSION=${NEBULA_BUILD_VERSION})
endif()

if (${CMAKE_INSTALL_PREFIX} STREQUAL "/usr/local")
    set(CMAKE_INSTALL_PREFIX "/usr/local/nebula")
endif()
message(STATUS "CMAKE_INSTALL_PREFIX: " ${CMAKE_INSTALL_PREFIX})

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

# When NEBULA_THIRDPARTY_ROOT is null, set default value as /opt/nebula/third-party
if("${NEBULA_THIRDPARTY_ROOT}" STREQUAL "")
    SET(NEBULA_THIRDPARTY_ROOT "/opt/nebula/third-party")
endif()

# third-party
if(NOT ${NEBULA_THIRDPARTY_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_THIRDPARTY_ROOT: " ${NEBULA_THIRDPARTY_ROOT})
    # bzip2
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/bzip2/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/bzip2/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/bzip2/bin)

    # double-conversion
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/double-conversion/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/double-conversion/lib)

    # fatal
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/fatal/include)

    # fbthrift
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/fbthrift/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/fbthrift/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/fbthrift/bin)

    # folly
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/folly/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/folly/lib)

    # gflags
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/gflags/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/gflags/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/gflags/bin)

    # glog
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/glog/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/glog/lib)

    # googletest
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/googletest/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/googletest/lib)

    # jemalloc
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/jemalloc/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/jemalloc/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/jemalloc/bin)

    # libevent
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/libevent/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/libevent/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/libevent/bin)

    # mstch
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/mstch/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/mstch/lib)

    # proxygen
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/proxygen/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/proxygen/lib)

    # rocksdb
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/rocksdb/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/rocksdb/lib)
    set(ROCKSDB_LIBRARIES ${NEBULA_ROCKSDB_ROOT}/lib/librocksdb.a)

    # snappy
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/snappy/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/snappy/lib)

    # wangle
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/wangle/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/wangle/lib)

    # zlib
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/zlib/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/zlib/lib)

    # zstd
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/zstd/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/zstd/lib)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/zstd/bin)

endif()

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

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # This requries GCC 5.1+
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1)
        add_compile_options(-Wsuggest-override)
    endif()
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  add_compile_options(-Wno-overloaded-virtual)
  add_compile_options(-Wno-self-assign-overloaded)
  add_compile_options(-Wno-self-move)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -latomic")
endif()


if(ENABLE_ASAN)
    add_definitions(-DBUILT_WITH_SANITIZER)
    add_compile_options(-fsanitize=address)
    add_compile_options(-g)
    add_compile_options(-fno-omit-frame-pointer)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

macro(nebula_add_executable)
    cmake_parse_arguments(
        nebula_exec                 # prefix
        ""                          # <options>
        "NAME"                      # <one_value_args>
        "SOURCES;OBJECTS;LIBRARIES" # <multi_value_args>
        ${ARGN}
    )
    add_executable(
        ${nebula_exec_NAME}
        ${nebula_exec_SOURCES}
        ${nebula_exec_OBJECTS}
    )
    nebula_link_libraries(
        ${nebula_exec_NAME}
        ${nebula_exec_LIBRARIES}
    )
endmacro()

macro(nebula_add_test)
    cmake_parse_arguments(
        nebula_test                 # prefix
        "DISABLED"                  # <options>
        "NAME"                      # <one_value_args>
        "SOURCES;OBJECTS;LIBRARIES" # <multi_value_args>
        ${ARGN}
    )

    nebula_add_executable(
        NAME ${nebula_test_NAME}
        SOURCES ${nebula_test_SOURCES}
        OBJECTS ${nebula_test_OBJECTS}
        LIBRARIES ${nebula_test_LIBRARIES}
    )

    if (NOT ${nebula_test_DISABLED})
        string(REGEX REPLACE "${CMAKE_SOURCE_DIR}/src/(.*)/test" "\\1" test_group ${CMAKE_CURRENT_SOURCE_DIR})
        add_test(NAME ${nebula_test_NAME} COMMAND ${nebula_test_NAME})
        set_tests_properties(${nebula_test_NAME} PROPERTIES LABELS ${test_group})
        # e.g. cmake -DNEBULA_ASAN_PRELOAD=/path/to/libasan.so
        # or,  cmake -DNEBULA_ASAN_PRELOAD=`/path/to/gcc --print-file-name=libasan.so`
        if (NEBULA_ASAN_PRELOAD)
            set_property(
                TEST ${nebula_test_NAME}
                PROPERTY ENVIRONMENT LD_PRELOAD=${NEBULA_ASAN_PRELOAD}
            )
        endif()
    endif()
endmacro()

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
        common_thrift_headers
        graph_thrift_headers
        storage_thrift_headers
        meta_thrift_headers
        raftex_thrift_headers
        hbase_thrift_headers
        parser_target
    )
endmacro()

include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/bzip2/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/double-conversion/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/fatal/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/fbthrift/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/folly/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/gflags/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/glog/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/googletest/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/jemalloc/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/libevent/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/mstch/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/proxygen/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/rocksdb/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/snappy/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/wangle/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/zlib/include)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/zstd/include)
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
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/kvstore/plugins)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/kvstore/plugins/hbase)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/parser)

link_directories(
    ${NEBULA_THIRDPARTY_ROOT}/bzip2/lib
    ${NEBULA_THIRDPARTY_ROOT}/double-conversion/lib
    ${NEBULA_THIRDPARTY_ROOT}/fatal/lib
    ${NEBULA_THIRDPARTY_ROOT}/fbthrift/lib
    ${NEBULA_THIRDPARTY_ROOT}/folly/lib
    ${NEBULA_THIRDPARTY_ROOT}/gflags/lib
    ${NEBULA_THIRDPARTY_ROOT}/glog/lib
    ${NEBULA_THIRDPARTY_ROOT}/googletest/lib
    ${NEBULA_THIRDPARTY_ROOT}/jemalloc/lib
    ${NEBULA_THIRDPARTY_ROOT}/libevent/lib
    ${NEBULA_THIRDPARTY_ROOT}/mstch/lib
    ${NEBULA_THIRDPARTY_ROOT}/proxygen/lib
    ${NEBULA_THIRDPARTY_ROOT}/rocksdb/lib
    ${NEBULA_THIRDPARTY_ROOT}/snappy/lib
    ${NEBULA_THIRDPARTY_ROOT}/wangle/lib
    ${NEBULA_THIRDPARTY_ROOT}/zlib/lib
    ${NEBULA_THIRDPARTY_ROOT}/zstd/lib
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

function(nebula_add_subdirectory dir_name)
    if ((NOT ENABLE_TESTING) AND (${dir_name} STREQUAL test))
        return()
    endif()
    add_subdirectory(${dir_name})
endfunction()

if (NOT SKIP_JAVA_CLIENT)
    set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch clean-hbase clean-java-client)
else()
    set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch clean-hbase)
endif()

add_subdirectory(src)
add_subdirectory(etc)
add_subdirectory(scripts)
add_subdirectory(share)

add_custom_target(
    clean-build
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS clean-interface clean-pch clean-hbase
)

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
ENDIF()

# package nebula to a deb/rpm file
set(CPACK_PACKAGE_DESCRIPTION "nebula-graph")
set(CPACK_PACKAGE_CONTACT "nebula-graph")
set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0 + Common Clause 1.0")
set(CPACK_PACKAGE_NAME nebula-graph)
set(CPACK_SET_DESTDIR TRUE)
set(CPACK_PACKAGE_RELOCATABLE FALSE)
set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)

set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
        %define __os_install_post %{nil}")
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
set(CPACK_RPM_PACKAGE_URL "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/local)

include(CPack)
