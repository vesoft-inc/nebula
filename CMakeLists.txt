# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# CMakeLists.txt file contains a set of directives and instructions describing the project's source files and targets (executable, library, or both).
#
# The building can be controlled by defining the following variables on the
# <cmake> command line.
#
#   CMAKE_C_COMPILER                -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER              -- Specify the compiler for C++ language
#
#   NEBULA_THIRDPARTY_ROOT          -- Specify the root directory for third-party
#   NEBULA_OTHER_ROOT               -- Specify the root directory for user building
#                                   -- Split with ":", exp: DIR:DIR
#   NEBULA_CLANG_USE_GCC_TOOLCHAIN  -- Specify the root of GCC's toolchain, used by Clang
#   NEBULA_USE_LINKER               -- Set linker to use, options are: bfd, gold
#
#   ENABLE_JEMALLOC                 -- Link jemalloc into all executables
#   ENABLE_NATIVE                   -- Build native client
#   ENABLE_TESTING                  -- Build unit test
#   ENABLE_PACK_ONE                 -- Package to one or multi packages
#
# CMake version check
cmake_minimum_required(VERSION 3.5.0)

# Set the project name
project("Nebula Graph" C CXX)

# Set the project home dir
set(NEBULA_HOME ${CMAKE_CURRENT_SOURCE_DIR})

if(${CMAKE_SYSTEM_NAME} MATCHES "(Darwin|FreeBSD|Windows)")
	MESSAGE(FATAL_ERROR "Only Linux is supported")
endif ()

# To include customized FindXXX.cmake modules
set(CMAKE_MODULE_PATH "${NEBULA_HOME}/cmake" ${CMAKE_MODULE_PATH})
include(LinkerConfig)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -no-pie")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic")

set(CMAKE_SKIP_RPATH TRUE)

option(ENABLE_JEMALLOC "Whether to link jemalloc to all executables" ON)
option(ENABLE_NATIVE "Whether to build native client" OFF)
option(ENABLE_CCACHE "Whether to use ccache to speed up compiling" ON)
option(ENABLE_ASAN "Whether to turn AddressSanitizer ON or OFF" OFF)
option(ENABLE_TESTING "Whether to turn unit test ON or OFF" ON)
option(ENABLE_COVERAGE "Whether to turn unit test coverage ON or OFF" OFF)
option(ENABLE_UBSAN "Whether to turn Undefined Behavior Sanitizer ON or OFF" OFF)
option(ENABLE_FUZZ_TEST "Whether to turn Fuzz Test ON or OFF" OFF)
option(ENABLE_WERROR "Whether to error on warnings" ON)
option(ENABLE_STRICT_ALIASING "Whether to turn strict-aliasing ON or OFF" OFF)
option(ENABLE_TSAN "Whether to turn Thread Sanitizer ON or OFF" OFF)
option(ENABLE_STATIC_ASAN "Whether directs the GCC driver to link libasan statically" OFF)
option(ENABLE_STATIC_UBSAN "Whether directs the GCC driver to link libubsan statically" OFF)
option(ENABLE_PACK_ONE "Whether to package into one" ON)
option(ENABLE_PCH "Whether to enable use of precompiled header" OFF)
option(ENABLE_FRAME_POINTER "Whether to build with -fno-omit-frame-pointer" ON)

message(STATUS "ENABLE_ASAN: ${ENABLE_ASAN}")
message(STATUS "ENABLE_TESTING: ${ENABLE_TESTING}")
message(STATUS "ENABLE_UBSAN: ${ENABLE_UBSAN}")
message(STATUS "ENABLE_FUZZ_TEST: ${ENABLE_FUZZ_TEST}")
message(STATUS "ENABLE_TSAN: ${ENABLE_TSAN}")
message(STATUS "ENABLE_STATIC_ASAN : ${ENABLE_STATIC_ASAN}")
message(STATUS "ENABLE_STATIC_UBSAN : ${ENABLE_STATIC_UBSAN}")
message(STATUS "ENABLE_WERROR: ${ENABLE_WERROR}")
message(STATUS "ENABLE_STRICT_ALIASING: ${ENABLE_STRICT_ALIASING}")
message(STATUS "ENABLE_PCH: ${ENABLE_PCH}")
message(STATUS "ENABLE_FRAME_POINTER: ${ENABLE_FRAME_POINTER}")

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

set(CMAKE_CXX_STANDARD 14)  # specify the C++ standard
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# coverage
if (ENABLE_TESTING AND ENABLE_COVERAGE)
    add_compile_options(--coverage)
    add_compile_options(-g)
    add_compile_options(-O0)

    set(COVERAGES --coverage)
endif()

# To detect if ccache is available
find_program(ccache_program_found "ccache")
if (ENABLE_CCACHE AND ccache_program_found)
    message(STATUS "ENABLE_CCACHE: ON")
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

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--compress-debug-sections=zlib")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        add_compile_options(-fno-limit-debug-info)
    endif()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    if (NOT ${NEBULA_USE_LINKER} STREQUAL "gold")
        # gold linker is buggy for `--gc-sections', disabled for now
        add_compile_options(-ffunction-sections)
        add_compile_options(-fdata-sections)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")
    endif()
endif()

message(STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE} "
               "(Options are: Debug, Release, RelWithDebInfo, MinSizeRel)")

# By default, all dynamic and static libraries will be placed at ${CMAKE_BINARY_DIR}/lib,
# while all executables at ${CMAKE_BINARY_DIR}/bin.
# But for the sake of cleanliness, all executables ending with `_test' will be placed
# at ${CMAKE_BINARY_DIR}/bin/test, while those ending with `_bm' at ${CMAKE_BINARY_DIR}/bin/bench.
# Please see `nebula_add_executable' for this rule.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

add_definitions(-DNEBULA_HOME=${NEBULA_HOME})

# Let s2 use glog
add_definitions(-DS2_USE_GLOG)

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

# The precedence to decide NEBULA_THIRDPARTY_ROOT is:
#   1. The path defined with CMake argument, i.e -DNEBULA_THIRDPARTY_ROOT=path
#   2. ${CMAKE_BINARY_DIR}/third-party/install, if exists
#   3. The path specified with environment variable NEBULA_THIRDPARTY_ROOT=path
#   4. /opt/vesoft/third-party, if exists
#   5. At last, one copy will be downloaded and installed to ${CMAKE_BINARY_DIR}/third-party/install
if("${NEBULA_THIRDPARTY_ROOT}" STREQUAL "")
    if(EXISTS ${CMAKE_BINARY_DIR}/third-party/install)
        SET(NEBULA_THIRDPARTY_ROOT ${CMAKE_BINARY_DIR}/third-party/install)
    elseif(NOT $ENV{NEBULA_THIRDPARTY_ROOT} STREQUAL "")
        SET(NEBULA_THIRDPARTY_ROOT $ENV{NEBULA_THIRDPARTY_ROOT})
    elseif(EXISTS /opt/vesoft/third-party)
        SET(NEBULA_THIRDPARTY_ROOT "/opt/vesoft/third-party")
    else()
        include(InstallThirdParty)
    endif()
endif()

# third-party
if(NOT ${NEBULA_THIRDPARTY_ROOT} STREQUAL "")
    message(STATUS "Specified NEBULA_THIRDPARTY_ROOT: " ${NEBULA_THIRDPARTY_ROOT})
    list(INSERT CMAKE_INCLUDE_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/include)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/lib)
    list(INSERT CMAKE_LIBRARY_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/lib64)
    list(INSERT CMAKE_PROGRAM_PATH 0 ${NEBULA_THIRDPARTY_ROOT}/bin)
    include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/include)
    link_directories(
        ${NEBULA_THIRDPARTY_ROOT}/lib
        ${NEBULA_THIRDPARTY_ROOT}/lib64
    )
endif()

if(NOT ${NEBULA_OTHER_ROOT} STREQUAL "")
    string(REPLACE ":" ";" DIR_LIST ${NEBULA_OTHER_ROOT})
    list(LENGTH DIR_LIST len)
    foreach(DIR IN LISTS DIR_LIST )
        list(INSERT CMAKE_INCLUDE_PATH 0 ${DIR}/include)
        list(INSERT CMAKE_LIBRARY_PATH 0 ${DIR}/lib)
        list(INSERT CMAKE_PROGRAM_PATH 0 ${DIR}/bin)
        include_directories(SYSTEM ${DIR}/include)
        link_directories(${DIR}/lib)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${DIR}/lib")
    endforeach()
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
if(ENABLE_JEMALLOC)
    find_package(Jemalloc REQUIRED)
endif()
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
include(MakeBisonRelocatable)
find_package(FLEX REQUIRED)
find_package(Readline REQUIRED)
find_package(NCURSES REQUIRED)
find_package(LibLZMA MODULE)
if(ENABLE_PCH AND NOT ENABLE_ASAN AND NOT ENABLE_NATIVE)
    find_package(PCHSupport)
    add_compile_options(-Winvalid-pch)
else()
    # Remove if existing
    file(REMOVE ${CMAKE_SOURCE_DIR}/src/common/base/Base.h.gch)
endif()

add_compile_options(-Wall)
add_compile_options(-Wextra)
add_compile_options(-Wpedantic)
add_compile_options(-Wunused-parameter)
add_compile_options(-Wshadow)
add_compile_options(-Wnon-virtual-dtor)
add_compile_options(-Woverloaded-virtual)
add_compile_options(-Wignored-qualifiers)
if(ENABLE_WERROR)
    add_compile_options(-Werror)
endif()

if(NOT ENABLE_STRICT_ALIASING)
    add_compile_options(-fno-strict-aliasing)
endif()

if(ENABLE_FRAME_POINTER)
    add_compile_options(-fno-omit-frame-pointer)
else()
    add_compile_options(-fomit-frame-pointer)
endif()

include(CheckCXXCompilerFlag)

# TODO(dutor) Move all compiler-config-related operations to a separate file
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # This requries GCC 5.1+
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1)
        add_compile_options(-Wsuggest-override)
    endif()
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    MESSAGE(STATUS "NEBULA_CLANG_USE_GCC_TOOLCHAIN: ${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
    # Here we need to specify the path of libstdc++ used by Clang
    if(NOT ${NEBULA_CLANG_USE_GCC_TOOLCHAIN} STREQUAL "")
        execute_process(
            COMMAND ${NEBULA_CLANG_USE_GCC_TOOLCHAIN}/bin/gcc -dumpmachine
            OUTPUT_VARIABLE gcc_target_triplet
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        add_compile_options("--gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --gcc-toolchain=${NEBULA_CLANG_USE_GCC_TOOLCHAIN}")
        if(NOT "${gcc_target_triplet}" STREQUAL "")
            add_compile_options(--target=${gcc_target_triplet})
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --target=${gcc_target_triplet}")
        endif()
    endif()
    add_compile_options(-Wno-self-assign-overloaded)
    add_compile_options(-Wno-self-move)
    add_compile_options(-Wno-format-pedantic)
    add_compile_options(-Wno-gnu-zero-variadic-macro-arguments)
    set(libatomic_link_flags "-Wl,-Bstatic -latomic -Wl,-Bdynamic")
    set(CMAKE_REQUIRED_FLAGS "${libatomic_link_flags}")
    check_cxx_compiler_flag("${libatomic_link_flags}" has_static_libatomic)
    if(NOT has_static_libatomic)
        set(libatomic_link_flags "-latomic")
    endif()
endif()

if(ENABLE_ASAN OR ENABLE_UBSAN)
    add_definitions(-DBUILT_WITH_SANITIZER)
endif()

if(ENABLE_ASAN)
    set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
    check_cxx_compiler_flag("-fsanitize=address" ENABLE_ASAN_OK)
    if (NOT ENABLE_ASAN_OK)
        MESSAGE(FATAL_ERROR "The compiler does not support address sanitizer")
    endif()
    if(ENABLE_STATIC_ASAN AND "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-static-libasan)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libasan")
    endif()
    add_compile_options(-fsanitize=address)
    add_compile_options(-g)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

if(ENABLE_TSAN)
    if (ENABLE_ASAN)
        MESSAGE(FATAL_ERROR "ENABLE_TSAN cannot be combined with ENABLE_ASAN")
    endif()
    set(CMAKE_REQUIRED_FLAGS "-fsanitize=thread")
    check_cxx_compiler_flag("-fsanitize=thread" ENABLE_TSAN_OK)
    if (NOT ENABLE_TSAN_OK)
        MESSAGE(FATAL_ERROR "The compiler does not support thread sanitizer")
    endif()
    set(ENV{TSAN_OPTIONS} "report_atomic_races=0")
    add_compile_options(-fsanitize=thread)
    add_compile_options(-g)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()

if(ENABLE_UBSAN)
    check_cxx_compiler_flag("-fsanitize=undefined -fno-sanitize=alignment" ENABLE_UBSAN_OK)
    if (NOT ENABLE_UBSAN_OK)
        MESSAGE(FATAL_ERROR "The compiler does not support Undefined Behavior Sanitizer")
    endif()
    if(ENABLE_STATIC_UBSAN AND "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-static-libubsan)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libubsan")
    endif()
    add_compile_options(-fsanitize=undefined -fno-sanitize=alignment)
    add_compile_options(-fno-sanitize-recover=all)  # Exit on failure
    # TODO(dutor) Remove the following line when RTTI-enabled RocksDB is ready
    add_compile_options(-fno-sanitize=vptr)
    if(NOT ENABLE_ASAN)
        add_compile_options(-g)
    endif()
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
endif()

if(ENABLE_FUZZ_TEST)
    check_cxx_compiler_flag("-fsanitize=fuzzer" ENABLE_FUZZ_OK)
    if (NOT ENABLE_FUZZ_OK)
        MESSAGE(FATAL_ERROR "The compiler does not support fuzz testing")
    endif()
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

    if(${nebula_exec_NAME} MATCHES "_test$")
        set_target_properties(
            ${nebula_exec_NAME}
            PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/test
        )
    elseif(${nebula_exec_NAME} MATCHES "_bm$")
        set_target_properties(
            ${nebula_exec_NAME}
            PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/bench
        )
    endif()
endmacro()

macro(nebula_add_test)
    cmake_parse_arguments(
        nebula_test                 # prefix
        "DISABLED;FUZZER"           # <options>
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

    if (${nebula_test_FUZZER})
        #Currently only Clang supports fuzz test
        if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
            set_target_properties(${nebula_test_NAME} PROPERTIES COMPILE_FLAGS "-g -fsanitize=fuzzer")
            set_target_properties(${nebula_test_NAME} PROPERTIES LINK_FLAGS "-fsanitize=fuzzer")
        endif()
    elseif (NOT ${nebula_test_DISABLED})
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

include_directories(AFTER ${NEBULA_HOME}/src)
include_directories(AFTER src/common)
include_directories(AFTER src/interface)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/interface)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/kvstore/plugins)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/kvstore/plugins/hbase)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/parser)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${NEBULA_THIRDPARTY_ROOT}/lib")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${NEBULA_THIRDPARTY_ROOT}/lib64")

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
set(COMPRESSION_LIBRARIES bz2 snappy zstd z lz4)
if (LIBLZMA_FOUND)
    include_directories(SYSTEM ${LIBLZMA_INCLUDE_DIRS})
    list(APPEND COMPRESSION_LIBRARIES ${LIBLZMA_LIBRARIES})
endif()

if (NOT ENABLE_JEMALLOC OR ENABLE_ASAN OR ENABLE_UBSAN)
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
        s2
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${KRB5_LIBRARIES}
        ${COMPRESSION_LIBRARIES}
        ${JEMALLOC_LIB}
        ${LIBUNWIND_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        proxygenhttpserver
        proxygenlib
        keyutils
        resolv
        dl
        ${GETTIME_LIB}
        ${libatomic_link_flags}
        -pthread
        ${COVERAGES}
    )
endmacro(nebula_link_libraries)

function(nebula_add_subdirectory dir_name)
    if ((NOT ENABLE_TESTING) AND (${dir_name} STREQUAL test))
        add_subdirectory(${dir_name} EXCLUDE_FROM_ALL)
        return()
    endif()
    add_subdirectory(${dir_name})
endfunction()

set(NEBULA_CLEAN_ALL_DEPS clean-interface clean-pch clean-hbase)

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

# package to rpm/deb
MESSAGE(STATUS "ENABLE_PACK_ONE: ${ENABLE_PACK_ONE}")
include(CPackage)
package(${ENABLE_PACK_ONE})
