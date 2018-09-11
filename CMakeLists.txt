cmake_minimum_required(VERSION 3.0.0)

project("vGraph" CXX)

set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_VERBOSE_MAKEFILE TRUE)

enable_testing()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++")

# Possible values are Debug, Release, RelWithDebInfo, MinSizeRel
set(CMAKE_BUILD_TYPE "RelWithDebInfo")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "_build")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "_build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

# Set the project home dir
set(VGRAPH_HOME ${CMAKE_CURRENT_SOURCE_DIR})

# To include customized FindXXX.cmake modules
set(CMAKE_MODULE_PATH "${VGRAPH_HOME}/cmake" ${CMAKE_MODULE_PATH})

find_package(Boost)
find_package(OpenSSL)
find_package(Krb5 REQUIRED gssapi)
find_package(PCHSupport)

add_compile_options(-Winvalid-pch)

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
    server
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
#add_subdirectory(storage)
add_subdirectory(server)
add_subdirectory(console)
add_subdirectory(consensus)

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

