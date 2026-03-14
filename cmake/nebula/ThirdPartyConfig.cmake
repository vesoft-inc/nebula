message(">>>> Configuring third party for '${PROJECT_NAME}' <<<<")

# Find all required packages from vcpkg
find_package(BISON REQUIRED)
find_package(BZip2 REQUIRED)
find_package(Boost REQUIRED COMPONENTS context system regex program_options filesystem)
find_package(CURL REQUIRED)
find_package(FBThrift CONFIG REQUIRED)
find_package(FLEX REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(Libevent REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(Snappy CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(double-conversion CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(liblzma CONFIG REQUIRED)
find_package(lz4 REQUIRED)
find_package(proxygen CONFIG REQUIRED)
find_package(unofficial-s2 CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)
find_package(wangle REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(robin_hood CONFIG REQUIRED)

# Configure GTest targets
add_library(gtest ALIAS GTest::gtest)
add_library(gtest_main ALIAS GTest::gtest_main)
add_library(boost_regex ALIAS Boost::regex)

# Configure Folly benchmark target
add_library(follybenchmark ALIAS Folly::follybenchmark)

# Configure CURL target alias - handle case where CURL::libcurl is itself an alias
if(TARGET CURL::libcurl)
    get_target_property(_curl_is_alias CURL::libcurl ALIASED_TARGET)
    if(_curl_is_alias)
        # CURL::libcurl is an alias, create alias to the real target
        add_library(curl ALIAS ${_curl_is_alias})
    else()
        # CURL::libcurl is a real target
        add_library(curl ALIAS CURL::libcurl)
    endif()
endif()


execute_process(
    COMMAND ldd --version
    COMMAND head -1
    COMMAND cut -d ")" -f 2
    COMMAND cut -d " " -f 2
    OUTPUT_VARIABLE GLIBC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
print_config(GLIBC_VERSION)

if (GLIBC_VERSION VERSION_LESS "2.17")
    set(GETTIME_LIB rt)
else()
    set(GETTIME_LIB)
endif()

message("")

# All compression libraries


message(">>>> Configuring third party for '${PROJECT_NAME}' done <<<<")



if (GLIBC_VERSION VERSION_LESS "2.17")
    set(GETTIME_LIB rt)
else()
    set(GETTIME_LIB)
endif()

# Breakpad configuration
if (NOT ${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "x86_64")
    set(ENABLE_BREAKPAD OFF)
endif()

if (ENABLE_BREAKPAD)
    if (NOT ${CMAKE_BUILD_TYPE} STREQUAL "Debug" AND NOT ${CMAKE_BUILD_TYPE} STREQUAL "RelWithDebInfo")
      MESSAGE(FATAL_ERROR "Breakpad need debug info.")
    endif()
    find_package(unofficial-breakpad CONFIG REQUIRED)
    set(Breakpad_LIBRARY unofficial-breakpad::breakpad::libbreakpad)
    add_compile_options(-DENABLE_BREAKPAD=1)
else()
    set(Breakpad_LIBRARY)
endif()

message("")

if(ENABLE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED IMPORTED_TARGET jemalloc)
    add_definitions(-DENABLE_JEMALLOC)
endif()

# All thrift libraries
set(THRIFT_LIBRARIES
    FBThrift::thriftcpp2
    FBThrift::async
    FBThrift::thriftprotocol
    FBThrift::transport
    FBThrift::concurrency
    FBThrift::thriftfrozen2
    FBThrift::thrift-core
    FBThrift::rpcmetadata
    FBThrift::thriftmetadata
    wangle::wangle
)

set(PROXYGEN_LIBRARIES
    proxygen::proxygenhttpserver
    proxygen::proxygen
    wangle::wangle
)

set(ROCKSDB_LIBRARIES RocksDB::rocksdb)

# All compression libraries
set(COMPRESSION_LIBRARIES BZip2::BZip2 zstd::libzstd_static ZLIB::ZLIB lz4::lz4)
if (LIBLZMA_FOUND)
    list(APPEND COMPRESSION_LIBRARIES liblzma::liblzma)
endif()

if (NOT ENABLE_JEMALLOC OR ENABLE_ASAN OR ENABLE_UBSAN)
    set(JEMALLOC_LIB )
else()
    set(JEMALLOC_LIB PkgConfig::JEMALLOC)
endif()


message(">>>> Configuring third party for '${PROJECT_NAME}' done <<<<")
