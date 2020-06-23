set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${NEBULA_THIRDPARTY_ROOT}/lib")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L ${NEBULA_THIRDPARTY_ROOT}/lib64")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")

message(STATUS "CMAKE_SHARED_LINKER_FLAGS: " ${CMAKE_SHARED_LINKER_FLAGS})

nebula_add_library(nebula_storage_client SHARED
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:gflags_man_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:parser_obj>
    NebulaStorageClient.cpp
    NebulaStorageClientBase.cpp
    NebulaStorageClientImpl.cpp
    NebulaStorageClientQueryImpl.cpp)

target_link_libraries(
    nebula_storage_client
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_context
    boost_system
    glog
    gflags
    snappy
    unwind
    bz2
    lz4
    lzma
    z
    zstd
    event
    s2
    double-conversion
    pthread
    dl
    ${OPENSSL_SSL_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    -static-libgcc
    -static-libstdc++
)

