
add_executable(
    nebula-graphd
    GraphDaemon.cpp
    $<TARGET_OBJECTS:graph_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:process_obj>
)

target_link_libraries(
    nebula-graphd
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    glog
    gflags
    event
    ${COMPRESSION_LIBRARIES}
    resolv
    double-conversion
    dl
    -pthread
)

install(TARGETS nebula-graphd DESTINATION bin)

add_executable(
    storaged
    StorageDaemon.cpp
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
)
target_link_libraries(
    storaged
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    glog
    gflags
    event
    ${COMPRESSION_LIBRARIES}
    resolv
    double-conversion
    dl
    jemalloc
    -pthread
)


