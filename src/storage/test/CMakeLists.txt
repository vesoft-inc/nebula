add_executable(
    add_vertices_test
    AddVerticesTest.cpp
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
)
target_link_libraries(
    add_vertices_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    gtest
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

add_test(NAME add_vertices_test COMMAND add_vertices_test)


