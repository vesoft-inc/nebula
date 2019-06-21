set(storage_test_deps
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
)


add_library(
    adHocSchema_obj OBJECT
    AdHocSchemaManager.cpp
)
nebula_add_base_obj(adHocSchema_obj)


add_executable(
    add_vertices_test
    AddVerticesTest.cpp
    ${storage_test_deps}
)
nebula_link_libraries(
    add_vertices_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(add_vertices_test)


add_executable(
    storage_service_handler_test
    StorageServiceHandlerTest.cpp
    ${storage_test_deps}
)
nebula_link_libraries(
    storage_service_handler_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(storage_service_handler_test)


add_executable(
    add_edges_test
    AddEdgesTest.cpp
    ${storage_test_deps}
)
nebula_link_libraries(
    add_edges_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(add_edges_test)


add_executable(
    query_bound_test
    QueryBoundTest.cpp
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${storage_test_deps}
)
nebula_link_libraries(
    query_bound_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(query_bound_test)


add_executable(
    vertex_props_test
    QueryVertexPropsTest.cpp
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${storage_test_deps}
)
nebula_link_libraries(
    vertex_props_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(vertex_props_test)


add_executable(
    edge_props_test
    QueryEdgePropsTest.cpp
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${storage_test_deps}
)
nebula_link_libraries(
    edge_props_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(edge_props_test)


add_executable(
    query_stats_test
    QueryStatsTest.cpp
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${storage_test_deps}
)
nebula_link_libraries(
    query_stats_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(query_stats_test)


add_executable(
    storage_client_test
    StorageClientTest.cpp
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:meta_service_handler>
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${storage_test_deps}
)
nebula_link_libraries(
    storage_client_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(storage_client_test)


add_executable(
    storage_http_status_test
    StorageHttpStatusHandlerTest.cpp
    $<TARGET_OBJECTS:storage_http_handler>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    $<TARGET_OBJECTS:meta_service_handler>
    ${storage_test_deps}
)
nebula_link_libraries(
    storage_http_status_test
    proxygenhttpserver
    proxygenlib
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(storage_http_status_test)

add_executable(
    storage_http_download_test
    StorageHttpDownloadHandlerTest.cpp
    $<TARGET_OBJECTS:storage_http_handler>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    $<TARGET_OBJECTS:meta_service_handler>
    ${storage_test_deps}
)
nebula_link_libraries(
    storage_http_download_test
    proxygenhttpserver
    proxygenlib
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(storage_http_download_test)

add_executable(
    compaction_test
    CompactionTest.cpp
    $<TARGET_OBJECTS:adHocSchema_obj>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:process_obj>
)

nebula_link_libraries(
    compaction_test
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(compaction_test)

add_executable(
    query_bound_bm
    QueryBoundBenchmark.cpp
    $<TARGET_OBJECTS:adHocSchema_obj>
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
)
nebula_link_libraries(
    query_bound_bm
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    follybenchmark
    wangle
    boost_regex
)

