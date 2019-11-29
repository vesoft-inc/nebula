set(storage_test_deps
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:time_obj>
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
    $<TARGET_OBJECTS:gflags_man_obj>
    $<TARGET_OBJECTS:stats_obj>
)


nebula_add_library(
    adHocSchema_obj OBJECT
    AdHocSchemaManager.cpp
)

nebula_add_test(
    NAME add_vertices_test
    SOURCES AddVerticesTest.cpp
    OBJECTS ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME delete_vertex_test
    SOURCES DeleteVertexTest.cpp
    OBJECTS ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME delete_edges_test
    SOURCES DeleteEdgesTest.cpp
    OBJECTS ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME query_edgekeys_test
    SOURCES QueryEdgeKeysTest.cpp
    OBJECTS ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME storage_service_handler_test
    SOURCES StorageServiceHandlerTest.cpp
    OBJECTS ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME add_edges_test
    SOURCES AddEdgesTest.cpp
    OBJECTS ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME query_bound_test
    SOURCES QueryBoundTest.cpp
    OBJECTS $<TARGET_OBJECTS:adHocSchema_obj> ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME vertex_props_test
    SOURCES QueryVertexPropsTest.cpp
    OBJECTS $<TARGET_OBJECTS:adHocSchema_obj> ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME edge_props_test
    SOURCES QueryEdgePropsTest.cpp
    OBJECTS $<TARGET_OBJECTS:adHocSchema_obj> ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME query_stats_test
    SOURCES QueryStatsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:adHocSchema_obj>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:process_obj>
        ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME
        storage_client_test
    SOURCES
        StorageClientTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:adHocSchema_obj>
        ${storage_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        storage_http_status_test
    SOURCES
        StorageHttpStatusHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:storage_http_handler>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:adHocSchema_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        ${storage_test_deps}
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        storage_http_admin_test
    SOURCES
        StorageHttpAdminHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:storage_http_handler>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:adHocSchema_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        ${storage_test_deps}
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        storage_http_download_test
    SOURCES
        StorageHttpDownloadHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:storage_http_handler>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:adHocSchema_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        ${storage_test_deps}
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        storage_http_ingest_test
    SOURCES
        StorageHttpIngestHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:storage_http_handler>
        $<TARGET_OBJECTS:http_client_obj>
        $<TARGET_OBJECTS:ws_obj>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:adHocSchema_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        ${storage_test_deps}
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        compaction_test
    SOURCES
        CompactionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:adHocSchema_obj>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:raftex_thrift_obj>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:filter_obj>
        $<TARGET_OBJECTS:storage_client>
        $<TARGET_OBJECTS:meta_client>
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
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
        $<TARGET_OBJECTS:gflags_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_executable(
    NAME
        query_bound_bm
    SOURCES
        QueryBoundBenchmark.cpp
    OBJECTS
        ${storage_test_deps}
        $<TARGET_OBJECTS:adHocSchema_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        follybenchmark
        wangle
        boost_regex
)


nebula_add_test(
    NAME update_vertex_test
    SOURCES UpdateVertexTest.cpp
    OBJECTS $<TARGET_OBJECTS:adHocSchema_obj> ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME update_edge_test
    SOURCES UpdateEdgeTest.cpp
    OBJECTS $<TARGET_OBJECTS:adHocSchema_obj> ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)

nebula_add_test(
    NAME vertex_cache_test
    SOURCES VertexCacheTest.cpp
    OBJECTS $<TARGET_OBJECTS:adHocSchema_obj> ${storage_test_deps}
    LIBRARIES ${ROCKSDB_LIBRARIES} ${THRIFT_LIBRARIES} wangle gtest
)

