set(STORAGE_TEST_LIBS
    kvstore_storage_utils_lib
    storage_service_handler_lib
    filter_lib
    storage_client_lib
    storage_thrift_lib
    kvstore_lib
    meta_client_lib
    time_lib
    meta_thrift_lib
    common_thrift_lib
    raftex_lib
    raftex_thrift_lib
    wal_lib
    dataman_lib
    schema_lib
    index_lib
    base_lib
    thrift_lib
    thread_lib
    time_lib
    fs_lib
    network_lib
    charset_lib
    gflags_man_lib
    stats_lib
    adHocSchema_lib
    adHocIndex_lib
)

set(STORAGE_HTTP_TEST_LIBS
    storage_http_handler_lib
    http_client_lib
    ws_lib
    ws_common_lib
    process_lib
)

nebula_add_all_library(
    adHocSchema
    AdHocSchemaManager.cpp
)

nebula_add_all_library(
    adHocIndex
    AdHocIndexManager.cpp
)

nebula_add_test(
    NAME
        add_vertices_test
    SOURCES
        AddVerticesTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        delete_vertices_test
    SOURCES
        DeleteVerticesTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        delete_edges_test
    SOURCES
        DeleteEdgesTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        storage_service_handler_test
    SOURCES
        StorageServiceHandlerTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        add_edges_test
    SOURCES
        AddEdgesTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        query_bound_test
    SOURCES
        QueryBoundTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        vertex_props_test
    SOURCES
        QueryVertexPropsTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        edge_props_test
    SOURCES
        QueryEdgePropsTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        query_stats_test
    SOURCES
        QueryStatsTest.cpp
    LIBRARIES
        http_client_lib
        process_lib
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        storage_client_test
    SOURCES
        StorageClientTest.cpp
    LIBRARIES
        meta_service_handler_lib
        ws_common_lib
        http_client_lib
        process_lib
        ${STORAGE_TEST_LIBS}
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
    LIBRARIES
        ${STORAGE_HTTP_TEST_LIBS}
        ${STORAGE_TEST_LIBS}
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
    LIBRARIES
        ${STORAGE_HTTP_TEST_LIBS}
        ${STORAGE_TEST_LIBS}
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
    LIBRARIES
        ${STORAGE_HTTP_TEST_LIBS}
        ${STORAGE_TEST_LIBS}
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
    LIBRARIES
        ${STORAGE_TEST_LIBS}
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
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        follybenchmark
        wangle
        boost_regex
)


nebula_add_test(
    NAME
        update_vertex_test
    SOURCES
        UpdateVertexTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        update_edge_test
    SOURCES
        UpdateEdgeTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        vertex_cache_test
    SOURCES
        VertexCacheTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        checkpoint_test
    SOURCES
        CheckpointTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        index_write_test
    SOURCES
        IndexTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        scan_edge_test
    SOURCES
        ScanEdgeTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        scan_vertex_test
    SOURCES
        ScanVertexTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_executable(
    NAME
        storage_index_write_bm
    SOURCES
        StorageIndexWriteBenchmark.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        follybenchmark
        boost_regex
        wangle
        gtest
)

nebula_add_test(    
    NAME
        index_scan_test    
    SOURCES
        IndexScanTest.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest 
)

nebula_add_executable(
    NAME
        storage_lookup_bm
    SOURCES
        StorageLookupBenchmark.cpp
    LIBRARIES
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        follybenchmark
        boost_regex
        wangle
        gtest
)

nebula_add_test(
    NAME
        kv_test
    SOURCES
        KVTest.cpp
    LIBRARIES
        meta_service_handler_lib
        adHocSchema_lib
        ws_common_lib
        http_client_lib
        process_lib
        ${STORAGE_TEST_LIBS}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

