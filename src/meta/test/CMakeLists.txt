set(meta_test_deps
    $<TARGET_OBJECTS:mock_obj>
    $<TARGET_OBJECTS:meta_service_handler>
    $<TARGET_OBJECTS:general_storage_service_handler>
    $<TARGET_OBJECTS:storage_admin_service_handler>
    $<TARGET_OBJECTS:graph_storage_service_handler>
    $<TARGET_OBJECTS:storage_common_obj>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:keyutils_obj>
    $<TARGET_OBJECTS:common_meta_client_obj>
    $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:common_ws_common_obj>
    $<TARGET_OBJECTS:common_http_client_obj>
    $<TARGET_OBJECTS:common_raftex_thrift_obj>
    $<TARGET_OBJECTS:common_storage_thrift_obj>
    $<TARGET_OBJECTS:common_meta_thrift_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_process_obj>
    $<TARGET_OBJECTS:common_meta_obj>
    $<TARGET_OBJECTS:common_stats_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_conf_obj>
    $<TARGET_OBJECTS:codec_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_expression_obj>
    $<TARGET_OBJECTS:common_function_manager_obj>
    $<TARGET_OBJECTS:common_time_function_obj>
)

nebula_add_test(
    NAME
        meta_utils_test
    SOURCES
        MetaServiceUtilsTest.cpp
        ../MetaServiceUtils.cpp
    OBJECTS
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        gtest
)

nebula_add_test(
    NAME
        processor_test
    SOURCES
        ProcessorTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        hb_processor_test
    SOURCES
        HBProcessorTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        meta_client_test
    SOURCES
        MetaClientTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        active_hosts_man_test
    SOURCES
        ActiveHostsManTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

#nebula_add_test(
#    NAME
#        meta_http_download_test
#    SOURCES
#        MetaHttpDownloadHandlerTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:storage_http_handler>
#        $<TARGET_OBJECTS:meta_http_handler>
#        $<TARGET_OBJECTS:ws_obj>
#        $<TARGET_OBJECTS:ws_common_obj>
#        $<TARGET_OBJECTS:hdfs_helper_obj>
#        $<TARGET_OBJECTS:http_client_obj>
#        ${meta_test_deps}
#    LIBRARIES
#        proxygenhttpserver
#        proxygenlib
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)
#
#nebula_add_test(
#    NAME
#        meta_http_ingest_test
#    SOURCES
#        MetaHttpIngestHandlerTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:storage_http_handler>
#        $<TARGET_OBJECTS:meta_http_handler>
#        $<TARGET_OBJECTS:ws_obj>
#        $<TARGET_OBJECTS:ws_common_obj>
#        $<TARGET_OBJECTS:hdfs_helper_obj>
#        $<TARGET_OBJECTS:http_client_obj>
#        ${meta_test_deps}
#    LIBRARIES
#        proxygenhttpserver
#        proxygenlib
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)

nebula_add_test(
    NAME
        balancer_test
    SOURCES
        BalancerTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


#nebula_add_test(
#    NAME
#        balance_integration_test
#    SOURCES
#        BalanceIntegrationTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:index_obj>
#        $<TARGET_OBJECTS:storage_service_handler>
#        $<TARGET_OBJECTS:storage_client>
#        $<TARGET_OBJECTS:dataman_obj>
#        $<TARGET_OBJECTS:filter_obj>
#        $<TARGET_OBJECTS:adHocSchema_obj>
#        $<TARGET_OBJECTS:adHocIndex_obj>
#        ${meta_test_deps}
#    LIBRARIES
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)


nebula_add_test(
    NAME
        admin_client_test
    SOURCES
        AdminClientTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        authentication_test
    SOURCES
        AuthProcessorTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        cluster_id_man_test
    SOURCES
        ClusterIdManTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

#nebula_add_test(
#    NAME
#        meta_http_replace_test
#    SOURCES
#        MetaHttpReplaceHandlerTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:storage_http_handler>
#        $<TARGET_OBJECTS:meta_http_handler>
#        $<TARGET_OBJECTS:ws_obj>
#        $<TARGET_OBJECTS:ws_common_obj>
#        $<TARGET_OBJECTS:hdfs_helper_obj>
#        $<TARGET_OBJECTS:http_client_obj>
#        ${meta_test_deps}
#    LIBRARIES
#        proxygenhttpserver
#        proxygenlib
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)

nebula_add_test(
    NAME
        job_manager_test
    SOURCES
        JobManagerTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


