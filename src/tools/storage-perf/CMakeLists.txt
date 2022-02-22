set(perf_test_deps
    meta_service_handler
    meta_version_man_obj
    meta_v2_thrift_obj
    meta_data_upgrade_obj
    storage_admin_service_handler
    graph_storage_service_handler
    storage_transaction_executor
    storage_common_obj
    kvstore_obj
    raftex_obj
    wal_obj
    disk_man_obj
    codec_obj
    keyutils_obj
    meta_keyutils_obj
    log_str_list_iterator_obj
    internal_storage_client_obj
    storage_client_obj
    storage_client_base_obj
    ws_common_obj
    http_client_obj
    meta_client_obj
    file_based_cluster_id_man_obj
    storage_thrift_obj
    raftex_thrift_obj
    meta_thrift_obj
    common_thrift_obj
    thrift_obj
    meta_obj
    thread_obj
    time_obj
    fs_obj
    network_obj
    charset_obj
    stats_obj
    storage_stats_obj
    meta_client_stats_obj
    storage_client_stats_obj
    kv_stats_obj
    process_obj
    conf_obj
    datatypes_obj
    base_obj
    expression_obj
    function_manager_obj
    wkt_wkb_io_obj
    agg_function_manager_obj
    time_utils_obj
    datetime_parser_obj
    ft_es_storage_adapter_obj
    version_obj
    ssl_obj
    geo_index_obj
)

nebula_add_executable(
    NAME
        storage_perf
    SOURCES
        StoragePerfTool.cpp
    LIBRARIES
        ${perf_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        gtest
)

#install(
#    TARGETS
#        storage_perf
#    DESTINATION
#        bin
#    COMPONENT
#        tool
#)

nebula_add_executable(
    NAME
        storage_integrity
    SOURCES
        StorageIntegrityTool.cpp
    LIBRARIES
        ${perf_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        gtest
)

#install(
#    TARGETS
#        storage_integrity
#    DESTINATION
#        bin
#    COMPONENT
#        tool
#)
