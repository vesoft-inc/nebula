set(perf_test_deps
    $<TARGET_OBJECTS:meta_service_handler>
    $<TARGET_OBJECTS:storage_admin_service_handler>
    $<TARGET_OBJECTS:graph_storage_service_handler>
    $<TARGET_OBJECTS:storage_transaction_executor>
    $<TARGET_OBJECTS:storage_common_obj>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:disk_man_obj>
    $<TARGET_OBJECTS:codec_obj>
    $<TARGET_OBJECTS:keyutils_obj>
    $<TARGET_OBJECTS:common_internal_storage_client_obj>
    $<TARGET_OBJECTS:common_graph_storage_client_obj>
    $<TARGET_OBJECTS:common_storage_client_base_obj>
    $<TARGET_OBJECTS:common_ws_common_obj>
    $<TARGET_OBJECTS:common_http_client_obj>
    $<TARGET_OBJECTS:common_meta_client_obj>
    $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:common_storage_thrift_obj>
    $<TARGET_OBJECTS:common_raftex_thrift_obj>
    $<TARGET_OBJECTS:common_meta_thrift_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_meta_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_charset_obj>
    $<TARGET_OBJECTS:common_stats_obj>
    $<TARGET_OBJECTS:common_process_obj>
    $<TARGET_OBJECTS:common_conf_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_expression_obj>
    $<TARGET_OBJECTS:common_function_manager_obj>
    $<TARGET_OBJECTS:common_agg_function_manager_obj>
    $<TARGET_OBJECTS:common_time_utils_obj>
    $<TARGET_OBJECTS:common_encryption_obj>
    $<TARGET_OBJECTS:common_ft_es_storage_adapter_obj>
    $<TARGET_OBJECTS:common_version_obj>
)

nebula_add_executable(
    NAME
        storage_perf
    SOURCES
        StoragePerfTool.cpp
    OBJECTS
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
    OBJECTS
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
