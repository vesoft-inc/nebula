set(perf_test_deps
    $<TARGET_OBJECTS:meta_service_handler>
    $<TARGET_OBJECTS:meta_version_man_obj>
    $<TARGET_OBJECTS:meta_v1_thrift_obj>
    $<TARGET_OBJECTS:meta_data_upgrade_obj>
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
    $<TARGET_OBJECTS:internal_storage_client_obj>
    $<TARGET_OBJECTS:graph_storage_client_obj>
    $<TARGET_OBJECTS:storage_client_base_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:http_client_obj>
    $<TARGET_OBJECTS:meta_client_obj>
    $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:charset_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:conf_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:expression_obj>
    $<TARGET_OBJECTS:function_manager_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:agg_function_manager_obj>
    $<TARGET_OBJECTS:time_utils_obj>
    $<TARGET_OBJECTS:encryption_obj>
    $<TARGET_OBJECTS:ft_es_storage_adapter_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:ssl_obj>
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
