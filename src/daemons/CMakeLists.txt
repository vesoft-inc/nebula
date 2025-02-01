# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(common_deps
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:meta_client_obj>
    $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:http_client_obj>
    $<TARGET_OBJECTS:snowflake_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:conf_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:memory_obj>
    $<TARGET_OBJECTS:expression_obj>
    $<TARGET_OBJECTS:ast_match_path_obj>
    $<TARGET_OBJECTS:function_manager_obj>
    $<TARGET_OBJECTS:agg_function_manager_obj>
    $<TARGET_OBJECTS:time_utils_obj>
    $<TARGET_OBJECTS:datetime_parser_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:ssl_obj>
    $<TARGET_OBJECTS:geo_index_obj>
    $<TARGET_OBJECTS:log_monitor_obj>
)

set(storage_meta_deps
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:storage_stats_obj>
    $<TARGET_OBJECTS:meta_stats_obj>
    $<TARGET_OBJECTS:meta_client_stats_obj>
    $<TARGET_OBJECTS:storage_client_stats_obj>
    $<TARGET_OBJECTS:kv_stats_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:listener_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:disk_man_obj>
    $<TARGET_OBJECTS:keyutils_obj>
    $<TARGET_OBJECTS:meta_keyutils_obj>
    $<TARGET_OBJECTS:log_str_list_iterator_obj>
    $<TARGET_OBJECTS:codec_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:hdfs_helper_obj>
    $<TARGET_OBJECTS:es_adapter_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:geo_index_obj>
)

if(NOT ENABLE_STANDALONE_VERSION)

nebula_add_executable(
    NAME
        nebula-storaged
    SOURCES
        StorageDaemon.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    OBJECTS
        $<TARGET_OBJECTS:storage_server>
        $<TARGET_OBJECTS:internal_storage_service_handler>
        $<TARGET_OBJECTS:graph_storage_service_handler>
        $<TARGET_OBJECTS:storage_admin_service_handler>
        $<TARGET_OBJECTS:storage_http_handler>
        $<TARGET_OBJECTS:storage_transaction_executor>
        $<TARGET_OBJECTS:internal_storage_client_obj>
        $<TARGET_OBJECTS:storage_client_obj>
        $<TARGET_OBJECTS:storage_client_base_obj>
        $<TARGET_OBJECTS:storage_common_obj>
        $<TARGET_OBJECTS:embedding_client_obj>
        ${common_deps}
        ${storage_meta_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        curl
)

nebula_add_executable(
    NAME
        nebula-metad
    SOURCES
        MetaDaemon.cpp
        MetaDaemonInit.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:meta_http_handler>
        $<TARGET_OBJECTS:embedding_client_obj>
        $<TARGET_OBJECTS:meta_version_man_obj>
        $<TARGET_OBJECTS:meta_data_upgrade_obj>
        $<TARGET_OBJECTS:meta_v2_thrift_obj>
        ${common_deps}
        ${storage_meta_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        curl
)

nebula_add_executable(
    NAME
        nebula-graphd
    SOURCES
        GraphDaemon.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    OBJECTS
        $<TARGET_OBJECTS:graph_stats_obj>
        $<TARGET_OBJECTS:meta_client_stats_obj>
        $<TARGET_OBJECTS:storage_client_stats_obj>
        $<TARGET_OBJECTS:util_obj>
        $<TARGET_OBJECTS:service_obj>
        $<TARGET_OBJECTS:graph_session_obj>
        $<TARGET_OBJECTS:query_engine_obj>
        $<TARGET_OBJECTS:parser_obj>
        $<TARGET_OBJECTS:ast_match_path_obj>
        $<TARGET_OBJECTS:validator_obj>
        $<TARGET_OBJECTS:expr_visitor_obj>
        $<TARGET_OBJECTS:optimizer_obj>
        $<TARGET_OBJECTS:plan_node_visitor_obj>
        $<TARGET_OBJECTS:planner_obj>
        $<TARGET_OBJECTS:plan_obj>
        $<TARGET_OBJECTS:executor_obj>
        $<TARGET_OBJECTS:scheduler_obj>
        $<TARGET_OBJECTS:idgenerator_obj>
        $<TARGET_OBJECTS:graph_context_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:graph_auth_obj>
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:storage_client_base_obj>
        $<TARGET_OBJECTS:storage_client_obj>
        $<TARGET_OBJECTS:charset_obj>
        $<TARGET_OBJECTS:graph_obj>
        $<TARGET_OBJECTS:es_adapter_obj>
        $<TARGET_OBJECTS:storage_thrift_obj>
        $<TARGET_OBJECTS:gc_obj>
        ${common_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        curl
)

install(
    TARGETS
        nebula-graphd
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        graph
)

install(
    TARGETS
        nebula-storaged
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        storage
)

install(
    TARGETS
        nebula-metad
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        meta
)

else()

nebula_add_executable(
    NAME
        nebula-standalone
    SOURCES
        StandAloneDaemon.cpp
        MetaDaemonInit.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    OBJECTS
        $<TARGET_OBJECTS:graph_stats_obj>
        $<TARGET_OBJECTS:meta_client_stats_obj>
        $<TARGET_OBJECTS:storage_client_stats_obj>
        $<TARGET_OBJECTS:util_obj>
        $<TARGET_OBJECTS:service_obj>
        $<TARGET_OBJECTS:graph_session_obj>
        $<TARGET_OBJECTS:query_engine_obj>
        $<TARGET_OBJECTS:parser_obj>
        $<TARGET_OBJECTS:ast_match_path_obj>
        $<TARGET_OBJECTS:validator_obj>
        $<TARGET_OBJECTS:expr_visitor_obj>
        $<TARGET_OBJECTS:optimizer_obj>
        $<TARGET_OBJECTS:plan_node_visitor_obj>
        $<TARGET_OBJECTS:planner_obj>
        $<TARGET_OBJECTS:plan_obj>
        $<TARGET_OBJECTS:executor_obj>
        $<TARGET_OBJECTS:scheduler_obj>
        $<TARGET_OBJECTS:idgenerator_obj>
        $<TARGET_OBJECTS:graph_context_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:graph_auth_obj>
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:storage_client_base_obj>
        $<TARGET_OBJECTS:storage_client_obj>
        $<TARGET_OBJECTS:charset_obj>
        $<TARGET_OBJECTS:graph_obj>
        $<TARGET_OBJECTS:es_adapter_obj>
        $<TARGET_OBJECTS:storage_thrift_obj>
        $<TARGET_OBJECTS:storage_server>
        $<TARGET_OBJECTS:internal_storage_service_handler>
        $<TARGET_OBJECTS:graph_storage_service_handler>
        $<TARGET_OBJECTS:storage_admin_service_handler>
        $<TARGET_OBJECTS:storage_http_handler>
        $<TARGET_OBJECTS:storage_transaction_executor>
        $<TARGET_OBJECTS:internal_storage_client_obj>
        $<TARGET_OBJECTS:storage_common_obj>
        $<TARGET_OBJECTS:storage_local_server_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:meta_http_handler>
        $<TARGET_OBJECTS:meta_version_man_obj>
        $<TARGET_OBJECTS:meta_data_upgrade_obj>
        $<TARGET_OBJECTS:meta_v2_thrift_obj>
        $<TARGET_OBJECTS:gc_obj>
        $<TARGET_OBJECTS:embedding_client_obj>
        ${storage_meta_deps}
        ${common_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        curl
)

install(
    TARGETS
        nebula-standalone
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        graph ## tmp use graph component
)

endif()
