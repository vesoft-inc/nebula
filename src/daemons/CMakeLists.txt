# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(common_deps
    meta_obj
    meta_client_obj
    file_based_cluster_id_man_obj
    http_client_obj
    snowflake_obj
    thrift_obj
    storage_thrift_obj
    meta_thrift_obj
    common_thrift_obj
    ws_obj
    ws_common_obj
    process_obj
    stats_obj
    fs_obj
    time_obj
    network_obj
    thread_obj
    conf_obj
    datatypes_obj
    wkt_wkb_io_obj
    base_obj
    memory_obj
    expression_obj
    function_manager_obj
    agg_function_manager_obj
    time_utils_obj
    datetime_parser_obj
    version_obj
    ssl_obj
    geo_index_obj
    log_monitor_obj
)

set(storage_meta_deps
    kvstore_obj
    storage_stats_obj
    meta_stats_obj
    meta_client_stats_obj
    storage_client_stats_obj
    kv_stats_obj
    raftex_obj
    wal_obj
    disk_man_obj
    keyutils_obj
    meta_keyutils_obj
    log_str_list_iterator_obj
    codec_obj
    raftex_thrift_obj
    hdfs_helper_obj
    ft_es_storage_adapter_obj
    storage_thrift_obj
    geo_index_obj
)

if(NOT ENABLE_STANDALONE_VERSION)

nebula_add_executable(
    NAME
        nebula-storaged
    SOURCES
        StorageDaemon.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    LIBRARIES
        storage_server
        internal_storage_service_handler
        graph_storage_service_handler
        storage_admin_service_handler
        storage_http_handler
        storage_transaction_executor
        internal_storage_client_obj
        storage_client_obj
        storage_client_base_obj
        storage_common_obj
        ${common_deps}
        ${storage_meta_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
)

nebula_add_executable(
    NAME
        nebula-metad
    SOURCES
        MetaDaemon.cpp
        MetaDaemonInit.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    LIBRARIES
        meta_service_handler
        meta_http_handler
        meta_version_man_obj
        meta_data_upgrade_obj
        meta_v2_thrift_obj
        ${common_deps}
        ${storage_meta_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
)

nebula_add_executable(
    NAME
        nebula-graphd
    SOURCES
        GraphDaemon.cpp
        SetupLogging.cpp
        SetupBreakpad.cpp
    LIBRARIES
        graph_stats_obj
        meta_client_stats_obj
        storage_client_stats_obj
        util_obj
        service_obj
        graph_session_obj
        query_engine_obj
        parser_obj
        validator_obj
        expr_visitor_obj
        optimizer_obj
        plan_node_visitor_obj
        planner_obj
        plan_obj
        executor_obj
        scheduler_obj
        idgenerator_obj
        graph_context_obj
        graph_flags_obj
        graph_auth_obj
        graph_thrift_obj
        storage_client_base_obj
        storage_client_obj
        charset_obj
        graph_obj
        ft_es_graph_adapter_obj
        storage_thrift_obj
        ${common_deps}
    LIBRARIES
        ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
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
    LIBRARIES
        graph_stats_obj
        meta_client_stats_obj
        storage_client_stats_obj
        util_obj
        service_obj
        graph_session_obj
        query_engine_obj
        parser_obj
        validator_obj
        expr_visitor_obj
        optimizer_obj
        plan_node_visitor_obj
        planner_obj
        plan_obj
        executor_obj
        scheduler_obj
        idgenerator_obj
        graph_context_obj
        graph_flags_obj
        graph_auth_obj
        graph_thrift_obj
        storage_client_base_obj
        storage_client_obj
        charset_obj
        graph_obj
        ft_es_graph_adapter_obj
        storage_thrift_obj
        storage_server
        internal_storage_service_handler
        graph_storage_service_handler
        storage_admin_service_handler
        storage_http_handler
        storage_transaction_executor
        internal_storage_client_obj
        storage_common_obj
        storage_local_server_obj
        meta_service_handler
        meta_http_handler
        meta_version_man_obj
        meta_data_upgrade_obj
        meta_v2_thrift_obj
        ${storage_meta_deps}
        ${common_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
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
