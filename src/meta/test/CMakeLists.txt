# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        processor_test
    SOURCES
        ProcessorTest.cpp
)

target_link_libraries(
        processor_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        index_processor_test
    SOURCES
        IndexProcessorTest.cpp
)

target_link_libraries(
        index_processor_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        hb_processor_test
    SOURCES
        HBProcessorTest.cpp
)

target_link_libraries(
        hb_processor_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        agent_hb_processor_test
    SOURCES
        AgentHBProcessorTest.cpp
)

target_link_libraries(
        agent_hb_processor_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        meta_client_test
    SOURCES
        MetaClientTest.cpp
)

target_link_libraries(
        meta_client_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        active_hosts_man_test
    SOURCES
        ActiveHostsManTest.cpp
)

target_link_libraries(
        active_hosts_man_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME
        balancer_test
    SOURCES
        BalancerTest.cpp
)

target_link_libraries(
        balancer_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gmock
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

#nebula_add_test(
#    NAME
#        balance_integration_test
#    SOURCES
#        BalanceIntegrationTest.cpp
#    LIBRARIES
#        index_obj
#        storage_service_handler
#        storage_client
#        codec_obj
#        filter_obj
#        adHocSchema_obj
#        adHocIndex_obj
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
)

target_link_libraries(
        admin_client_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME
        authentication_test
    SOURCES
        AuthProcessorTest.cpp
)

target_link_libraries(
        authentication_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        cluster_id_man_test
    SOURCES
        ClusterIdManTest.cpp
)

target_link_libraries(
        cluster_id_man_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        job_manager_test
    SOURCES
        JobManagerTest.cpp
)

target_link_libraries(
        job_manager_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gmock
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        get_stats_test
    SOURCES
        GetStatsTest.cpp
)

target_link_libraries(
        get_stats_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gmock
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        create_backup_test
    SOURCES
        CreateBackupProcessorTest.cpp
)

target_link_libraries(
        create_backup_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        restore_test
    SOURCES
        RestoreProcessorTest.cpp
)

target_link_libraries(
        restore_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        list_meta_cluster_info_test
    SOURCES
        ListClusterInfoTest.cpp
)

target_link_libraries(
        list_meta_cluster_info_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME
        verify_client_version_test
    SOURCES
        VerifyClientVersionTest.cpp
)
target_link_libraries(
        verify_client_version_test
        PRIVATE
        ${meta_test_deps}
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
