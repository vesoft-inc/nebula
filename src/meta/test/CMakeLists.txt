# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        processor_test
    SOURCES
        ProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        index_processor_test
    SOURCES
        IndexProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        hb_processor_test
    SOURCES
        HBProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        agent_hb_processor_test
    SOURCES
        AgentHBProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        meta_client_test
    SOURCES
        MetaClientTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        active_hosts_man_test
    SOURCES
        ActiveHostsManTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        balancer_test
    SOURCES
        BalancerTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gmock
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
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        authentication_test
    SOURCES
        AuthProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        cluster_id_man_test
    SOURCES
        ClusterIdManTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        job_manager_test
    SOURCES
        JobManagerTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gmock
)

nebula_add_test(
    NAME
        get_stats_test
    SOURCES
        GetStatsTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
        gmock
)

nebula_add_test(
    NAME
        create_backup_test
    SOURCES
        CreateBackupProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        restore_test
    SOURCES
        RestoreProcessorTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        list_meta_cluster_info_test
    SOURCES
        ListClusterInfoTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        verify_client_version_test
    SOURCES
        VerifyClientVersionTest.cpp
    LIBRARIES
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)
