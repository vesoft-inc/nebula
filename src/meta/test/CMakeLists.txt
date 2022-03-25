# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

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
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        index_processor_test
    SOURCES
        IndexProcessorTest.cpp
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
#    OBJECTS
#        $<TARGET_OBJECTS:index_obj>
#        $<TARGET_OBJECTS:storage_service_handler>
#        $<TARGET_OBJECTS:storage_client>
#        $<TARGET_OBJECTS:codec_obj>
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
        ${PROXYGEN_LIBRARIES}
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
        ${PROXYGEN_LIBRARIES}
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
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        job_manager_test
    SOURCES
        JobManagerTest.cpp
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
    OBJECTS
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
        id_processor_test
    SOURCES
        IdProcessorTest.cpp
    OBJECTS
        ${meta_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)
