# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        meta_utils_test
    SOURCES
        MetaServiceUtilsTest.cpp
        ../MetaServiceUtils.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:thrift_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:wkt_wkb_io_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:encryption_obj>
        $<TARGET_OBJECTS:ft_es_storage_adapter_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
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
        group_zone_test
    SOURCES
        GroupZoneTest.cpp
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
