set(storage_test_deps
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:index_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:charset_obj>
    $<TARGET_OBJECTS:gflags_man_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    $<TARGET_OBJECTS:adHocIndex_obj>
)


nebula_add_library(
    adHocSchema_obj OBJECT
    AdHocSchemaManager.cpp
)

nebula_add_library(
    adHocIndex_obj OBJECT
    AdHocIndexManager.cpp
)

nebula_add_test(
    NAME
        task_manager_test
    SOURCES
        TaskManagerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:adHocSchema_obj>
        ${storage_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

