set(GRAPH_TEST_LIBS
    $<TARGET_OBJECTS:graph_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:storage_service_handler>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:meta_service_handler>
    $<TARGET_OBJECTS:kvstore_obj>
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:base_obj>
)

add_library(graph_test_common_obj OBJECT
    TestMain.cpp
    TestEnv.cpp
    TestBase.cpp
)

nebula_add_base_obj(graph_test_common_obj)


add_executable(
    session_manager_test
    SessionManagerTest.cpp
    ${GRAPH_TEST_LIBS}
)
nebula_link_libraries(
    session_manager_test
    ${THRIFT_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    wangle
    gtest
    gtest_main
)
nebula_add_test(session_manager_test)


add_executable(
    query_engine_test
    YieldTest.cpp
    SchemaTest.cpp
    $<TARGET_OBJECTS:graph_test_common_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${GRAPH_TEST_LIBS}
)
nebula_link_libraries(
    query_engine_test
    ${THRIFT_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(query_engine_test)


add_executable(
    go_test
    GoTest.cpp
    $<TARGET_OBJECTS:graph_test_common_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${GRAPH_TEST_LIBS}
)
nebula_link_libraries(
    go_test
    ${THRIFT_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(go_test)


add_executable(
    graph_http_test
    GraphHttpHandlerTest.cpp
    $<TARGET_OBJECTS:graph_test_common_obj>
    $<TARGET_OBJECTS:graph_http_handler>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${GRAPH_TEST_LIBS}
)
nebula_link_libraries(
    graph_http_test
    proxygenhttpserver
    proxygenlib
    ${THRIFT_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(graph_http_test)

add_executable(
    data_test
    DataTest.cpp
    $<TARGET_OBJECTS:graph_test_common_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${GRAPH_TEST_LIBS}
)
nebula_link_libraries(
    data_test
    ${THRIFT_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(data_test)

add_executable(
    order_by_test
    OrderByTest.cpp
    $<TARGET_OBJECTS:graph_test_common_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:adHocSchema_obj>
    ${GRAPH_TEST_LIBS}
)

nebula_link_libraries(
    order_by_test
    ${THRIFT_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    wangle
    gtest
)
nebula_add_test(order_by_test)
