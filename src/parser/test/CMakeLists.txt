# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(PARSER_TEST_LIBS
    parser_obj
    expression_obj
    network_obj
    fs_obj
    stats_obj
    graph_stats_obj
    meta_client_stats_obj
    storage_client_stats_obj
    time_obj
    common_thrift_obj
    thrift_obj
    thread_obj
    datatypes_obj
    base_obj
    function_manager_obj
    wkt_wkb_io_obj
    agg_function_manager_obj
    meta_thrift_obj
    graph_thrift_obj
    http_client_obj
    storage_thrift_obj
    meta_obj
    meta_client_obj
    conf_obj
    charset_obj
    file_based_cluster_id_man_obj
    process_obj
    time_utils_obj
    datetime_parser_obj
    ft_es_graph_adapter_obj
    ws_common_obj
    version_obj
    graph_session_obj
    graph_flags_obj
    graph_auth_obj
    util_obj
    expr_visitor_obj
    graph_context_obj
    idgenerator_obj
    graph_obj
    ssl_obj
    memory_obj
    plan_obj
)

if(ENABLE_STANDALONE_VERSION)
set(PARSER_TEST_LIBS
    ${PARSER_TEST_LIBS}
    sa_test_graph_flags_obj
)
endif()

nebula_add_test(
    NAME parser_test
    SOURCES ParserTest.cpp
)

target_link_libraries(
        parser_test
        PRIVATE
      ${PARSER_TEST_LIBS}
    gtest
    gtest_main 
    ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME scanner_test
    SOURCES ScannerTest.cpp
)

target_link_libraries(
        scanner_test
        PRIVATE
      ${PARSER_TEST_LIBS}
    gtest 
    gtest_main 
    ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_executable(
    NAME parser_bm
    SOURCES ParserBenchmark.cpp
)

target_link_libraries(
        parser_bm
        PRIVATE
    ${PARSER_TEST_LIBS}
    follybenchmark
    boost_regex 
    ${THRIFT_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME expression_parsing_test
    SOURCES ExpressionParsingTest.cpp
)

target_link_libraries(
        expression_parsing_test
        PRIVATE
    ${PARSER_TEST_LIBS}
    gtest
    gtest_main
    ${THRIFT_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
if(ENABLE_FUZZ_TEST)
    nebula_add_subdirectory(fuzzing)
endif()
