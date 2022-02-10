# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(PARSER_TEST_LIBS
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:expression_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:graph_stats_obj>
    $<TARGET_OBJECTS:meta_client_stats_obj>
    $<TARGET_OBJECTS:storage_client_stats_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:function_manager_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:agg_function_manager_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:http_client_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:meta_client_obj>
    $<TARGET_OBJECTS:conf_obj>
    $<TARGET_OBJECTS:charset_obj>
    $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:time_utils_obj>
    $<TARGET_OBJECTS:datetime_parser_obj>
    $<TARGET_OBJECTS:ft_es_graph_adapter_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:graph_session_obj>
    $<TARGET_OBJECTS:graph_flags_obj>
    $<TARGET_OBJECTS:graph_auth_obj>
    $<TARGET_OBJECTS:util_obj>
    $<TARGET_OBJECTS:expr_visitor_obj>
    $<TARGET_OBJECTS:graph_context_obj>
    $<TARGET_OBJECTS:idgenerator_obj>
    $<TARGET_OBJECTS:graph_obj>
    $<TARGET_OBJECTS:ssl_obj>
    $<TARGET_OBJECTS:memory_obj>
    $<TARGET_OBJECTS:plan_obj>
)

if(ENABLE_STANDALONE_VERSION)
set(PARSER_TEST_LIBS
    ${PARSER_TEST_LIBS}
    $<TARGET_OBJECTS:sa_test_graph_flags_obj>
)
endif()

nebula_add_test(
    NAME parser_test
    SOURCES ParserTest.cpp
    OBJECTS
      ${PARSER_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)

nebula_add_test(
    NAME scanner_test
    SOURCES ScannerTest.cpp
    OBJECTS
      ${PARSER_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)

nebula_add_executable(
    NAME parser_bm
    SOURCES ParserBenchmark.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES follybenchmark boost_regex ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)

nebula_add_test(
    NAME expression_parsing_test
    SOURCES ExpressionParsingTest.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)

if(ENABLE_FUZZ_TEST)
    nebula_add_subdirectory(fuzzing)
endif()
