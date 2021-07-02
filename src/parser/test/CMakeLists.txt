# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(PARSER_TEST_LIBS
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:common_expression_obj>
    $<TARGET_OBJECTS:common_encryption_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_stats_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_function_manager_obj>
    $<TARGET_OBJECTS:common_agg_function_manager_obj>
    $<TARGET_OBJECTS:common_meta_thrift_obj>
    $<TARGET_OBJECTS:common_graph_thrift_obj>
    $<TARGET_OBJECTS:common_http_client_obj>
    $<TARGET_OBJECTS:common_storage_thrift_obj>
    $<TARGET_OBJECTS:common_meta_obj>
    $<TARGET_OBJECTS:common_meta_client_obj>
    $<TARGET_OBJECTS:common_conf_obj>
    $<TARGET_OBJECTS:common_charset_obj>
    $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:common_process_obj>
    $<TARGET_OBJECTS:common_time_utils_obj>
    $<TARGET_OBJECTS:common_ft_es_graph_adapter_obj>
    $<TARGET_OBJECTS:common_ws_common_obj>
    $<TARGET_OBJECTS:common_version_obj>
    $<TARGET_OBJECTS:graph_session_obj>
    $<TARGET_OBJECTS:graph_flags_obj>
    $<TARGET_OBJECTS:graph_auth_obj>
    $<TARGET_OBJECTS:util_obj>
    $<TARGET_OBJECTS:expr_visitor_obj>
    $<TARGET_OBJECTS:context_obj>
    $<TARGET_OBJECTS:planner_obj>
    $<TARGET_OBJECTS:validator_obj>
    $<TARGET_OBJECTS:idgenerator_obj>
    $<TARGET_OBJECTS:common_graph_obj>
)

nebula_add_test(
    NAME parser_test
    SOURCES ParserTest.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)

nebula_add_test(
    NAME scanner_test
    SOURCES ScannerTest.cpp
    OBJECTS ${PARSER_TEST_LIBS}
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
