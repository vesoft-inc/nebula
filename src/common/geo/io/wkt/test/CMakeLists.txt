# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(WKT_TEST_LIBS
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:expression_obj>
    $<TARGET_OBJECTS:encryption_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:stats_obj>
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
    $<TARGET_OBJECTS:ft_es_graph_adapter_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:graph_session_obj>
    $<TARGET_OBJECTS:graph_flags_obj>
    $<TARGET_OBJECTS:graph_auth_obj>
    $<TARGET_OBJECTS:util_obj>
    $<TARGET_OBJECTS:expr_visitor_obj>
    $<TARGET_OBJECTS:graph_context_obj>
    $<TARGET_OBJECTS:planner_obj>
    $<TARGET_OBJECTS:validator_obj>
    $<TARGET_OBJECTS:idgenerator_obj>
    $<TARGET_OBJECTS:graph_obj>
    $<TARGET_OBJECTS:ssl_obj>
    $<TARGET_OBJECTS:memory_obj>
)

nebula_add_test(
    NAME wkt_test
    SOURCES WKTTest.cpp
    OBJECTS ${WKT_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} ${PROXYGEN_LIBRARIES}
)
