# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(VALIDATOR_TEST_LIBS
    $<TARGET_OBJECTS:schema_util_obj>
    $<TARGET_OBJECTS:validator_obj>
    $<TARGET_OBJECTS:planner_obj>
    $<TARGET_OBJECTS:session_obj>
    $<TARGET_OBJECTS:query_engine_obj>
    $<TARGET_OBJECTS:graph_flags_obj>
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:schedule_obj>
    $<TARGET_OBJECTS:common_expression_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_stats_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_meta_thrift_obj>
    $<TARGET_OBJECTS:common_charset_obj>
)

nebula_add_test(
    NAME validator_test
    SOURCES ValidatorTest.cpp
    OBJECTS ${VALIDATOR_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} wangle
)
