# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

SET(CONTEXT_TEST_LIBS
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_graph_thrift_obj>
    $<TARGET_OBJECTS:context_obj>
    $<TARGET_OBJECTS:graph_flags_obj>
    $<TARGET_OBJECTS:session_obj>
    $<TARGET_OBJECTS:planner_obj>
    $<TARGET_OBJECTS:idgenerator_obj>
)

nebula_add_test(
    NAME context_test
    SOURCES
        IteratorTest.cpp
        ExpressionContextTest.cpp
        ExecutionContextTest.cpp
    OBJECTS
        ${CONTEXT_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        gtest
)

nebula_add_executable(
    NAME
        iterator_bm
    SOURCES
        IteratorBenchmark.cpp
    OBJECTS
        ${CONTEXT_TEST_LIBS}
    LIBRARIES
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
)
