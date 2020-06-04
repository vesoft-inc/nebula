# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME exec_context_test
    SOURCES ExecutionContextTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:context_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:session_obj>
        $<TARGET_OBJECTS:common_time_obj>
    LIBRARIES
        gtest
        gtest_main
)

nebula_add_test(
    NAME
        expression_context_test
    SOURCES
        ExpressionContextTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:context_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:session_obj>
        $<TARGET_OBJECTS:common_time_obj>
    LIBRARIES
        gtest
        gtest_main
)

nebula_add_test(
    NAME
        iterator_test
    SOURCES
        IteratorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:context_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:session_obj>
        $<TARGET_OBJECTS:common_time_obj>
    LIBRARIES
        gtest
        gtest_main
)
