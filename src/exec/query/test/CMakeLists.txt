# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME query_executors_test
    SOURCES
        ProjectTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:exec_obj>
        $<TARGET_OBJECTS:context_obj>
        $<TARGET_OBJECTS:planner_obj>
        $<TARGET_OBJECTS:idgenerator_obj>
        $<TARGET_OBJECTS:session_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_expression_obj>
        $<TARGET_OBJECTS:common_time_obj>
    LIBRARIES
        gtest gtest_main
)
