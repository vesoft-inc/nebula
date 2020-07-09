# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        function_manager_test
    SOURCES
        FunctionManagerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        gtest
        gtest_main
)

nebula_add_test(
    NAME
        agg_func_test
    SOURCES
        AggregateFunctionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:agg_function_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
    LIBRARIES
        gtest
        gtest_main
)
