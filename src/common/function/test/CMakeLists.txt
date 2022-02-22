# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        function_manager_test
    SOURCES
        FunctionManagerTest.cpp
    LIBRARIES
        function_manager_obj
        wkt_wkb_io_obj
        base_obj
        datatypes_obj
        time_utils_obj
        datetime_parser_obj
        time_obj
        fs_obj
    LIBRARIES
        gtest
        gtest_main
)

nebula_add_test(
    NAME
        agg_function_manager_test
    SOURCES
        AggFunctionManagerTest.cpp
    LIBRARIES
        agg_function_manager_obj
        base_obj
        datatypes_obj
        wkt_wkb_io_obj
    LIBRARIES
        gtest
        gtest_main
)

