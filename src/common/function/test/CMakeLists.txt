# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        function_manager_test
    SOURCES
        FunctionManagerTest.cpp
)
target_link_libraries(
        function_manager_test
        PRIVATE
        function_manager_obj
        base_obj
        datatypes_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
        time_utils_obj
        datetime_parser_obj
        time_obj
        fs_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME
        agg_function_manager_test
    SOURCES
        AggFunctionManagerTest.cpp
)

target_link_libraries(
        agg_function_manager_test
        PRIVATE
        agg_function_manager_obj
        base_obj
        datatypes_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
        gtest
        gtest_main
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
