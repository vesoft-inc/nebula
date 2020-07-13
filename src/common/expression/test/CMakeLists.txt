# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    expr_ctx_mock_obj OBJECT
    ExpressionContextMock.cpp
    )

nebula_add_test(
    NAME
        expression_test
    SOURCES
        ExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_executable(
    NAME
        expression_bm
    SOURCES
        ExpressionBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME expression_encode_decode_test
    SOURCES EncodeDecodeTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)
