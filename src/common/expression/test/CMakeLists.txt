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
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
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
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
)

nebula_add_executable(
    NAME
        func_call_expression_bm
    SOURCES
        FunctionCallExpressionBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
    LIBRARIES
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
)

nebula_add_executable(
    NAME
        aggregate_expression_bm
    SOURCES
        AggregateExpressionBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
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
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME aggregate_expression_test
    SOURCES AggregateExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME arithmetic_expression_test
    SOURCES ArithmeticExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME attribute_expression_test
    SOURCES AttributeExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME case_expression_test
    SOURCES CaseExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME column_expression_test
    SOURCES ColumnExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME constant_expression_test
    SOURCES ConstantExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME container_expression_test
    SOURCES ContainerExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME function_call_expression_test
    SOURCES FunctionCallExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME label_expression_test
    SOURCES LabelExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME list_comprehension_expression_test
    SOURCES ListComprehensionExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME logical_expression_test
    SOURCES LogicalExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME relational_expression_test
    SOURCES RelationalExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME path_build_expression_test
    SOURCES PathBuildExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME property_expression_test
    SOURCES PropertyExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME predicate_expression_test
    SOURCES PredicateExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME reduce_expression_test
    SOURCES ReduceExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME subscript_expression_test
    SOURCES SubscriptExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME type_casting_expression_test
    SOURCES TypeCastingExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME versioned_variable_expression_test
    SOURCES VersionedVariableExpressionTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:expr_ctx_mock_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)
