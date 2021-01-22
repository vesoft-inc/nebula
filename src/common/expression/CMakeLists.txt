# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    expression_obj OBJECT
    Expression.cpp
    BinaryExpression.cpp
    ConstantExpression.cpp
    ArithmeticExpression.cpp
    UnaryExpression.cpp
    RelationalExpression.cpp
    LogicalExpression.cpp
    TypeCastingExpression.cpp
    FunctionCallExpression.cpp
    AggregateExpression.cpp
    PropertyExpression.cpp
    UUIDExpression.cpp
    VariableExpression.cpp
    ContainerExpression.cpp
    SubscriptExpression.cpp
    AttributeExpression.cpp
    LabelAttributeExpression.cpp
    LabelExpression.cpp
    VertexExpression.cpp
    EdgeExpression.cpp
    CaseExpression.cpp
    PathBuildExpression.cpp
    TextSearchExpression.cpp
    ColumnExpression.cpp
    PredicateExpression.cpp
    ListComprehensionExpression.cpp
    ReduceExpression.cpp
)

nebula_add_subdirectory(test)
