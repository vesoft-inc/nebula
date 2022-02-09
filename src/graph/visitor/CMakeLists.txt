# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    expr_visitor_obj OBJECT
    ExprVisitorImpl.cpp
    DeducePropsVisitor.cpp
    DeduceTypeVisitor.cpp
    ExtractPropExprVisitor.cpp
    ExtractFilterExprVisitor.cpp
    FoldConstantExprVisitor.cpp
    RewriteSymExprVisitor.cpp
    RewriteVisitor.cpp
    FindVisitor.cpp
    VidExtractVisitor.cpp
    EvaluableExprVisitor.cpp
)

nebula_add_subdirectory(test)
