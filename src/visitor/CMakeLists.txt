# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    expr_visitor_obj OBJECT
    ExprVisitorImpl.cpp
    CollectAllExprsVisitor.cpp
    DeducePropsVisitor.cpp
    DeduceTypeVisitor.cpp
    ExtractPropExprVisitor.cpp
    ExtractFilterExprVisitor.cpp
    FindAnyExprVisitor.cpp
    FindAnySubExprVisitor.cpp
    FoldConstantExprVisitor.cpp
    RewriteSymExprVisitor.cpp
    RewriteVisitor.cpp
    VidExtractVisitor.cpp
    RewriteUnaryNotExprVisitor.cpp
)

nebula_add_subdirectory(test)
