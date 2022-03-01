# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    expr_visitor_obj SHARED
    ExprVisitorImpl.cpp
    DeducePropsVisitor.cpp
    PropertyTrackerVisitor.cpp
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

nebula_add_library(
    plan_node_visitor_obj SHARED
    PrunePropertiesVisitor.cpp
)
target_link_libraries(expr_visitor_obj PUBLIC  "${Glog_LIBRARY}" )
target_link_libraries(plan_node_visitor_obj PUBLIC  "${Glog_LIBRARY}" )

nebula_add_subdirectory(test)
