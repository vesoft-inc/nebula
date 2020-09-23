/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/DeducePropsVisitor.h"

#include <sstream>

#include "context/QueryContext.h"
#include "validator/ExpressionProps.h"

namespace nebula {
namespace graph {

DeducePropsVisitor::DeducePropsVisitor(QueryContext *qctx,
                                       GraphSpaceID space,
                                       ExpressionProps *exprProps)
    : qctx_(qctx), space_(space), exprProps_(exprProps) {
    DCHECK(qctx != nullptr);
    DCHECK(exprProps != nullptr);
}

void DeducePropsVisitor::visit(EdgePropertyExpression *expr) {
    visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(TagPropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toTagID(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertTagNameIds(*expr->sym(), status.value());
    exprProps_->insertTagProp(status.value(), *expr->prop());
}

void DeducePropsVisitor::visit(InputPropertyExpression *expr) {
    exprProps_->insertInputProp(*expr->prop());
}

void DeducePropsVisitor::visit(VariablePropertyExpression *expr) {
    exprProps_->insertVarProp(*expr->sym(), *expr->prop());
}

void DeducePropsVisitor::visit(DestPropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toTagID(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertDstTagProp(std::move(status).value(), *expr->prop());
}

void DeducePropsVisitor::visit(SourcePropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toTagID(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertSrcTagProp(std::move(status).value(), *expr->prop());
}

void DeducePropsVisitor::visit(EdgeSrcIdExpression *expr) {
    visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(EdgeTypeExpression *expr) {
    visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(EdgeRankExpression *expr) {
    visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(EdgeDstIdExpression *expr) {
    visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(UUIDExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visit(VariableExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visit(VersionedVariableExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visit(LabelExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visit(AttributeExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visit(LabelAttributeExpression *expr) {
    reportError(expr);
}

void DeducePropsVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
}

void DeducePropsVisitor::visit(VertexExpression *expr) {
    UNUSED(expr);
}

void DeducePropsVisitor::visit(EdgeExpression *expr) {
    UNUSED(expr);
}

void DeducePropsVisitor::visitEdgePropExpr(PropertyExpression *expr) {
    auto status = qctx_->schemaMng()->toEdgeType(space_, *expr->sym());
    if (!status.ok()) {
        status_ = std::move(status).status();
        return;
    }
    exprProps_->insertEdgeProp(status.value(), *expr->prop());
}

void DeducePropsVisitor::reportError(const Expression *expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for props deduction.";
    status_ = Status::SemanticError(ss.str());
}

}   // namespace graph
}   // namespace nebula
