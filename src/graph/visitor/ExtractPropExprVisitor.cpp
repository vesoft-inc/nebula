/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/ExtractPropExprVisitor.h"

namespace nebula {
namespace graph {

ExtractPropExprVisitor::ExtractPropExprVisitor(
    ValidateContext* vctx,
    YieldColumns* srcAndEdgePropCols,
    YieldColumns* dstPropCols,
    YieldColumns* inputPropCols,
    std::unordered_map<std::string, YieldColumn*>& propExprColMap)
    : vctx_(DCHECK_NOTNULL(vctx)),
      srcAndEdgePropCols_(srcAndEdgePropCols),
      dstPropCols_(dstPropCols),
      inputPropCols_(inputPropCols),
      propExprColMap_(propExprColMap) {}

void ExtractPropExprVisitor::visit(ConstantExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(VertexExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(EdgeExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(VariableExpression* expr) {
    UNUSED(expr);
}

void ExtractPropExprVisitor::visit(SubscriptExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(LabelExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(LabelAttributeExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(VersionedVariableExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(UUIDExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(ColumnExpression* expr) {
    reportError(expr);
}

void ExtractPropExprVisitor::visit(UnaryExpression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kIsNull:
        case Expression::Kind::kIsNotNull:
        case Expression::Kind::kIsEmpty:
        case Expression::Kind::kIsNotEmpty: {
            expr->operand()->accept(this);
            break;
        }
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kUnaryIncr: {
            reportError(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid Kind " << expr->kind();
        }
    }
}

void ExtractPropExprVisitor::visitPropertyExpr(PropertyExpression* expr) {
    PropertyExpression* propExpr = nullptr;
    switch (expr->kind()) {
        case Expression::Kind::kInputProperty: {
            propExpr = static_cast<InputPropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kVarProperty: {
            propExpr = static_cast<VariablePropertyExpression*>(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid Kind " << expr->kind();
        }
    }
    auto found = propExprColMap_.find(propExpr->toString());
    if (found == propExprColMap_.end()) {
        auto newExpr = propExpr->clone();
        auto col = new YieldColumn(newExpr, expr->prop());
        propExprColMap_.emplace(propExpr->toString(), col);
        inputPropCols_->addColumn(col);
    }
}

void ExtractPropExprVisitor::visit(VariablePropertyExpression* expr) {
    visitPropertyExpr(expr);
}

void ExtractPropExprVisitor::visit(InputPropertyExpression* expr) {
    visitPropertyExpr(expr);
}

void ExtractPropExprVisitor::visitVertexEdgePropExpr(PropertyExpression* expr) {
    PropertyExpression* propExpr = nullptr;
    switch (expr->kind()) {
        case Expression::Kind::kTagProperty: {
            propExpr = static_cast<TagPropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kSrcProperty: {
            propExpr = static_cast<SourcePropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeProperty: {
            propExpr = static_cast<EdgePropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeSrc: {
            propExpr = static_cast<EdgeSrcIdExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeType: {
            propExpr = static_cast<EdgeTypeExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeRank: {
            propExpr = static_cast<EdgeRankExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeDst: {
            propExpr = static_cast<EdgeDstIdExpression*>(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid Kind " << expr->kind();
        }
    }
    auto found = propExprColMap_.find(propExpr->toString());
    if (found == propExprColMap_.end()) {
        auto newExpr = propExpr->clone();
        auto col = new YieldColumn(newExpr, vctx_->anonColGen()->getCol());
        propExprColMap_.emplace(propExpr->toString(), col);
        srcAndEdgePropCols_->addColumn(col);
    }
}

void ExtractPropExprVisitor::visit(TagPropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(SourcePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeSrcIdExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeTypeExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeRankExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(EdgeDstIdExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void ExtractPropExprVisitor::visit(DestPropertyExpression* expr) {
    auto found = propExprColMap_.find(expr->toString());
    if (found == propExprColMap_.end()) {
        auto newExpr = expr->clone();
        auto col = new YieldColumn(newExpr, vctx_->anonColGen()->getCol());
        propExprColMap_.emplace(expr->toString(), col);
        dstPropCols_->addColumn(col);
    }
}

void ExtractPropExprVisitor::reportError(const Expression* expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for ExtractPropsExpression.";
    status_ = Status::SemanticError(ss.str());
}

}   // namespace graph
}   // namespace nebula
