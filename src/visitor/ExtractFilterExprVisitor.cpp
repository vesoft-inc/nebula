/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ExtractFilterExprVisitor.h"

namespace nebula {
namespace graph {

void ExtractFilterExprVisitor::visit(ConstantExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(LabelExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(UUIDExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(VariableExpression *expr) {
    canBePushed_ = static_cast<VariableExpression *>(expr)->isInner();
}

void ExtractFilterExprVisitor::visit(VersionedVariableExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(TagPropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(EdgePropertyExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(InputPropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(VariablePropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(DestPropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(SourcePropertyExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeSrcIdExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeTypeExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeRankExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeDstIdExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(VertexExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(EdgeExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(ColumnExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(LogicalExpression *expr) {
    // TODO(dutor) It's buggy when there are multi-level embedded logical expressions
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
        auto &operands = expr->operands();
        std::vector<bool> flags(operands.size(), false);
        auto canBePushed = false;
        for (auto i = 0u; i < operands.size(); i++) {
            operands[i]->accept(this);
            flags[i] = canBePushed_;
            canBePushed = canBePushed || canBePushed_;
        }
        if (canBePushed) {
            auto remainedExpr = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd);
            for (auto i = 0u; i < operands.size(); i++) {
                if (flags[i]) {
                    continue;
                }
                remainedExpr->addOperand(operands[i]->clone().release());
                expr->setOperand(i, new ConstantExpression(true));
            }
            if (remainedExpr->operands().size() > 0) {
                remainedExpr_ = std::move(remainedExpr);
            }
        }
    } else {
        ExprVisitorImpl::visit(expr);
    }
}

}   // namespace graph
}   // namespace nebula
