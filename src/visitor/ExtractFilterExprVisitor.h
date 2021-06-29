/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef VISITOR_EXTRACTFILTEREXPRVISITOR_H_
#define VISITOR_EXTRACTFILTEREXPRVISITOR_H_

#include <memory>

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class ExtractFilterExprVisitor final : public ExprVisitorImpl {
public:
    explicit ExtractFilterExprVisitor(ObjectPool *ObjPool) : pool_(ObjPool) {}

    bool ok() const override {
        return canBePushed_;
    }

    Expression* remainedExpr() {
        return remainedExpr_;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(ConstantExpression *) override;
    void visit(LabelExpression *) override;
    void visit(UUIDExpression *) override;
    void visit(VariableExpression *) override;
    void visit(VersionedVariableExpression *) override;
    void visit(TagPropertyExpression *) override;
    void visit(EdgePropertyExpression *) override;
    void visit(InputPropertyExpression *) override;
    void visit(VariablePropertyExpression *) override;
    void visit(DestPropertyExpression *) override;
    void visit(SourcePropertyExpression *) override;
    void visit(EdgeSrcIdExpression *) override;
    void visit(EdgeTypeExpression *) override;
    void visit(EdgeRankExpression *) override;
    void visit(EdgeDstIdExpression *) override;
    void visit(VertexExpression *) override;
    void visit(EdgeExpression *) override;
    void visit(LogicalExpression *) override;
    void visit(ColumnExpression *) override;
    void visit(SubscriptRangeExpression *) override;

private:
    ObjectPool *pool_;
    bool canBePushed_{true};
    Expression* remainedExpr_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_EXTRACTFILTEREXPRVISITOR_H_
