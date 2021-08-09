/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_EVALUABLEEXPRVISITOR_H_
#define VISITOR_EVALUABLEEXPRVISITOR_H_

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class EvaluableExprVisitor : public ExprVisitorImpl {
public:
    bool ok() const override {
        return isEvaluable_;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(ConstantExpression *) override {
        isEvaluable_ = true;
    }

    void visit(LabelExpression *) override {
        isEvaluable_ = false;
    }

    void visit(UUIDExpression *) override {
        isEvaluable_ = false;
    }

    void visit(VariableExpression *) override {
        isEvaluable_ = false;
    }

    void visit(VersionedVariableExpression *) override {
        isEvaluable_ = false;
    }

    void visit(TagPropertyExpression *) override {
        isEvaluable_ = false;
    }

    void visit(EdgePropertyExpression *) override {
        isEvaluable_ = false;
    }

    void visit(InputPropertyExpression *) override {
        isEvaluable_ = false;
    }

    void visit(VariablePropertyExpression *) override {
        isEvaluable_ = false;
    }

    void visit(DestPropertyExpression *) override {
        isEvaluable_ = false;
    }

    void visit(SourcePropertyExpression *) override {
        isEvaluable_ = false;
    }

    void visit(EdgeSrcIdExpression *) override {
        isEvaluable_ = false;
    }

    void visit(EdgeTypeExpression *) override {
        isEvaluable_ = false;
    }

    void visit(EdgeRankExpression *) override {
        isEvaluable_ = false;
    }

    void visit(EdgeDstIdExpression *) override {
        isEvaluable_ = false;
    }

    void visit(VertexExpression *) override {
        isEvaluable_ = false;
    }

    void visit(EdgeExpression *) override {
        isEvaluable_ = false;
    }

    void visit(ColumnExpression *) override {
        isEvaluable_ = false;
    }

    void visit(SubscriptRangeExpression *) override {
        isEvaluable_ = false;
    }

    bool isEvaluable_{true};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_EVALUABLEEXPRVISITOR_H_
