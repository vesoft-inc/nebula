/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITEVISITOR_H_
#define VISITOR_REWRITEVISITOR_H_

#include <vector>
#include <functional>
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class RewriteVisitor final : public ExprVisitorImpl {
public:
    using Matcher = std::function<bool(const Expression*)>;
    using Rewriter = std::function<Expression*(const Expression*)>;

    static Expression* transform(const Expression* expr,
                                 Matcher matcher,
                                 Rewriter rewriter);

    static Expression* transform(const Expression* expr,
                                 Matcher matcher,
                                 Rewriter rewriter,
                                 const std::unordered_set<Expression::Kind>& needVisitedTypes);

    const Matcher matcher() const {
        return matcher_;
    }

    const Rewriter rewriter() const {
        return rewriter_;
    }

private:
    explicit RewriteVisitor(Matcher matcher,
                            Rewriter rewriter)
        : matcher_(std::move(matcher)),
          rewriter_(std::move(rewriter)) {}

    explicit RewriteVisitor(Matcher matcher,
                            Rewriter rewriter,
                            const std::unordered_set<Expression::Kind>& needVisitedTypes)
        : matcher_(std::move(matcher)),
          rewriter_(std::move(rewriter)),
          needVisitedTypes_(std::move(needVisitedTypes)) {}

    bool ok() const override {
        return true;
    }

private:
    using ExprVisitorImpl::visit;
    void visit(TypeCastingExpression*) override;
    void visit(UnaryExpression*) override;
    void visit(FunctionCallExpression*) override;
    void visit(AggregateExpression*) override;
    void visit(ListExpression*) override;
    void visit(SetExpression*) override;
    void visit(MapExpression*) override;
    void visit(CaseExpression *) override;
    void visit(ReduceExpression *) override;
    void visit(PredicateExpression *) override;
    void visit(ListComprehensionExpression*) override;
    void visit(LogicalExpression*) override;
    void visit(ArithmeticExpression*) override;
    void visit(AttributeExpression*) override;
    void visit(RelationalExpression*) override;
    void visit(SubscriptExpression*) override;
    void visit(PathBuildExpression*) override;
    void visit(SubscriptRangeExpression*) override;
    void visit(ConstantExpression *) override {}
    void visit(LabelExpression*) override {}
    void visit(UUIDExpression*) override {}
    void visit(LabelAttributeExpression*) override {}
    void visit(VariableExpression*) override {}
    void visit(VersionedVariableExpression*) override {}
    void visit(TagPropertyExpression*) override {}
    void visit(EdgePropertyExpression*) override {}
    void visit(InputPropertyExpression*) override {}
    void visit(VariablePropertyExpression*) override {}
    void visit(DestPropertyExpression*) override {}
    void visit(SourcePropertyExpression*) override {}
    void visit(EdgeSrcIdExpression*) override {}
    void visit(EdgeTypeExpression*) override {}
    void visit(EdgeRankExpression*) override {}
    void visit(EdgeDstIdExpression*) override {}
    void visit(VertexExpression*) override {}
    void visit(EdgeExpression*) override {}
    void visit(ColumnExpression*) override {}

    void visitBinaryExpr(BinaryExpression *) override;
    bool care(Expression::Kind kind);

private:
    Matcher                             matcher_;
    Rewriter                            rewriter_;

    std::unordered_set<Expression::Kind> needVisitedTypes_{};
};

}   // namespace graph
}   // namespace nebula


#endif  // VISITOR_REWRITEVISITOR_H_
