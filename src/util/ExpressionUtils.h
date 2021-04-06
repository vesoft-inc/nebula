/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/base/ObjectPool.h"
#include "common/base/Status.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "visitor/CollectAllExprsVisitor.h"
#include "visitor/EvaluableExprVisitor.h"
#include "visitor/FindAnyExprVisitor.h"
#include "visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {

class ExpressionUtils {
public:
    explicit ExpressionUtils(...) = delete;

    static inline bool isKindOf(const Expression* expr,
                                const std::unordered_set<Expression::Kind>& expected) {
        return expected.find(expr->kind()) != expected.end();
    }

    // null for not found
    static const Expression* findAny(const Expression* self,
                                     const std::unordered_set<Expression::Kind>& expected) {
        FindAnyExprVisitor visitor(expected);
        const_cast<Expression*>(self)->accept(&visitor);
        return visitor.expr();
    }

    // Find all expression fit any kind
    // Empty for not found any one
    static std::vector<const Expression*> collectAll(
        const Expression* self,
        const std::unordered_set<Expression::Kind>& expected) {
        CollectAllExprsVisitor visitor(expected);
        const_cast<Expression*>(self)->accept(&visitor);
        return std::move(visitor).exprs();
    }

    static bool hasAny(const Expression* expr,
                       const std::unordered_set<Expression::Kind>& expected) {
        return findAny(expr, expected) != nullptr;
    }

    static std::vector<const Expression*> findAllStorage(const Expression* expr) {
        return collectAll(expr,
                          {Expression::Kind::kTagProperty,
                           Expression::Kind::kEdgeProperty,
                           Expression::Kind::kDstProperty,
                           Expression::Kind::kSrcProperty,
                           Expression::Kind::kEdgeSrc,
                           Expression::Kind::kEdgeType,
                           Expression::Kind::kEdgeRank,
                           Expression::Kind::kEdgeDst,
                           Expression::Kind::kVertex,
                           Expression::Kind::kEdge});
    }

    static std::vector<const Expression*> findAllInputVariableProp(const Expression* expr) {
        return collectAll(expr, {Expression::Kind::kInputProperty, Expression::Kind::kVarProperty});
    }

    // **Expression type check**
    static bool isConstExpr(const Expression* expr) {
        return !hasAny(expr,
                       {Expression::Kind::kInputProperty,
                        Expression::Kind::kVarProperty,
                        Expression::Kind::kVar,
                        Expression::Kind::kVersionedVar,
                        Expression::Kind::kLabelAttribute,
                        Expression::Kind::kTagProperty,
                        Expression::Kind::kEdgeProperty,
                        Expression::Kind::kDstProperty,
                        Expression::Kind::kSrcProperty,
                        Expression::Kind::kEdgeSrc,
                        Expression::Kind::kEdgeType,
                        Expression::Kind::kEdgeRank,
                        Expression::Kind::kEdgeDst,
                        Expression::Kind::kVertex,
                        Expression::Kind::kEdge});
    }

    // rewrite LabelAttr to tagProp  (just for nGql)
    static Expression* rewriteLabelAttr2TagProp(const Expression* expr) {
        auto matcher = [](const Expression* e) -> bool {
            return e->kind() == Expression::Kind::kLabelAttribute;
        };
        auto rewriter = [](const Expression* e) -> Expression* {
            DCHECK_EQ(e->kind(), Expression::Kind::kLabelAttribute);
            auto labelAttrExpr = static_cast<const LabelAttributeExpression*>(e);
            auto leftName = new std::string(*labelAttrExpr->left()->name());
            auto rightName = new std::string(labelAttrExpr->right()->value().getStr());
            return new TagPropertyExpression(leftName, rightName);
        };

        return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
    }

    static bool isEvaluableExpr(const Expression* expr) {
        EvaluableExprVisitor visitor;
        const_cast<Expression*>(expr)->accept(&visitor);
        return visitor.ok();
    }

    // rewrite LabelAttr to EdgeProp  (just for nGql)
    static Expression* rewriteLabelAttr2EdgeProp(const Expression* expr) {
        auto matcher = [](const Expression* e) -> bool {
            return e->kind() == Expression::Kind::kLabelAttribute;
        };
        auto rewriter = [](const Expression* e) -> Expression* {
            DCHECK_EQ(e->kind(), Expression::Kind::kLabelAttribute);
            auto labelAttrExpr = static_cast<const LabelAttributeExpression*>(e);
            auto leftName = new std::string(*labelAttrExpr->left()->name());
            auto rightName = new std::string(labelAttrExpr->right()->value().getStr());
            return new EdgePropertyExpression(leftName, rightName);
        };

        return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
    }

    // rewrite Agg to VarProp
    static Expression* rewriteAgg2VarProp(const Expression* expr) {
        auto matcher = [](const Expression* e) -> bool {
            return e->kind() == Expression::Kind::kAggregate;
        };
        auto rewriter = [](const Expression* e) -> Expression* {
            return new VariablePropertyExpression(new std::string(""),
                                                  new std::string(e->toString()));
        };

        return RewriteVisitor::transform(expr,
                                         std::move(matcher),
                                         std::move(rewriter),
                                         {Expression::Kind::kFunctionCall,
                                          Expression::Kind::kTypeCasting,
                                          Expression::Kind::kAdd,
                                          Expression::Kind::kMinus,
                                          Expression::Kind::kMultiply,
                                          Expression::Kind::kDivision,
                                          Expression::Kind::kMod});
    }

    // **Expression Transformation**
    // Clone and fold constant expression
    static std::unique_ptr<Expression> foldConstantExpr(const Expression* expr);

    // Clone and reduce constant expression
    static Expression* reduceUnaryNotExpr(const Expression* expr, ObjectPool* objPool);

    // Negate the given logical expr: (A && B) -> (!A || !B)
    static std::unique_ptr<LogicalExpression> reverseLogicalExpr(LogicalExpression* expr);

    // Negate the given relational expr: (A > B) -> (A <= B)
    static std::unique_ptr<RelationalExpression> reverseRelExpr(RelationalExpression* expr);

    // Return the negation of the given relational kind
    static Expression::Kind getNegatedRelExprKind(const Expression::Kind kind);

    // Return the negation of the given logical kind
    static Expression::Kind getNegatedLogicalExprKind(const Expression::Kind kind);

    static Expression* pullAnds(Expression* expr);
    static void pullAndsImpl(LogicalExpression* expr,
                             std::vector<std::unique_ptr<Expression>>& operands);

    static Expression* pullOrs(Expression* expr);
    static void pullOrsImpl(LogicalExpression* expr,
                            std::vector<std::unique_ptr<Expression>>& operands);

    static VariablePropertyExpression* newVarPropExpr(const std::string& prop,
                                                      const std::string& var = "");

    static std::unique_ptr<InputPropertyExpression> inputPropExpr(const std::string& prop);

    static std::unique_ptr<Expression> pushOrs(
        const std::vector<std::unique_ptr<Expression>>& rels);

    static std::unique_ptr<Expression> pushAnds(
        const std::vector<std::unique_ptr<Expression>>& rels);

    static std::unique_ptr<Expression> pushImpl(
        Expression::Kind kind,
        const std::vector<std::unique_ptr<Expression>>& rels);

    static std::unique_ptr<Expression> expandExpr(const Expression* expr);

    static std::unique_ptr<Expression> expandImplAnd(const Expression* expr);

    static std::vector<std::unique_ptr<Expression>> expandImplOr(const Expression* expr);

    static Status checkAggExpr(const AggregateExpression* aggExpr);
};

}   // namespace graph
}   // namespace nebula

#endif   // _UTIL_EXPRESSION_UTILS_H_
