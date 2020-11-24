/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "visitor/CollectAllExprsVisitor.h"
#include "visitor/FindAnyExprVisitor.h"
#include "visitor/RewriteLabelAttrVisitor.h"

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

    // determine the detail about symbol property expression
    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static void rewriteLabelAttribute(Expression* expr) {
        RewriteLabelAttrVisitor visitor(std::is_same<To, TagPropertyExpression>::value);
        expr->accept(&visitor);
    }

    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static To* rewriteLabelAttribute(LabelAttributeExpression* expr) {
        return new To(new std::string(std::move(*expr->left()->name())),
                      new std::string(std::move(*expr->right()->name())));
    }

    // Clone and fold constant expression
    static std::unique_ptr<Expression> foldConstantExpr(const Expression* expr);

    static Expression* pullAnds(Expression *expr);
    static void pullAndsImpl(LogicalExpression *expr,
                             std::vector<std::unique_ptr<Expression>> &operands);

    static Expression* pullOrs(Expression *expr);
    static void pullOrsImpl(LogicalExpression *expr,
                            std::vector<std::unique_ptr<Expression>> &operands);

    static VariablePropertyExpression* newVarPropExpr(const std::string& prop,
                                                      const std::string& var = "");

    static std::unique_ptr<InputPropertyExpression> inputPropExpr(const std::string& prop);
};

}   // namespace graph
}   // namespace nebula

#endif   // _UTIL_EXPRESSION_UTILS_H_
