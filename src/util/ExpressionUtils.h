/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/base/Status.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "visitor/EvaluableExprVisitor.h"
#include "visitor/FindVisitor.h"
#include "visitor/RewriteVisitor.h"

namespace nebula {
class ObjectPool;
namespace graph {

class ExpressionUtils {
public:
    explicit ExpressionUtils(...) = delete;

    static inline bool isKindOf(const Expression* expr,
                                const std::unordered_set<Expression::Kind>& expected) {
        return expected.find(expr->kind()) != expected.end();
    }

    static bool isPropertyExpr(const Expression* expr);

    static const Expression* findAny(const Expression* self,
                                     const std::unordered_set<Expression::Kind>& expected);

    static bool hasAny(const Expression* expr,
                       const std::unordered_set<Expression::Kind>& expected) {
        return findAny(expr, expected) != nullptr;
    }

    static std::vector<const Expression*> collectAll(
        const Expression* self,
        const std::unordered_set<Expression::Kind>& expected);

    static std::vector<const Expression*> findAllStorage(const Expression* expr);

    static std::vector<const Expression*> findAllInputVariableProp(const Expression* expr);

    // **Expression type check**
    static bool isConstExpr(const Expression* expr);

    static bool isEvaluableExpr(const Expression* expr);

    static Expression* rewriteLabelAttr2TagProp(const Expression* expr);

    static Expression* rewriteLabelAttr2EdgeProp(const Expression* expr);

    static Expression* rewriteAgg2VarProp(const Expression* expr);

    static std::unique_ptr<Expression> rewriteInnerVar(const Expression* expr,
                                                       std::string newVar);

    // Rewrite relational expression, gather evaluable expressions to one side
    static Expression* rewriteRelExpr(const Expression* expr, ObjectPool* pool);
    static Expression* rewriteRelExprHelper(const Expression* expr,
                                            std::unique_ptr<Expression>& relRightOperandExpr);

    // Clone and fold constant expression
    static Expression* foldConstantExpr(const Expression* expr, ObjectPool* objPool);

    // Clone and reduce unaryNot expression
    static Expression* reduceUnaryNotExpr(const Expression* expr, ObjectPool* pool);

    // Transform filter using multiple expression rewrite strategies
    static Expression* filterTransform(const Expression* expr, ObjectPool* objPool);

    // Negate the given logical expr: (A && B) -> (!A || !B)
    static std::unique_ptr<LogicalExpression> reverseLogicalExpr(LogicalExpression* expr);

    // Negate the given relational expr: (A > B) -> (A <= B)
    static std::unique_ptr<RelationalExpression> reverseRelExpr(RelationalExpression* expr);

    // Return the negation of the given relational kind
    static Expression::Kind getNegatedRelExprKind(const Expression::Kind kind);

    // Return the negation of the given logical kind
    static Expression::Kind getNegatedLogicalExprKind(const Expression::Kind kind);

    // Return the negation of the given arithmetic kind: plus -> minus
    static Expression::Kind getNegatedArithmeticType(const Expression::Kind kind);

    static void pullAnds(Expression* expr);

    static void pullAndsImpl(LogicalExpression* expr,
                             std::vector<std::unique_ptr<Expression>>& operands);

    static void pullOrs(Expression* expr);
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

    static std::unique_ptr<Expression> flattenInnerLogicalAndExpr(const Expression* expr);

    static std::unique_ptr<Expression> flattenInnerLogicalOrExpr(const Expression* expr);

    static std::unique_ptr<Expression> flattenInnerLogicalExpr(const Expression* expr);

    static void splitFilter(const Expression* expr,
                            std::function<bool(const Expression*)> picker,
                            std::unique_ptr<Expression>* filterPicked,
                            std::unique_ptr<Expression>* filterUnpicked);

    static std::unique_ptr<Expression> expandExpr(const Expression* expr);

    static std::unique_ptr<Expression> expandImplAnd(const Expression* expr);

    static std::vector<std::unique_ptr<Expression>> expandImplOr(const Expression* expr);

    static Status checkAggExpr(const AggregateExpression* aggExpr);

    static bool findInnerRandFunction(const Expression *expr);

    static Expression* And(Expression *l, Expression* r) {
        return new LogicalExpression(Expression::Kind::kLogicalAnd, l, r);
    }

    static Expression* Or(Expression* l, Expression *r) {
        return new LogicalExpression(Expression::Kind::kLogicalOr, l, r);
    }

    static Expression* Eq(Expression* l, Expression *r) {
        return new RelationalExpression(Expression::Kind::kRelEQ, l, r);
    }

    // loop condition
    // ++loopSteps <= steps
    static std::unique_ptr<Expression> stepCondition(const std::string& loopStep, uint32_t steps);

    // size(var) == 0
    static std::unique_ptr<Expression> zeroCondition(const std::string& var);

    // size(var) != 0
    static std::unique_ptr<Expression> neZeroCondition(const std::string& var);

    // var == value
    static std::unique_ptr<Expression> equalCondition(const std::string& var, const Value& value);
};

}   // namespace graph
}   // namespace nebula

#endif   // _UTIL_EXPRESSION_UTILS_H_
