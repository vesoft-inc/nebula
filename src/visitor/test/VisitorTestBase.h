/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _VISITOR_TEST_VISITOR_TEST_BASE_H_
#define _VISITOR_TEST_VISITOR_TEST_BASE_H_

#include <gtest/gtest.h>

#include "common/expression/ExprVisitor.h"

using Type = nebula::Value::Type;

namespace nebula {
namespace graph {

class VisitorTestBase : public ::testing::Test {
    void SetUp() override {
        pool_ = std::make_unique<ObjectPool>();
        pool = pool_.get();
    }

protected:
    ConstantExpression *constantExpr(Value value) {
        return ConstantExpression::make(pool, std::move(value));
    }

    ArithmeticExpression *addExpr(Expression *lhs, Expression *rhs) {
        return ArithmeticExpression::makeAdd(pool, lhs, rhs);
    }

    ArithmeticExpression *minusExpr(Expression *lhs, Expression *rhs) {
        return ArithmeticExpression::makeMinus(pool, lhs, rhs);
    }

    ArithmeticExpression *multiplyExpr(Expression *lhs, Expression *rhs) {
        return ArithmeticExpression::makeMultiply(pool, lhs, rhs);
    }

    ArithmeticExpression *divideExpr(Expression *lhs, Expression *rhs) {
        return ArithmeticExpression::makeDivision(pool, lhs, rhs);
    }

    RelationalExpression *eqExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeEQ(pool, lhs, rhs);
    }

    RelationalExpression *neExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeNE(pool, lhs, rhs);
    }

    RelationalExpression *ltExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeLT(pool, lhs, rhs);
    }

    RelationalExpression *leExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeLE(pool, lhs, rhs);
    }

    RelationalExpression *gtExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeGT(pool, lhs, rhs);
    }

    RelationalExpression *geExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeGE(pool, lhs, rhs);
    }

    RelationalExpression *inExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeIn(pool, lhs, rhs);
    }

    RelationalExpression *notInExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeNotIn(pool, lhs, rhs);
    }

    RelationalExpression *containsExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeContains(pool, lhs, rhs);
    }

    RelationalExpression *notContainsExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeNotContains(pool, lhs, rhs);
    }

    RelationalExpression *startsWithExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeStartsWith(pool, lhs, rhs);
    }

    RelationalExpression *notStartsWithExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeNotStartsWith(pool, lhs, rhs);
    }

    RelationalExpression *endsWithExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeEndsWith(pool, lhs, rhs);
    }

    RelationalExpression *notEndsWithExpr(Expression *lhs, Expression *rhs) {
        return RelationalExpression::makeNotEndsWith(pool, lhs, rhs);
    }

    TypeCastingExpression *castExpr(Type type, Expression *expr) {
        return TypeCastingExpression::make(pool, type, expr);
    }

    UnaryExpression *notExpr(Expression *expr) {
        return UnaryExpression::makeNot(pool, expr);
    }

    LogicalExpression *andExpr(Expression *lhs, Expression *rhs) {
        return LogicalExpression::makeAnd(pool, lhs, rhs);
    }

    LogicalExpression *orExpr(Expression *lhs, Expression *rhs) {
        return LogicalExpression::makeOr(pool, lhs, rhs);
    }

    ListExpression *listExpr(std::initializer_list<Expression *> exprs) {
        auto exprList = ExpressionList::make(pool);
        for (auto expr : exprs) {
            exprList->add(expr);
        }
        return ListExpression::make(pool, exprList);
    }

    SetExpression *setExpr(std::initializer_list<Expression *> exprs) {
        auto exprList = ExpressionList::make(pool);
        for (auto expr : exprs) {
            exprList->add(expr);
        }
        return SetExpression::make(pool, exprList);
    }

    MapExpression *mapExpr(std::initializer_list<std::pair<std::string, Expression *>> exprs) {
        auto mapItemList = MapItemList::make(pool);
        for (auto expr : exprs) {
            mapItemList->add(expr.first, expr.second);
        }
        return MapExpression::make(pool, mapItemList);
    }

    SubscriptExpression *subExpr(Expression *lhs, Expression *rhs) {
        return SubscriptExpression::make(pool, lhs, rhs);
    }

    FunctionCallExpression *fnExpr(std::string fn, std::initializer_list<Expression *> args) {
        auto argsList = ArgumentList::make(pool);
        for (auto arg : args) {
            argsList->addArgument(arg);
        }
        return FunctionCallExpression::make(pool, std::move(fn), argsList);
    }

    VariableExpression *varExpr(const std::string &name) {
        return VariableExpression::make(pool, name);
    }

    CaseExpression *caseExpr(Expression *cond,
                             Expression *defaltResult,
                             Expression *when,
                             Expression *then) {
        auto caseList = CaseList::make(pool);
        caseList->add(when, then);
        auto expr = CaseExpression::make(pool, caseList);
        expr->setCondition(cond);
        expr->setDefault(defaltResult);
        return expr;
    }

    LabelExpression *labelExpr(const std::string &name) {
        return LabelExpression::make(pool, name);
    }

    LabelAttributeExpression *laExpr(const std::string &name, Value value) {
        return LabelAttributeExpression::make(pool,
                                              LabelExpression::make(pool, name),
                                              ConstantExpression::make(pool, std::move(value)));
    }

protected:
    std::unique_ptr<ObjectPool> pool_;
    ObjectPool *pool;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_TEST_VISITORTESTBASE_H_
