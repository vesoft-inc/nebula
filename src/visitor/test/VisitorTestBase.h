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

class ValidatorTestBase : public ::testing::Test {
protected:
    static ConstantExpression *constantExpr(Value value) {
        return new ConstantExpression(std::move(value));
    }

    static ArithmeticExpression *addExpr(Expression *lhs, Expression *rhs) {
        return new ArithmeticExpression(Expression::Kind::kAdd, lhs, rhs);
    }

    static ArithmeticExpression *minusExpr(Expression *lhs, Expression *rhs) {
        return new ArithmeticExpression(Expression::Kind::kMinus, lhs, rhs);
    }

    static ArithmeticExpression *multiplyExpr(Expression *lhs, Expression *rhs) {
        return new ArithmeticExpression(Expression::Kind::kMultiply, lhs, rhs);
    }

    static ArithmeticExpression *divideExpr(Expression *lhs, Expression *rhs) {
        return new ArithmeticExpression(Expression::Kind::kDivision, lhs, rhs);
    }

    static RelationalExpression *eqExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelEQ, lhs, rhs);
    }

    static RelationalExpression *neExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelNE, lhs, rhs);
    }

    static RelationalExpression *ltExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelLT, lhs, rhs);
    }

    static RelationalExpression *leExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelLE, lhs, rhs);
    }

    static RelationalExpression *gtExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelGT, lhs, rhs);
    }

    static RelationalExpression *geExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelGE, lhs, rhs);
    }

    static RelationalExpression *inExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelIn, lhs, rhs);
    }

    static RelationalExpression *notInExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelNotIn, lhs, rhs);
    }

    static RelationalExpression *containsExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kContains, lhs, rhs);
    }

    static RelationalExpression *notContainsExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kNotContains, lhs, rhs);
    }

    static RelationalExpression *startsWithExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kStartsWith, lhs, rhs);
    }

    static RelationalExpression *notStartsWithExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kNotStartsWith, lhs, rhs);
    }

    static RelationalExpression *endsWithExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kEndsWith, lhs, rhs);
    }

    static RelationalExpression *notEndsWithExpr(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kNotEndsWith, lhs, rhs);
    }

    static TypeCastingExpression *castExpr(Type type, Expression *expr) {
        return new TypeCastingExpression(type, expr);
    }

    static UnaryExpression *notExpr(Expression *expr) {
        return new UnaryExpression(Expression::Kind::kUnaryNot, expr);
    }

    static LogicalExpression *andExpr(Expression *lhs, Expression *rhs) {
        return new LogicalExpression(Expression::Kind::kLogicalAnd, lhs, rhs);
    }

    static LogicalExpression *orExpr(Expression *lhs, Expression *rhs) {
        return new LogicalExpression(Expression::Kind::kLogicalOr, lhs, rhs);
    }

    static ListExpression *listExpr(std::initializer_list<Expression *> exprs) {
        auto exprList = new ExpressionList;
        for (auto expr : exprs) {
            exprList->add(expr);
        }
        return new ListExpression(exprList);
    }

    static SetExpression *setExpr(std::initializer_list<Expression *> exprs) {
        auto exprList = new ExpressionList;
        for (auto expr : exprs) {
            exprList->add(expr);
        }
        return new SetExpression(exprList);
    }

    static MapExpression *mapExpr(
        std::initializer_list<std::pair<std::string, Expression *>> exprs) {
        auto mapItemList = new MapItemList;
        for (auto expr : exprs) {
            mapItemList->add(new std::string(expr.first), expr.second);
        }
        return new MapExpression(mapItemList);
    }

    static SubscriptExpression *subExpr(Expression *lhs, Expression *rhs) {
        return new SubscriptExpression(lhs, rhs);
    }

    static FunctionCallExpression *fnExpr(std::string fn,
                                          std::initializer_list<Expression *> args) {
        auto argsList = new ArgumentList;
        for (auto arg : args) {
            argsList->addArgument(std::unique_ptr<Expression>(arg));
        }
        return new FunctionCallExpression(new std::string(std::move(fn)), argsList);
    }

    static VariableExpression *varExpr(const std::string &name) {
        return new VariableExpression(new std::string(name));
    }

    static CaseExpression *caseExpr(Expression *cond,
                                    Expression *defaltResult,
                                    Expression *when,
                                    Expression *then) {
        auto caseList = new CaseList;
        caseList->add(when, then);
        auto expr = new CaseExpression(caseList);
        expr->setCondition(cond);
        expr->setDefault(defaltResult);
        return expr;
    }

    static LabelExpression *labelExpr(const std::string &name) {
        return new LabelExpression(new std::string(name));
    }

    static LabelAttributeExpression *laExpr(const std::string &name, Value value) {
        return new LabelAttributeExpression(new LabelExpression(new std::string(name)),
                                            new ConstantExpression(std::move(value)));
    }
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_TEST_VISITORTESTBASE_H_
