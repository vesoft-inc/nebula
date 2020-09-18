
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/FoldConstantExprVisitor.h"

#include <gtest/gtest.h>

#include "util/ObjectPool.h"

using Type = nebula::Value::Type;

namespace nebula {
namespace graph {

class FoldConstantExprVisitorTest : public ::testing::Test {
public:
    void TearDown() override {
        pool.clear();
    }

    static ConstantExpression *constant(Value value) {
        return new ConstantExpression(std::move(value));
    }

    static ArithmeticExpression *add(Expression *lhs, Expression *rhs) {
        return new ArithmeticExpression(Expression::Kind::kAdd, lhs, rhs);
    }

    static ArithmeticExpression *minus(Expression *lhs, Expression *rhs) {
        return new ArithmeticExpression(Expression::Kind::kMinus, lhs, rhs);
    }

    static RelationalExpression *gt(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelGT, lhs, rhs);
    }

    static RelationalExpression *eq(Expression *lhs, Expression *rhs) {
        return new RelationalExpression(Expression::Kind::kRelEQ, lhs, rhs);
    }

    static TypeCastingExpression *cast(Type type, Expression *expr) {
        return new TypeCastingExpression(type, expr);
    }

    static UnaryExpression *not_(Expression *expr) {
        return new UnaryExpression(Expression::Kind::kUnaryNot, expr);
    }

    static LogicalExpression *and_(Expression *lhs, Expression *rhs) {
        return new LogicalExpression(Expression::Kind::kLogicalAnd, lhs, rhs);
    }

    static LogicalExpression *or_(Expression *lhs, Expression *rhs) {
        return new LogicalExpression(Expression::Kind::kLogicalOr, lhs, rhs);
    }

    static ListExpression *list_(std::initializer_list<Expression *> exprs) {
        auto exprList = new ExpressionList;
        for (auto expr : exprs) {
            exprList->add(expr);
        }
        return new ListExpression(exprList);
    }

    static SubscriptExpression *sub(Expression *lhs, Expression *rhs) {
        return new SubscriptExpression(lhs, rhs);
    }

    static FunctionCallExpression *fn(std::string fn, std::initializer_list<Expression *> args) {
        auto argsList = new ArgumentList;
        for (auto arg : args) {
            argsList->addArgument(std::unique_ptr<Expression>(arg));
        }
        return new FunctionCallExpression(new std::string(std::move(fn)), argsList);
    }

    static VariableExpression *var(const std::string &name) {
        return new VariableExpression(new std::string(name));
    }

protected:
    ObjectPool pool;
};

TEST_F(FoldConstantExprVisitorTest, TestArithmeticExpr) {
    // (5 - 1) + 2 => 4 + 2
    auto expr = pool.add(add(minus(constant(5), constant(1)), constant(2)));
    FoldConstantExprVisitor visitor;
    expr->accept(&visitor);
    auto expected = pool.add(add(constant(4), constant(2)));
    ASSERT_EQ(*expr, *expected) << expr->toString() << " vs. " << expected->toString();
    ASSERT(visitor.canBeFolded());

    // 4+2 => 6
    auto root = pool.add(visitor.fold(expr));
    auto rootExpected = pool.add(constant(6));
    ASSERT_EQ(*root, *rootExpected) << root->toString() << " vs. " << rootExpected->toString();
}

TEST_F(FoldConstantExprVisitorTest, TestRelationExpr) {
    // false == !(3 > (1+1)) => false == false
    auto expr = pool.add(eq(constant(false), not_(gt(constant(3), add(constant(1), constant(1))))));
    auto expected = pool.add(eq(constant(false), constant(false)));
    FoldConstantExprVisitor visitor;
    expr->accept(&visitor);
    ASSERT_EQ(*expr, *expected) << expr->toString() << " vs. " << expected->toString();
    ASSERT(visitor.canBeFolded());

    // false==false => true
    auto root = pool.add(visitor.fold(expr));
    auto rootExpected = pool.add(constant(true));
    ASSERT_EQ(*root, *rootExpected) << root->toString() << " vs. " << rootExpected->toString();
}

TEST_F(FoldConstantExprVisitorTest, TestLogicalExpr) {
    // false && (false || (3 > (1 + 1))) => false && true
    auto expr = pool.add(and_(
        constant(false), or_(constant(false), gt(constant(3), add(constant(1), constant(1))))));
    auto expected = pool.add(and_(constant(false), constant(true)));
    FoldConstantExprVisitor visitor;
    expr->accept(&visitor);
    ASSERT_EQ(*expr, *expected) << expr->toString() << " vs. " << expected->toString();
    ASSERT(visitor.canBeFolded());

    // false && true => false
    auto root = pool.add(visitor.fold(expr));
    auto rootExpected = pool.add(constant(false));
    ASSERT_EQ(*root, *rootExpected) << root->toString() << " vs. " << rootExpected->toString();
}

TEST_F(FoldConstantExprVisitorTest, TestSubscriptExpr) {
    // 1 + [1, pow(2, 2+1), 2][2-1] => 1 + 8
    auto expr = pool.add(add(constant(1),
                             sub(list_({constant(1),
                                        fn("pow", {constant(2), add(constant(2), constant(1))}),
                                        constant(2)}),
                                 minus(constant(2), constant(1)))));
    auto expected = pool.add(add(constant(1), constant(8)));
    FoldConstantExprVisitor visitor;
    expr->accept(&visitor);
    ASSERT_EQ(*expr, *expected) << expr->toString() << " vs. " << expected->toString();
    ASSERT(visitor.canBeFolded());

    // 1+8 => 9
    auto root = pool.add(visitor.fold(expr));
    auto rootExpected = pool.add(constant(9));
    ASSERT_EQ(*root, *rootExpected) << root->toString() << " vs. " << rootExpected->toString();
}

TEST_F(FoldConstantExprVisitorTest, TestFoldFailed) {
    // function call
    {
        // pow($v, (1+2)) => pow($v, 3)
        auto expr = pool.add(fn("pow", {var("v"), add(constant(1), constant(2))}));
        auto expected = pool.add(fn("pow", {var("v"), constant(3)}));
        FoldConstantExprVisitor visitor;
        expr->accept(&visitor);
        ASSERT_EQ(*expr, *expected) << expr->toString() << " vs. " << expected->toString();
        ASSERT_FALSE(visitor.canBeFolded());
    }
    // list
    {
        // [$v, pow(1, 2), 1+2][2-1] => [$v, 1, 3][0]
        auto expr = pool.add(sub(
            list_({var("v"), fn("pow", {constant(1), constant(2)}), add(constant(1), constant(2))}),
            minus(constant(1), constant(1))));
        auto expected = pool.add(sub(list_({var("v"), constant(1), constant(3)}), constant(0)));
        FoldConstantExprVisitor visitor;
        expr->accept(&visitor);
        ASSERT_EQ(*expr, *expected) << expr->toString() << " vs. " << expected->toString();
        ASSERT_FALSE(visitor.canBeFolded());
    }
}

}   // namespace graph
}   // namespace nebula
