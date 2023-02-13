/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"
#include "common/expression/Expression.h"
#include "common/expression/PropertyExpression.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"
#include "graph/visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {

class ExtractFilterExprVisitorTest : public VisitorTestBase {
 public:
  SourcePropertyExpression* getPushableExpr(const std::string& tag, const std::string& prop) {
    return SourcePropertyExpression::make(pool_.get(), tag, prop);
  }

  DestPropertyExpression* getCannotPushExpr(const std::string& tag, const std::string& prop) {
    return DestPropertyExpression::make(pool_.get(), tag, prop);
  }

  void checkIfEqual(Expression* expr, Expression* expect, Expression* remain, bool canBePush) {
    std::string exprInfo = "The original expr is: " + expr->toString();
    ExtractFilterExprVisitor visitor(pool_.get());
    expr->accept(&visitor);
    ASSERT_EQ(visitor.ok(), canBePush) << exprInfo;
    ASSERT_EQ(expect->toString(), expr->toString()) << exprInfo;
    if (remain == nullptr) {
      ASSERT_EQ(nullptr, std::move(visitor).remainedExpr())
          << "The remainedExpr is supposed to be nullptr!";
    } else {
      ASSERT_EQ(remain->toString(), std::move(visitor).remainedExpr()->toString()) << exprInfo;
    }
  }

  LogicalExpression* getAndExpr(Expression* operand) {
    auto expr = LogicalExpression::makeAnd(pool_.get());
    expr->addOperand(operand);
    return expr;
  }

  LogicalExpression* getOrExpr(Expression* operand) {
    auto expr = LogicalExpression::makeOr(pool_.get());
    expr->addOperand(operand);
    return expr;
  }

  void getExprVec(std::vector<Expression*>& A, std::vector<Expression*>& B, int A_num, int B_num) {
    A.resize(A_num + 1);
    B.resize(B_num + 1);
    for (int i = 1; i <= A_num; i++) {
      A[i] = getPushableExpr("A", std::to_string(i));
    }
    for (int i = 1; i <= B_num; i++) {
      B[i] = getCannotPushExpr("B", std::to_string(i));
    }
    A.insert(A.end(), B.begin(), B.end());
  }

  Expression* getOrs(std::vector<Expression*>& A,
                     std::vector<int> index,
                     Expression* expr = nullptr) {
    DCHECK_GT(index.size(), 1);
    Expression* newExpr = orExpr(A[index[0]], A[index[1]]);
    for (auto i = 2u; i < index.size(); i++) {
      Expression* tmp = orExpr(newExpr, A[index[i]]);
      newExpr = std::move(tmp);
    }
    if (expr != nullptr) {
      newExpr = orExpr(newExpr, expr);
    }
    ExpressionUtils::pullOrs(newExpr);
    return newExpr;
  }

  Expression* getAnds(std::vector<Expression*>& A,
                      std::vector<int> index,
                      Expression* expr = nullptr) {
    DCHECK_GT(index.size(), 1);
    Expression* newExpr = andExpr(A[index[0]], A[index[1]]);
    for (auto i = 2u; i < index.size(); i++) {
      Expression* tmp = andExpr(newExpr, A[index[i]]);
      newExpr = std::move(tmp);
    }
    if (expr != nullptr) {
      newExpr = andExpr(newExpr, expr);
    }
    ExpressionUtils::pullAnds(newExpr);
    return newExpr;
  }
};

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushNoAnd) {
  // true
  ExtractFilterExprVisitor visitor(pool_.get());
  auto expr = constantExpr(true);
  expr->accept(&visitor);
  ASSERT_TRUE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushAnd) {
  // true AND false
  ExtractFilterExprVisitor visitor(pool_.get());
  auto expr = andExpr(constantExpr(true), constantExpr(false));
  expr->accept(&visitor);
  ASSERT_TRUE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushOr) {
  // true OR false
  ExtractFilterExprVisitor visitor(pool_.get());
  auto expr = orExpr(constantExpr(true), constantExpr(false));
  expr->accept(&visitor);
  ASSERT_TRUE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanNotBePushNoAnd) {
  // $$.player.name
  ExtractFilterExprVisitor visitor(pool_.get());
  auto expr = DestPropertyExpression::make(pool_.get(), "player", "name");
  expr->accept(&visitor);
  ASSERT_FALSE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanNotBePushAnd) {
  // $$.player.name AND $var.name
  ExtractFilterExprVisitor visitor(pool_.get());
  auto dstExpr = DestPropertyExpression::make(pool_.get(), "player", "name");
  auto varExpr = VariablePropertyExpression::make(pool_.get(), "var", "name");
  auto expr = andExpr(dstExpr, varExpr);
  expr->accept(&visitor);
  ASSERT_FALSE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanNotBePushOr) {
  // $$.player.name OR $var.name
  ExtractFilterExprVisitor visitor(pool_.get());
  auto dstExpr = DestPropertyExpression::make(pool_.get(), "player", "name");
  auto varExpr = VariablePropertyExpression::make(pool_.get(), "var", "name");
  auto expr = orExpr(dstExpr, varExpr);
  expr->accept(&visitor);
  ASSERT_FALSE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushSomeAnd) {
  // $$.player.name AND true
  ExtractFilterExprVisitor visitor(pool_.get());
  auto dstExpr = DestPropertyExpression::make(pool_.get(), "player", "name");
  auto expr = andExpr(dstExpr, constantExpr(true));
  expr->accept(&visitor);
  ASSERT_TRUE(visitor.ok());
  auto rmExpr = std::move(visitor).remainedExpr();
  ASSERT_EQ(rmExpr->kind(), Expression::Kind::kDstProperty);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushSomeOr) {
  // $$.player.name OR $$.player.name
  ExtractFilterExprVisitor visitor(pool_.get());
  auto dstExpr = DestPropertyExpression::make(pool_.get(), "player", "name");
  auto expr = orExpr(dstExpr, constantExpr(true));
  expr->accept(&visitor);
  ASSERT_FALSE(visitor.ok());
  ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushSomeAndOr) {
  // $$.player.name AND (true OR $^.player.name)
  ExtractFilterExprVisitor visitor(pool_.get());
  auto dstExpr = DestPropertyExpression::make(pool_.get(), "player", "name");
  auto srcExpr = SourcePropertyExpression::make(pool_.get(), "player", "name");
  auto rExpr = orExpr(srcExpr, constantExpr(true));
  auto expr = andExpr(dstExpr, rExpr);
  expr->accept(&visitor);
  ASSERT_TRUE(visitor.ok());
  auto rmexpr = std::move(visitor).remainedExpr();
  ASSERT_EQ(rmexpr->kind(), Expression::Kind::kDstProperty);
  ASSERT_EQ(expr->kind(), Expression::Kind::kLogicalAnd);
  // auto newExpr = ExpressionUtils::foldConstantExpr(expr, &pool_);
  // ASSERT_EQ(newExpr->kind(), Expression::Kind::kLogicalOr);
}

// let A indicates (can be pushed expr), and B denotes (can not push expr) in the following.
TEST_F(ExtractFilterExprVisitorTest, TestSingleExpr) {
  {
    auto A = getPushableExpr("A", "1");
    auto expr = getAndExpr(A);
    auto expect = expr->clone();
    auto remain = nullptr;
    checkIfEqual(expr, expect, remain, true);
  }
  {
    auto A = getPushableExpr("A", "1");
    auto expr = getOrExpr(A);
    auto expect = expr->clone();
    auto remain = nullptr;
    checkIfEqual(expr, expect, remain, true);
  }
  {
    auto B = getCannotPushExpr("B", "1");
    auto expr = getAndExpr(B);
    auto expect = expr->clone();
    auto remain = nullptr;
    checkIfEqual(expr, expect, remain, false);
  }
  {
    auto B = getCannotPushExpr("B", "1");
    auto expr = getOrExpr(B);
    auto expect = expr->clone();
    auto remain = nullptr;
    checkIfEqual(expr, expect, remain, false);
  }
}

TEST_F(ExtractFilterExprVisitorTest, TestMultiAndExpr) {
  {
    // (A1 and B2) and B3
    // expect: can push
    // expect expr: A1
    // remain: and(B2, B3)
    auto A1 = getPushableExpr("A", "1");
    auto B2 = getCannotPushExpr("B", "2");
    auto B3 = getCannotPushExpr("B", "3");

    auto expr = andExpr(andExpr(A1, B2), B3);
    auto expect = getAndExpr(A1);
    auto remain = andExpr(B2, B3);

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // B1 and B2 and A1 and B3 and A2 and B4
    // expect: can push
    // expect expr: A1 and A2
    // remain: and(B1, B2, B3, B4)
    std::vector<Expression*> A, B;
    getExprVec(A, B, 2, 4);

    auto expect = andExpr(A[1], A[2]);
    auto remain = getAnds(B, {1, 2, 3, 4});

    auto expr = getAnds(A, {4, 5, 1, 6, 2, 7});

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // A1 and (A2 or (A3 and B1 and A5 and B2) or A6) and A7 and B3
    // expect: can push
    // expect expr: and(A1, or(A2, A6, (A3 and A5)), A7)
    // remain: B3 and or(A2, A6, (B1 and B2))

    std::vector<Expression*> A, B;
    getExprVec(A, B, 7, 3);

    auto a3b2 = getAnds(A, {3, 9, 5, 10});
    auto a2a6 = orExpr(A[2], orExpr(a3b2, A[6]));
    auto expr = andExpr(andExpr(A[1], a2a6), andExpr(A[7], B[3]));

    auto expect1 = getOrs(A, {2, 6}, andExpr(A[3], A[5]));
    auto expect = andExpr(A[1], andExpr(expect1, A[7]));
    ExpressionUtils::pullAnds(expect);

    auto remain = andExpr(B[3], getOrs(A, {2, 6}, andExpr(B[1], B[2])));

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // (A1 or (A2 and B1 and B2) or A3) and A4 and (A5 or (A6 and B3 and B4))
    // expect: can push
    // expect expr: and(or(A1, A3, A2), A4, or(A5, A6)）
    // remain: or(A1, A3, (B1 and B2)) and or(A5, (B3 and B4))

    std::vector<Expression*> A, B;
    getExprVec(A, B, 6, 4);

    auto a1b2 = getAnds(A, {2, 8, 9});
    auto a1a3 = orExpr(orExpr(A[1], a1b2), A[3]);
    auto a5b4 = orExpr(A[5], getAnds(A, {6, 10, 11}));
    auto expr = andExpr(andExpr(a1a3, A[4]), a5b4);

    auto a1a2 = getOrs(A, {1, 3, 2});
    auto expect = andExpr(a1a2, andExpr(A[4], orExpr(A[5], A[6])));
    ExpressionUtils::pullAnds(expect);

    auto remain1 = orExpr(A[1], orExpr(A[3], andExpr(B[1], B[2])));
    ExpressionUtils::pullOrs(remain1);
    auto remain2 = orExpr(A[5], andExpr(B[3], B[4]));
    auto remain = andExpr(remain1, remain2);

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // B1 and (A1 or (A2 and B2))) and B3
    // expect: can push
    // expect expr: (A1 or A2)
    // remain: B1 and B3 and (A1 o B2)

    std::vector<Expression*> A, B;
    getExprVec(A, B, 2, 3);

    auto a1b2 = orExpr(A[1], andExpr(A[2], B[2]));
    auto expr = andExpr(B[1], andExpr(a1b2, B[3]));
    auto expect = getAndExpr(orExpr(A[1], A[2]));
    auto remain = andExpr(B[1], andExpr(B[3], orExpr(A[1], B[2])));
    ExpressionUtils::pullAnds(remain);

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // A1 or (A2 and (A3 or (A4 and B1))) or A5
    // expect: can push
    // expect expr: or(A1, A5, and(A2, or(A3, A4)))
    // remain: or(A1, A5, A3, B1)
    std::vector<Expression*> A, B;
    getExprVec(A, B, 5, 1);

    auto a2b1 = andExpr(A[2], orExpr(A[3], andExpr(A[4], B[1])));
    auto expr = orExpr(orExpr(A[1], a2b1), A[5]);

    auto expect = orExpr(A[1], orExpr(A[5], andExpr(A[2], orExpr(A[3], A[4]))));
    ExpressionUtils::pullOrs(expect);
    expect = getOrExpr(expect);
    auto remain = getOrs(A, {1, 5, 3, 7});

    checkIfEqual(expr, expect, remain, true);
  }
}

TEST_F(ExtractFilterExprVisitorTest, TestMultiOrExpr) {
  {
    // A1 or (A2 and B3)
    // expect: can push
    // expect expr: A1 or A2
    // remain: A1 or B3
    auto A1 = getPushableExpr("A", "1");
    auto A2 = getPushableExpr("A", "2");
    auto B3 = getCannotPushExpr("B", "3");

    auto expr = orExpr(A1, andExpr(A2, B3));
    auto expect = getOrExpr(orExpr(A1, A2));
    auto remain = orExpr(A1, B3);

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // A1 or (A2 and B1) or A3 or A4
    // expect: can push
    // expect expr: A1 or A3 or A4 or A2
    // remain: A1 or A3 or A4 or B1
    std::vector<Expression*> A, B;
    getExprVec(A, B, 4, 1);

    auto a2b1 = andExpr(A[2], B[1]);

    auto expr1 = orExpr(A[1], a2b1);
    auto expr = orExpr(expr1, getOrs(A, {3, 4}));

    auto expect = getOrs(A, {1, 3, 4, 2});
    expect = getOrExpr(expect);
    auto remain = orExpr(orExpr(A[1], A[3]), orExpr(A[4], B[1]));
    ExpressionUtils::pullOrs(remain);

    checkIfEqual(expr, expect, remain, true);
  }
  {
    // A1 or (A2 and B1 and B2 and A3 and B3) or A4 or (A5 and A6)
    // expect: can push
    // expect expr: A1 or A4 or (A5 and A6) or (A2 and A3)
    // remain: A1 or A4 or or (A5 and A6) or (B1 and B2 and B3)
    std::vector<Expression*> A, B;
    getExprVec(A, B, 6, 4);

    auto expr1 = getAnds(A, {2, 8, 9, 3, 10});
    auto expr2 = andExpr(A[5], A[6]);
    auto expr = orExpr(orExpr(A[1], expr1), orExpr(A[4], expr2));

    auto expect1 = getOrs(A, {1, 4});
    auto expect2 = orExpr(expect1, orExpr(expr2, andExpr(A[2], A[3])));
    ExpressionUtils::pullOrs(expect2);
    auto expect = getOrExpr(expect2);
    auto remain = orExpr(orExpr(expect1, expr2), getAnds(A, {8, 9, 10}));
    ExpressionUtils::pullOrs(remain);

    checkIfEqual(expr, expect, remain, true);
  }
}

TEST_F(ExtractFilterExprVisitorTest, TestMultiCanNotPush) {
  {
    // A1 or B1 or A2
    // expect: can not push
    // expect expr: A1 or B1 or A2
    // remain: nullptr
    std::vector<Expression*> A, B;
    getExprVec(A, B, 2, 1);

    auto expr = orExpr(A[1], orExpr(B[1], A[2]));

    auto expect = expr->clone();
    ExpressionUtils::pullOrs(expect);
    auto remain = nullptr;

    checkIfEqual(expr, expect, remain, false);
  }
  {
    // B1 and B2 and B3
    // expect: can not push
    // expect expr: B1 and B2 and B3
    // remain: nullptr
    std::vector<Expression*> A, B;
    getExprVec(A, B, 0, 3);

    auto expr = andExpr(B[1], andExpr(B[2], B[3]));

    auto expect = expr->clone();
    ExpressionUtils::pullAnds(expect);
    auto remain = nullptr;

    checkIfEqual(expr, expect, remain, false);
  }
  {
    // A1 or (A2 and B1) or (A3 and B2) or A4
    // expect: can not push
    // expect expr: A1 or (A2 and B1) or (A3 and B2) or A4
    // remain: nullptr
    std::vector<Expression*> A, B;
    getExprVec(A, B, 4, 2);

    auto expr = orExpr(orExpr(A[1], andExpr(A[2], B[1])), orExpr(andExpr(A[3], B[2]), A[4]));

    auto expect = expr->clone();
    ExpressionUtils::pullOrs(expect);
    auto remain = nullptr;

    checkIfEqual(expr, expect, remain, false);
  }
  {
    // A1 or (A2 and (A3 or (A4 and B1))) or B2
    // expect: can not push
    // expect expr: A1 or (A2 and (A3 or (A4 and B1))) or B2
    // remain: nullptr
    // comment: has been split, however, the total expr can not push
    std::vector<Expression*> A, B;
    getExprVec(A, B, 4, 2);

    auto a2b1 = andExpr(A[2], orExpr(A[3], andExpr(A[4], B[1])));
    auto expr = orExpr(orExpr(A[1], a2b1), B[2]);

    auto expect = expr->clone();
    auto remain = nullptr;

    checkIfEqual(expr, expect, remain, false);
  }
}

TEST_F(ExtractFilterExprVisitorTest, TestPushLabelTagPropExpr) {
  // $-.input.tag.prop can't push down
  auto expr = LabelTagPropertyExpression::make(
      pool_.get(), InputPropertyExpression::make(pool_.get(), "input"), "tag", "prop");
  checkIfEqual(expr, expr->clone(), nullptr, false);
}

}  // namespace graph
}  // namespace nebula
