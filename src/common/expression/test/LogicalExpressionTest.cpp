/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class LogicalExpressionTest : public ExpressionTest {};

TEST_F(LogicalExpressionTest, LogicalCalculation) {
  {
    TEST_EXPR(true, true);
    TEST_EXPR(false, false);
  }
  {
    TEST_EXPR(true XOR true, false);
    TEST_EXPR(true XOR false, true);
    TEST_EXPR(false XOR false, false);
    TEST_EXPR(false XOR true, true);
  }
  {
    TEST_EXPR(true AND true AND true, true);
    TEST_EXPR(true AND true OR false, true);
    TEST_EXPR(true AND true AND false, false);
    TEST_EXPR(true OR false AND true OR false, true);
    TEST_EXPR(true XOR true XOR false, false);
  }
  {
    // AND
    TEST_EXPR(true AND true, true);
    TEST_EXPR(true AND false, false);
    TEST_EXPR(false AND true, false);
    TEST_EXPR(false AND false, false);

    // AND AND  ===  (AND) AND
    TEST_EXPR(true AND true AND true, true);
    TEST_EXPR(true AND true AND false, false);
    TEST_EXPR(true AND false AND true, false);
    TEST_EXPR(true AND false AND false, false);
    TEST_EXPR(false AND true AND true, false);
    TEST_EXPR(false AND true AND false, false);
    TEST_EXPR(false AND false AND true, false);
    TEST_EXPR(false AND false AND false, false);

    // OR
    TEST_EXPR(true OR true, true);
    TEST_EXPR(true OR false, true);
    TEST_EXPR(false OR true, true);
    TEST_EXPR(false OR false, false);

    // OR OR  ===  (OR) OR
    TEST_EXPR(true OR true OR true, true);
    TEST_EXPR(true OR true OR false, true);
    TEST_EXPR(true OR false OR true, true);
    TEST_EXPR(true OR false OR false, true);
    TEST_EXPR(false OR true OR true, true);
    TEST_EXPR(false OR true OR false, true);
    TEST_EXPR(false OR false OR true, true);
    TEST_EXPR(false OR false OR false, false);

    // AND OR  ===  (AND) OR
    TEST_EXPR(true AND true OR true, true);
    TEST_EXPR(true AND true OR false, true);
    TEST_EXPR(true AND false OR true, true);
    TEST_EXPR(true AND false OR false, false);
    TEST_EXPR(false AND true OR true, true);
    TEST_EXPR(false AND true OR false, false);
    TEST_EXPR(false AND false OR true, true);
    TEST_EXPR(false AND false OR false, false);

    // OR AND  === OR (AND)
    TEST_EXPR(true OR true AND true, true);
    TEST_EXPR(true OR true AND false, true);
    TEST_EXPR(true OR false AND true, true);
    TEST_EXPR(true OR false AND false, true);
    TEST_EXPR(false OR true AND true, true);
    TEST_EXPR(false OR true AND false, false);
    TEST_EXPR(false OR false AND true, false);
    TEST_EXPR(false OR false AND false, false);
  }
  {
    TEST_EXPR(2 > 1 AND 3 > 2, true);
    TEST_EXPR(2 <= 1 AND 3 > 2, false);
    TEST_EXPR(2 > 1 AND 3 < 2, false);
    TEST_EXPR(2 < 1 AND 3 < 2, false);
  }
  {
    // test bad null
    TEST_EXPR(2 / 0, Value::kNullDivByZero);
    TEST_EXPR(2 / 0 AND true, Value::kNullDivByZero);
    TEST_EXPR(2 / 0 AND false, Value::Value::kNullDivByZero);
    TEST_EXPR(true AND 2 / 0, Value::kNullDivByZero);
    TEST_EXPR(false AND 2 / 0, false);
    TEST_EXPR(2 / 0 AND 2 / 0, Value::kNullDivByZero);
    {
      // empty AND null AND 2 / 0 AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand3 = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value(2));
      auto *operand5 = ConstantExpression::make(&pool, Value(0));
      auto *operand6 = ArithmeticExpression::makeDivision(&pool, operand4, operand5);
      auto *operand7 = LogicalExpression::makeAnd(&pool, operand3, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand7, operand8);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullDivByZero.type());
      EXPECT_EQ(eval, Value::kNullDivByZero) << "check failed: " << expr->toString();
    }

    TEST_EXPR(2 / 0 OR true, Value::kNullDivByZero);
    TEST_EXPR(2 / 0 OR false, Value::kNullDivByZero);
    TEST_EXPR(true OR 2 / 0, true);
    TEST_EXPR(false OR 2 / 0, Value::kNullDivByZero);
    TEST_EXPR(2 / 0 OR 2 / 0, Value::kNullDivByZero);
    {
      // empty OR null OR 2 / 0 OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand3 = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value(2));
      auto *operand5 = ConstantExpression::make(&pool, Value(0));
      auto *operand6 = ArithmeticExpression::makeDivision(&pool, operand4, operand5);
      auto *operand7 = LogicalExpression::makeOr(&pool, operand3, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand7, operand8);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullDivByZero.type());
      EXPECT_EQ(eval, Value::kNullDivByZero) << "check failed: " << expr->toString();
    }

    TEST_EXPR(2 / 0 XOR true, Value::kNullDivByZero);
    TEST_EXPR(2 / 0 XOR false, Value::kNullDivByZero);
    TEST_EXPR(true XOR 2 / 0, Value::kNullDivByZero);
    TEST_EXPR(false XOR 2 / 0, Value::kNullDivByZero);
    TEST_EXPR(2 / 0 XOR 2 / 0, Value::kNullDivByZero);
    {
      // empty XOR 2 / 0 XOR null XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(2));
      auto *operand3 = ConstantExpression::make(&pool, Value(0));
      auto *operand4 = ArithmeticExpression::makeDivision(&pool, operand2, operand3);
      auto *operand5 = LogicalExpression::makeXor(&pool, operand1, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand7 = LogicalExpression::makeXor(&pool, operand5, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand7, operand8);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullDivByZero.type());
      EXPECT_EQ(eval, Value::kNullDivByZero) << "check failed: " << expr->toString();
    }

    // test normal null
    TEST_EXPR(null AND true, Value::kNullValue);
    TEST_EXPR(null AND false, false);
    TEST_EXPR(true AND null, Value::kNullValue);
    TEST_EXPR(false AND null, false);
    TEST_EXPR(null AND null, Value::kNullValue);
    {
      // empty AND null AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand3 = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand3, operand4);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }

    TEST_EXPR(null OR true, true);
    TEST_EXPR(null OR false, Value::kNullValue);
    TEST_EXPR(true OR null, true);
    TEST_EXPR(false OR null, Value::kNullValue);
    TEST_EXPR(null OR null, Value::kNullValue);
    {
      // empty OR null OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand3 = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand3, operand4);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }

    TEST_EXPR(null XOR true, Value::kNullValue);
    TEST_EXPR(null XOR false, Value::kNullValue);
    TEST_EXPR(true XOR null, Value::kNullValue);
    TEST_EXPR(false XOR null, Value::kNullValue);
    TEST_EXPR(null XOR null, Value::kNullValue);
    {
      // empty XOR null XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand3 = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand3, operand4);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }

    // test empty
    {
      // empty
      auto *expr = ConstantExpression::make(&pool, Value());
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty AND true
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(true));
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty AND false
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value(false).type());
      EXPECT_EQ(eval, Value(false)) << "check failed: " << expr->toString();
    }
    {
      // true AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value(true));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // false AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value(false));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value(false).type());
      EXPECT_EQ(eval, Value(false)) << "check failed: " << expr->toString();
    }
    {
      // empty AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty AND null
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // null AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // empty AND true AND empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(true));
      auto *operand3 = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeAnd(&pool, operand3, operand4);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }

    {
      // empty OR true
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(true));
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value(true).type());
      EXPECT_EQ(eval, Value(true)) << "check failed: " << expr->toString();
    }
    {
      // empty OR false
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // true OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value(true));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value(true).type());
      EXPECT_EQ(eval, Value(true)) << "check failed: " << expr->toString();
    }
    {
      // false OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value(false));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty OR null
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // null OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // empty OR false OR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeOr(&pool, operand3, operand4);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }

    {
      // empty XOR true
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(true));
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty XOR false
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // true XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value(true));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // false XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value(false));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty XOR null
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // null XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // true XOR empty XOR false
      auto *operand1 = ConstantExpression::make(&pool, Value(true));
      auto *operand2 = ConstantExpression::make(&pool, Value());
      auto *operand3 = LogicalExpression::makeXor(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value(false));
      auto *expr = LogicalExpression::makeXor(&pool, operand3, operand4);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }

    {
      // empty OR false AND true AND null XOR empty
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = ConstantExpression::make(&pool, Value(true));
      auto *operand4 = LogicalExpression::makeAnd(&pool, operand2, operand3);
      auto *operand5 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand6 = LogicalExpression::makeAnd(&pool, operand4, operand5);
      auto *operand7 = LogicalExpression::makeOr(&pool, operand1, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value());
      auto *expr = LogicalExpression::makeXor(&pool, operand7, operand8);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kEmpty.type());
      EXPECT_EQ(eval, Value::kEmpty) << "check failed: " << expr->toString();
    }
    {
      // empty OR false AND true XOR empty OR true
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = ConstantExpression::make(&pool, Value(true));
      auto *operand4 = LogicalExpression::makeAnd(&pool, operand2, operand3);
      auto *operand5 = LogicalExpression::makeOr(&pool, operand1, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value());
      auto *operand7 = LogicalExpression::makeXor(&pool, operand5, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value(true));
      auto *expr = LogicalExpression::makeOr(&pool, operand7, operand8);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value(true).type());
      EXPECT_EQ(eval, Value(true)) << "check failed: " << expr->toString();
    }

    // clang-format off
    {
      // (empty OR false) AND true XOR empty XOR null AND 2 / 0
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = LogicalExpression::makeOr(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value(true));
      auto *operand5 = LogicalExpression::makeAnd(&pool, operand3, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value());
      auto *operand7 = LogicalExpression::makeXor(&pool, operand5, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand9 = ConstantExpression::make(&pool, Value(2));
      auto *operand10 = ConstantExpression::make(&pool, Value(0));
      auto *operand11 = ArithmeticExpression::makeDivision(&pool, operand9, operand10);
      auto *operand12 = LogicalExpression::makeAnd(&pool, operand8, operand11);
      auto *expr = LogicalExpression::makeXor(&pool, operand7, operand12);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }

    // clang-format on
    // empty OR false AND 2/0
    {
      // empty OR false AND true XOR empty XOR null AND 2 / 0
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = ConstantExpression::make(&pool, Value(true));
      auto *operand4 = LogicalExpression::makeAnd(&pool, operand2, operand3);
      auto *operand5 = LogicalExpression::makeOr(&pool, operand1, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value());
      auto *operand7 = LogicalExpression::makeXor(&pool, operand5, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand9 = ConstantExpression::make(&pool, Value(2));
      auto *operand10 = ConstantExpression::make(&pool, Value(0));
      auto *operand11 = ArithmeticExpression::makeDivision(&pool, operand9, operand10);
      auto *operand12 = LogicalExpression::makeAnd(&pool, operand8, operand11);
      auto *expr = LogicalExpression::makeXor(&pool, operand7, operand12);
      auto eval = Expression::eval(expr, gExpCtxt);
      // caution
      EXPECT_EQ(eval.type(), Value::kNullDivByZero.type());
      EXPECT_EQ(eval, Value::kNullDivByZero) << "check failed: " << expr->toString();
    }
    {
      // empty AND true XOR empty XOR null AND 2 / 0
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(true));
      auto *operand3 = LogicalExpression::makeAnd(&pool, operand1, operand2);
      auto *operand4 = ConstantExpression::make(&pool, Value());
      auto *operand5 = LogicalExpression::makeXor(&pool, operand3, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand7 = ConstantExpression::make(&pool, Value(2));
      auto *operand8 = ConstantExpression::make(&pool, Value(0));
      auto *operand9 = ArithmeticExpression::makeDivision(&pool, operand7, operand8);
      auto *operand10 = LogicalExpression::makeAnd(&pool, operand6, operand9);
      auto *expr = LogicalExpression::makeXor(&pool, operand5, operand10);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
    {
      // empty OR false AND true XOR empty OR null AND 2 / 0
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = ConstantExpression::make(&pool, Value(true));
      auto *operand4 = LogicalExpression::makeAnd(&pool, operand2, operand3);
      auto *operand5 = LogicalExpression::makeOr(&pool, operand1, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value());
      auto *operand7 = LogicalExpression::makeXor(&pool, operand5, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *operand9 = ConstantExpression::make(&pool, Value(2));
      auto *operand10 = ConstantExpression::make(&pool, Value(0));
      auto *operand11 = ArithmeticExpression::makeDivision(&pool, operand9, operand10);
      auto *operand12 = LogicalExpression::makeAnd(&pool, operand8, operand11);
      auto *expr = LogicalExpression::makeOr(&pool, operand7, operand12);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullDivByZero.type());
      EXPECT_EQ(eval, Value::kNullDivByZero) << "check failed: " << expr->toString();
    }
    {
      // TEST_EXPR(empty OR false AND empty XOR empty OR null, Value::kNullValue);
      // 7 OR null
      auto *operand1 = ConstantExpression::make(&pool, Value());
      auto *operand2 = ConstantExpression::make(&pool, Value(false));
      auto *operand3 = ConstantExpression::make(&pool, Value());
      auto *operand4 = LogicalExpression::makeAnd(&pool, operand2, operand3);
      auto *operand5 = LogicalExpression::makeOr(&pool, operand1, operand4);
      auto *operand6 = ConstantExpression::make(&pool, Value());
      auto *operand7 = LogicalExpression::makeXor(&pool, operand5, operand6);
      auto *operand8 = ConstantExpression::make(&pool, Value(NullType::__NULL__));
      auto *expr = LogicalExpression::makeOr(&pool, operand7, operand8);
      auto eval = Expression::eval(expr, gExpCtxt);
      EXPECT_EQ(eval.type(), Value::kNullValue.type());
      EXPECT_EQ(eval, Value::kNullValue) << "check failed: " << expr->toString();
    }
  }
}
}  // namespace nebula

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
