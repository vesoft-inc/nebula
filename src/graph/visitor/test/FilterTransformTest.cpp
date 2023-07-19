/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"
#include "common/base/Status.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {
class FilterTransformTest : public VisitorTestBase {};

TEST_F(FilterTransformTest, TestComplexExprRewrite) {
  // !!!(v.age - 1 < 40)  =>  (v.age >= 41)
  auto expr = notExpr(
      notExpr(notExpr(ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(40)))));
  auto res = ExpressionUtils::filterTransform(expr);
  auto expected = geExpr(laExpr("v", "age"), constantExpr(41));
  ASSERT_EQ(*res.value(), *expected) << res.value()->toString() << " vs. " << expected->toString();
}

// If the expression transformation causes an overflow, it should not be done.
TEST_F(FilterTransformTest, TestCalculationOverflow) {
  // (v.age - 1 < 9223372036854775807)  =>  unchanged
  {
    auto expr =
        ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(9223372036854775807));
    auto res = ExpressionUtils::filterTransform(expr);
    ASSERT(res.ok());
    auto expected = expr;
    ASSERT_EQ(res.value()->toString(), expected->toString())
        << res.value()->toString() << " vs. " << expected->toString();
  }
  // (v.age + 1 < -9223372036854775808)  =>  unchanged
  {
    auto expr = ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(INT64_MIN));
    auto res = ExpressionUtils::filterTransform(expr);
    ASSERT(res.ok());
    auto expected = expr;
    ASSERT_EQ(res.value()->toString(), expected->toString())
        << res.value()->toString() << " vs. " << expected->toString();
  }
  // (v.age - 1 < 9223372036854775807 + 1)  =>  overflow
  {
    auto expr = ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)),
                       addExpr(constantExpr(9223372036854775807), constantExpr(1)));
    auto res = ExpressionUtils::filterTransform(expr);
    auto expected = Status::SemanticError(
        "result of (9223372036854775807+1) cannot be represented as an "
        "integer");
    ASSERT(!res.ok());
    ASSERT_EQ(res.status(), expected) << res.status().toString() << " vs. " << expected.toString();
  }
  // (v.age + 1 < -9223372036854775808 - 1)  =>  overflow
  {
    auto expr = ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)),
                       minusExpr(constantExpr(INT64_MIN), constantExpr(1)));
    auto res = ExpressionUtils::filterTransform(expr);
    auto expected = Status::SemanticError(
        "result of (-9223372036854775808-1) cannot be represented as an "
        "integer");
    ASSERT(!res.ok());
    ASSERT_EQ(res.status(), expected) << res.status().toString() << " vs. " << expected.toString();
  }
  // !!!(v.age - 1 < 9223372036854775807)  =>  unchanged
  {
    auto expr = notExpr(notExpr(notExpr(ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)),
                                               constantExpr(9223372036854775807)))));
    auto res = ExpressionUtils::filterTransform(expr);
    ASSERT(res.ok());
    auto expected = expr;
    ASSERT_EQ(res.value()->toString(), expected->toString())
        << res.value()->toString() << " vs. " << expected->toString();
  }
  // !!!(v.age + 1 < -9223372036854775808)  =>  unchanged
  {
    auto expr = notExpr(notExpr(
        notExpr(ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(INT64_MIN)))));
    auto res = ExpressionUtils::filterTransform(expr);
    ASSERT(res.ok());
    auto expected = expr;
    ASSERT_EQ(res.value()->toString(), expected->toString())
        << res.value()->toString() << " vs. " << expected->toString();
  }
}

TEST_F(FilterTransformTest, TestNoRewrite) {
  // Do not rewrite if the filter contains more than one different Label expr
  {
    // (v.age - 1 < v2.age + 2)  =>  Unchanged
    auto expr = ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)),
                       addExpr(laExpr("v2", "age"), constantExpr(40)));
    auto res = ExpressionUtils::filterTransform(expr);
    ASSERT(res.ok());
    auto expected = expr;
    ASSERT_EQ(res.value()->toString(), expected->toString())
        << res.value()->toString() << " vs. " << expected->toString();
  }
  // Do not rewrite if the arithmetic expression contains string/char constant
  {
    // (v.name - "ab" < "name")  =>  Unchanged
    auto expr = ltExpr(minusExpr(laExpr("v", "name"), constantExpr("ab")), constantExpr("name"));
    auto res = ExpressionUtils::filterTransform(expr);
    ASSERT(res.ok());
    auto expected = expr;
    ASSERT_EQ(res.value()->toString(), expected->toString())
        << res.value()->toString() << " vs. " << expected->toString();
  }
}

}  // namespace graph
}  // namespace nebula
