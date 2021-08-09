/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"
#include "visitor/test/VisitorTestBase.h"

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"
#include "common/base/Status.h"

namespace nebula {
namespace graph {
class FilterTransformTest : public VisitorTestBase {};

TEST_F(FilterTransformTest, TestComplexExprRewrite) {
    // !!!(v.age - 1 < 40)  =>  (v.age >= 41)
    auto expr = notExpr(notExpr(
        notExpr(ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(40)))));
    auto res = ExpressionUtils::filterTransform(expr);
    auto expected = geExpr(laExpr("v", "age"), constantExpr(41));
    ASSERT_EQ(*res.value(), *expected)
        << res.value()->toString() << " vs. " << expected->toString();
}

TEST_F(FilterTransformTest, TestCalculationOverflow) {
    // (v.age - 1 < 9223372036854775807)  =>  overflow
    {
        auto expr = ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)),
                                    constantExpr(9223372036854775807));
        auto res = ExpressionUtils::filterTransform(expr);
        auto expected =
            Status::Error("result of (9223372036854775807+1) cannot be represented as an integer");
        ASSERT(!res.status().ok());
        ASSERT_EQ(res.status(), expected)
            << res.status().toString() << " vs. " << expected.toString();
    }
    // (v.age + 1 < -9223372036854775808)  =>  overflow
    {
        auto expr =
            ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(INT64_MIN));
        auto res = ExpressionUtils::filterTransform(expr);
        auto expected =
            Status::Error("result of (-9223372036854775808-1) cannot be represented as an integer");
        ASSERT(!res.status().ok());
        ASSERT_EQ(res.status(), expected)
            << res.status().toString() << " vs. " << expected.toString();
    }
    // (v.age - 1 < 9223372036854775807 + 1)  =>  overflow
    {
        auto expr = ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)),
                                    addExpr(constantExpr(9223372036854775807), constantExpr(1)));
        auto res = ExpressionUtils::filterTransform(expr);
        auto expected =
            Status::Error("result of (9223372036854775807+1) cannot be represented as an integer");
        ASSERT(!res.status().ok());
        ASSERT_EQ(res.status(), expected)
            << res.status().toString() << " vs. " << expected.toString();
    }
    // (v.age + 1 < -9223372036854775808 - 1)  =>  overflow
    {
        auto expr = ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)),
                                    minusExpr(constantExpr(INT64_MIN), constantExpr(1)));
        auto res = ExpressionUtils::filterTransform(expr);
        auto expected =
            Status::Error("result of (-9223372036854775808-1) cannot be represented as an integer");
        ASSERT(!res.status().ok());
        ASSERT_EQ(res.status(), expected)
            << res.status().toString() << " vs. " << expected.toString();
    }
    // !!!(v.age - 1 < 9223372036854775807)  =>  overflow
    {
        auto expr = notExpr(notExpr(notExpr(ltExpr(
            minusExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(9223372036854775807)))));
        auto res = ExpressionUtils::filterTransform(expr);
        auto expected =
            Status::Error("result of (9223372036854775807+1) cannot be represented as an integer");
        ASSERT(!res.status().ok());
        ASSERT_EQ(res.status(), expected)
            << res.status().toString() << " vs. " << expected.toString();
    }
    // !!!(v.age + 1 < -9223372036854775808)  =>  overflow
    {
        auto expr = notExpr(notExpr(notExpr(
            ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(INT64_MIN)))));
        auto res = ExpressionUtils::filterTransform(expr);
        auto expected =
            Status::Error("result of (-9223372036854775808-1) cannot be represented as an integer");
        ASSERT(!res.status().ok());
        ASSERT_EQ(res.status(), expected)
            << res.status().toString() << " vs. " << expected.toString();
    }
}

}   // namespace graph
}   // namespace nebula
