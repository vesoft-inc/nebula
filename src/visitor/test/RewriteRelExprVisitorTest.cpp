/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"
#include "visitor/test/VisitorTestBase.h"

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"

namespace nebula {
namespace graph {
class RewriteRelExprVisitorTest : public VisitorTestBase {};

TEST_F(RewriteRelExprVisitorTest, TestArithmeticalExpr) {
    // (label + 1 < 40)  =>  (label < 40 - 1)
    {
        auto expr = ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = ltExpr(laExpr("v", "age"), minusExpr(constantExpr(40), constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (1 + label < 40)  =>  (label < 40 - 1)
    {
        auto expr = ltExpr(addExpr(constantExpr(1), laExpr("v", "age")), constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = ltExpr(laExpr("v", "age"), minusExpr(constantExpr(40), constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (-1 + label < 40)  =>  (label < 40 - (-1))
    {
        auto expr = ltExpr(addExpr(constantExpr(-1), laExpr("v", "age")), constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = ltExpr(laExpr("v", "age"), minusExpr(constantExpr(40), constantExpr(-1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (label1 + label2 < 40)  =>  (label1 + label2 < 40) Unchaged
    // TODO: replace list with set in object pool and avoid copy
    {
        auto expr = ltExpr(addExpr(laExpr("v", "age"), laExpr("v2", "age2")), constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = expr;
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (label * 2 < 40)  =>  (2*label < 40) Unchanged
    {
        auto expr = ltExpr(multiplyExpr(laExpr("v", "age"), constantExpr(2)), constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = expr;
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (label / 3 < 40)  =>  (label / 3 < 40) Unchanged
    {
        auto expr = ltExpr(divideExpr(laExpr("v", "age"), constantExpr(3)), constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = expr;
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteRelExprVisitorTest, TestNestedArithmeticalExpr) {
    // (label + 1 + 2 < 40)  =>  (label < 40 - 2 - 1)
    {
        auto expr = ltExpr(addExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                           constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected =
            ltExpr(laExpr("v", "age"),
                   minusExpr(minusExpr(constantExpr(40), constantExpr(2)), constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (label + 1 - 2 < 40)  =>  (label < 40 + 2 - 1)
    {
        auto expr = ltExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                           constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected =
            ltExpr(laExpr("v", "age"),
                   minusExpr(addExpr(constantExpr(40), constantExpr(2)), constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (label + 1 - 2 + 3 < 40)  =>  (label < 40 - 3 + 2 - 1)
    {
        auto expr =
            ltExpr(addExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                           constantExpr(3)),
                   constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected =
            ltExpr(laExpr("v", "age"),
                   minusExpr(addExpr(minusExpr(constantExpr(40), constantExpr(3)), constantExpr(2)),
                             constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (2 * label + 1 < 40)  =>  (2*label < 40 - 1) Partial rewrite
    {
        auto expr =
            ltExpr(addExpr(multiplyExpr(constantExpr(2), laExpr("v", "age")), constantExpr(1)),
                   constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = ltExpr(multiplyExpr(constantExpr(2), laExpr("v", "age")),
                               minusExpr(constantExpr(40), constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (label / 3 - 1 < 40)  =>  (label / 3 < 40 + 1) Partial rewrite
    {
        auto expr =
            ltExpr(minusExpr(divideExpr(laExpr("v", "age"), constantExpr(3)), constantExpr(1)),
                   constantExpr(40));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = ltExpr(divideExpr(laExpr("v", "age"), constantExpr(3)),
                               addExpr(constantExpr(40), constantExpr(1)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteRelExprVisitorTest, TestReduceBoolNullExpr) {
    // (v.age > 40 == true)  => (v.age > 40)
    {
        auto expr = eqExpr(gtExpr(laExpr("v", "age"), constantExpr(40)), constantExpr(true));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = gtExpr(laExpr("v", "age"), constantExpr(40));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (v.age > 40 == false)  => !(v.age > 40)
    {
        auto expr = eqExpr(gtExpr(laExpr("v", "age"), constantExpr(40)), constantExpr(false));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = notExpr(gtExpr(laExpr("v", "age"), constantExpr(40)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (v.age > 40 == null)  => (null)
    {
        auto expr = eqExpr(gtExpr(laExpr("v", "age"), constantExpr(40)),
                           constantExpr(Value(NullType::__NULL__)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = constantExpr(Value(NullType::__NULL__));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (v.age <= null)  =>  (null)
    {
        auto expr = leExpr(laExpr("v", "age"), constantExpr(Value(NullType::__NULL__)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = constantExpr(Value(NullType::__NULL__));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (v.age + 10 > null)  =>  (null)
    {
        auto expr = gtExpr(addExpr(laExpr("v", "age"), constantExpr(10)),
                           constantExpr(Value(NullType::__NULL__)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = constantExpr(Value(NullType::__NULL__));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteRelExprVisitorTest, TestLogicalExpr) {
    // (label + 1 + 2 < 40) AND (label + 1 - 2 + 3 < 40)  =>
    // (label < 40 - 2 - 1) AND (label < 40 - 3 + 2 - 1)
    {
        auto expr = andExpr(
            ltExpr(addExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                   constantExpr(40)),
            ltExpr(addExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                           constantExpr(3)),
                   constantExpr(40)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = andExpr(
            ltExpr(laExpr("v", "age"),
                   minusExpr(minusExpr(constantExpr(40), constantExpr(2)), constantExpr(1))),
            ltExpr(laExpr("v", "age"),
                   minusExpr(addExpr(minusExpr(constantExpr(40), constantExpr(3)), constantExpr(2)),
                             constantExpr(1))));

        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteRelExprVisitorTest, TestContainer) {
    // List
    {
        // [(label + 1 + 2 < 40), (label + 1 - 2 < 40)]  =>
        // [(label < 40 - 2 - 1), (label < 40 + 2 - 1)]
        auto expr = listExpr(
            {ltExpr(addExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                    constantExpr(40)),
             ltExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                    constantExpr(40))});
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = listExpr(
            {ltExpr(laExpr("v", "age"),
                    minusExpr(minusExpr(constantExpr(40), constantExpr(2)), constantExpr(1))),
             ltExpr(laExpr("v", "age"),
                    minusExpr(addExpr(constantExpr(40), constantExpr(2)), constantExpr(1)))});

        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // Set
    {
        // {(label + 1 + 2 < 40), (label + 1 - 2 < 40)}  =>
        // {(label < 40 - 2 - 1), (label < 40 + 2 - 1)}
        auto expr = setExpr(
            {ltExpr(addExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                    constantExpr(40)),
             ltExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                    constantExpr(40))});
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = setExpr(
            {ltExpr(laExpr("v", "age"),
                    minusExpr(minusExpr(constantExpr(40), constantExpr(2)), constantExpr(1))),
             ltExpr(laExpr("v", "age"),
                    minusExpr(addExpr(constantExpr(40), constantExpr(2)), constantExpr(1)))});

        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // Map
    {
        // {"k1":(label + 1 + 2 < 40), "k2":(label + 1 - 2 < 40)} =>
        // {"k1":(label < 40 - 2 - 1), "k2":(label < 40 + 2 - 1)}
        auto expr = mapExpr(
            {{"k1",
              ltExpr(addExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                     constantExpr(40))},
             {"k2",
              ltExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(2)),
                     constantExpr(40))}});
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = mapExpr(
            {{"k1",
              ltExpr(laExpr("v", "age"),
                     minusExpr(minusExpr(constantExpr(40), constantExpr(2)), constantExpr(1)))},
             {"k2",
              ltExpr(laExpr("v", "age"),
                     minusExpr(addExpr(constantExpr(40), constantExpr(2)), constantExpr(1)))}});
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

}   // namespace graph
}   // namespace nebula
