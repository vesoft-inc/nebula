
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
class RewriteUnaryNotExprVisitorTest : public VisitorTestBase {};

TEST_F(RewriteUnaryNotExprVisitorTest, TestNestedMultipleUnaryNotExpr) {
    // !!(5 == 10)  =>  (5 == 10)
    {
        auto expr = notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = eqExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!!(5 == 10)  =>  (5 == 10)
    {
        auto expr = notExpr(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = eqExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!(5 == 10)  =>  (5 != 10)
    {
        auto expr = notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = neExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!!!(5 == 10)  =>  (5 != 10)
    {
        auto expr =
            notExpr(notExpr(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = neExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotExprLogicalRelExpr) {
    // !!(5 == 10) AND !!(30 > 20)  =>  (5 == 10) AND (30 > 20)
    {
        auto expr = andExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))),
                            notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = andExpr(eqExpr(constantExpr(5), constantExpr(10)),
                                gtExpr(constantExpr(30), constantExpr(20)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!(5 <= 10) AND !(30 > 20)  =>  (5 > 10) AND (30 <= 20)
    {
        auto expr = andExpr(notExpr(notExpr(notExpr(leExpr(constantExpr(5), constantExpr(10))))),
                            notExpr(gtExpr(constantExpr(30), constantExpr(20))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = andExpr(gtExpr(constantExpr(5), constantExpr(10)),
                                leExpr(constantExpr(30), constantExpr(20)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotContainerExpr) {
    // List
    {
        // [!!(5 == 10), !!!(30 > 20)]  =>  [(5 == 10), (30 <= 20)]
        auto expr =
            listExpr({notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))),
                      notExpr(notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))))});
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = listExpr({eqExpr(constantExpr(5), constantExpr(10)),
                                  leExpr(constantExpr(30), constantExpr(20))});
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // Set
    {
        // {!!(5 == 10), !!!(30 > 20)}  =>  {(5 == 10), (30 <= 20)}
        auto expr =
            setExpr({notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))),
                     notExpr(notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))))});
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = setExpr({eqExpr(constantExpr(5), constantExpr(10)),
                                 leExpr(constantExpr(30), constantExpr(20))});
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }

    // Map
    {
        // {"k1":!!(5 == 10), "k2":!!!(30 > 20)}} => {"k1":(5 == 10), "k2":(30 <= 20)}
        auto expr = mapExpr({{"k1", notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))}, {
            "k2", notExpr(notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))))}});
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = mapExpr({{"k1", eqExpr(constantExpr(5), constantExpr(10))},
                                 {"k2", leExpr(constantExpr(30), constantExpr(20))}});
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestRelExpr) {
    // (5 == 10)  =>  (5 == 10)
    // no change should be made to the orginal expression
    {
        auto original = eqExpr(constantExpr(5), constantExpr(10));
        auto res = ExpressionUtils::reduceUnaryNotExpr(original);
        ASSERT_EQ(*original, *res) << original->toString() << " vs. " << res->toString();
    }
    // !(5 == 10)  =>  (5 != 10)
    {
        auto expr = notExpr(eqExpr(constantExpr(5), constantExpr(10)));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = neExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !(5 > 10) => (5 <= 10)
    {
        auto expr = notExpr(gtExpr(constantExpr(5), constantExpr(10)));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = leExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !(5 >= 10) => (5 < 10)
    {
        auto expr = notExpr(geExpr(constantExpr(5), constantExpr(10)));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = ltExpr(constantExpr(5), constantExpr(10));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !("bcd" IN "abcde")  =>  ("bcd" NOT IN "abcde")
    {
        auto expr = notExpr(inExpr(constantExpr("bcd"), constantExpr("abcde")));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = notInExpr(constantExpr("bcd"), constantExpr("abcde"));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }

    // !("bcd" NOT IN "abcde")  =>  ("bcd" IN "abcde")
    {
        auto expr = notExpr(notInExpr(constantExpr("bcd"), constantExpr("abcde")));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = inExpr(constantExpr("bcd"), constantExpr("abcde"));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }

    // !("bcd" STARTS WITH "abc")  =>  ("bcd" NOT STARTS WITH "abc")
    {
        auto expr = notExpr(startsWithExpr(constantExpr("bcd"), constantExpr("abcde")));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = notStartsWithExpr(constantExpr("bcd"), constantExpr("abcde"));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }

    // !("bcd" ENDS WITH "abc")  =>  ("bcd" NOT ENDS WITH "abc")
    {
        auto expr = notExpr(endsWithExpr(constantExpr("bcd"), constantExpr("abcde")));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);
        auto expected = notEndsWithExpr(constantExpr("bcd"), constantExpr("abcde"));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestLogicalExpr) {
    // !( 1 != 1 && 2 >= 3 && 30 <= 20)  =>  (1 == 1 || 2 < 3 || 30 > 20)
    {
        auto expr = notExpr(andExpr(andExpr(neExpr(constantExpr(1), constantExpr(1)),
                                            geExpr(constantExpr(2), constantExpr(3))),
                                    leExpr(constantExpr(30), constantExpr(20))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);

        auto expected = LogicalExpression::makeOr(pool);
        expected->addOperand(eqExpr(constantExpr(1), constantExpr(1)));
        expected->addOperand(ltExpr(constantExpr(2), constantExpr(3)));
        expected->addOperand(gtExpr(constantExpr(30), constantExpr(20)));

        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !( 1 != 1 || 2 >= 3 || 30 <= 20)  =>  (1 == 1 && 2 < 3 && 30 > 20)
    {
        auto expr = notExpr(orExpr(orExpr(neExpr(constantExpr(1), constantExpr(1)),
                                          geExpr(constantExpr(2), constantExpr(3))),
                                   leExpr(constantExpr(30), constantExpr(20))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);

        auto expected = LogicalExpression::makeAnd(pool);
        expected->addOperand(eqExpr(constantExpr(1), constantExpr(1)));
        expected->addOperand(ltExpr(constantExpr(2), constantExpr(3)));
        expected->addOperand(gtExpr(constantExpr(30), constantExpr(20)));

        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !( 1 != 1 && 2 >= 3 || 30 <= 20)  =>  ((1 == 1 || 2 < 3) && 30 > 20)
    {
        auto expr = notExpr(orExpr(andExpr(neExpr(constantExpr(1), constantExpr(1)),
                                           geExpr(constantExpr(2), constantExpr(3))),
                                   leExpr(constantExpr(30), constantExpr(20))));
        auto res = ExpressionUtils::reduceUnaryNotExpr(expr);

        auto expected = andExpr(orExpr(eqExpr(constantExpr(1), constantExpr(1)),
                                       ltExpr(constantExpr(2), constantExpr(3))),
                                gtExpr(constantExpr(30), constantExpr(20)));

        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

}   // namespace graph
}   // namespace nebula
