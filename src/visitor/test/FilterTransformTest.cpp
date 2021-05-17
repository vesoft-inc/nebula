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
class FilterTransformTest : public ValidatorTestBase {
public:
    void TearDown() override {
        pool.clear();
    }

protected:
    ObjectPool pool;
};

TEST_F(FilterTransformTest, TestComplexExprRewrite) {
    // !!!(v.age - 1 < 40)  =>  (v.age >= 41)
    auto expr = pool.add(notExpr(notExpr(
        notExpr(ltExpr(minusExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(40))))));
    auto res = ExpressionUtils::filterTransform(expr, &pool);
    auto expected = pool.add(geExpr(laExpr("v", "age"), constantExpr(41)));
    ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
}

}   // namespace graph
}   // namespace nebula
