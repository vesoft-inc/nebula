/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "context/ExecutionContext.h"
#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {
TEST(ExpressionContextTest, GetVar) {
    ExecutionContext qctx;
    qctx.setValue("v1", 10);
    qctx.setValue("v2", "Hello world");

    graph::QueryExpressionContext ectx(&qctx, nullptr);
    EXPECT_EQ(Value(10), ectx.getVar("v1"));
    EXPECT_EQ(Value("Hello world"), ectx.getVar("v2"));

    qctx.setValue("v1", "Hello world");
    qctx.setValue("v1", 3.14);
    qctx.setValue("v1", true);
    EXPECT_EQ(Value(true), ectx.getVersionedVar("v1", 0));
    EXPECT_EQ(Value(3.14), ectx.getVersionedVar("v1", -1));
    EXPECT_EQ(Value(10), ectx.getVersionedVar("v1", 1));
}
}  // namespace graph
}  // namespace nebula
