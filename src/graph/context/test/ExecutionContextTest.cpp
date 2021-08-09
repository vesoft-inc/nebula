/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/ExecutionContext.h"

#include <gtest/gtest.h>
#include "common/base/Base.h"

namespace nebula {
namespace graph {

TEST(ExecutionContext, ReadWriteTest) {
    ExecutionContext ctx;
    ctx.setValue("v1", 10);
    ctx.setValue("v2", "Hello world");
    EXPECT_EQ(Value(10), ctx.getValue("v1"));
    EXPECT_EQ(Value("Hello world"), ctx.getValue("v2"));
}

TEST(ExecutionContext, HistoryTest) {
    ExecutionContext ctx;
    ctx.setValue("v1", 10);
    ctx.setValue("v1", "Hello world");
    ctx.setValue("v1", 3.14);
    ctx.setValue("v1", true);

    ASSERT_EQ(4, ctx.numVersions("v1"));
    const auto& hist1 = ctx.getHistory("v1");
    auto it = hist1.begin();
    EXPECT_EQ(Value(10), (it++)->value());
    EXPECT_EQ(Value("Hello world"), (it++)->value());
    EXPECT_EQ(Value(3.14), (it++)->value());
    EXPECT_EQ(Value(true), (it++)->value());
    EXPECT_TRUE(it == hist1.end());

    ctx.truncHistory("v1", 5);
    ASSERT_EQ(4, ctx.numVersions("v1"));

    ctx.truncHistory("v1", 2);
    ASSERT_EQ(2, ctx.numVersions("v1"));

    const auto& hist2 = ctx.getHistory("v1");
    auto it2 = hist2.begin();
    EXPECT_EQ(Value(3.14), (it2++)->value());
    EXPECT_EQ(Value(true), (it2++)->value());
    EXPECT_TRUE(it2 == hist2.end());
}

TEST(ExecutionContextTest, TestResult) {
    DataSet ds;
    auto expected =
        ResultBuilder().value(Value(std::move(ds))).iter(Iterator::Kind::kDefault).finish();
    ExecutionContext ctx;
    ctx.setResult("ds", std::move(expected));
    auto& result = ctx.getResult("ds");
    EXPECT_TRUE(result.value().isDataSet());
    EXPECT_TRUE(result.valuePtr()->isDataSet());
}

}   // namespace graph
}   // namespace nebula
