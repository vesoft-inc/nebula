/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "context/QueryContext.h"

namespace nebula {

TEST(QueryContext, ReadWriteTest) {
    QueryContext ctx;
    ctx.setValue("v1", 10);
    ctx.setValue("v2", "Hello world");
    EXPECT_EQ(Value(10), ctx.getValue("v1"));
    EXPECT_EQ(Value("Hello world"), ctx.getValue("v2"));
}


TEST(QueryContext, HistoryTest) {
    QueryContext ctx;
    ctx.setValue("v1", 10);
    ctx.setValue("v1", "Hello world");
    ctx.setValue("v1", 3.14);
    ctx.setValue("v1", true);

    ASSERT_EQ(4, ctx.numVersions("v1"));
    const auto& hist1 = ctx.getHistory("v1");
    auto it = hist1.begin();
    EXPECT_EQ(Value(true), *it++);
    EXPECT_EQ(Value(3.14), *it++);
    EXPECT_EQ(Value("Hello world"), *it++);
    EXPECT_EQ(Value(10), *it++);
    EXPECT_TRUE(it == hist1.end());

    ctx.truncHistory("v1", 5);
    ASSERT_EQ(4, ctx.numVersions("v1"));

    ctx.truncHistory("v1", 2);
    ASSERT_EQ(2, ctx.numVersions("v1"));

    const auto& hist2 = ctx.getHistory("v1");
    auto it2 = hist2.begin();
    EXPECT_EQ(Value(true), *it2++);
    EXPECT_EQ(Value(3.14), *it2++);
    EXPECT_TRUE(it2 == hist2.end());
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
