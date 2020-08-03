/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/function/TimeFunction.h"

namespace nebula {

TEST(TimeFunctionTest, toTimestamp) {
    // incorrect type
    EXPECT_FALSE(TimeFunction::toTimestamp(Value(10.0)).ok());

    // incorrect int value
    EXPECT_FALSE(TimeFunction::toTimestamp(Value(std::numeric_limits<int64_t>::max()-10)).ok());

    // incorrect string value
    EXPECT_FALSE(TimeFunction::toTimestamp(Value("2001-02-29 10:00:10")).ok());

    // incorrect string value
    EXPECT_FALSE(TimeFunction::toTimestamp(Value("2001-04-31 10:00:10")).ok());

    // incorrect string value
    EXPECT_FALSE(TimeFunction::toTimestamp(Value("2001-02-01 10:00")).ok());

    // correct int
    EXPECT_TRUE(TimeFunction::toTimestamp(Value(0)).ok());

    // correct int
    EXPECT_TRUE(TimeFunction::toTimestamp(
                Value(std::numeric_limits<int64_t>::max() / 1000000000)).ok());

    // correct string
    EXPECT_TRUE(TimeFunction::toTimestamp("2020-8-1 9:0:0").ok());
}

}   // namespace nebula

