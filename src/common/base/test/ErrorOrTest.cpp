/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/base/ErrorOr.h"


namespace nebula {

enum class ErrorCode {
    E_1,
    E_2,
    E_3,
    E_4,
    E_5,
};


TEST(ErrorOr, VerifyOk) {
    {
        ErrorOr<ErrorCode, std::string> e;
        ASSERT_TRUE(ok(e));
        ASSERT_FALSE(hasValue(e));
    }
    {
        ErrorOr<ErrorCode, int> e(ErrorCode::E_2);
        ASSERT_FALSE(ok(e));
        ASSERT_FALSE(hasValue(e));
        EXPECT_EQ(ErrorCode::E_2, error(e));
    }
}


TEST(ErrorOr, RetrieveValue) {
    {
        const ErrorOr<ErrorCode, std::string> e("Hello World");
        ASSERT_TRUE(ok(e));
        ASSERT_TRUE(hasValue(e));
        ASSERT_TRUE(std::is_const<decltype(e)>::value);
        EXPECT_EQ("Hello World", value(e));
    }
    {
        ErrorOr<ErrorCode, int> e(10);
        ASSERT_TRUE(ok(e));
        ASSERT_TRUE(hasValue(e));
        EXPECT_EQ(10, value(e));

        value(e) = 20;
        ASSERT_TRUE(ok(e));
        ASSERT_TRUE(hasValue(e));
        EXPECT_EQ(20, value(e));
    }
    {
        ErrorOr<ErrorCode, int> e(10);
        ASSERT_TRUE(ok(e));
        ASSERT_TRUE(hasValue(e));
        EXPECT_EQ(10, value(e));

        auto res = value(std::move(e));
        EXPECT_EQ(10, res);
        ASSERT_TRUE(ok(e));  // void is considered as succeeded
        ASSERT_FALSE(hasValue(e));
    }
}

}  // namespace nebula
