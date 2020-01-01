/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "base/Status.h"


namespace nebula {

TEST(Status, Basic) {
    ASSERT_TRUE(Status().ok());
    ASSERT_TRUE(Status::OK().ok());
    ASSERT_FALSE(Status::Error("Error").ok());
    ASSERT_EQ(8UL, sizeof(Status));
}


TEST(Status, toString) {
    ASSERT_EQ("OK", Status().toString());
    ASSERT_EQ("OK", Status::OK().toString());
    ASSERT_EQ("Error", Status::Error("Error").toString());
    ASSERT_EQ("SomeError", Status::Error("SomeError").toString());
    ASSERT_EQ("SomeError(-1)", Status::Error("%s(%d)", "SomeError", -1).toString());
}


TEST(Status, StreamOperator) {
    {
        auto result = testing::AssertionSuccess();
        result << Status::OK();
        ASSERT_STREQ("OK", result.message());
    }
    {
        auto result = testing::AssertionSuccess();
        result << Status::Error("SomeError");
        ASSERT_STREQ("SomeError", result.message());
    }
    {
        std::ostringstream os;
        os << Status();
        ASSERT_EQ("OK", os.str());
    }
    {
        std::ostringstream os;
        os << Status::OK();
        ASSERT_EQ("OK", os.str());
    }
    {
        std::ostringstream os;
        os << Status::Error("SomeError");
        ASSERT_EQ("SomeError", os.str());
    }
}


TEST(Status, Copy) {
    {
        Status ok;
        auto copy = ok;
        ASSERT_TRUE(ok.ok());
        ASSERT_TRUE(copy.ok());
    }
    {
        auto error = Status::Error("SomeError");
        auto copy = error;
        ASSERT_FALSE(error.ok());
        ASSERT_FALSE(copy.ok());
        ASSERT_EQ("SomeError", error.toString());
        ASSERT_EQ("SomeError", copy.toString());
    }
}


TEST(Status, Move) {
    {
        Status ok;
        auto move = std::move(ok);
        ASSERT_TRUE(ok.ok());
        ASSERT_TRUE(move.ok());
    }
    {
        auto error = Status::Error("SomeError");
        ASSERT_FALSE(error.ok());
        ASSERT_EQ("SomeError", error.toString());
        auto move = std::move(error);
        ASSERT_TRUE(error.ok());
        ASSERT_EQ("OK", error.toString());
        ASSERT_FALSE(move.ok());
        ASSERT_EQ("SomeError", move.toString());
    }
}


}   // namespace nebula
