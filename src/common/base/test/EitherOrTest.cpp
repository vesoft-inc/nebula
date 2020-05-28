/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/base/EitherOr.h"


namespace nebula {

TEST(EitherOr, ConstructFromDefault) {
    {
        EitherOr<bool, std::string> result;
        ASSERT_TRUE(result.isVoid());
    }
    {
        EitherOr<int16_t, uint16_t> result;
        ASSERT_TRUE(result.isVoid());
    }
    {
        EitherOr<char*, std::string> result;
        ASSERT_TRUE(result.isVoid());
    }
}


TEST(EitherOr, ConstructFromValue) {
    {
        EitherOr<bool, std::string> result(true);
        ASSERT_FALSE(result.isVoid());
        ASSERT_TRUE(result.isLeftType());
        EXPECT_TRUE(result.left());
    }
    {
        EitherOr<int, std::string> result("Hello World");
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isLeftType());
        EXPECT_EQ("Hello World", result.right());
    }
    {
        static char str[] = "Hello World";
        EitherOr<char*, std::string> result(str);
        ASSERT_TRUE(result.isLeftType());
        EXPECT_EQ(str, result.left());
    }
    {
        EitherOr<char*, std::string> result(6, 'a');
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ("aaaaaa", result.right());
    }
}


TEST(EitherOr, ConstructFromTaggedValue) {
    {
        // This will cause compile failure
        // EitherOr<int16_t, uint16_t> result(1);
        EitherOr<int16_t, uint16_t> result(kConstructLeft, 1);
        ASSERT_TRUE(result.isLeftType());
        EXPECT_EQ(1, result.left());
    }
    {
        EitherOr<int16_t, uint16_t> result(kConstructRight, 65535);
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ(65535U, result.right());
    }
}


TEST(EitherOr, ReturnFromFunctionCall) {
    {
        auto foo = [] () -> EitherOr<float, std::string> {
            return "Hello World";
        };

        auto result = foo();
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isLeftType());
        EXPECT_EQ("Hello World", result.right());
    }
    {
        auto foo = [] () -> EitherOr<int, std::string> {
            return 101;
        };

        auto result = foo();
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isRightType());
        EXPECT_EQ(101, result.left());
    }
    {
        auto foo = [] () -> EitherOr<int16_t, int32_t> {
            return static_cast<int16_t>(101);
        };

        auto result = foo();
        ASSERT_TRUE(result.isLeftType());
        EXPECT_EQ(101, result.left());
    }
}


TEST(EitherOr, ReturnFromMoveOnlyValue) {
    // return move only from anonymous value
    {
        auto foo = [] () -> EitherOr<int, std::unique_ptr<std::string>> {
            return std::make_unique<std::string>("SomeValue");
        };
        auto result = foo();
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isLeftType());
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ("SomeValue", *result.right());
    }
    // return move only from named value
    {
        auto foo = [] () -> EitherOr<int, std::unique_ptr<std::string>> {
            auto ptr = std::make_unique<std::string>("SomeValue");
            return ptr;
        };
        auto result = foo();
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ("SomeValue", *result.right());
    }
}


TEST(EitherOr, CopyConstructFromDefault) {
    EitherOr<int, std::string> result1;
    auto result2 = result1;
    ASSERT_TRUE(result2.isVoid());
}


TEST(EitherOr, CopyConstructFromValue) {
    {
        EitherOr<int, std::string> result1(101);
        auto result2 = result1;
        ASSERT_FALSE(result1.isVoid());
        ASSERT_FALSE(result2.isVoid());
        ASSERT_EQ(101, result1.left());
        ASSERT_EQ(101, result2.left());
    }
    {
        EitherOr<int, std::string> result1("Something");
        auto result2 = result1;
        ASSERT_FALSE(result1.isVoid());
        ASSERT_FALSE(result2.isVoid());
        ASSERT_EQ("Something", result1.right());
        ASSERT_EQ("Something", result2.right());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2 = r1;
        ASSERT_TRUE(r1.isRightType());
        ASSERT_TRUE(r2.isRightType());
        EXPECT_EQ(r1.right(), r2.right());
    }
    {
        EitherOr<int16_t, std::string> r1(256);
        EitherOr<int32_t, std::string> r2 = r1;
        ASSERT_TRUE(r1.isLeftType());
        ASSERT_TRUE(r2.isLeftType());
        EXPECT_EQ(r1.left(), r2.left());
    }
}


TEST(EitherOr, CopyAssignFromDefault) {
    EitherOr<int, std::string> result1;
    decltype(result1) result2;
    result2 = result1;
    ASSERT_TRUE(result1.isVoid());
    ASSERT_TRUE(result2.isVoid());
}


TEST(EitherOr, CopyAssignFromValue) {
    {
        EitherOr<int, std::string> result1(101);
        decltype(result1) result2;
        result2 = result1;
        ASSERT_FALSE(result1.isVoid());
        ASSERT_FALSE(result2.isVoid());
        ASSERT_TRUE(result1.isLeftType());
        ASSERT_TRUE(result2.isLeftType());
        EXPECT_EQ(101, result1.left());
        EXPECT_EQ(101, result2.left());
    }
    {
        EitherOr<int, std::string> result1("SomeValue");
        decltype(result1) result2;
        result2 = result1;
        ASSERT_TRUE(result1.isRightType());
        ASSERT_TRUE(result2.isRightType());
        ASSERT_EQ("SomeValue", result1.right());
        ASSERT_EQ("SomeValue", result2.right());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2;
        r2 = r1;
        ASSERT_TRUE(r1.isRightType());
        ASSERT_TRUE(r2.isRightType());
        EXPECT_EQ(r1.right(), r2.right());

        r1 = 256;
        r2 = r1;
        ASSERT_TRUE(r1.isLeftType());
        ASSERT_TRUE(r2.isLeftType());
        EXPECT_EQ(r1.left(), r2.left());
    }
}


TEST(EitherOr, MoveConstructFromDefault) {
    EitherOr<int, std::string> result1;
    auto result2 = std::move(result1);
    ASSERT_TRUE(result1.isVoid());
    ASSERT_TRUE(result2.isVoid());
}


TEST(EitherOr, MoveConstructFromValue) {
    {
        EitherOr<int, std::string> result1(101);
        auto result2 = std::move(result1);
        ASSERT_TRUE(result1.isVoid());
        ASSERT_FALSE(result2.isVoid());
        ASSERT_TRUE(result2.isLeftType());
        EXPECT_EQ(101, result2.left());
    }
    {
        EitherOr<int, std::string> result1("SomeValue");
        auto result2 = std::move(result1);
        ASSERT_TRUE(result1.isVoid());
        ASSERT_TRUE(result2.isRightType());
        EXPECT_EQ("SomeValue", result2.right());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isRightType());
        EXPECT_EQ("Hello World", r2.right());
    }
    {
        EitherOr<int16_t, std::string> r1(256);
        EitherOr<int32_t, std::string> r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isLeftType());
        EXPECT_EQ(256, r2.left());
    }
}


TEST(EitherOr, MoveAssignFromDefault) {
    EitherOr<int, std::string> result1;
    decltype(result1) result2;
    result2 = std::move(result1);
    ASSERT_TRUE(result1.isVoid());
    ASSERT_TRUE(result2.isVoid());
}


TEST(EitherOr, MoveAssignFromValue) {
    {
        EitherOr<int, std::string> result1(101);
        decltype(result1) result2;
        result2 = std::move(result1);
        ASSERT_TRUE(result1.isVoid());
        ASSERT_FALSE(result2.isVoid());
        ASSERT_TRUE(result2.isLeftType());
        EXPECT_EQ(101, result2.left());
    }
    {
        EitherOr<int, std::string> result1("SomeValue");
        decltype(result1) result2;
        result2 = std::move(result1);
        ASSERT_TRUE(result1.isVoid());
        ASSERT_TRUE(result2.isRightType());
        EXPECT_EQ("SomeValue", result2.right());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2;
        r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isRightType());
        EXPECT_EQ("Hello World", r2.right());

        r1 = 256;
        r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isLeftType());
        EXPECT_EQ(256, r2.left());
    }
}


TEST(EitherOr, AssignFromValue) {
    {
        EitherOr<int, std::string> result;
        result = 101;
        ASSERT_FALSE(result.isVoid());
        EXPECT_EQ(101, result.left());

        result = "SomeValue";
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ("SomeValue", result.right());
    }
    {
        EitherOr<int, std::string> result;
        result = "SomeValue";
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ("SomeValue", result.right());

        result = 101;
        ASSERT_TRUE(result.isLeftType());
        EXPECT_EQ(101, result.left());
    }
    {
        EitherOr<std::unique_ptr<std::string>, std::string> result;
        auto val = std::make_unique<std::string>("Hello World");
        result = std::move(val);
        ASSERT_TRUE(!val);
        ASSERT_TRUE(result.isLeftType());
        EXPECT_EQ("Hello World", *result.left());

        auto str = std::string("SomeValue");
        result = std::move(str);
        ASSERT_TRUE(str.empty());
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ("SomeValue", result.right());
    }
    {
        EitherOr<int16_t, int32_t> result;
        result = static_cast<int16_t>(101);
        ASSERT_TRUE(result.isLeftType());
        EXPECT_EQ(101, result.left());

        result = static_cast<int32_t>(202);
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ(202, result.right());
    }
}


TEST(EitherOr, MoveOutValue) {
    EitherOr<int, std::string> result("SomeValue");
    ASSERT_FALSE(result.isVoid());
    EXPECT_EQ("SomeValue", result.right());
    auto str = std::move(result).right();
    ASSERT_TRUE(result.isVoid());
    EXPECT_EQ("SomeValue", str);
}


TEST(EitherOr, SelfAssignment) {
    {
        EitherOr<int, std::string> r("SomeValue");
        r = r;
        ASSERT_TRUE(r.isRightType());
        EXPECT_EQ("SomeValue", r.right());
    }
    {
        EitherOr<int, std::string> r("SomeValue");
        r = std::move(r);
        ASSERT_TRUE(r.isRightType());
        EXPECT_EQ("SomeValue", r.right());
    }
}


TEST(EitherOr, Destruct) {
    auto ptr = std::make_shared<std::string>("SomeValue");
    ASSERT_EQ(1UL, ptr.use_count());
    {
        EitherOr<int, std::shared_ptr<std::string>> result;
        result = ptr;
        ASSERT_TRUE(result.isRightType());
        EXPECT_EQ(2UL, ptr.use_count());
        auto ptr2 = result.right();
        ASSERT_EQ(3UL, ptr.use_count());
    }
    ASSERT_EQ(1UL, ptr.use_count());
}

}   // namespace nebula
