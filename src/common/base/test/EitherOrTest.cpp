/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "base/EitherOr.h"


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
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_TRUE(result.v1());
    }
    {
        EitherOr<int, std::string> result("Hello World");
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isTypeOne());
        EXPECT_EQ("Hello World", result.v2());
    }
    {
        static char str[] = "Hello World";
        EitherOr<char*, std::string> result(str);
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_EQ(str, result.v1());
    }
    {
        EitherOr<char*, std::string> result(6, 'a');
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ("aaaaaa", result.v2());
    }
}


TEST(EitherOr, ConstructFromTaggedValue) {
    {
        // This will cause compile failure
        // EitherOr<int16_t, uint16_t> result(1);
        EitherOr<int16_t, uint16_t> result(kConstructT1, 1);
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_EQ(1, result.v1());
    }
    {
        EitherOr<int16_t, uint16_t> result(kConstructT2, 65535);
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ(65535U, result.v2());
    }
}


TEST(EitherOr, ReturnFromFunctionCall) {
    {
        auto foo = [] () -> EitherOr<float, std::string> {
            return "Hello World";
        };

        auto result = foo();
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isTypeOne());
        EXPECT_EQ("Hello World", result.v2());
    }
    {
        auto foo = [] () -> EitherOr<int, std::string> {
            return 101;
        };

        auto result = foo();
        ASSERT_FALSE(result.isVoid());
        ASSERT_FALSE(result.isTypeTwo());
        EXPECT_EQ(101, result.v1());
    }
    {
        auto foo = [] () -> EitherOr<int16_t, int32_t> {
            return static_cast<int16_t>(101);
        };

        auto result = foo();
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_EQ(101, result.v1());
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
        ASSERT_FALSE(result.isTypeOne());
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ("SomeValue", *result.v2());
    }
    // return move only from named value
    {
        auto foo = [] () -> EitherOr<int, std::unique_ptr<std::string>> {
            auto ptr = std::make_unique<std::string>("SomeValue");
            return ptr;
        };
        auto result = foo();
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ("SomeValue", *result.v2());
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
        ASSERT_EQ(101, result1.v1());
        ASSERT_EQ(101, result2.v1());
    }
    {
        EitherOr<int, std::string> result1("Something");
        auto result2 = result1;
        ASSERT_FALSE(result1.isVoid());
        ASSERT_FALSE(result2.isVoid());
        ASSERT_EQ("Something", result1.v2());
        ASSERT_EQ("Something", result2.v2());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2 = r1;
        ASSERT_TRUE(r1.isTypeTwo());
        ASSERT_TRUE(r2.isTypeTwo());
        EXPECT_EQ(r1.v2(), r2.v2());
    }
    {
        EitherOr<int16_t, std::string> r1(256);
        EitherOr<int32_t, std::string> r2 = r1;
        ASSERT_TRUE(r1.isTypeOne());
        ASSERT_TRUE(r2.isTypeOne());
        EXPECT_EQ(r1.v1(), r2.v1());
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
        ASSERT_TRUE(result1.isTypeOne());
        ASSERT_TRUE(result2.isTypeOne());
        EXPECT_EQ(101, result1.v1());
        EXPECT_EQ(101, result2.v1());
    }
    {
        EitherOr<int, std::string> result1("SomeValue");
        decltype(result1) result2;
        result2 = result1;
        ASSERT_TRUE(result1.isTypeTwo());
        ASSERT_TRUE(result2.isTypeTwo());
        ASSERT_EQ("SomeValue", result1.v2());
        ASSERT_EQ("SomeValue", result2.v2());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2;
        r2 = r1;
        ASSERT_TRUE(r1.isTypeTwo());
        ASSERT_TRUE(r2.isTypeTwo());
        EXPECT_EQ(r1.v2(), r2.v2());

        r1 = 256;
        r2 = r1;
        ASSERT_TRUE(r1.isTypeOne());
        ASSERT_TRUE(r2.isTypeOne());
        EXPECT_EQ(r1.v1(), r2.v1());
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
        ASSERT_TRUE(result2.isTypeOne());
        EXPECT_EQ(101, result2.v1());
    }
    {
        EitherOr<int, std::string> result1("SomeValue");
        auto result2 = std::move(result1);
        ASSERT_TRUE(result1.isVoid());
        ASSERT_TRUE(result2.isTypeTwo());
        EXPECT_EQ("SomeValue", result2.v2());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isTypeTwo());
        EXPECT_EQ("Hello World", r2.v2());
    }
    {
        EitherOr<int16_t, std::string> r1(256);
        EitherOr<int32_t, std::string> r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isTypeOne());
        EXPECT_EQ(256, r2.v1());
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
        ASSERT_TRUE(result2.isTypeOne());
        EXPECT_EQ(101, result2.v1());
    }
    {
        EitherOr<int, std::string> result1("SomeValue");
        decltype(result1) result2;
        result2 = std::move(result1);
        ASSERT_TRUE(result1.isVoid());
        ASSERT_TRUE(result2.isTypeTwo());
        EXPECT_EQ("SomeValue", result2.v2());
    }
    {
        EitherOr<int16_t, std::string> r1("Hello World");
        EitherOr<int32_t, std::string> r2;
        r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isTypeTwo());
        EXPECT_EQ("Hello World", r2.v2());

        r1 = 256;
        r2 = std::move(r1);
        ASSERT_TRUE(r1.isVoid());
        ASSERT_TRUE(r2.isTypeOne());
        EXPECT_EQ(256, r2.v1());
    }
}


TEST(EitherOr, AssignFromValue) {
    {
        EitherOr<int, std::string> result;
        result = 101;
        ASSERT_FALSE(result.isVoid());
        EXPECT_EQ(101, result.v1());

        result = "SomeValue";
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ("SomeValue", result.v2());
    }
    {
        EitherOr<int, std::string> result;
        result = "SomeValue";
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ("SomeValue", result.v2());

        result = 101;
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_EQ(101, result.v1());
    }
    {
        EitherOr<std::unique_ptr<std::string>, std::string> result;
        auto val = std::make_unique<std::string>("Hello World");
        result = std::move(val);
        ASSERT_TRUE(!val);
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_EQ("Hello World", *result.v1());

        auto str = std::string("SomeValue");
        result = std::move(str);
        ASSERT_TRUE(str.empty());
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ("SomeValue", result.v2());
    }
    {
        EitherOr<int16_t, int32_t> result;
        result = static_cast<int16_t>(101);
        ASSERT_TRUE(result.isTypeOne());
        EXPECT_EQ(101, result.v1());

        result = static_cast<int32_t>(202);
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ(202, result.v2());
    }
}


TEST(EitherOr, MoveOutValue) {
    EitherOr<int, std::string> result("SomeValue");
    ASSERT_FALSE(result.isVoid());
    EXPECT_EQ("SomeValue", result.v2());
    auto str = std::move(result).v2();
    ASSERT_TRUE(result.isVoid());
    EXPECT_EQ("SomeValue", str);
}


TEST(EitherOr, Destruct) {
    auto ptr = std::make_shared<std::string>("SomeValue");
    ASSERT_EQ(1UL, ptr.use_count());
    {
        EitherOr<int, std::shared_ptr<std::string>> result;
        result = ptr;
        ASSERT_TRUE(result.isTypeTwo());
        EXPECT_EQ(2UL, ptr.use_count());
        auto ptr2 = result.v2();
        ASSERT_EQ(3UL, ptr.use_count());
    }
    ASSERT_EQ(1UL, ptr.use_count());
}

}   // namespace nebula

