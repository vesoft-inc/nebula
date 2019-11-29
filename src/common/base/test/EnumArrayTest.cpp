/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "base/EnumArray.h"


namespace nebula {



TEST(NextPowerTwoTest, SimpleTest) {
    EXPECT_EQ(2, NextPowerTwo<0>::ret);
    EXPECT_EQ(1, NextPowerTwo<1>::ret);
    EXPECT_EQ(2, NextPowerTwo<2>::ret);
    EXPECT_EQ(4, NextPowerTwo<3>::ret);
    EXPECT_EQ(4, NextPowerTwo<4>::ret);
    EXPECT_EQ(8, NextPowerTwo<5>::ret);
    EXPECT_EQ(8, NextPowerTwo<8>::ret);
    EXPECT_EQ(16, NextPowerTwo<9>::ret);
    EXPECT_EQ(32, NextPowerTwo<31>::ret);
    EXPECT_EQ(32, NextPowerTwo<32>::ret);
    EXPECT_EQ(128, NextPowerTwo<127>::ret);
}

template<typename Type, typename TypeArray>
void checkReadWrite() {
    TypeArray arr;
    for (size_t i = 0; i < arr.size(); i++) {
        arr.put(i, static_cast<Type>(i % arr.enumNum()));
    }

    for (size_t i = 0; i < arr.size(); i++) {
        EXPECT_EQ(static_cast<Type>(i % arr.enumNum()), arr[i]);
    }
}

TEST(EnumArrayTest, ReadWriteTest) {
    {
        LOG(INFO) << "Specify the items number by _ITEMS_NUM...";
        enum Type : uint8_t {
            one,
            two,
            three,
            _ITEMS_NUM = 3,
        };
        {
            using TypeArray = EnumArray<Type, 10>;
            checkReadWrite<Type, TypeArray>();
        }
        {
            using TypeArray = EnumArray<Type, 100>;
            checkReadWrite<Type, TypeArray>();
        }
    }

    {
        LOG(INFO) << "Specify the items number manually...";
        enum Type : uint8_t {
            one = 0x00,
            two = 0x01,
            three = 0x02,
        };
        {
            using TypeArray = EnumArray<Type, 10, 3>;
            checkReadWrite<Type, TypeArray>();
        }
        {
            using TypeArray = EnumArray<Type, 1000, 3>;
            checkReadWrite<Type, TypeArray>();
        }
    }
}

TEST(EnumArrayTest, OverwriteTest) {
    enum Type : uint8_t {
        one,
        two,
        three,
        _ITEMS_NUM = 3,
    };
    using TypeArray = EnumArray<Type, 10>;
    TypeArray arr;
    arr.put(0, Type::two);
    arr.put(0, Type::one);

    EXPECT_EQ(Type::one, arr[0]);
}

TEST(EnumArrayTest, IteratorTest) {
    enum Type : uint8_t {
        one,
        two,
        three,
        _ITEMS_NUM = 3,
    };

    using TypeArray = EnumArray<Type, 100>;
    TypeArray arr;
    for (size_t i = 0; i < arr.size(); i++) {
        arr.put(i, static_cast<Type>(i % arr.enumNum()));
    }
    size_t idx = 0;
    for (auto it = arr.begin(); it != arr.end(); it++, idx++) {
        EXPECT_EQ(static_cast<Type>(idx % arr.enumNum()), *it);
    }

    {
        auto it = std::find(arr.begin(), arr.end(), Type::one);
        CHECK(it == arr.begin());
    }
    {
        auto it = std::find(arr.begin(), arr.end(), Type::two);
        CHECK(it == arr.begin() + 1);
    }
    {
        auto it = std::find(arr.begin(), arr.end(), Type::three);
        CHECK(it == arr.begin() + 2);
        CHECK(arr.begin() + 90 == arr.end() - 10);
    }
}

}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

