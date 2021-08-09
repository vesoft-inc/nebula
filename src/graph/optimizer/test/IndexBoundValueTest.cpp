/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "optimizer/OptimizerUtils.h"

namespace nebula {
namespace graph {

using OP = OptimizerUtils::BoundValueOperator;

TEST(IndexBoundValueTest, StringTest) {
    meta::cpp2::ColumnDef col;
    {
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(meta::cpp2::PropertyType::FIXED_STRING);
        typeDef.set_type_length(8);
        col.set_type(std::move(typeDef));
    }
    std::vector<unsigned char> max = {255, 255, 255, 255, 255, 255, 255, 255};
    std::vector<unsigned char> min = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
    auto maxStr = std::string(max.begin(), max.end());
    auto minStr = std::string(min.begin(), min.end());
    EXPECT_EQ(maxStr, OptimizerUtils::boundValue(col, OP::MAX, Value("aa")).getStr());
    EXPECT_EQ(maxStr, OptimizerUtils::boundValue(col, OP::MAX, Value(maxStr)).getStr());
    EXPECT_EQ(minStr, OptimizerUtils::boundValue(col, OP::MIN, Value("aa")).getStr());
    EXPECT_EQ(minStr, OptimizerUtils::boundValue(col, OP::MIN, Value("")).getStr());

    std::string retVal = OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value("aa")).getStr();
    std::vector<unsigned char> expected = {'a', 'a', '\0', '\0', '\0', '\0', '\0', '\001'};
    EXPECT_EQ(std::string(expected.begin(), expected.end()), retVal);

    EXPECT_EQ(maxStr, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(maxStr)).getStr());

    retVal = OptimizerUtils::boundValue(col, OP::LESS_THAN, Value("aa")).getStr();
    expected = {'a', '`', 255, 255, 255, 255, 255, 255};
    EXPECT_EQ(std::string(expected.begin(), expected.end()), retVal);

    retVal = OptimizerUtils::boundValue(col, OP::LESS_THAN, Value("")).getStr();
    EXPECT_EQ(minStr, retVal);

    retVal = OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(minStr)).getStr();
    EXPECT_EQ(minStr, retVal);

    {
        auto actual = "ABCDEFGHIJKLMN";
        auto expectGT = "ABCDEFGI";
        auto expectLT = "ABCDEFGG";
        EXPECT_EQ(expectGT,
                  OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(actual)).getStr());
        EXPECT_EQ(expectLT,
                  OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(actual)).getStr());
    }
    {
        std::vector<unsigned char> act = {255, 255, 255, 254, 255, 255, 255, 255};
        std::vector<unsigned char> exp = {255, 255, 255, 255, 0, 0, 0, 0};
        auto actStr = std::string(act.begin(), act.end());
        auto expStr = std::string(exp.begin(), exp.end());
        EXPECT_EQ(expStr,
                  OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(actStr)).getStr());
    }
    {
        std::vector<unsigned char> act = {255, 255, 255, 0, 0, 0, 0, 0};
        std::vector<unsigned char> exp = {255, 255, 254, 255, 255, 255, 255, 255};
        auto actStr = std::string(act.begin(), act.end());
        auto expStr = std::string(exp.begin(), exp.end());
        EXPECT_EQ(expStr, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(actStr)).getStr());
    }
}

TEST(IndexBoundValueTest, IntTest) {
    meta::cpp2::ColumnDef col;
    {
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(meta::cpp2::PropertyType::INT64);
        col.set_type(std::move(typeDef));
    }
    auto maxInt = std::numeric_limits<int64_t>::max();
    auto minInt = std::numeric_limits<int64_t>::min();
    EXPECT_EQ(maxInt, OptimizerUtils::boundValue(col, OP::MAX, Value(maxInt)).getInt());
    EXPECT_EQ(maxInt, OptimizerUtils::boundValue(col, OP::MAX, Value(1L)).getInt());
    EXPECT_EQ(minInt, OptimizerUtils::boundValue(col, OP::MIN, Value(minInt)).getInt());
    EXPECT_EQ(minInt, OptimizerUtils::boundValue(col, OP::MIN, Value(1L)).getInt());

    EXPECT_EQ(minInt, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(minInt)).getInt());
    EXPECT_EQ(maxInt, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(maxInt)).getInt());
    EXPECT_EQ(minInt + 1,
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(minInt)).getInt());
    EXPECT_EQ(maxInt - 1,
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(maxInt)).getInt());

    EXPECT_EQ(1, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(0L)).getInt());
    EXPECT_EQ(-1, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(0L)).getInt());

    EXPECT_EQ(6, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(5L)).getInt());
    EXPECT_EQ(4, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(5L)).getInt());
}

TEST(IndexBoundValueTest, DoubleTest) {
    meta::cpp2::ColumnDef col;
    {
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(meta::cpp2::PropertyType::DOUBLE);
        col.set_type(std::move(typeDef));
    }
    auto maxDouble = std::numeric_limits<double_t>::max();
    auto minDouble = std::numeric_limits<double_t>::min();
    EXPECT_EQ(maxDouble, OptimizerUtils::boundValue(col, OP::MAX, Value(maxDouble)).getFloat());
    EXPECT_EQ(maxDouble, OptimizerUtils::boundValue(col, OP::MAX, Value(1.1)).getFloat());
    EXPECT_EQ(-maxDouble, OptimizerUtils::boundValue(col, OP::MIN, Value(minDouble)).getFloat());
    EXPECT_EQ(-maxDouble, OptimizerUtils::boundValue(col, OP::MIN, Value(1.1)).getFloat());

    EXPECT_EQ(0.0,
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(-minDouble)).getFloat());
    EXPECT_EQ(maxDouble - kEpsilon,
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(maxDouble)).getFloat());
    EXPECT_EQ(-minDouble, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(0.0)).getFloat());

    EXPECT_EQ(5.1 - kEpsilon,
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(5.1)));

    EXPECT_EQ(-(5.1 + kEpsilon),
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(-5.1)));

    EXPECT_EQ(maxDouble,
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(maxDouble)).getFloat());
    EXPECT_EQ(0.0,
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(-minDouble)).getFloat());

    EXPECT_EQ(minDouble, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(0.0)).getFloat());

    EXPECT_EQ(5.1 + kEpsilon,
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(5.1)).getFloat());

    EXPECT_EQ(-(5.1 - kEpsilon),
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(-5.1)).getFloat());
}

TEST(IndexBoundValueTest, DateTest) {
    meta::cpp2::ColumnDef col;
    {
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(meta::cpp2::PropertyType::DATE);
        col.set_type(std::move(typeDef));
    }
    auto maxYear = std::numeric_limits<int16_t>::max();
    EXPECT_EQ(Date(maxYear, 12, 31),
              OptimizerUtils::boundValue(col, OP::MAX, Value(Date())));
    EXPECT_EQ(Date(),
              OptimizerUtils::boundValue(col, OP::MIN, Value(Date(maxYear, 12, 31))));
    EXPECT_EQ(Date(2021, 1, 1),
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(Date(2020, 12, 31))));
    EXPECT_EQ(Date(maxYear, 12, 31),
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(Date(maxYear, 12, 31))));
    EXPECT_EQ(Date(2020, 1, 2),
              OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(Date(2020, 1, 1))));
    EXPECT_EQ(Date(0, 1, 2), OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(Date())));
    EXPECT_EQ(Date(), OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(Date())));
    EXPECT_EQ(Date(2019, 12, 30),
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(Date(2019, 12, 31))));
    EXPECT_EQ(Date(2018, 12, 31),
              OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(Date(2019, 1, 1))));
}

TEST(IndexBoundValueTest, TimeTest) {
    meta::cpp2::ColumnDef col;
    {
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(meta::cpp2::PropertyType::TIME);
        col.set_type(std::move(typeDef));
    }
    Time maxT{23, 59, 59, 999999};

    Time minT = Time();

    EXPECT_EQ(maxT, OptimizerUtils::boundValue(col, OP::MAX, Value(maxT)).getTime());
    EXPECT_EQ(minT, OptimizerUtils::boundValue(col, OP::MIN, Value(maxT)).getTime());
    EXPECT_EQ(maxT, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(maxT)).getTime());

    {
        Time actual, expect;
        actual.microsec = 999999;
        actual.sec = 59;
        actual.minute = 59;
        actual.hour = 22;

        expect.microsec = 0;
        expect.sec = 0;
        expect.minute = 0;
        expect.hour = 23;
        EXPECT_EQ(expect,
                  OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(actual)).getTime());
    }
    {
        Time actual, expect;
        actual.microsec = 999999;
        actual.sec = 34;
        actual.minute = 59;
        actual.hour = 23;

        expect.microsec = 0;
        expect.sec = 35;
        expect.minute = 59;
        expect.hour = 23;
        EXPECT_EQ(expect,
                  OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(actual)).getTime());
    }
    {
        Time expect = Time();
        EXPECT_EQ(expect, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(expect)));
    }
    {
        Time actual, expect;
        actual.microsec = 999999;
        actual.sec = 34;
        actual.minute = 59;
        actual.hour = 23;

        expect.microsec = 999998;
        expect.sec = 34;
        expect.minute = 59;
        expect.hour = 23;
        EXPECT_EQ(expect,
                  OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(actual)).getTime());
    }
}

TEST(IndexBoundValueTest, DateTimeTest) {
    meta::cpp2::ColumnDef col;
    {
        meta::cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(meta::cpp2::PropertyType::DATETIME);
        col.set_type(std::move(typeDef));
    }
    DateTime maxDT;
    {
        maxDT.microsec = 999999;
        maxDT.sec = 59;
        maxDT.minute = 59;
        maxDT.hour = 23;
        maxDT.day = 31;
        maxDT.month = 12;
        maxDT.year = std::numeric_limits<int16_t>::max();
    }

    DateTime minDT = DateTime();

    EXPECT_EQ(maxDT, OptimizerUtils::boundValue(col, OP::MAX, Value(maxDT)).getDateTime());
    EXPECT_EQ(minDT, OptimizerUtils::boundValue(col, OP::MIN, Value(maxDT)).getDateTime());
    EXPECT_EQ(maxDT, OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(maxDT)).getDateTime());

    {
        DateTime actual, expect;
        actual.microsec = 999999;
        actual.sec = 59;
        actual.minute = 59;
        actual.hour = 23;
        actual.day = 31;
        actual.month = 12;
        actual.year = 2020;

        expect.microsec = 0;
        expect.sec = 0;
        expect.minute = 0;
        expect.hour = 0;
        expect.day = 1;
        expect.month = 1;
        expect.year = 2021;
        EXPECT_EQ(expect,
                  OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(actual)).getDateTime());
    }
    {
        DateTime actual, expect;
        actual.microsec = 999999;
        actual.sec = 34;
        actual.minute = 59;
        actual.hour = 23;
        actual.day = 31;
        actual.month = 12;
        actual.year = 2020;

        expect.microsec = 0;
        expect.sec = 35;
        expect.minute = 59;
        expect.hour = 23;
        expect.day = 31;
        expect.month = 12;
        expect.year = 2020;
        EXPECT_EQ(expect,
                  OptimizerUtils::boundValue(col, OP::GREATER_THAN, Value(actual)).getDateTime());
    }
    {
        DateTime expect = DateTime();
        EXPECT_EQ(expect, OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(expect)));
    }
    {
        DateTime actual, expect;
        actual.microsec = 999999;
        actual.sec = 34;
        actual.minute = 60;
        actual.hour = 24;
        actual.day = 31;
        actual.month = 12;
        actual.year = 2020;

        expect.microsec = 999998;
        expect.sec = 34;
        expect.minute = 60;
        expect.hour = 24;
        expect.day = 31;
        expect.month = 12;
        expect.year = 2020;
        EXPECT_EQ(expect,
                  OptimizerUtils::boundValue(col, OP::LESS_THAN, Value(actual)).getDateTime());
    }
}

}   // namespace graph
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
