/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "datatypes/Value.h"

namespace nebula {

TEST(Value, Arithmetics) {
    Value vInt1(1);
    Value vInt2(2);
    Value vFloat1(3.14);
    Value vFloat2(2.67);
    Value vStr1("Hello ");
    Value vStr2("World");
    Value vBool1(false);
    Value vBool2(true);
    Value vDate1(Date(2020, 1, 1));
    Value vDate2(Date(2019, 12, 1));

    Value v = vInt1 + vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(3L, v.getInt());
    v = vInt1 - vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(-1L, v.getInt());

    v = vFloat1 + vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(5.81, v.getFloat());
    v = vFloat1 - vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(0.47, v.getFloat());

    v = vStr1 + vStr2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello World"), v.getStr());

    v = vDate1 - vDate2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(31L, v.getInt());
    v = vDate2 - vDate1;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(-31L, v.getInt());

    v = vStr1 + vBool1;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello false"), v.getStr());
    v = vBool1 + vBool2;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());

    v = vInt1 + vFloat1;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(4.14, v.getFloat());
    v = vStr1 + vInt1;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 1"), v.getStr());

    v = vInt1 + vBool2;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());

    v = vInt2 + vDate1;
    EXPECT_EQ(Value::Type::DATE, v.type());
    EXPECT_EQ(Date(2020, 1, 3), v.getDate());
    v = vDate1 - vInt2;
    EXPECT_EQ(Value::Type::DATE, v.type());
    EXPECT_EQ(Date(2019, 12, 30), v.getDate());

    v = vFloat1 + vStr2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("3.140000World"), v.getStr());

    v = vFloat2 + vInt2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(4.67, v.getFloat());

    v = vStr1 + vInt2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 2"), v.getStr());

    v = vStr1 + vFloat2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 2.670000"), v.getStr());

    v = vStr1 + vBool2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello true"), v.getStr());

    v = vStr1 + vDate1;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 2020/01/01"), v.getStr());
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
