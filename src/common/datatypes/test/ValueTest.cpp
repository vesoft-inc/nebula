/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "common/base/Base.h"
#include "common/datatypes/CommonCpp2Ops.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Date.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Geography.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

TEST(Value, Arithmetics) {
  Value vZero(0);
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
  Value vList1(List({1, 3, 2}));
  Value vList2(List({6, 4, 5}));
  Value vSet(Set({8, 7}));
  Value vMap(Map({{"a", 9}, {"b", 10}}));

  // +
  {
    Value v = vInt1 + vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(3L, v.getInt());

    // overflow
    v = Value(INT64_MAX) + 1;
    EXPECT_EQ(Value::kNullOverflow, v);
    v = Value(INT64_MIN) + -1;
    EXPECT_EQ(Value::kNullOverflow, v);

    v = vFloat1 + vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(5.81, v.getFloat());

    v = vStr1 + vStr2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello World"), v.getStr());

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

    v = vFloat1 + vStr2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("3.14World"), v.getStr());

    v = vFloat2 + vInt2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(4.67, v.getFloat());

    v = vStr1 + vInt2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 2"), v.getStr());

    v = vStr1 + vFloat2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 2.67"), v.getStr());

    v = vStr1 + vBool2;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello true"), v.getStr());

    v = vStr1 + vDate1;
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Hello 2020-01-01"), v.getStr());

    v = vList1 + vList2;
    EXPECT_EQ(Value::Type::LIST, v.type());
    EXPECT_EQ(List({1, 3, 2, 6, 4, 5}), v.getList());

    v = vList1 + vBool2;
    EXPECT_EQ(Value::Type::LIST, v.type());
    EXPECT_EQ(List({1, 3, 2, true}), v.getList());

    v = vStr1 + vList2;
    EXPECT_EQ(Value::Type::LIST, v.type());
    EXPECT_EQ(List({std::string("Hello "), 6, 4, 5}), v.getList());

    v = vMap + vList1;
    EXPECT_EQ(Value::Type::LIST, v.type());
    EXPECT_EQ(List({Map({{"a", 9}, {"b", 10}}), 1, 3, 2}), v.getList());

    v = vList2 + vSet;
    EXPECT_EQ(Value::Type::LIST, v.type());
    EXPECT_EQ(List({6, 4, 5, Set({8, 7})}), v.getList());

    // str + float
    v = Value("Allen Wilson") + Value(30.142857142857142);
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Allen Wilson30.142857142857142"), v.getStr());
    v = Value("Allen Wilson") + Value(-30.142857142857142);
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("Allen Wilson-30.142857142857142"), v.getStr());
    v = Value(30.142857142857142) + Value("Allen Wilson");
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("30.142857142857142Allen Wilson"), v.getStr());
    v = Value(-30.142857142857142) + Value("Allen Wilson");
    EXPECT_EQ(Value::Type::STRING, v.type());
    EXPECT_EQ(std::string("-30.142857142857142Allen Wilson"), v.getStr());
  }
  {
    // Duration
    Value lhs = Duration(-1, -2, -3);
    Value rhs = Duration(-1, -2, -3);
    auto result = lhs + rhs;
    EXPECT_EQ(result, Value(Duration(-2, -4, -6)));
  }
  {
    // Duration
    Value lhs = Duration(0, 0, 0);
    Value rhs = Duration(-1, -2, -3);
    auto result = lhs + rhs;
    EXPECT_EQ(result, Value(Duration(-1, -2, -3)));
  }
  {
    // Duration
    Value lhs = Duration(1, 2, 3);
    Value rhs = Duration(-1, -2, -3);
    auto result = lhs + rhs;
    EXPECT_EQ(result, Value(Duration(0, 0, 0)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration();
    EXPECT_EQ(lhs, lhs + rhs);
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addYears(3);
    EXPECT_EQ(lhs + rhs, Value(Date(2023, 1, 1)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addMonths(3);
    EXPECT_EQ(lhs + rhs, Value(Date(2020, 4, 1)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addDays(31);
    EXPECT_EQ(lhs + rhs, Value(Date(2020, 2, 1)));
  }
  {
    // Date leap year
    Value lhs = Date(2020, 2, 1);
    Value rhs = Duration().addDays(28);
    EXPECT_EQ(lhs + rhs, Value(Date(2020, 2, 29)));
  }
  {
    // Date common year
    Value lhs = Date(2021, 2, 1);
    Value rhs = Duration().addDays(28);
    EXPECT_EQ(lhs + rhs, Value(Date(2021, 3, 1)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addHours(24);
    EXPECT_EQ(lhs + rhs, Value(Date(2020, 1, 2)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addHours(23);
    EXPECT_EQ(lhs + rhs, Value(Date(2020, 1, 1)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addHours(23);
    EXPECT_EQ(lhs + rhs, Value(Time(2, 30, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addHours(3);
    EXPECT_EQ(lhs + rhs, Value(Time(6, 30, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addHours(3).addMinutes(30);
    EXPECT_EQ(lhs + rhs, Value(Time(7, 0, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(100);
    EXPECT_EQ(lhs + rhs, Value(Time(3, 30, 30, 200)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(999900);
    EXPECT_EQ(lhs + rhs, Value(Time(3, 30, 31, 0)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration();
    EXPECT_EQ(lhs, lhs + rhs);
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addYears(3);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2023, 1, 1, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMonths(3);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 4, 1, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addDays(31);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 2, 1, 3, 30, 30, 100)));
  }
  {
    // DateTime leap year
    Value lhs = DateTime(2020, 2, 1, 3, 30, 30, 100);
    Value rhs = Duration().addDays(28);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 2, 29, 3, 30, 30, 100)));
  }
  {
    // DateTime common year
    Value lhs = DateTime(2021, 2, 1, 3, 30, 30, 100);
    Value rhs = Duration().addDays(28);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2021, 3, 1, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(24);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 2, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(20);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 1, 23, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(23);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 2, 2, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(3);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 1, 6, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(3).addMinutes(30);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 1, 7, 0, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(100);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 1, 3, 30, 30, 200)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(999900);
    EXPECT_EQ(lhs + rhs, Value(DateTime(2020, 1, 1, 3, 30, 31, 0)));
  }

  // -
  {
    Value v = vInt1 - vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(-1L, v.getInt());

    // overflow
    v = Value(INT64_MAX) - -1;
    EXPECT_EQ(Value::kNullOverflow, v);
    v = Value(INT64_MIN) - 1;
    EXPECT_EQ(Value::kNullOverflow, v);

    v = vFloat1 - vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(0.47, v.getFloat());

    v = vDate1 - vDate2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(31L, v.getInt());
    v = vDate2 - vDate1;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(-31L, v.getInt());

    v = vDate1 - vInt2;
    EXPECT_EQ(Value::Type::DATE, v.type());
    EXPECT_EQ(Date(2019, 12, 30), v.getDate());
  }
  {
    // Duration
    Value lhs = Duration(1, 2, 3);
    Value rhs = Duration();
    auto result = lhs - rhs;
    EXPECT_EQ(result, Value(Duration(1, 2, 3)));
  }
  {
    // Duration
    Value lhs = Duration(1, 2, 3);
    Value rhs = Duration(-1, -2, -3);
    auto result = lhs - rhs;
    EXPECT_EQ(result, Value(Duration(2, 4, 6)));
  }
  {
    // Duration
    Value lhs = Duration(-1, -2, -3);
    Value rhs = Duration(-1, -2, -3);
    auto result = lhs - rhs;
    EXPECT_EQ(result, Value(Duration(0, 0, 0)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addYears(3);
    EXPECT_EQ(lhs - rhs, Value(Date(2017, 1, 1)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addMonths(3);
    EXPECT_EQ(lhs - rhs, Value(Date(2019, 10, 1)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addDays(3);
    EXPECT_EQ(lhs - rhs, Value(Date(2019, 12, 29)));
  }
  {
    // Date leap year
    Value lhs = Date(2020, 3, 1);
    Value rhs = Duration().addDays(3);
    EXPECT_EQ(lhs - rhs, Value(Date(2020, 2, 27)));
  }
  {
    // Date common year
    Value lhs = Date(2021, 3, 1);
    Value rhs = Duration().addDays(3);
    EXPECT_EQ(lhs - rhs, Value(Date(2021, 2, 26)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addHours(24);
    EXPECT_EQ(lhs - rhs, Value(Date(2019, 12, 31)));
  }
  {
    // Date
    Value lhs = Date(2020, 1, 1);
    Value rhs = Duration().addHours(23);
    EXPECT_EQ(lhs - rhs, Value(Date(2020, 1, 1)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addHours(3);
    EXPECT_EQ(lhs - rhs, Value(Time(0, 30, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addHours(4);
    EXPECT_EQ(lhs - rhs, Value(Time(23, 30, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addMinutes(30);
    EXPECT_EQ(lhs - rhs, Value(Time(3, 0, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addMinutes(40);
    EXPECT_EQ(lhs - rhs, Value(Time(2, 50, 30, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addSeconds(30);
    EXPECT_EQ(lhs - rhs, Value(Time(3, 30, 0, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addSeconds(40);
    EXPECT_EQ(lhs - rhs, Value(Time(3, 29, 50, 100)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(100);
    EXPECT_EQ(lhs - rhs, Value(Time(3, 30, 30, 0)));
  }
  {
    // Time
    Value lhs = Time(3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(200);
    EXPECT_EQ(lhs - rhs, Value(Time(3, 30, 29, 999900)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addYears(3);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2017, 1, 1, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMonths(3);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2019, 10, 1, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addDays(3);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2019, 12, 29, 3, 30, 30, 100)));
  }
  {
    // DateTime leap year
    Value lhs = DateTime(2020, 3, 1, 3, 30, 30, 100);
    Value rhs = Duration().addDays(3);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 2, 27, 3, 30, 30, 100)));
  }
  {
    // DateTime common year
    Value lhs = DateTime(2021, 3, 1, 3, 30, 30, 100);
    Value rhs = Duration().addDays(3);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2021, 2, 26, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(24);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2019, 12, 31, 3, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(3);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 0, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addHours(4);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2019, 12, 31, 23, 30, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMinutes(30);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 3, 0, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMinutes(40);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 2, 50, 30, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addSeconds(30);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 3, 30, 0, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addSeconds(40);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 3, 29, 50, 100)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(100);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 3, 30, 30, 0)));
  }
  {
    // DateTime
    Value lhs = DateTime(2020, 1, 1, 3, 30, 30, 100);
    Value rhs = Duration().addMicroseconds(200);
    EXPECT_EQ(lhs - rhs, Value(DateTime(2020, 1, 1, 3, 30, 29, 999900)));
  }

  // *
  {
    Value v = vInt2 * vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ((vInt2.getInt() * vInt2.getInt()), v.getInt());

    // overflow
    v = Value(INT64_MAX) * 2;
    EXPECT_EQ(Value::kNullOverflow, v);
    // edge case -1 * MIN  =>  overflow
    v = Value(INT64_MIN) * -1;
    EXPECT_EQ(Value::kNullOverflow, v);
    v = -1 * Value(INT64_MIN);
    EXPECT_EQ(Value::kNullOverflow, v);
    v = Value(INT64_MIN) * 2;
    EXPECT_EQ(Value::kNullOverflow, v);

    v = vInt2 * vFloat1;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ((vInt2.getInt() * vFloat1.getFloat()), v.getFloat());

    v = vFloat1 * vInt2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ((vFloat1.getFloat() * vInt2.getInt()), v.getFloat());

    v = vFloat1 * vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ((vFloat1.getFloat() * vFloat2.getFloat()), v.getFloat());
  }
  {
    // Duration
    Value lhs = Value(Duration(1, 2, 3));
    Value rhs = Value(Duration());
    auto result = lhs * rhs;
    EXPECT_TRUE(result.isNull());
  }

  // /
  {
    Value v = vInt2 / vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ((vInt2.getInt() / vInt2.getInt()), v.getInt());

    // overflow
    v = Value(INT64_MIN) / -1;
    EXPECT_EQ(Value::kNullOverflow, v);

    v = vInt2 / vFloat1;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ((vInt2.getInt() / vFloat1.getFloat()), v.getFloat());

    v = vFloat1 / vInt2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ((vFloat1.getFloat() / vInt2.getInt()), v.getFloat());

    v = vFloat1 / vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ((vFloat1.getFloat() / vFloat2.getFloat()), v.getFloat());

    v = vFloat1 / vZero;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
    v = vInt1 / vZero;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
  }
  {
    // Duration
    Value lhs = Value(Duration(1, 2, 3));
    Value rhs = Value(Duration());
    auto result = lhs / rhs;
    EXPECT_TRUE(result.isNull());
  }

  // %
  {
    Value v = vInt2 % vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ((vInt2.getInt() % vInt2.getInt()), v.getInt());

    v = vInt2 % vFloat1;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ(std::fmod(vInt2.getInt(), vFloat1.getFloat()), v.getFloat());

    v = vFloat1 % vInt2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ(std::fmod(vFloat1.getFloat(), vInt2.getInt()), v.getFloat());

    v = vFloat1 % vFloat2;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_EQ(std::fmod(vFloat1.getFloat(), vFloat2.getFloat()), v.getFloat());

    v = vFloat1 % vZero;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
    v = vInt1 % vZero;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
  }
  {
    // Duration
    Value lhs = Duration(1, 2, 3);
    Value rhs = Duration();
    auto result = lhs % rhs;
    EXPECT_TRUE(result.isNull());
  }

  // unary -,!
  {
    Value v = -vInt1;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(-1L, v.getInt());

    v = -vFloat1;
    EXPECT_EQ(Value::Type::FLOAT, v.type());
    EXPECT_DOUBLE_EQ(-3.14, v.getFloat());

    // Overflow
    v = -Value(INT64_MIN);
    EXPECT_EQ(Value::kNullOverflow, v);

    v = !vBool1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_DOUBLE_EQ(true, v.getBool());

    v = !vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_DOUBLE_EQ(false, v.getBool());
  }
  {
    // Duration
    Value v = Duration(1, 2, 3);
    EXPECT_EQ(Duration(-1, -2, -3), -v);
  }
  {
    // Duration
    Value v = Duration(-1, -2, -3);
    EXPECT_EQ(Duration(1, 2, 3), -v);
  }
}

TEST(Value, Comparison) {
  Value vNull(nebula::NullType::__NULL__);
  Value vEmpty;
  Value vInt1(1);
  Value vInt2(2);
  Value vFloat1(3.14);
  Value vFloat2(2.67);
  Value vStr1("Hello ");
  Value vStr2("World");
  Value vBool1(false);
  Value vBool2(true);
  List vList1({2, 1, 3});
  List vList2({2, 1});
  List vList3({-4, 1, 30});
  Value vTime1(Time(23, 19, 55, 23));
  Value vTime2(Time(00, 12, 45, 32));
  Value vDate1(Date(2020, 1, 1));
  Value vDate2(Date(2019, 12, 1));
  Value vDateTime1(DateTime(1998, 9, 8, 12, 30, 04, 56));
  Value vDateTime2(DateTime(1998, 9, 8, 13, 30, 04, 56));
  Value vDateTime3(DateTime(1998, 9, 8, 13, 30, 04, 56));  // for further tests
  Value vGeo1 = Geography::fromWKT("POINT(4 7)").value();
  Value vGeo2 = Geography::fromWKT("POINT(5 7)").value();
  Value vGeo3 = Geography::fromWKT("POINT(5 7)").value();

  // null/empty
  {
    Value v = vNull == vNull;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vInt1 == vNull;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = Value() == vEmpty;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vInt1 == vEmpty;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vNull < vNull;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 < vNull;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vInt1 > vNull;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = Value() < vEmpty;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 < vEmpty;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 > vEmpty;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vList1 > vList2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vList1 > vList3;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vList3 < vList2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }

  // int
  {
    Value v = vInt1 == vInt2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 != vInt2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vInt1 > vInt2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 < vInt2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vInt1 >= vInt2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 <= vInt2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }

  // float
  {
    Value v = vFloat1 == vFloat1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vFloat1 == vFloat2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vFloat1 != vFloat2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vFloat1 > vFloat2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vFloat1 < vFloat2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vFloat1 >= vFloat2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vFloat1 <= vFloat2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());
  }

  // int and float
  {
    Value v = vInt1 == vFloat1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vInt1 < vFloat1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vInt1 > vFloat1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());
  }

  // str
  {
    Value v = vStr1 == vStr2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vStr1 != vStr2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vStr1 > vStr2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vStr1 < vStr2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vStr1 >= vStr2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vStr1 <= vStr2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }

  // bool
  {
    Value v = vBool1 == vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vBool1 != vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vBool1 > vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vBool1 < vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vBool1 >= vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vBool1 <= vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }
  // time
  {
    Value v = vTime1 == vTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDate1 != vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDate1 > vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDate1 < vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDate1 >= vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDate1 <= vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());
  }
  // date
  {
    Value v = vDate1 == vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDate1 != vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDate1 > vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDate1 < vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDate1 >= vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDate1 <= vDate2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());
  }
  // datetime
  {
    Value v = vDateTime1 == vDateTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDateTime1 != vDateTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDateTime1 > vDateTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDateTime1 < vDateTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vDateTime1 >= vDateTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vDateTime1 <= vDateTime2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }
  // geography
  {
    Value v = vGeo1 == vGeo2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vGeo1 != vGeo2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vGeo2 == vGeo3;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vGeo2 != vGeo3;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());
    // TODO(jie) Add more geo test
  }
  // Duration
  {
    Value lhs{Duration(1, 2, 4)};
    Value rhs{Duration(1, 2, 3)};
    EXPECT_TRUE(lhs.lessThan(rhs).isNull());
    EXPECT_FALSE(lhs.equal(rhs).getBool());
  }
  {
    Value lhs{Duration(1, 2, 3)};
    Value rhs{Duration(1, 2, 3)};
    EXPECT_TRUE(lhs.lessThan(rhs).isNull());
    EXPECT_TRUE(lhs.equal(rhs).getBool());
  }
}

TEST(Value, Logical) {
  Value vZero(0);
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

  {
    Value v = vBool1 && vBool1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vBool1 && vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vBool2 && vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }

  {
    Value v = vBool1 || vBool1;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(false, v.getBool());

    v = vBool1 || vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());

    v = vBool2 || vBool2;
    EXPECT_EQ(Value::Type::BOOL, v.type());
    EXPECT_EQ(true, v.getBool());
  }
}

TEST(Value, TypeCast) {
  Value vInt(1);
  Value vFloat(3.14);
  Value vStr1("TrUe");
  Value vStr2("3.14");
  Value vBool1(false);
  Value vBool2(true);
  Value vDate(Date(2020, 1, 1));
  Value vNull(NullType::__NULL__);
  Value vIntMin(std::numeric_limits<int64_t>::min());
  Value vIntMax(std::numeric_limits<int64_t>::max());
  Value vFloatMin(std::numeric_limits<double_t>::lowest());  // non-negative
  Value vFloatMax(std::numeric_limits<double_t>::max());

  {
    auto vb1 = vInt.toBool();
    EXPECT_EQ(Value::Type::NULLVALUE, vb1.type());

    auto vb2 = vFloat.toBool();
    EXPECT_EQ(Value::Type::NULLVALUE, vb2.type());

    auto vb3 = vStr1.toBool();
    EXPECT_EQ(Value::Type::BOOL, vb3.type());
    EXPECT_EQ(true, vb3.getBool());

    auto vb4 = vStr2.toBool();
    EXPECT_EQ(Value::Type::NULLVALUE, vb4.type());

    auto vb5 = vBool1.toBool();
    EXPECT_EQ(Value::Type::BOOL, vb5.type());
    EXPECT_EQ(false, vb5.getBool());

    auto vb6 = vBool2.toBool();
    EXPECT_EQ(Value::Type::BOOL, vb6.type());
    EXPECT_EQ(true, vb6.getBool());

    auto vb7 = vDate.toBool();
    EXPECT_EQ(Value::Type::NULLVALUE, vb7.type());

    auto vb8 = vNull.toBool();
    EXPECT_EQ(Value::Type::NULLVALUE, vb8.type());
  }
  {
    auto vf = vInt.toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), 1.0);

    vf = vFloat.toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), 3.14);

    vf = vStr1.toFloat();
    EXPECT_EQ(Value::Type::NULLVALUE, vf.type());

    vf = vStr2.toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), 3.14);

    vf = vBool1.toFloat();
    EXPECT_EQ(Value::Type::NULLVALUE, vf.type());

    vf = vBool2.toFloat();
    EXPECT_EQ(Value::Type::NULLVALUE, vf.type());

    vf = vDate.toFloat();
    EXPECT_EQ(Value::Type::NULLVALUE, vf.type());

    vf = vNull.toFloat();
    EXPECT_EQ(Value::Type::NULLVALUE, vf.type());

    vf = vIntMin.toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), std::numeric_limits<int64_t>::min());

    vf = vIntMax.toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), static_cast<double>(std::numeric_limits<int64_t>::max()));

    // String of int
    vf = Value("-9223372036854775807").toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), static_cast<double>(std::numeric_limits<int64_t>::min() + 1));

    vf = Value("-9223372036854775808").toFloat();  // will be converted to -9223372036854776000.0
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), static_cast<double>(std::numeric_limits<int64_t>::min()));

    vf = Value("9223372036854775807").toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), static_cast<double>(std::numeric_limits<int64_t>::max()));

    // String of double
    vf = Value("123.").toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), 123.0);

    vf = Value(std::to_string(std::numeric_limits<double_t>::max())).toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), std::numeric_limits<double_t>::max());

    vf = Value(std::to_string(std::numeric_limits<double_t>::max())).toFloat();
    EXPECT_EQ(Value::Type::FLOAT, vf.type());
    EXPECT_EQ(vf.getFloat(), std::numeric_limits<double_t>::max());

    // Invalid string
    vf = Value("12abc").toFloat();
    EXPECT_EQ(Value::kNullValue, vf);

    vf = Value("1.2abc").toFloat();
    EXPECT_EQ(Value::kNullValue, vf);
  }
  {
    auto vi = vInt.toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), 1);

    vi = vFloat.toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), 3);

    vi = vStr1.toInt();
    EXPECT_EQ(Value::Type::NULLVALUE, vi.type());

    vi = vStr2.toInt();

    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), 3);

    vi = vBool1.toInt();
    EXPECT_EQ(Value::Type::NULLVALUE, vi.type());

    vi = vBool2.toInt();
    EXPECT_EQ(Value::Type::NULLVALUE, vi.type());

    vi = vDate.toInt();
    EXPECT_EQ(Value::Type::NULLVALUE, vi.type());

    vi = vNull.toInt();
    EXPECT_EQ(Value::Type::NULLVALUE, vi.type());

    vi = vFloatMin.toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), std::numeric_limits<int64_t>::min());

    vi = vFloatMax.toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), std::numeric_limits<int64_t>::max());

    // string of int
    vi = Value("123.").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), 123);

    vi = Value("-9223372036854775807").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), std::numeric_limits<int64_t>::min() + 1);

    vi = Value("-9223372036854775808").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), std::numeric_limits<int64_t>::min());

    vi = Value("9223372036854775807").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(vi.getInt(), std::numeric_limits<int64_t>::max());

    // String to int Overflow
    vi = Value("9223372036854775808").toInt();
    EXPECT_EQ(Value::kNullOverflow, vi);

    vi = Value("-9223372036854775809").toInt();
    EXPECT_EQ(Value::kNullOverflow, vi);

    // Invalid string
    vi = Value("12abc").toInt();
    EXPECT_EQ(Value::kNullValue, vi);

    vi = Value("1.2abc").toInt();
    EXPECT_EQ(Value::kNullValue, vi);

    // string representing scientific notation
    vi = Value("1e3").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(1000, vi.getInt());

    vi = Value("1.0e3").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(1000, vi.getInt());

    vi = Value("5E4").toInt();
    EXPECT_EQ(Value::Type::INT, vi.type());
    EXPECT_EQ(50000, vi.getInt());

    vi = Value("1.5E3").toInt();
    EXPECT_EQ(1500, vi.getInt());

    // fail to parse
    vi = Value("1.5.2E3").toInt();
    EXPECT_EQ(Value::kNullValue, vi);

    vi = Value("eeeEE").toInt();
    EXPECT_EQ(Value::kNullValue, vi);

    vi = Value("1e").toInt();
    EXPECT_EQ(Value::kNullValue, vi);

    vi = Value("E3").toInt();
    EXPECT_EQ(Value::kNullValue, vi);
  }
}

TEST(Value, Bit) {
  Value vNull(nebula::NullType::__NULL__);
  Value vEmpty;
  Value vZero(0);
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

  {
    Value v = vInt1 & vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(0, v.getInt());

    v = vBool1 & vBool2;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
    EXPECT_EQ(NullType::BAD_TYPE, v.getNull());

    v = vStr1 & vStr2;
    EXPECT_TRUE(v.isNull());

    v = vFloat1 & vFloat2;
    EXPECT_TRUE(v.isNull());

    v = vDate1 & vDate2;
    EXPECT_TRUE(v.isNull());

    v = vEmpty & true;
    EXPECT_TRUE(v.empty());

    v = vEmpty & false;
    EXPECT_TRUE(v.empty());

    v = true & vEmpty;
    EXPECT_TRUE(v.empty());

    v = false & vEmpty;
    EXPECT_TRUE(v.empty());

    v = vNull & true;
    EXPECT_TRUE(v.isNull());

    v = vNull & false;
    EXPECT_TRUE(v.isNull());

    v = true & vNull;
    EXPECT_TRUE(v.isNull());

    v = false & vNull;
    EXPECT_TRUE(v.isNull());

    v = vEmpty & vNull;
    EXPECT_TRUE(v.isNull());

    v = vEmpty & vEmpty;
    EXPECT_TRUE(v.empty());

    v = vNull & vNull;
    EXPECT_TRUE(v.isNull());

    v = vNull & vEmpty;
    EXPECT_TRUE(v.isNull());
  }
  // Duration
  {
    Value v = Date() & Duration();
    EXPECT_TRUE(v.isNull());
  }

  {
    Value v = vInt1 | vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(3, v.getInt());

    v = vBool1 | vBool2;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
    EXPECT_EQ(NullType::BAD_TYPE, v.getNull());

    v = vStr1 | vStr2;
    EXPECT_TRUE(v.isNull());

    v = vFloat1 | vFloat2;
    EXPECT_TRUE(v.isNull());

    v = vDate1 | vDate2;
    EXPECT_TRUE(v.isNull());

    v = vEmpty | true;
    EXPECT_TRUE(v.empty());

    v = vEmpty | false;
    EXPECT_TRUE(v.empty());

    v = true | vEmpty;
    EXPECT_TRUE(v.empty());

    v = false | vEmpty;
    EXPECT_TRUE(v.empty());

    v = vNull | true;
    EXPECT_TRUE(v.isNull());

    v = vNull | false;
    EXPECT_TRUE(v.isNull());

    v = true | vNull;
    EXPECT_TRUE(v.isNull());

    v = false | vNull;
    EXPECT_TRUE(v.isNull());

    v = vEmpty | vNull;
    EXPECT_TRUE(v.isNull());

    v = vEmpty | vEmpty;
    EXPECT_TRUE(v.empty());

    v = vNull | vNull;
    EXPECT_TRUE(v.isNull());

    v = vNull | vEmpty;
    EXPECT_TRUE(v.isNull());
  }
  // Duration
  {
    Value v = Date() | Duration();
    EXPECT_TRUE(v.isNull());
  }

  {
    Value v = vInt1 ^ vInt2;
    EXPECT_EQ(Value::Type::INT, v.type());
    EXPECT_EQ(3, v.getInt());

    v = vBool1 ^ vBool2;
    EXPECT_EQ(Value::Type::NULLVALUE, v.type());
    EXPECT_EQ(NullType::BAD_TYPE, v.getNull());

    v = vStr1 ^ vStr2;
    EXPECT_TRUE(v.isNull());

    v = vFloat1 ^ vFloat2;
    EXPECT_TRUE(v.isNull());

    v = vDate1 ^ vDate2;
    EXPECT_TRUE(v.isNull());

    v = vEmpty ^ true;
    EXPECT_TRUE(v.empty());

    v = vEmpty ^ false;
    EXPECT_TRUE(v.empty());

    v = true ^ vEmpty;
    EXPECT_TRUE(v.empty());

    v = false ^ vEmpty;
    EXPECT_TRUE(v.empty());

    v = vNull ^ true;
    EXPECT_TRUE(v.isNull());

    v = vNull ^ false;
    EXPECT_TRUE(v.isNull());

    v = true ^ vNull;
    EXPECT_TRUE(v.isNull());

    v = false ^ vNull;
    EXPECT_TRUE(v.isNull());

    v = vEmpty ^ vNull;
    EXPECT_TRUE(v.isNull());

    v = vEmpty ^ vEmpty;
    EXPECT_TRUE(v.empty());

    v = vNull ^ vNull;
    EXPECT_TRUE(v.isNull());

    v = vNull ^ vEmpty;
    EXPECT_TRUE(v.isNull());
  }
  // Duration
  {
    Value v = Date() ^ Duration();
    EXPECT_TRUE(v.isNull());
  }
}

TEST(Value, typeName) {
  EXPECT_EQ("__EMPTY__", Value::kEmpty.typeName());
  EXPECT_EQ("bool", Value(false).typeName());
  EXPECT_EQ("float", Value(10.0).typeName());
  EXPECT_EQ("string", Value("").typeName());
  EXPECT_EQ("date", Value(Date()).typeName());
  EXPECT_EQ("time", Value(Time()).typeName());
  EXPECT_EQ("datetime", Value(DateTime()).typeName());
  EXPECT_EQ("vertex", Value(Vertex()).typeName());
  EXPECT_EQ("edge", Value(Edge()).typeName());
  EXPECT_EQ("path", Value(Path()).typeName());
  EXPECT_EQ("list", Value(List()).typeName());
  EXPECT_EQ("map", Value(Map()).typeName());
  EXPECT_EQ("set", Value(Set()).typeName());
  EXPECT_EQ("dataset", Value(DataSet()).typeName());
  EXPECT_EQ("geography", Value(Geography()).typeName());
  EXPECT_EQ("duration", Value(Duration()).typeName());
  EXPECT_EQ("__NULL__", Value::kNullValue.typeName());
  EXPECT_EQ("NaN", Value::kNullNaN.typeName());
  EXPECT_EQ("BAD_DATA", Value::kNullBadData.typeName());
  EXPECT_EQ("BAD_TYPE", Value::kNullBadType.typeName());
  EXPECT_EQ("ERR_OVERFLOW", Value::kNullOverflow.typeName());
  EXPECT_EQ("UNKNOWN_PROP", Value::kNullUnknownProp.typeName());
  EXPECT_EQ("DIV_BY_ZERO", Value::kNullDivByZero.typeName());
}

using serializer = apache::thrift::CompactSerializer;

TEST(Value, DecodeEncode) {
  std::vector<Value> values{
      // empty
      Value(),

      // null
      Value(NullType::__NULL__),
      Value(NullType::DIV_BY_ZERO),
      Value(NullType::BAD_DATA),
      Value(NullType::ERR_OVERFLOW),
      Value(NullType::OUT_OF_RANGE),
      Value(NullType::UNKNOWN_PROP),

      // int
      Value(0),
      Value(1),
      Value(2),

      // float
      Value(3.14),
      Value(2.67),

      // string
      Value("Hello "),
      Value("World"),

      // bool
      Value(false),
      Value(true),

      // date
      Value(Date(2020, 1, 1)),
      Value(Date(2019, 12, 1)),

      // time
      Value(Time{1, 2, 3, 4}),

      // datetime
      Value(DateTime{1, 2, 3, 4, 5, 6, 7}),

      // vertex
      Value(
          Vertex({"Vid",
                  {
                      Tag("tagName", {{"prop", Value(2)}}),
                      Tag("tagName1", {{"prop1", Value(2)}, {"prop2", Value(NullType::__NULL__)}}),
                  }})),

      // integerID vertex
      Value(
          Vertex({001,
                  {
                      Tag("tagName", {{"prop", Value(2)}}),
                      Tag("tagName1", {{"prop1", Value(2)}, {"prop2", Value(NullType::__NULL__)}}),
                  }})),

      // edge
      Value(Edge("Src", "Dst", 1, "Edge", 233, {{"prop1", Value(233)}, {"prop2", Value(2.3)}})),

      // integerID edge
      Value(Edge(001, 002, 1, "Edge", 233, {{"prop1", Value(233)}, {"prop2", Value(2.3)}})),

      // Path
      Value(Path(
          Vertex({"1", {Tag("tagName", {{"prop", Value(2)}})}}),
          {Step(Vertex({"1", {Tag("tagName", {{"prop", Value(2)}})}}), 1, "1", 1, {{"1", 1}})})),
      Value(Path()),

      // List
      Value(List({Value(2), Value(true), Value(2.33)})),

      // Set
      Value(Set({Value(2), Value(true), Value(2.33)})),

      // Map
      Value(Map({{"Key1", Value(2)}, {"Key2", Value(true)}, {"Key3", Value(2.33)}})),

      // DataSet
      Value(DataSet({"col1", "col2"})),

      // Geography
      Value(Geography::fromWKT("Point(3 8)").value()),
      Value(Geography::fromWKT("LineString(3 8, 4 6, 5 7)").value()),
      Value(Geography::fromWKT(
                "Polygon((1 2, 3 4, 5 6, 7 8, 1 2), (1.2 3.6, 4.7 5.2, 3.9 8.8, 1.2 3.6))")
                .value()),
      Value(Geography(Point(Coordinate(4, 9)))),
      Value(Geography(LineString(
          std::vector<Coordinate>{Coordinate(0, 1), Coordinate(2, 3), Coordinate(0, 1)}))),
      Value(Geography(Polygon(std::vector<std::vector<Coordinate>>{
          std::vector<Coordinate>{Coordinate(0, 1),
                                  Coordinate(2, 3),
                                  Coordinate(4, 5),
                                  Coordinate(6, 7),
                                  Coordinate(0, 1)},
          std::vector<Coordinate>{
              Coordinate(2, 4), Coordinate(5, 6), Coordinate(3, 8), Coordinate(2, 4)}}))),
      // Duration
      Value(Duration()),
      Value(Duration(1, 2, 3)),
      Value(Duration(-1, -2, -3)),
  };
  for (const auto& val : values) {
    std::string buf;
    buf.reserve(128);
    serializer::serialize(val, &buf);
    Value valCopy;
    std::size_t s = serializer::deserialize(buf, valCopy);
    ASSERT_EQ(s, buf.size());
    if (val.isNull()) {
      EXPECT_EQ(valCopy.isNull(), true);
      EXPECT_EQ(val.getNull(), valCopy.getNull());
      continue;
    }
    if (val.empty()) {
      EXPECT_EQ(valCopy.empty(), true);
      continue;
    }
    EXPECT_EQ(val, valCopy);
  }
}

TEST(Value, Ctor) {
  Value vZero(0);
  EXPECT_TRUE(vZero.isInt());
  Value vFloat(3.14);
  EXPECT_TRUE(vFloat.isFloat());
  Value vStr("Hello ");
  EXPECT_TRUE(vStr.isStr());
  Value vBool(false);
  EXPECT_TRUE(vBool.isBool());
  Value vDate(Date(2020, 1, 1));
  EXPECT_TRUE(vDate.isDate());
  Value vList(List({1, 3, 2}));
  EXPECT_TRUE(vList.isList());
  Value vSet(Set({8, 7}));
  EXPECT_TRUE(vSet.isSet());
  Value vMap(Map({{"a", 9}, {"b", 10}}));
  EXPECT_TRUE(vMap.isMap());
  // TODO(jie) Add more geography value test
  Value vGeogPoint{Geography(Point(Coordinate(3, 7)))};
  EXPECT_TRUE(vGeogPoint.isGeography());
  Value vGeogLine{
      Geography(LineString(std::vector<Coordinate>{Coordinate(0, 1), Coordinate(2, 7)}))};
  EXPECT_TRUE(vGeogLine.isGeography());
  Value vGeogPolygon{Geography(Polygon(std::vector<std::vector<Coordinate>>{
      std::vector<Coordinate>{
          Coordinate(0, 1), Coordinate(2, 3), Coordinate(4, 5), Coordinate(6, 7), Coordinate(0, 1)},
      std::vector<Coordinate>{
          Coordinate(2, 4), Coordinate(5, 6), Coordinate(3, 8), Coordinate(2, 4)}}))};
  EXPECT_TRUE(vGeogPolygon.isGeography());

  // Duration
  Value vD{Duration()};
  Value vD2{Duration(1, 2, 3)};

  // Disabled
  // Lead to compile error
  // Value v(nullptr);
  // std::map<std::string, std::string> tmp;
  // Value v2(&tmp);
}

TEST(Value, ToString) {
  {
    Duration d;
    d.addYears(1);
    d.addMonths(2);
    d.addDays(500);
    d.addSeconds(10);
    d.addMicroseconds(20);
    EXPECT_EQ(d.toString(), "P14MT43200010.000020000S");
  }
  {
    Duration d;
    d.addYears(1);
    d.addMonths(2);
    d.addDays(500);
    d.addSeconds(10);
    d.addMicroseconds(20000000);
    EXPECT_EQ(d.toString(), "P14MT43200030.000000000S");
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
