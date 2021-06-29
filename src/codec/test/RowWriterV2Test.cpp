/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/expression/ConstantExpression.h"
#include "common/time/WallClock.h"
#include <gtest/gtest.h>
#include "codec/RowWriterV2.h"
#include "codec/RowReaderWrapper.h"
#include "codec/test/SchemaWriter.h"

namespace nebula {

using meta::cpp2::PropertyType;

const double e = 2.71828182845904523536028747135266249775724709369995;
const float pi = 3.14159265358979;
const std::string str = "Hello world!";    // NOLINT
const std::string fixed = "Nebula Graph";  // NOLINT
const Timestamp now = 1582183355;
// Convert timestamp now to datetime string
const Date date = {2020, 2, 20};
const DateTime dt = {2020, 2, 20, 10, 30, 45, 0};
const Value sVal("Hello world!");
const Value iVal(64);
const Time t = {10, 30, 45, 0};

TEST(RowWriterV2, NoDefaultValue) {
    SchemaWriter schema(12 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::BOOL);
    schema.appendCol("Col02", PropertyType::INT8);
    schema.appendCol("Col03", PropertyType::INT16);
    schema.appendCol("Col04", PropertyType::INT32);
    schema.appendCol("Col05", PropertyType::INT64);
    schema.appendCol("Col06", PropertyType::FLOAT);
    schema.appendCol("Col07", PropertyType::DOUBLE);
    schema.appendCol("Col08", PropertyType::STRING);
    schema.appendCol("Col09", PropertyType::FIXED_STRING, 12);
    schema.appendCol("Col10", PropertyType::TIMESTAMP);
    schema.appendCol("Col11", PropertyType::DATE);
    schema.appendCol("Col12", PropertyType::TIME);
    schema.appendCol("Col13", PropertyType::DATETIME);
    schema.appendCol("Col14", PropertyType::INT64, 0, true);
    schema.appendCol("Col15", PropertyType::INT32, 0, true);

    ASSERT_EQ(Value::Type::STRING, sVal.type());
    ASSERT_EQ(Value::Type::INT, iVal.type());

    RowWriterV2 writer1(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(0, true));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(1, 8));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(2, 16));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(3, 32));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.setValue(4, iVal));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(5, pi));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(6, e));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.setValue(7, sVal));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(8, fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(9, now));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(10, date));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(11, t));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(12, dt));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.setNull(13));
    // Purposely skip the col15
    ASSERT_EQ(WriteResult::SUCCEEDED, writer1.finish());

    RowWriterV2 writer2(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col01", true));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col02", 8));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col03", 16));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col04", 32));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.setValue("Col05", iVal));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col06", pi));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col07", e));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.setValue("Col08", sVal));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col09", fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col10", now));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col11", date));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col12", t));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col13", dt));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.setNull("Col14"));
    // Purposely skip the col15
    ASSERT_EQ(WriteResult::SUCCEEDED, writer2.finish());

    std::string encoded1 = std::move(writer1).moveEncodedStr();
    std::string encoded2 = writer2.getEncodedStr();

    auto reader1 = RowReaderWrapper::getRowReader(&schema, encoded1);
    auto reader2 = RowReaderWrapper::getRowReader(&schema, encoded2);

    // Col01
    Value v1 = reader1->getValueByName("Col01");
    Value v2 = reader2->getValueByIndex(0);
    EXPECT_EQ(Value::Type::BOOL, v1.type());
    EXPECT_TRUE(v1.getBool());
    EXPECT_EQ(v1, v2);

    // Col02
    v1 = reader1->getValueByName("Col02");
    v2 = reader2->getValueByIndex(1);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(8, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col03
    v1 = reader1->getValueByName("Col03");
    v2 = reader2->getValueByIndex(2);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(16, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col04
    v1 = reader1->getValueByName("Col04");
    v2 = reader2->getValueByIndex(3);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(32, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col05
    v1 = reader1->getValueByName("Col05");
    v2 = reader2->getValueByIndex(4);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(iVal.getInt(), v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col06
    v1 = reader1->getValueByName("Col06");
    v2 = reader2->getValueByIndex(5);
    EXPECT_EQ(Value::Type::FLOAT, v1.type());
    EXPECT_DOUBLE_EQ(pi, v1.getFloat());
    EXPECT_EQ(v1, v2);

    // Col07
    v1 = reader1->getValueByName("Col07");
    v2 = reader2->getValueByIndex(6);
    EXPECT_EQ(Value::Type::FLOAT, v1.type());
    EXPECT_DOUBLE_EQ(e, v1.getFloat());
    EXPECT_EQ(v1, v2);

    // Col08
    v1 = reader1->getValueByName("Col08");
    v2 = reader2->getValueByIndex(7);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(sVal.getStr(), v1.getStr());
    EXPECT_EQ(v1, v2);

    // Col09
    v1 = reader1->getValueByName("Col09");
    v2 = reader2->getValueByIndex(8);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(fixed, v1.getStr());
    EXPECT_EQ(v1, v2);

    // Col10
    v1 = reader1->getValueByName("Col10");
    v2 = reader2->getValueByIndex(9);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(now, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col11
    v1 = reader1->getValueByName("Col11");
    v2 = reader2->getValueByIndex(10);
    EXPECT_EQ(Value::Type::DATE, v1.type());
    EXPECT_EQ(date, v1.getDate());
    EXPECT_EQ(v1, v2);

    // Col12
    v1 = reader1->getValueByName("Col12");
    v2 = reader2->getValueByIndex(11);
    EXPECT_EQ(Value::Type::TIME, v1.type());
    EXPECT_EQ(t, v1.getTime());
    EXPECT_EQ(v1, v2);

    // Col1333
    v1 = reader1->getValueByName("Col13");
    v2 = reader2->getValueByIndex(12);
    EXPECT_EQ(Value::Type::DATETIME, v1.type());
    EXPECT_EQ(dt, v1.getDateTime());
    EXPECT_EQ(v1, v2);

    // Col14
    v1 = reader1->getValueByName("Col14");
    v2 = reader2->getValueByIndex(13);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);

    // Col15
    v1 = reader1->getValueByName("Col15");
    v2 = reader2->getValueByIndex(14);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);
}

TEST(RowWriterV2, WithDefaultValue) {
    ObjectPool objPool;
    auto pool = &objPool;

    SchemaWriter schema(7 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::BOOL, 0, true);
    schema.appendCol("Col02", PropertyType::INT64, 0, false, ConstantExpression::make(pool, 12345));
    schema.appendCol("Col03", PropertyType::STRING, 0, true, ConstantExpression::make(pool, str));
    schema.appendCol(
        "Col04", PropertyType::FIXED_STRING, 12, false, ConstantExpression::make(pool, fixed));

    RowWriterV2 writer(&schema);
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded = std::move(writer).moveEncodedStr();
    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);

    // Col01
    Value v1 = reader->getValueByName("Col01");
    Value v2 = reader->getValueByIndex(0);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);

    // Col02
    v1 = reader->getValueByName("Col02");
    v2 = reader->getValueByIndex(1);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(12345, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col03
    v1 = reader->getValueByName("Col03");
    v2 = reader->getValueByIndex(2);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(str, v1.getStr());
    EXPECT_EQ(v1, v2);

    // Col04
    v1 = reader->getValueByName("Col04");
    v2 = reader->getValueByIndex(3);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(fixed, v1.getStr());
    EXPECT_EQ(v1, v2);
}

TEST(RowWriterV2, DoubleSet) {
    SchemaWriter schema(3 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::BOOL, 0, true);
    schema.appendCol("Col02", PropertyType::INT64);
    schema.appendCol("Col03", PropertyType::STRING);
    schema.appendCol("Col04", PropertyType::STRING, 0, true);
    schema.appendCol("Col05", PropertyType::FIXED_STRING, 12);

    RowWriterV2 writer(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", false));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", true));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.setNull("Col01"));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col02", 1234567));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col02", 7654321));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col03", str));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col03", fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col04", str));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col04", fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.setNull("Col04"));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col05", fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col05", str));
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded = std::move(writer).moveEncodedStr();
    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);

    // Col01
    Value v1 = reader->getValueByName("Col01");
    Value v2 = reader->getValueByIndex(0);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);

    // Col02
    v1 = reader->getValueByName("Col02");
    v2 = reader->getValueByIndex(1);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(7654321, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col03
    v1 = reader->getValueByName("Col03");
    v2 = reader->getValueByIndex(2);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(fixed, v1.getStr());
    EXPECT_EQ(v1, v2);

    // Col04
    v1 = reader->getValueByName("Col04");
    v2 = reader->getValueByIndex(3);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);

    // Col05
    v1 = reader->getValueByName("Col05");
    v2 = reader->getValueByIndex(4);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(str, v1.getStr());
    EXPECT_EQ(v1, v2);
}


TEST(RowWriterV2, Update) {
    SchemaWriter schema(2 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::BOOL, 0, true);
    schema.appendCol("Col02", PropertyType::INT64);
    schema.appendCol("Col03", PropertyType::STRING);
    schema.appendCol("Col04", PropertyType::STRING, 0, true);
    schema.appendCol("Col05", PropertyType::FIXED_STRING, 12);

    RowWriterV2 writer(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", true));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col02", 1234567));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col03", str));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.setNull("Col04"));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col05", fixed));
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded1 = writer.moveEncodedStr();

    auto oldReader = RowReaderWrapper::getRowReader(&schema, encoded1);

    RowWriterV2 updater2(*oldReader);
    RowWriterV2 updater1(&schema, std::move(encoded1));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater1.set("Col02", 7654321));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater2.set("Col02", 7654321));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater1.set("Col03", fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater2.set("Col03", fixed));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater1.set("Col04", str));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater2.set("Col04", str));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater1.set("Col05", str));
    EXPECT_EQ(WriteResult::SUCCEEDED, updater2.set("Col05", str));
    ASSERT_EQ(WriteResult::SUCCEEDED, updater1.finish());
    ASSERT_EQ(WriteResult::SUCCEEDED, updater2.finish());

    std::string encoded2 = updater1.moveEncodedStr();
    std::string encoded3 = updater2.moveEncodedStr();
    EXPECT_EQ(encoded2.substr(0, encoded2.size() - sizeof(int64_t)),
              encoded3.substr(0, encoded3.size() - sizeof(int64_t)));

    auto reader1 = RowReaderWrapper::getRowReader(&schema, encoded2);
    auto reader2 = RowReaderWrapper::getRowReader(&schema, encoded3);

    // Col01
    Value v1 = reader1->getValueByName("Col01");
    Value v2 = reader2->getValueByIndex(0);
    EXPECT_EQ(Value::Type::BOOL, v1.type());
    EXPECT_TRUE(v1.getBool());
    EXPECT_EQ(v1, v2);

    // Col02
    v1 = reader1->getValueByName("Col02");
    v2 = reader2->getValueByIndex(1);
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(7654321, v1.getInt());
    EXPECT_EQ(v1, v2);

    // Col03
    v1 = reader1->getValueByName("Col03");
    v2 = reader2->getValueByIndex(2);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(fixed, v1.getStr());
    EXPECT_EQ(v1, v2);

    // Col04
    v1 = reader1->getValueByName("Col04");
    v2 = reader2->getValueByIndex(3);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(str, v1.getStr());
    EXPECT_EQ(v1, v2);

    // Col05
    v1 = reader1->getValueByName("Col05");
    v2 = reader2->getValueByIndex(4);
    EXPECT_EQ(Value::Type::STRING, v1.type());
    EXPECT_EQ(str, v1.getStr());
    EXPECT_EQ(v1, v2);
}

TEST(RowWriterV2, Timestamp) {
    SchemaWriter schema(20 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::TIMESTAMP);

    RowWriterV2 writer(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", 1582183355));
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded1 = writer.moveEncodedStr();
    auto reader1 = RowReaderWrapper::getRowReader(&schema, encoded1);

    // Col01
    Value v1 = reader1->getValueByName("Col01");
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(now, v1.getInt());

    // Invalid value test
    RowWriterV2 writer2(&schema);
    EXPECT_NE(WriteResult::SUCCEEDED, writer2.set("Col01", 9223372037));

    EXPECT_NE(WriteResult::SUCCEEDED, writer2.set("Col01", -1));

    EXPECT_NE(WriteResult::SUCCEEDED, writer2.set("Col01", "2020-02-20 15:22:35"));

    std::string dateStr = "3220-02-20 15:22:35";  // NOLINT
    EXPECT_NE(WriteResult::SUCCEEDED, writer2.set(1, dateStr));
}

TEST(RowWriterV2, EmptyString) {
    SchemaWriter schema(0 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::STRING);

    RowWriterV2 writer(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", ""));
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded = std::move(writer).moveEncodedStr();
    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);

    // Col01
    Value v1 = reader->getValueByName("Col01");
    Value v2 = reader->getValueByIndex(0);
    EXPECT_EQ("", v1.getStr());
    EXPECT_EQ("", v2.getStr());
}

TEST(RowWriterV2, NumericLimit) {
    SchemaWriter schema(1 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::INT8);
    schema.appendCol("Col03", PropertyType::INT16);
    schema.appendCol("Col06", PropertyType::INT32);
    schema.appendCol("Col07", PropertyType::INT64);
    schema.appendCol("Col09", PropertyType::FLOAT);
    schema.appendCol("Col11", PropertyType::DOUBLE);

    {
        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int8_t>(std::numeric_limits<int8_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int16_t>(std::numeric_limits<int8_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int32_t>(std::numeric_limits<int8_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int64_t>(std::numeric_limits<int8_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<float>(std::numeric_limits<int8_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<double>(std::numeric_limits<int8_t>::min())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int8_t>(std::numeric_limits<int8_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int16_t>(std::numeric_limits<int8_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int32_t>(std::numeric_limits<int8_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<int64_t>(std::numeric_limits<int8_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<float>(std::numeric_limits<int8_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(0, static_cast<double>(std::numeric_limits<int8_t>::max())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<int16_t>(std::numeric_limits<int16_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<int32_t>(std::numeric_limits<int16_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<int64_t>(std::numeric_limits<int16_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<float>(std::numeric_limits<int16_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<double>(std::numeric_limits<int16_t>::min())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<int16_t>(std::numeric_limits<int16_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<int32_t>(std::numeric_limits<int16_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<int64_t>(std::numeric_limits<int16_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<float>(std::numeric_limits<int16_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(1, static_cast<double>(std::numeric_limits<int16_t>::max())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<int32_t>(std::numeric_limits<int32_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<int64_t>(std::numeric_limits<int32_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<float>(std::numeric_limits<int32_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<double>(std::numeric_limits<int32_t>::min())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<int32_t>(std::numeric_limits<int32_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<int64_t>(std::numeric_limits<int32_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<float>(std::numeric_limits<int32_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(2, static_cast<double>(std::numeric_limits<int32_t>::max())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(3, static_cast<int64_t>(std::numeric_limits<int64_t>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(3, static_cast<double>(std::numeric_limits<int64_t>::min())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(3, static_cast<int64_t>(std::numeric_limits<int64_t>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(3, static_cast<double>(std::numeric_limits<int64_t>::max())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<int32_t>(std::numeric_limits<float>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<int64_t>(std::numeric_limits<float>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<float>(std::numeric_limits<float>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<double>(std::numeric_limits<float>::min())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<int32_t>(std::numeric_limits<float>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<int64_t>(std::numeric_limits<float>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<float>(std::numeric_limits<float>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(4, static_cast<double>(std::numeric_limits<float>::max())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(5, static_cast<int64_t>(std::numeric_limits<double>::min())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(5, static_cast<double>(std::numeric_limits<double>::min())));

        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(5, static_cast<int64_t>(std::numeric_limits<double>::max())));
        EXPECT_EQ(WriteResult::SUCCEEDED,
                  writer.set(5, static_cast<double>(std::numeric_limits<double>::max())));
    }
    {
        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(0, static_cast<int8_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(0, static_cast<int16_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(0, static_cast<int32_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(0, static_cast<int64_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(0, static_cast<float>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(0, static_cast<double>(0)));

        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, static_cast<int8_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, static_cast<int16_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, static_cast<int32_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, static_cast<int64_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, static_cast<float>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, static_cast<double>(0)));

        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(2, static_cast<int8_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(2, static_cast<int16_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(2, static_cast<int32_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(2, static_cast<int64_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(2, static_cast<float>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(2, static_cast<double>(0)));

        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(3, static_cast<int8_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(3, static_cast<int16_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(3, static_cast<int32_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(3, static_cast<int64_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(3, static_cast<float>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(3, static_cast<double>(0)));

        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(4, static_cast<int8_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(4, static_cast<int16_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(4, static_cast<int32_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(4, static_cast<int64_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(4, static_cast<float>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(4, static_cast<double>(0)));

        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(5, static_cast<int8_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(5, static_cast<int16_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(5, static_cast<int32_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(5, static_cast<int64_t>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(5, static_cast<float>(0)));
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(5, static_cast<double>(0)));
    }
    {
        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, static_cast<int16_t>(-0x81)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, static_cast<int16_t>(0x80)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, static_cast<int32_t>(-0x81)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, static_cast<int32_t>(0x80)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, static_cast<int64_t>(-0x81)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, static_cast<int64_t>(0x80)));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, std::numeric_limits<float>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, std::numeric_limits<float>::max()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, std::numeric_limits<double>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(0, std::numeric_limits<double>::max()));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, static_cast<int32_t>(-0x8001)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, static_cast<int32_t>(0x8000)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, static_cast<int64_t>(-0x8001)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, static_cast<int64_t>(0x8000)));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, std::numeric_limits<float>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, std::numeric_limits<float>::max()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, std::numeric_limits<double>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(1, std::numeric_limits<double>::max()));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(2, static_cast<int64_t>(-0x80000001L)));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(2, static_cast<int64_t>(0x80000000L)));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(2, std::numeric_limits<float>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(2, std::numeric_limits<float>::max()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(2, std::numeric_limits<double>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(2, std::numeric_limits<double>::max()));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(3, std::numeric_limits<float>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(3, std::numeric_limits<float>::max()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(3, std::numeric_limits<double>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(3, std::numeric_limits<double>::max()));

        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(4, std::numeric_limits<double>::lowest()));
        EXPECT_EQ(WriteResult::OUT_OF_RANGE, writer.set(4, std::numeric_limits<double>::max()));
    }
}

TEST(RowWriterV2, TimestampTest) {
    {
        SchemaWriter schema(1);
        schema.appendCol("Col01", PropertyType::STRING);

        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", ""));
        auto ts = time::WallClock::fastNowInMicroSec();
        ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

        std::string encoded = std::move(writer).moveEncodedStr();
        auto reader = RowReaderWrapper::getRowReader(&schema, encoded);
        Value v1 = reader->getValueByName("Col01");
        Value v2 = reader->getValueByIndex(0);
        EXPECT_EQ("", v1.getStr());
        EXPECT_EQ("", v2.getStr());
        auto ret = (reader->getTimestamp() >= ts) &&
                   (reader->getTimestamp() <= time::WallClock::fastNowInMicroSec());
        EXPECT_TRUE(ret);
    }
    {
        SchemaWriter schema(1);
        schema.appendCol("Col01", PropertyType::STRING);

        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", ""));
        // rewrite, for processOutOfSpace test
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", "new"));
        auto ts = time::WallClock::fastNowInMicroSec();
        ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

        std::string encoded = std::move(writer).moveEncodedStr();
        auto reader = RowReaderWrapper::getRowReader(&schema, encoded);
        Value v1 = reader->getValueByName("Col01");
        Value v2 = reader->getValueByIndex(0);
        EXPECT_EQ("new", v1.getStr());
        EXPECT_EQ("new", v2.getStr());
        auto ret = (reader->getTimestamp() >= ts) &&
                   (reader->getTimestamp() <= time::WallClock::fastNowInMicroSec());
        EXPECT_TRUE(ret);
    }
    {
        SchemaWriter schema(1);
        schema.appendCol("Col01", PropertyType::STRING);

        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", ""));
        // rewrite, for processOutOfSpace test
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", "abc"));
        ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());
        std::string encoded = std::move(writer).moveEncodedStr();

        RowWriterV2 writer2(&schema, encoded);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col01", "abc"));
        ASSERT_EQ(WriteResult::SUCCEEDED, writer2.finish());
        std::string encoded2 = std::move(writer2).moveEncodedStr();
        EXPECT_EQ(encoded.substr(0, encoded.size() - sizeof(int64_t)),
                  encoded2.substr(0, encoded2.size() - sizeof(int64_t)));
    }
    {
        SchemaWriter schema(1);
        schema.appendCol("Col01", PropertyType::INT64);

        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", 1));
        // rewrite, for processOutOfSpace test
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", 2));
        ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());
        std::string encoded = std::move(writer).moveEncodedStr();

        RowWriterV2 writer2(&schema, encoded);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col01", 2));
        ASSERT_EQ(WriteResult::SUCCEEDED, writer2.finish());
        std::string encoded2 = std::move(writer2).moveEncodedStr();
        EXPECT_EQ(encoded.substr(0, encoded.size() - sizeof(int64_t)),
                  encoded2.substr(0, encoded2.size() - sizeof(int64_t)));
    }
    {
        // test checkUnsetFields
        SchemaWriter schema(1);
        schema.appendCol("Col01", PropertyType::INT64);
        schema.appendCol("Col02", PropertyType::INT64);

        RowWriterV2 writer(&schema);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", 1));
        ASSERT_EQ(WriteResult::FIELD_UNSET, writer.finish());
        EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col02", 2));
        auto ts = time::WallClock::fastNowInMicroSec();
        ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());
        std::string encoded = std::move(writer).moveEncodedStr();
        auto reader = RowReaderWrapper::getRowReader(&schema, encoded);
        Value v1 = reader->getValueByName("Col01");
        Value v2 = reader->getValueByIndex(0);
        EXPECT_EQ(1, v1.getInt());
        EXPECT_EQ(1, v2.getInt());
        v1 = reader->getValueByName("Col02");
        v2 = reader->getValueByIndex(1);
        EXPECT_EQ(2, v1.getInt());
        EXPECT_EQ(2, v2.getInt());
        auto ret = (reader->getTimestamp() >= ts) &&
                   (reader->getTimestamp() <= time::WallClock::fastNowInMicroSec());
        EXPECT_TRUE(ret);
        RowWriterV2 writer2(&schema, encoded);
        EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col02", 2));
        ASSERT_EQ(WriteResult::SUCCEEDED, writer2.finish());
        std::string encoded2 = std::move(writer2).moveEncodedStr();
        EXPECT_EQ(encoded.substr(0, encoded.size() - sizeof(int64_t)),
                  encoded2.substr(0, encoded2.size() - sizeof(int64_t)));
    }
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

