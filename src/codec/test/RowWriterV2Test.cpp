/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "codec/RowWriterV2.h"
#include "codec/RowReaderV2.h"
#include "codec/test/SchemaWriter.h"

namespace nebula {

using meta::cpp2::PropertyType;

const double e = 2.71828182845904523536028747135266249775724709369995;
const float pi = 3.14159265358979;
const std::string str = "Hello world!";    // NOLINT
const std::string fixed = "Nebula Graph";  // NOLINT
const Timestamp now = 1582183355;
// Convert timestamp now to datetime string
const std::string nowStr = "2020-02-20 15:22:35";  // NOLINT
const Date date = {2020, 2, 20};
const DateTime dt = {2020, 2, 20, 10, 30, 45, -8 * 3600, 0};
const Value sVal("Hello world!");
const Value iVal(64);

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
    schema.appendCol("Col12", PropertyType::DATETIME);
    schema.appendCol("Col13", PropertyType::INT64, 0, true);
    schema.appendCol("Col14", PropertyType::INT32, 0, true);

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
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.set(11, dt));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.setNull(12));
    // Purposely skip the col14
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
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.set("Col12", dt));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.setNull("Col13"));
    // Purposely skip the col14
    ASSERT_EQ(WriteResult::SUCCEEDED, writer2.finish());

    std::string encoded1 = std::move(writer1).moveEncodedStr();
    std::string encoded2 = writer2.getEncodedStr();

    auto reader1 = RowReader::getRowReader(&schema, encoded1);
    auto reader2 = RowReader::getRowReader(&schema, encoded2);

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
    EXPECT_EQ(Value::Type::DATETIME, v1.type());
    EXPECT_EQ(dt, v1.getDateTime());
    EXPECT_EQ(v1, v2);

    // Col13
    v1 = reader1->getValueByName("Col13");
    v2 = reader2->getValueByIndex(12);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);

    // Col14
    v1 = reader1->getValueByName("Col14");
    v2 = reader2->getValueByIndex(13);
    EXPECT_EQ(Value::Type::NULLVALUE, v1.type());
    EXPECT_EQ(v1, v2);
}


TEST(RowWriterV2, WithDefaultValue) {
    SchemaWriter schema(7 /*Schema version*/);
    schema.appendCol("Col01", PropertyType::BOOL, 0, true);
    schema.appendCol("Col02", PropertyType::INT64, 0, false, 12345);
    schema.appendCol("Col03", PropertyType::STRING, 0, true, str);
    schema.appendCol("Col04", PropertyType::FIXED_STRING, 12, false, fixed);

    RowWriterV2 writer(&schema);
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded = std::move(writer).moveEncodedStr();
    auto reader = RowReader::getRowReader(&schema, encoded);

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
    auto reader = RowReader::getRowReader(&schema, encoded);

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

    auto oldReader = RowReader::getRowReader(&schema, encoded1);

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
    EXPECT_EQ(encoded2, encoded3);

    auto reader1 = RowReader::getRowReader(&schema, encoded2);
    auto reader2 = RowReader::getRowReader(&schema, encoded3);

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
    schema.appendCol("Col02", PropertyType::TIMESTAMP);

    RowWriterV2 writer(&schema);
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set("Col01", 1582183355));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer.set(1, nowStr));
    ASSERT_EQ(WriteResult::SUCCEEDED, writer.finish());

    std::string encoded1 = writer.moveEncodedStr();
    auto reader1 = RowReader::getRowReader(&schema, encoded1);

    // Col01
    Value v1 = reader1->getValueByName("Col01");
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(now, v1.getInt());

    // Col02
    v1 = reader1->getValueByName("Col02");
    EXPECT_EQ(Value::Type::INT, v1.type());
    EXPECT_EQ(now, v1.getInt());

    // Invalid value test
    RowWriterV2 writer2(&schema);
    EXPECT_NE(WriteResult::SUCCEEDED, writer2.set("Col01", 9223372037));

    EXPECT_NE(WriteResult::SUCCEEDED, writer2.set("Col01", -1));

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
    auto reader = RowReader::getRowReader(&schema, encoded);

    // Col01
    Value v1 = reader->getValueByName("Col01");
    Value v2 = reader->getValueByIndex(0);
    EXPECT_EQ("", v1.getStr());
    EXPECT_EQ("", v2.getStr());
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

