/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include <gtest/gtest.h>
#include "codec/RowReaderWrapper.h"
#include "codec/test/SchemaWriter.h"

namespace nebula {

using meta::cpp2::PropertyType;

TEST(RowReaderV2, headerInfo) {
    // Simplest row, nothing in it
    char data1[] = {0x08};
    SchemaWriter schema1;
    auto reader = RowReaderWrapper::getRowReader(
        &schema1, folly::StringPiece(data1, sizeof(data1)));
    ASSERT_TRUE(!!reader);
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(sizeof(data1), reader->headerLen());

    // With schema version
    char data2[] = {0x0A, 0x01, static_cast<char>(0xFF)};
    SchemaWriter schema2(0x00FF01);
    ASSERT_TRUE(reader->reset(&schema2, folly::StringPiece(data2, sizeof(data2))));
    EXPECT_EQ(0x0000FF01, reader->schemaVer());
    EXPECT_EQ(sizeof(data2), reader->headerLen());

    // Insert 33 fields into schema, so we will get 2 offsets
    SchemaWriter schema3(0x00FFFF01);
    for (int i = 0; i < 33; i++) {
        schema3.appendCol(folly::stringPrintf("Column%02d", i), PropertyType::INT64);
    }

    // With schema version and offsets
    char data3[] = {0x0B, 0x01, static_cast<char>(0xFF), static_cast<char>(0xFF)};
    ASSERT_TRUE(reader->reset(&schema3, folly::StringPiece(data3, sizeof(data3))));
    EXPECT_EQ(0x00FFFF01, reader->schemaVer());
    EXPECT_EQ(sizeof(data3), reader->headerLen());

    // No schema version, with offsets
    SchemaWriter schema4;
    for (int i = 0; i < 33; i++) {
        schema4.appendCol(folly::stringPrintf("Column%02d", i), PropertyType::INT64);
    }

    char data4[] = {0x08};
    ASSERT_TRUE(reader->reset(&schema4, folly::StringPiece(data4, sizeof(data4))));
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(sizeof(data4), reader->headerLen());

    // Empty row, return illegal schema version
    SchemaWriter schema5;
    auto reader2 = RowReaderWrapper::getRowReader(&schema5, folly::StringPiece(""));
    ASSERT_FALSE(!!reader2);
    ASSERT_FALSE(reader2->reset(&schema5, folly::StringPiece("")));
}


TEST(RowReaderV2, encodedData) {
    SchemaWriter schema;
    // Col 0: bool_col1 -- BOOL
    schema.appendCol("bool_col1", PropertyType::BOOL);
    // Col 1: str_col1 -- FIXED_STRING
    schema.appendCol("fixed_str_col", PropertyType::FIXED_STRING, 12);
    // Col 2: int_col1 -- INT32
    schema.appendCol("int32_col", PropertyType::INT32);
    // Col 3: int_col2 -- INT64
    schema.appendCol("int64_col", PropertyType::INT64);
    // Col 4: vid_col -- VID
    schema.appendCol("vid_col", PropertyType::VID);
    // Col 5: str_col2 -- STRING
    schema.appendCol("str_col", PropertyType::STRING);
    // Col 6: bool_col2 -- BOOL
    schema.appendCol("bool_col2", PropertyType::BOOL);
    // Col 7: float_col -- FLOAT
    schema.appendCol("float_col", PropertyType::FLOAT);
    // Col 8: double_col -- DOUBLE
    schema.appendCol("double_col", PropertyType::DOUBLE);
    // Col 9: timestamp_col -- TIMESTAMP
    schema.appendCol("timestamp_col", PropertyType::TIMESTAMP);
    // Col 10: date_col -- DATE
    schema.appendCol("date_col", PropertyType::DATE);
    // Col 11: datetime_col -- DATETIME
    schema.appendCol("datetime_col", PropertyType::DATETIME);
    // Col 12: time_col -- TIME
    schema.appendCol("time_col", PropertyType::TIME);

    std::string encoded;
    // Single byte header (Schema version is 0, no offset)
    encoded.append(1, 0x08);
    // There is no nullable fields, so no need to reserve the space for
    // the NULL flag

    const char* str1 = "Hello World!";
    const char* str2 = "Welcome to the future!";

    // Col 0
    encoded.append(1, 0x01);

    // Col 1
    encoded.append(str1, strlen(str1));

    // Col 2
    encoded.append(1, 100);
    encoded.append(3, 0);

    // Col 3
    encoded.append(8, static_cast<char>(0xFF));

    // Col 4
    encoded.append(1, 0x11).append(1, 0x22).append(1, 0x33).append(1, 0x44)
           .append(1, 0x55).append(1, 0x66).append(1, 0x77).append(1, static_cast<char>(0x88));

    // Col 5
    int32_t offset = 1 + schema.size();  // Header and data (no NULL flag)
    int32_t len = strlen(str2);
    encoded.append(reinterpret_cast<char*>(&offset), sizeof(int32_t));
    encoded.append(reinterpret_cast<char*>(&len), sizeof(int32_t));
    // String content will be append at the end

    // Col 6
    encoded.append(1, 0x00);

    // Col 7
    float pi = 3.1415926;
    encoded.append(reinterpret_cast<char*>(&pi), sizeof(float));

    // Col 8
    double e = 2.71828182845904523536028747135266249775724709369995;
    encoded.append(reinterpret_cast<char*>(&e), sizeof(double));

    // Col 9
    Timestamp ts = 1551331827;
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(Timestamp));

    // Col 10
    int16_t year = 2020;
    int8_t month = 2;
    int8_t day = 20;
    encoded.append(reinterpret_cast<char*>(&year), sizeof(int16_t));
    encoded.append(reinterpret_cast<char*>(&month), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&day), sizeof(int8_t));

    // Col 11
    int8_t hour = 10;
    int8_t minute = 30;
    int8_t sec = 45;
    int32_t microsec = 54321;
    encoded.append(reinterpret_cast<char*>(&year), sizeof(int16_t));
    encoded.append(reinterpret_cast<char*>(&month), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&day), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&hour), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&minute), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&sec), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&microsec), sizeof(int32_t));

    // Col 12
    encoded.append(reinterpret_cast<char*>(&hour), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&minute), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&sec), sizeof(int8_t));
    encoded.append(reinterpret_cast<char*>(&microsec), sizeof(int32_t));

    // Append the Col5's string content
    encoded.append(str2, strlen(str2));

    /**************************
     * Now let's read it
     *************************/
    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);

    // Header info
    RowReaderWrapper* r = dynamic_cast<RowReaderWrapper*>(reader.get());
    ASSERT_EQ(&(r->readerV2_), r->currReader_);
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(1, reader->headerLen());
    // There is no nullable fields, so no space reserved for the NULL flag
    EXPECT_EQ(0, r->readerV2_.numNullBytes_);

    Value val;

    // Col 0
    val = reader->getValueByIndex(0);
    EXPECT_EQ(Value::Type::BOOL, val.type());
    EXPECT_TRUE(val.getBool());
    val = reader->getValueByName("bool_col1");
    EXPECT_EQ(Value::Type::BOOL, val.type());
    EXPECT_TRUE(val.getBool());

    // Col 1
    val = reader->getValueByIndex(1);
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(str1, val.getStr());
    val = reader->getValueByName("fixed_str_col");
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(str1, val.getStr());

    // Col 2
    val = reader->getValueByIndex(2);
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(100, val.getInt());
    val = reader->getValueByName("int32_col");
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(100, val.getInt());

    // Col 3
    val = reader->getValueByIndex(3);
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFL, val.getInt());
    val = reader->getValueByName("int64_col");
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFL, val.getInt());

    // Col 4
    val = reader->getValueByIndex(4);
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(sizeof(int64_t), val.getStr().size());
    int64_t vid = 0;
    memcpy(reinterpret_cast<void*>(&vid), val.getStr().data(), sizeof(int64_t));
    EXPECT_EQ(0x8877665544332211L, vid);
    val = reader->getValueByName("vid_col");
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(sizeof(int64_t), val.getStr().size());
    vid = 0;
    memcpy(reinterpret_cast<void*>(&vid), val.getStr().data(), sizeof(int64_t));
    EXPECT_EQ(0x8877665544332211L, vid);

    // Col 5
    val = reader->getValueByIndex(5);
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(str2, val.getStr());
    val = reader->getValueByName("str_col");
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(str2, val.getStr());

    // Col 6
    val = reader->getValueByIndex(6);
    EXPECT_EQ(Value::Type::BOOL, val.type());
    EXPECT_FALSE(val.getBool());
    val = reader->getValueByName("bool_col2");
    EXPECT_EQ(Value::Type::BOOL, val.type());
    EXPECT_FALSE(val.getBool());

    // Col 7
    val = reader->getValueByIndex(7);
    EXPECT_EQ(Value::Type::FLOAT, val.type());
    EXPECT_DOUBLE_EQ(pi, val.getFloat());
    val = reader->getValueByName("float_col");
    EXPECT_EQ(Value::Type::FLOAT, val.type());
    EXPECT_DOUBLE_EQ(pi, val.getFloat());

    // Col 8
    val = reader->getValueByIndex(8);
    EXPECT_EQ(Value::Type::FLOAT, val.type());
    EXPECT_DOUBLE_EQ(e, val.getFloat());
    val = reader->getValueByName("double_col");
    EXPECT_EQ(Value::Type::FLOAT, val.type());
    EXPECT_DOUBLE_EQ(e, val.getFloat());

    // Col 9
    val = reader->getValueByIndex(9);
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(1551331827, val.getInt());
    val = reader->getValueByName("timestamp_col");
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(1551331827, val.getInt());

    // Col 10
    Date date;
    date.year = 2020;
    date.month = 2;
    date.day = 20;
    val = reader->getValueByIndex(10);
    EXPECT_EQ(Value::Type::DATE, val.type());
    EXPECT_EQ(date, val.getDate());
    val = reader->getValueByName("date_col");
    EXPECT_EQ(Value::Type::DATE, val.type());
    EXPECT_EQ(date, val.getDate());

    // Col 11
    DateTime dt;
    dt.year = 2020;
    dt.month = 2;
    dt.day = 20;
    dt.hour = 10;
    dt.minute = 30;
    dt.sec = 45;
    dt.microsec = 54321;
    val = reader->getValueByIndex(11);
    EXPECT_EQ(Value::Type::DATETIME, val.type());
    EXPECT_EQ(dt, val.getDateTime());
    val = reader->getValueByName("datetime_col");
    EXPECT_EQ(Value::Type::DATETIME, val.type());
    EXPECT_EQ(dt, val.getDateTime());

    // Col 12
    Time t;
    t.hour = 10;
    t.minute = 30;
    t.sec = 45;
    t.microsec = 54321;
    val = reader->getValueByIndex(12);
    EXPECT_EQ(Value::Type::TIME, val.type());
    EXPECT_EQ(t, val.getTime());
    val = reader->getValueByName("time_col");
    EXPECT_EQ(Value::Type::TIME, val.type());
    EXPECT_EQ(t, val.getTime());

    // Col 13 -- non-existing column
    val = reader->getValueByIndex(13);
    EXPECT_EQ(Value::Type::NULLVALUE, val.type());
}


TEST(RowReaderV2, iterator) {
    std::string encoded;
    // Header
    encoded.append(1, 0x08);
    // There is no nullable field, so no need to reserve space for
    // the Null flags

    SchemaWriter schema;
    for (int i = 0; i < 64; i++) {
        schema.appendCol(folly::stringPrintf("Col%02d", i),
                         meta::cpp2::PropertyType::INT64);
        encoded.append(1, i + 1);
        encoded.append(7, 0);
    }

    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);
    auto it = reader->begin();
    int32_t index = 0;
    while (it != reader->end()) {
        Value v = reader->getValueByIndex(index);
        EXPECT_EQ(Value::Type::INT, v.type()) << index;
        EXPECT_EQ(v, it->value()) << index;
        ++it;
        ++index;
    }

    EXPECT_EQ(64, index);
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


