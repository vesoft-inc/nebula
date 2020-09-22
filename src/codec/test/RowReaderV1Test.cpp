/* Copyright (c) 2018 vesoft inc. All rights reserved.
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

TEST(RowReaderV1, headerInfo) {
    // Simplest row, nothing in it
    char data1[] = {0x00};
    SchemaWriter schema1;
    auto reader = RowReaderWrapper::getRowReader(&schema1,
                                                 folly::StringPiece(data1, sizeof(data1)));
    ASSERT_TRUE(!!reader);
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(sizeof(data1), reader->headerLen());

    // With schema version
    char data2[] = {0x40, 0x01, static_cast<char>(0xFF)};
    SchemaWriter schema2(0x00FF01);
    ASSERT_TRUE(reader->reset(&schema2, folly::StringPiece(data2, sizeof(data2))));
    EXPECT_EQ(0x0000FF01, reader->schemaVer());
    EXPECT_EQ(sizeof(data2), reader->headerLen());

    // Insert 33 fields into schema, so we will get 2 offsets
    SchemaWriter schema3(0x00FFFF01);
    for (int i = 0; i < 33; i++) {
        schema3.appendCol(folly::stringPrintf("Column%02d", i),
                          meta::cpp2::PropertyType::INT64);
    }

    // With schema version and offsets
    char data3[] = {0x60, 0x01, static_cast<char>(0xFF),
                    static_cast<char>(0xFF), 0x40,
                    static_cast<char>(0xF0)};
    ASSERT_TRUE(reader->reset(&schema3, folly::StringPiece(data3, sizeof(data3))));
    EXPECT_EQ(0x00FFFF01, reader->schemaVer());
    EXPECT_EQ(sizeof(data3), reader->headerLen());

    RowReaderWrapper* r = dynamic_cast<RowReaderWrapper*>(reader.get());
    ASSERT_EQ(&(r->readerV1_), r->currReader_);
    ASSERT_EQ(3, r->readerV1_.blockOffsets_.size());
    EXPECT_EQ(0x0000, r->readerV1_.blockOffsets_[0].first);
    EXPECT_EQ(0x0040, r->readerV1_.blockOffsets_[1].first);
    EXPECT_EQ(0x00F0, r->readerV1_.blockOffsets_[2].first);

    // No schema version, with offsets
    SchemaWriter schema4;
    for (int i = 0; i < 33; i++) {
        schema4.appendCol(folly::stringPrintf("Column%02d", i),
                          meta::cpp2::PropertyType::INT64);
    }

    char data4[] = {0x01, static_cast<char>(0xFF), 0x40, 0x08, static_cast<char>(0xF0)};
    ASSERT_TRUE(reader->reset(&schema4, folly::StringPiece(data4, sizeof(data4))));
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(sizeof(data4), reader->headerLen());

    ASSERT_EQ(&(r->readerV1_), r->currReader_);
    ASSERT_EQ(3, r->readerV1_.blockOffsets_.size());
    EXPECT_EQ(0x000000, r->readerV1_.blockOffsets_[0].first);
    EXPECT_EQ(0x0040FF, r->readerV1_.blockOffsets_[1].first);
    EXPECT_EQ(0x00F008, r->readerV1_.blockOffsets_[2].first);

    // Empty row, return illegal schema version
    SchemaWriter schema5;
    auto reader2 = RowReaderWrapper::getRowReader(&schema5, folly::StringPiece(""));
    ASSERT_FALSE(!!reader2);
    ASSERT_FALSE(reader2->reset(&schema5, folly::StringPiece("")));
}


TEST(RowReaderV1, encodedData) {
    const char* colName1 = "int_col1";
    const std::string colName2("int_col2");
    std::string colName3("vid_col");

    SchemaWriter schema;
    // Col 0: bool_col1 -- BOOL
    schema.appendCol("bool_col1", meta::cpp2::PropertyType::BOOL);
    // Col 1: str_col1 -- STRING
    schema.appendCol(folly::stringPrintf("str_col1"),
                      meta::cpp2::PropertyType::STRING);
    // Col 2: int_col1 -- INT
    schema.appendCol(colName1, meta::cpp2::PropertyType::INT64);
    // Col 3: int_col2 -- INT
    schema.appendCol(colName2, meta::cpp2::PropertyType::INT64);
    // Col 4: vid_col -- VID
    schema.appendCol(folly::StringPiece(colName3),
                      meta::cpp2::PropertyType::VID);
    // Col 5: str_col2 -- STRING
    schema.appendCol("str_col2", meta::cpp2::PropertyType::STRING);
    // Col 6: bool_col2 -- BOOL
    schema.appendCol(std::string("bool_col2"),
                      meta::cpp2::PropertyType::BOOL);
    // Col 7: float_col -- FLOAT
    schema.appendCol(std::string("float_col"),
                      meta::cpp2::PropertyType::FLOAT);
    // Col 8: double_col -- DOUBLE
    schema.appendCol(std::string("double_col"),
                      meta::cpp2::PropertyType::DOUBLE);
    // Col 9: timestamp_col -- TIMESTAMP
    schema.appendCol("timestamp_col", meta::cpp2::PropertyType::TIMESTAMP);

    std::string encoded;
    // Single byte header (Schema version is 0, no offset)
    encoded.append(1, 0x00);
    // Col 0
    encoded.append(1, 0x01);

    const char* str1 = "Hello World!";
    const char* str2 = "Welcome to the future!";

    // Col 1
    uint8_t buf[10];
    size_t len = folly::encodeVarint(strlen(str1), buf);
    encoded.append(reinterpret_cast<char*>(buf), len);
    encoded.append(str1, strlen(str1));

    // Col 2
    len = folly::encodeVarint(100, buf);
    encoded.append(reinterpret_cast<char*>(buf), len);

    // Col 3
    len = folly::encodeVarint(0xFFFFFFFFFFFFFFFFL, buf);
    encoded.append(reinterpret_cast<char*>(buf), len);

    // Col 4
    int64_t id = 0x8877665544332211L;
    encoded.append(reinterpret_cast<char*>(&id), sizeof(int64_t));

    // Col 5
    len = folly::encodeVarint(strlen(str2), buf);
    encoded.append(reinterpret_cast<char*>(buf), len);
    encoded.append(str2, strlen(str2));

    // Col 6
    encoded.append(1, 0x00);

    // Col 7
    float pi = 3.1415926;
    encoded.append(reinterpret_cast<char*>(&pi), sizeof(float));

    // Col 8
    double e = 2.71828182845904523536028747135266249775724709369995;
    encoded.append(reinterpret_cast<char*>(&e), sizeof(double));

    // Col 9
    len = folly::encodeVarint(1551331827, buf);
    encoded.append(reinterpret_cast<char*>(buf), len);

    /**************************
     * Now let's read it
     *************************/
    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);

    // Header info
    RowReaderWrapper* r = dynamic_cast<RowReaderWrapper*>(reader.get());
    ASSERT_EQ(&(r->readerV1_), r->currReader_);
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(1, reader->headerLen());
    ASSERT_EQ(&(r->readerV1_), r->currReader_);
    EXPECT_EQ(1, r->readerV1_.blockOffsets_.size());
    EXPECT_EQ(0, r->readerV1_.blockOffsets_[0].first);

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
    val = reader->getValueByName("str_col1");
    EXPECT_EQ(Value::Type::STRING, val.type());
    EXPECT_EQ(str1, val.getStr());

    // Col 2
    val = reader->getValueByIndex(2);
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(100, val.getInt());
    val = reader->getValueByName("int_col1");
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(100, val.getInt());

    // Col 3
    val = reader->getValueByIndex(3);
    EXPECT_EQ(Value::Type::INT, val.type());
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFL, val.getInt());
    val = reader->getValueByName("int_col2");
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
    val = reader->getValueByName("str_col2");
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

    // Col 10 -- non-existing column
    val = reader->getValueByIndex(10);
    EXPECT_EQ(Value::Type::NULLVALUE, val.type());
}


TEST(RowReaderV1, iterator) {
    std::string encoded;
    encoded.append(1, 0);
    encoded.append(1, 16);
    encoded.append(1, 32);
    encoded.append(1, 48);
    encoded.append(1, 64);

    SchemaWriter schema;
    for (int i = 0; i < 64; i++) {
        schema.appendCol(folly::stringPrintf("Col%02d", i),
                         meta::cpp2::PropertyType::INT64);
        encoded.append(1, i + 1);
    }

    auto reader = RowReaderWrapper::getRowReader(&schema, encoded);
    auto it = reader->begin();
    int32_t index = 0;
    while (it != reader->end()) {
        Value v = reader->getValueByIndex(index);
        EXPECT_EQ(Value::Type::INT, v.type());
        EXPECT_EQ(v, it->value());
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


