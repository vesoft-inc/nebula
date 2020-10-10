/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "base/Base.h"
#include "base/Logging.h"
#include "datamanlite/Slice.h"
#include "datamanlite/RowReader.h"
#include "datamanlite/NebulaSchemaProvider.h"

namespace nebula {
namespace dataman {
namespace codec {


TEST(RowReader, headerInfo) {
    // Simplest row, nothing in it
    char data1[] = {0x00};
    auto schema1 = std::make_shared<NebulaSchemaProvider>(0);
    auto reader1 = RowReader::getRowReader(
        Slice(data1, sizeof(data1)),
        schema1);
    EXPECT_EQ(0, reader1->schemaVer());
    EXPECT_EQ(sizeof(data1), reader1->headerLen_);

    // With schema version
    char data2[] = {0x40, 0x01, static_cast<char>(0xFF)};
    auto schema2 = std::make_shared<NebulaSchemaProvider>(0x00FF01);
    auto reader2 = RowReader::getRowReader(
        Slice(data2, sizeof(data2)),
        schema2);
    EXPECT_EQ(0x0000FF01, reader2->schemaVer());
    EXPECT_EQ(sizeof(data2), reader2->headerLen_);

    // Insert 33 fields into schema, so we will get 2 offsets
    auto schema3 = std::make_shared<NebulaSchemaProvider>(0x00FFFF01);
    for (int i = 0; i < 33; i++) {
        schema3->addField(Slice(folly::stringPrintf("Column%02d", i)),
                          ValueType::INT);
    }

    // With schema version and offsets
    char data3[] = {0x60, 0x01, static_cast<char>(0xFF),
                    static_cast<char>(0xFF), 0x40,
                    static_cast<char>(0xF0)};
    auto reader3 = RowReader::getRowReader(
        Slice(data3, sizeof(data3)),
        schema3);
    EXPECT_EQ(0x00FFFF01, reader3->schemaVer());
    EXPECT_EQ(sizeof(data3), reader3->headerLen_);
    ASSERT_EQ(3, reader3->blockOffsets_.size());
    EXPECT_EQ(0x0000, reader3->blockOffsets_[0].first);
    EXPECT_EQ(0x0040, reader3->blockOffsets_[1].first);
    EXPECT_EQ(0x00F0, reader3->blockOffsets_[2].first);

    // No schema version, with offsets
    auto schema4 = std::make_shared<NebulaSchemaProvider>();
    for (int i = 0; i < 33; i++) {
        schema4->addField(Slice(folly::stringPrintf("Column%02d", i)),
                          ValueType::INT);
    }

    char data4[] = {0x01, static_cast<char>(0xFF), 0x40, 0x08, static_cast<char>(0xF0)};
    auto reader4 = RowReader::getRowReader(
        Slice(data4, sizeof(data4)),
        schema4);
    EXPECT_EQ(0, reader4->schemaVer());
    EXPECT_EQ(sizeof(data4), reader4->headerLen_);
    ASSERT_EQ(3, reader4->blockOffsets_.size());
    EXPECT_EQ(0x000000, reader4->blockOffsets_[0].first);
    EXPECT_EQ(0x0040FF, reader4->blockOffsets_[1].first);
    EXPECT_EQ(0x00F008, reader4->blockOffsets_[2].first);
}

TEST(RowReader, encodedData) {
    const char* colName1 = "int_col1";
    const std::string colName2("int_col2");
    std::string colName3("vid_col");

    auto schema = std::make_shared<NebulaSchemaProvider>();
    // Col 0: bool_col1 -- BOOL
    schema->addField(Slice("bool_col1"), ValueType::BOOL);
    // Col 1: str_col1 -- STRING
    schema->addField(Slice("str_col1"), ValueType::STRING);
    // Col 2: int_col1 -- INT
    schema->addField(Slice(colName1), ValueType::INT);
    // Col 3: int_col2 -- INT
    schema->addField(Slice(colName2), ValueType::INT);
    // Col 4: vid_col -- VID
    schema->addField(Slice(colName3),
                     ValueType::VID);
    // Col 5: str_col2 -- STRING
    schema->addField(Slice("str_col2"), ValueType::STRING);
    // Col 6: bool_col2 -- BOOL
    schema->addField(Slice("bool_col2"),
                     ValueType::BOOL);
    // Col 7: float_col -- FLOAT
    schema->addField(Slice("float_col"),
                     ValueType::FLOAT);
    // Col 8: double_col -- DOUBLE
    schema->addField(Slice("double_col"),
                     ValueType::DOUBLE);
    // Col 9: timestamp_col -- TIMESTAMP
    schema->addField(Slice("timestamp_col"), ValueType::TIMESTAMP);

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

    // Now let's read it
    auto reader = RowReader::getRowReader(Slice(encoded), schema);

    // Header info
    EXPECT_EQ(0, reader->schemaVer());
    EXPECT_EQ(1, reader->blockOffsets_.size());
    EXPECT_EQ(0, reader->blockOffsets_[0].first);
    EXPECT_EQ(1, reader->headerLen_);

    bool bVal;
    int32_t i32Val;
    int64_t i64Val;
    Slice sVal;
    float fVal;
    double dVal;

    // Col 0
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getBool(0, bVal));
    EXPECT_TRUE(bVal);

    // Col 1
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString(1, sVal));
    EXPECT_EQ(str1, sVal.toString());

    // Col 2
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt(2, i32Val));
    EXPECT_EQ(100, i32Val);

    // Col 3
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt(3, i32Val));
    EXPECT_EQ(0xFFFFFFFF, i32Val);
    i32Val = 0;

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt(3, i64Val));
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFL, i64Val);

    // Col 4
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getVid(4, i64Val));
    EXPECT_EQ(0x8877665544332211L, i64Val);

    // Col 5
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString(5, sVal));
    EXPECT_EQ(str2, sVal.toString());

    // Col 6
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getBool(6, bVal));
    EXPECT_FALSE(bVal);

    // Col 7
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getFloat(7, fVal));
    EXPECT_FLOAT_EQ(pi, fVal);

    // Col 8
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getDouble(8, dVal));
    EXPECT_DOUBLE_EQ(e, dVal);

    // Col 9
    i64Val = 0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt(9, i64Val));
    EXPECT_EQ(1551331827, i64Val);

    // Col 10 -- non-existing column
    EXPECT_EQ(ResultType::E_INDEX_OUT_OF_RANGE,
              reader->getBool(10, bVal));
}

TEST(RowReader, iterator) {
    std::string encoded;
    encoded.append(1, 0);
    encoded.append(1, 16);
    encoded.append(1, 32);
    encoded.append(1, 48);
    encoded.append(1, 64);

    auto schema = std::make_shared<NebulaSchemaProvider>();
    for (int i = 0; i < 64; i++) {
        schema->addField(Slice(folly::stringPrintf("Col%02d", i)),
                         ValueType::INT);
        encoded.append(1, i + 1);
    }

    auto reader = RowReader::getRowReader(Slice(encoded), schema);
    auto it = reader->begin();
    int32_t v1;
    int32_t v2;
    for (int i = 0; i < 64; i++) {
        EXPECT_EQ(ResultType::SUCCEEDED, reader->getInt(i, v1));
        EXPECT_EQ(ResultType::SUCCEEDED, it->getInt(v2));
        EXPECT_EQ(v1, v2);
        ++it;
    }

    EXPECT_FALSE((bool)it);
    EXPECT_EQ(it, reader->end());
}

}  // namespace codec
}  // namespace dataman
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


