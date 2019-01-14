/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "dataman/RowReader.h"
#include "dataman/SchemaWriter.h"

namespace nebula {

TEST(RowReader, headerInfo) {
    // Sinplest row, nothing in it
    char data1[] = {0x00};
    SchemaWriter schema;
    RowReader reader1(&schema, folly::StringPiece(data1, sizeof(data1)));
    EXPECT_EQ(0, reader1.schemaVer());
    EXPECT_EQ(sizeof(data1), reader1.headerLen_);

    // With schema version
    char data2[] = {0x40, 0x01, static_cast<char>(0xFF)};
    RowReader reader2(&schema, folly::StringPiece(data2, sizeof(data2)));
    EXPECT_EQ(0x0000FF01, reader2.schemaVer());
    EXPECT_EQ(sizeof(data2), reader2.headerLen_);

    // Insert 33 fields into schema, so we will get 2 offsets
    for (int i = 0; i < 33; i++) {
        schema.appendCol(folly::stringPrintf("Column%02d", i),
                         storage::cpp2::SupportedType::INT);
    }

    // With schema version and offsets
    char data3[] = {0x60, 0x01, static_cast<char>(0xFF),
                    static_cast<char>(0xFF), 0x40, static_cast<char>(0xFF)};
    RowReader reader3(&schema, folly::StringPiece(data3, sizeof(data3)));
    EXPECT_EQ(0x00FFFF01, reader3.schemaVer());
    EXPECT_EQ(sizeof(data3), reader3.headerLen_);
    ASSERT_EQ(3, reader3.blockOffsets_.size());
    EXPECT_EQ(0x0000, reader3.blockOffsets_[0].first);
    EXPECT_EQ(0x0040, reader3.blockOffsets_[1].first);
    EXPECT_EQ(0x00F0, reader3.blockOffsets_[2].first);

    // No schema version, with offsets
    char data4[] = {0x01, static_cast<char>(0xFF), 0x40, 0x08, static_cast<char>(0xFF)};
    RowReader reader4(&schema, folly::StringPiece(data4, sizeof(data4)));
    EXPECT_EQ(0, reader4.schemaVer());
    EXPECT_EQ(sizeof(data4), reader4.headerLen_);
    ASSERT_EQ(3, reader4.blockOffsets_.size());
    EXPECT_EQ(0x000000, reader4.blockOffsets_[0].first);
    EXPECT_EQ(0x0040FF, reader4.blockOffsets_[1].first);
    EXPECT_EQ(0x00F008, reader4.blockOffsets_[2].first);
}


TEST(RowReader, encodedData) {
    const char* colName1 = "int_col1";
    const std::string colName2("int_col2");
    std::string colName3("vid_col");

    SchemaWriter schema;
    // Col 0: bool_col1 -- BOOL
    schema.appendCol("bool_col1", storage::cpp2::SupportedType::BOOL);
    // Col 1: str_col1 -- STRING
    schema.appendCol(folly::stringPrintf("str_col1"),
                     storage::cpp2::SupportedType::STRING);
    // Col 2: int_col1 -- INT
    schema.appendCol(colName1, storage::cpp2::SupportedType::INT);
    // Col 3: int_col2 -- INT
    schema.appendCol(colName2, storage::cpp2::SupportedType::INT);
    // Col 4: vid_col -- VID
    schema.appendCol(folly::StringPiece(colName3),
                     storage::cpp2::SupportedType::VID);
    // Col 5: str_col2 -- STRING
    schema.appendCol("str_col2", storage::cpp2::SupportedType::STRING);
    // Col 6: bool_col2 -- BOOL
    schema.appendCol(std::string("bool_col2"),
                     storage::cpp2::SupportedType::BOOL);
    // Col 7: float_col -- FLOAT
    schema.appendCol(std::string("float_col"),
                     storage::cpp2::SupportedType::FLOAT);
    // Col 8: double_col -- DOUBLE
    schema.appendCol(std::string("double_col"),
                     storage::cpp2::SupportedType::DOUBLE);

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

    /**************************
     * Now let's read it
     *************************/
    RowReader reader(&schema, encoded);

    // Header info
    EXPECT_EQ(0, reader.schemaVer());
    EXPECT_EQ(1, reader.blockOffsets_.size());
    EXPECT_EQ(0, reader.blockOffsets_[0].first);
    EXPECT_EQ(1, reader.headerLen_);

    bool bVal;
    int32_t i32Val;
    int64_t i64Val;
    uint64_t u64Val;
    folly::StringPiece sVal;
    float fVal;
    double dVal;

    // Col 0
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool(0, bVal));
    EXPECT_TRUE(bVal);
    bVal = false;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("bool_col1", bVal));
    EXPECT_TRUE(bVal);

    // Col 1
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString(1, sVal));
    EXPECT_EQ(str1, sVal.toString());
    sVal.clear();
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("str_col1", sVal));
    EXPECT_EQ(str1, sVal.toString());

    // Col 2
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt(2, i32Val));
    EXPECT_EQ(100, i32Val);
    i32Val = 0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("int_col1", i32Val));
    EXPECT_EQ(100, i32Val);

    // Col 3
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt(3, i32Val));
    EXPECT_EQ(0xFFFFFFFF, i32Val);
    i32Val = 0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt(3, i64Val));
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFL, i64Val);
    i64Val = 0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("int_col2", u64Val));
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFL, u64Val);

    // Col 4
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid(4, i64Val));
    EXPECT_EQ(0x8877665544332211L, i64Val);
    i64Val = 0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid("vid_col", i64Val));
    EXPECT_EQ(0x8877665544332211L, i64Val);

    // Col 5
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString(5, sVal));
    EXPECT_EQ(str2, sVal.toString());
    sVal.clear();
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("str_col2", sVal));
    EXPECT_EQ(str2, sVal.toString());

    // Col 6
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool(6, bVal));
    EXPECT_FALSE(bVal);
    bVal = true;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("bool_col2", bVal));
    EXPECT_FALSE(bVal);

    // Col 7
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat(7, fVal));
    EXPECT_FLOAT_EQ(pi, fVal);
    fVal = 0.0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat("float_col", fVal));
    EXPECT_FLOAT_EQ(pi, fVal);

    // Col 8
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble(8, dVal));
    EXPECT_DOUBLE_EQ(e, dVal);
    dVal = 0.0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble("double_col", dVal));
    EXPECT_DOUBLE_EQ(e, dVal);

    // Col 9 -- non-existing column
    EXPECT_EQ(ResultType::E_INDEX_OUT_OF_RANGE,
              reader.getBool(9, bVal));
    EXPECT_EQ(ResultType::E_NAME_NOT_FOUND,
              reader.getBool("bool_col3", bVal));
}


TEST(RowReader, iterator) {
    std::string encoded;
    encoded.append(1, 0);
    encoded.append(1, 16);
    encoded.append(1, 32);
    encoded.append(1, 48);
    encoded.append(1, 64);

    SchemaWriter schema;
    for (int i = 0; i < 64; i++) {
        schema.appendCol(folly::stringPrintf("Col%02d", i),
                         storage::cpp2::SupportedType::INT);
        encoded.append(1, i + 1);
    }

    RowReader reader(&schema, encoded);
    auto it = reader.begin();
    int32_t v1;
    int32_t v2;
    for (int i = 0; i < 64; i++) {
        EXPECT_EQ(ResultType::SUCCEEDED, reader.getInt(i, v1));
        EXPECT_EQ(ResultType::SUCCEEDED, it->getInt(v2));
        EXPECT_EQ(v1, v2);
        ++it;
    }

    EXPECT_FALSE((bool)it);
    EXPECT_EQ(it, reader.end());
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


