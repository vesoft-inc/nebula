/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "dataman/RowWriter.h"
#include "dataman/RowReader.h"
#include "dataman/SchemaWriter.h"

namespace nebula {

TEST(RowWriter, withoutSchema) {
    RowWriter writer(nullptr, 0x0000FFFF);
    writer << true << 10 << "Hello World!"
           << 3.1415926  // By default, this will be a double
           << (float)3.1415926;

    std::string encoded = writer.encode();
    ResultSchemaProvider schema(writer.moveSchema());
    RowReader reader(&schema, encoded);

    EXPECT_EQ(0x0000FFFF, reader.schemaVer());
    EXPECT_EQ(5, reader.numFields());

    // Col 0
    bool bVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool(0, bVal));
    EXPECT_TRUE(bVal);
    bVal = false;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("Column1", bVal));
    EXPECT_TRUE(bVal);

    // Col 1
    int32_t iVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt(1, iVal));
    EXPECT_EQ(10, iVal);
    iVal = 0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("Column2", iVal));
    EXPECT_EQ(10, iVal);

    // Col 2
    folly::StringPiece sVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString(2, sVal));
    EXPECT_EQ("Hello World!", sVal.toString());
    sVal.clear();
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("Column3", sVal));
    EXPECT_EQ("Hello World!", sVal.toString());

    // Col 3
    double dVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble(3, dVal));
    EXPECT_DOUBLE_EQ(3.1415926, dVal);
    dVal = 0.0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble("Column4", dVal));
    EXPECT_DOUBLE_EQ(3.1415926, dVal);

    // Col 4
    float fVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat(4, fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);
    fVal = 0.0;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat("Column5", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);
}


TEST(RowWriter, streamControl) {
    RowWriter writer(nullptr, 0x0000FFFF);
    writer << RowWriter::ColName("bool_col") << true
           << RowWriter::ColName("vid_col")
           << RowWriter::ColType(storage::cpp2::SupportedType::VID) << 10
           << RowWriter::ColType(storage::cpp2::SupportedType::STRING)
           << "Hello World!"
           << RowWriter::ColType(storage::cpp2::SupportedType::DOUBLE)
           << (float)3.1415926;

    std::string encoded = writer.encode();
    ResultSchemaProvider schema(writer.moveSchema());
    RowReader reader(&schema, encoded);

    EXPECT_EQ(0x0000FFFF, reader.schemaVer());
    EXPECT_EQ(4, reader.numFields());

    // Col 0
    bool bVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("bool_col", bVal));
    EXPECT_TRUE(bVal);

    // Col 1
    int64_t iVal;
    EXPECT_EQ(ResultType::E_INCOMPATIBLE_TYPE,
              reader.getInt("vid_col", iVal));
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid("vid_col", iVal));
    EXPECT_EQ(10, iVal);

    // Col 2
    folly::StringPiece sVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("Column3", sVal));
    EXPECT_EQ("Hello World!", sVal.toString());

    // Col 3
    float fVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat("Column4", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);
}


TEST(RowWriter, offsetsCreation) {
    RowWriter writer(nullptr, 0x0000FFFF);
    // These fields should create two offsets
    for (int i = 0; i < 33; i++) {
        writer << i;
    }

    std::string encoded = writer.encode();
    ResultSchemaProvider schema(writer.moveSchema());
    RowReader reader(&schema, encoded);

    EXPECT_EQ(0x0000FFFF, reader.schemaVer());
    EXPECT_EQ(33, reader.numFields());
    EXPECT_EQ(3, reader.blockOffsets_.size());
    EXPECT_EQ(0, reader.blockOffsets_[0].first);
    EXPECT_EQ(16, reader.blockOffsets_[1].first);
    EXPECT_EQ(32, reader.blockOffsets_[2].first);
}


TEST(RowWriter, withSchema) {
    SchemaWriter schema;
    schema.appendCol("col1", storage::cpp2::SupportedType::INT);
    schema.appendCol("col2", storage::cpp2::SupportedType::INT);
    schema.appendCol("col3", storage::cpp2::SupportedType::STRING);
    schema.appendCol("col4", storage::cpp2::SupportedType::STRING);
    schema.appendCol("col5", storage::cpp2::SupportedType::BOOL);
    schema.appendCol("col6", storage::cpp2::SupportedType::FLOAT);
    schema.appendCol("col7", storage::cpp2::SupportedType::VID);

    RowWriter writer(&schema);
    writer << 1 << 2 << "Hello" << "World" << true
           << 3.1415926 /* By default, this is a double */
           << 1234567;
    std::string encoded = writer.encode();
    RowReader reader(&schema, encoded);

    int64_t iVal;
    folly::StringPiece sVal;
    bool bVal;
    double dVal;

    // Col 1
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col1", iVal));
    EXPECT_EQ(1, iVal);

    // Col 2
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col2", iVal));
    EXPECT_EQ(2, iVal);

    // Col 3
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col3", sVal));
    EXPECT_EQ("Hello", sVal);

    // Col 4
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col4", sVal));
    EXPECT_EQ("World", sVal);

    // Col 5
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("col5", bVal));
    EXPECT_TRUE(bVal);  // Skipped value

    // Col 6
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble("col6", dVal));
    EXPECT_FLOAT_EQ(3.1415926, dVal);

    // Col 7
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid("col7", iVal));
    EXPECT_EQ(1234567, iVal);
}


TEST(RowWriter, skip) {
    SchemaWriter schema;
    schema.appendCol("col1", storage::cpp2::SupportedType::INT);
    schema.appendCol("col2", storage::cpp2::SupportedType::FLOAT);
    schema.appendCol("col3", storage::cpp2::SupportedType::INT);
    schema.appendCol("col4", storage::cpp2::SupportedType::STRING);
    schema.appendCol("col5", storage::cpp2::SupportedType::STRING);
    schema.appendCol("col6", storage::cpp2::SupportedType::BOOL);
    schema.appendCol("col7", storage::cpp2::SupportedType::VID);
    schema.appendCol("col8", storage::cpp2::SupportedType::DOUBLE);

    RowWriter writer(&schema);
    // Implicitly skip the last two fields
    writer << RowWriter::Skip(1) << 3.14
           << RowWriter::Skip(1) << "Hello"
           << RowWriter::Skip(1) << true;
    std::string encoded = writer.encode();
    RowReader reader(&schema, encoded);

    int64_t iVal;
    float fVal;
    double dVal;
    folly::StringPiece sVal;
    bool bVal;

    // Col 1: Skipped field
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col1", iVal));
    EXPECT_EQ(0, iVal);

    // Col 2
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat("col2", fVal));
    EXPECT_FLOAT_EQ(3.14, fVal);

    // Col 3: Skipped field
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col3", iVal));
    EXPECT_EQ(0, iVal);

    // Col 4
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col4", sVal));
    EXPECT_EQ("Hello", sVal);

    // Col 5: Skipped field
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col5", sVal));
    EXPECT_TRUE(sVal.empty());

    // Col 6
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("col6", bVal));
    EXPECT_TRUE(bVal);

    // Col 7: Implicitly skipped field
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid("col7", iVal));
    EXPECT_EQ(0, iVal);

    // Col 8: Implicitly skipped field
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(0.0, dVal);
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

