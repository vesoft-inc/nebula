/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "dataman/RowWriter.h"
#include "dataman/RowUpdater.h"
#include "dataman/SchemaWriter.h"

using namespace vesoft::vgraph;

SchemaWriter schema;

void prepareSchema() {
    schema.appendCol("col1", vesoft::storage::cpp2::SupportedType::INT);
    schema.appendCol("col2", vesoft::storage::cpp2::SupportedType::INT);
    schema.appendCol("col3", vesoft::storage::cpp2::SupportedType::STRING);
    schema.appendCol("col4", vesoft::storage::cpp2::SupportedType::STRING);
    schema.appendCol("col5", vesoft::storage::cpp2::SupportedType::BOOL);
    schema.appendCol("col6", vesoft::storage::cpp2::SupportedType::FLOAT);
    schema.appendCol("col7", vesoft::storage::cpp2::SupportedType::VID);
    schema.appendCol("col8", vesoft::storage::cpp2::SupportedType::DOUBLE);
}


/**********************************
 *
 * Start testing
 *
 *********************************/
TEST(RowUpdater, noOrigin) {
    RowUpdater updater(&schema);

    bool bVal;
    int64_t iVal;
    float fVal;
    double dVal;
    folly::StringPiece sVal;

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col1", 123456789));
    EXPECT_EQ(ResultType::E_INCOMPATIBLE_TYPE,
              updater.getFloat("col1", fVal));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getInt("col1", iVal));
    EXPECT_EQ(123456789, iVal);

    EXPECT_EQ(ResultType::E_NAME_NOT_FOUND,
              updater.getString("col4", sVal));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setString("col4", "Hello"));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getString("col4", sVal));
    EXPECT_EQ("Hello", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setBool("col5", true));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getBool("col5", bVal));
    EXPECT_EQ(true, bVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setFloat("col6", 3.1415926));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getFloat("col6", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setVid("col7", 0xABCDABCDABCDABCD));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getVid("col7", iVal));
    EXPECT_EQ(0xABCDABCDABCDABCD, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setDouble("col8", 2.17));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(2.17, dVal);
}


TEST(RowUpdater, withOrigin) {
    RowWriter writer(&schema, 0);
    writer << 123 << 456 << "Hello" << "World"
           << true << 3.1415926 << 0xABCDABCDABCDABCD << 2.17;
    std::string encoded(writer.encode());

    RowUpdater updater(&schema, encoded);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col2", 789));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setFloat("col6", 2.17));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setVid("col7", 0x1234123412341234));

    bool bVal;
    int64_t iVal;
    folly::StringPiece sVal;
    float fVal;
    double dVal;

    EXPECT_EQ(ResultType::E_INCOMPATIBLE_TYPE,
              updater.getFloat("col1", fVal));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getInt("col1", iVal));
    EXPECT_EQ(123, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getInt("col2", iVal));
    EXPECT_EQ(789, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getString("col3", sVal));
    EXPECT_EQ("Hello", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getString("col4", sVal));
    EXPECT_EQ("World", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getBool("col5", bVal));
    EXPECT_EQ(true, bVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getFloat("col6", fVal));
    EXPECT_FLOAT_EQ(2.17, fVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getVid("col7", iVal));
    EXPECT_EQ(0x1234123412341234, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(2.17, dVal);
}


TEST(RowUpdater, encodeWithAllFields) {
    RowUpdater updater(&schema);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col1", 123));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col2", 456));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setString("col3", "Hello"));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setString("col4", "World"));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setBool("col5", true));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setFloat("col6", 3.1415926));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setVid("col7", 0xABCDABCDABCDABCD));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setDouble("col8", 2.17));

    std::string encoded(updater.encode());

    bool bVal;
    int64_t iVal;
    float fVal;
    double dVal;
    folly::StringPiece sVal;

    RowReader reader(&schema, encoded);

    EXPECT_EQ(ResultType::E_INCOMPATIBLE_TYPE,
              reader.getFloat("col1", fVal));
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col1", iVal));
    EXPECT_EQ(123, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col2", iVal));
    EXPECT_EQ(456, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col3", sVal));
    EXPECT_EQ("Hello", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col4", sVal));
    EXPECT_EQ("World", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("col5", bVal));
    EXPECT_EQ(true, bVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat("col6", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid("col7", iVal));
    EXPECT_EQ(0xABCDABCDABCDABCD, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(2.17, dVal);
}


TEST(RowUpdater, encodeWithMissingFields) {
    RowUpdater updater(&schema);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col2", 456));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setString("col4", "World"));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setFloat("col6", 3.1415926));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setVid("col7", 0xABCDABCDABCDABCD));

    std::string encoded(updater.encode());

    bool bVal;
    int64_t iVal;
    float fVal;
    double dVal;
    folly::StringPiece sVal;

    RowReader reader(&schema, encoded);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col1", iVal));
    EXPECT_EQ(0, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getInt("col2", iVal));
    EXPECT_EQ(456, iVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col3", sVal));
    EXPECT_EQ("", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getString("col4", sVal));
    EXPECT_EQ("World", sVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getBool("col5", bVal));
    EXPECT_EQ(false, bVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getFloat("col6", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getVid("col7", iVal));
    EXPECT_EQ(0xABCDABCDABCDABCD, iVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader.getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(0.0, dVal);
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    prepareSchema();

    return RUN_ALL_TESTS();
}


