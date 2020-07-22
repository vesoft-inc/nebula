/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "dataman/RowWriter.h"
#include "dataman/RowUpdater.h"
#include "dataman/SchemaWriter.h"

using nebula::ResultType;
using nebula::SchemaWriter;
using nebula::RowUpdater;
using nebula::RowWriter;
using nebula::RowReader;

auto schema = std::make_shared<SchemaWriter>();

void prepareSchema() {
    nebula::cpp2::Value value;
    value.set_int_value(0);
    schema->appendCol("col1", nebula::cpp2::SupportedType::INT, value);
    schema->appendCol("col2", nebula::cpp2::SupportedType::INT, value);
    value.set_string_value("");
    schema->appendCol("col3", nebula::cpp2::SupportedType::STRING, value);
    schema->appendCol("col4", nebula::cpp2::SupportedType::STRING, value);
    value.set_bool_value(false);
    schema->appendCol("col5", nebula::cpp2::SupportedType::BOOL, value);
    value.set_double_value(static_cast<float>(0.0));
    schema->appendCol("col6", nebula::cpp2::SupportedType::FLOAT, value);
    value.set_int_value(0);
    schema->appendCol("col7", nebula::cpp2::SupportedType::VID, value);
    value.set_double_value(static_cast<double>(0.0));
    schema->appendCol("col8", nebula::cpp2::SupportedType::DOUBLE, value);
    value.set_int_value(0);
    schema->appendCol("col9", nebula::cpp2::SupportedType::TIMESTAMP, value);
}


/**********************************
 *
 * Start testing
 *
 *********************************/
TEST(RowUpdater, noOrigin) {
    RowUpdater updater(schema);

    bool bVal;
    int64_t iVal, tVal;
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

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col9", 1551331827));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getInt("col9", tVal));
    EXPECT_EQ(1551331827, tVal);
}


TEST(RowUpdater, withOrigin) {
    RowWriter writer(schema);
    writer << 123 << 456 << "Hello" << "World"
           << true << 3.1415926 << 0xABCDABCDABCDABCD
           << 2.17 << 1551331828;
    std::string encoded(writer.encode());

    auto reader = RowReader::getRowReader(encoded, schema);
    RowUpdater updater(std::move(reader), schema);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col2", 789));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setFloat("col6", 2.17));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setVid("col7", 0x1234123412341234));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col9", 1551331830));

    bool bVal;
    int64_t iVal, tVal;
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

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.getInt("col9", tVal));
    EXPECT_EQ(1551331830, tVal);
}


TEST(RowUpdater, encodeWithAllFields) {
    RowUpdater updater(schema);

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
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col9", 1551331830));

    auto status = updater.encode();
    std::string encoded(status.value());

    bool bVal;
    int64_t iVal, tVal;
    float fVal;
    double dVal;
    folly::StringPiece sVal;

    auto reader = RowReader::getRowReader(encoded, schema);

    EXPECT_EQ(ResultType::E_INCOMPATIBLE_TYPE,
              reader->getFloat("col1", fVal));
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt("col1", iVal));
    EXPECT_EQ(123, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt("col2", iVal));
    EXPECT_EQ(456, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString("col3", sVal));
    EXPECT_EQ("Hello", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString("col4", sVal));
    EXPECT_EQ("World", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getBool("col5", bVal));
    EXPECT_EQ(true, bVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getFloat("col6", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getVid("col7", iVal));
    EXPECT_EQ(0xABCDABCDABCDABCD, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(2.17, dVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt("col9", tVal));
    EXPECT_EQ(1551331830, tVal);
}


TEST(RowUpdater, encodeWithMissingFields) {
    RowUpdater updater(schema);

    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setInt("col2", 456));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setString("col4", "World"));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setFloat("col6", 3.1415926));
    EXPECT_EQ(ResultType::SUCCEEDED,
              updater.setVid("col7", 0xABCDABCDABCDABCD));

    auto status = updater.encode();
    std::string encoded(status.value());

    bool bVal;
    int64_t iVal, tVal;
    float fVal;
    double dVal;
    folly::StringPiece sVal;

    auto reader = RowReader::getRowReader(encoded, schema);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt("col1", iVal));
    EXPECT_EQ(0, iVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt("col2", iVal));
    EXPECT_EQ(456, iVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString("col3", sVal));
    EXPECT_EQ("", sVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString("col4", sVal));
    EXPECT_EQ("World", sVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getBool("col5", bVal));
    EXPECT_EQ(false, bVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getFloat("col6", fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);

    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getVid("col7", iVal));
    EXPECT_EQ(0xABCDABCDABCDABCD, iVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getDouble("col8", dVal));
    EXPECT_DOUBLE_EQ(0.0, dVal);

    // Default value
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt("col9", tVal));
    EXPECT_DOUBLE_EQ(0, tVal);
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    prepareSchema();

    return RUN_ALL_TESTS();
}


