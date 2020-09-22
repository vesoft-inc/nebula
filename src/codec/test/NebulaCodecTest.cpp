/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "codec/RowReaderWrapper"
#include "codec/NebulaCodecImpl.h"
#include "codec/test/SchemaWriter.h"
#include <gtest/gtest.h>

namespace nebula {

TEST(NebulaCodec, encode) {
    std::vector<boost::any> v;
    v.emplace_back(1);
    v.emplace_back(false);
    v.emplace_back(3.14F);
    v.emplace_back(3.14);
    v.emplace_back(std::string("hi"));

    EXPECT_EQ(boost::any_cast<int>(v[0]), 1);
    EXPECT_EQ(boost::any_cast<bool>(v[1]), false);
    EXPECT_EQ(boost::any_cast<float>(v[2]), 3.14F);
    EXPECT_EQ(boost::any_cast<double>(v[3]), 3.14);
    EXPECT_EQ(boost::any_cast<std::string>(v[4]), "hi");

    SchemaWriter schemaWriter;
    schemaWriter.appendCol("i_field", cpp2::SupportedType::INT);
    schemaWriter.appendCol("b_field", cpp2::SupportedType::BOOL);
    schemaWriter.appendCol("f_field", cpp2::SupportedType::FLOAT);
    schemaWriter.appendCol("d_field", cpp2::SupportedType::DOUBLE);
    schemaWriter.appendCol("s_field", cpp2::SupportedType::STRING);

    auto schema = std::make_shared<ResultSchemaProvider>(schemaWriter.moveSchema());
    dataman::NebulaCodecImpl codec;
    std::string encoded = codec.encode(v, schema);

    auto reader = RowReaderWrapper::getRowReader(encoded, schema);
    EXPECT_EQ(5, reader->numFields());

    // check int field
    int32_t iVal;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getInt(0, iVal));
    EXPECT_EQ(1, iVal);
    iVal = 0;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getInt("i_field", iVal));
    EXPECT_EQ(1, iVal);

    // check bool field
    bool bVal;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getBool(1, bVal));
    EXPECT_FALSE(bVal);
    bVal = true;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getBool("b_field", bVal));
    EXPECT_FALSE(bVal);

    // check float field
    float fVal;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getFloat(2, fVal));
    EXPECT_FLOAT_EQ(3.14, fVal);
    fVal = 0.0;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getFloat("f_field", fVal));
    EXPECT_FLOAT_EQ(3.14, fVal);

    // check double field
    double dVal;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getDouble(3, dVal));
    EXPECT_DOUBLE_EQ(3.14, dVal);
    dVal = 0.0;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getDouble("d_field", dVal));
    EXPECT_DOUBLE_EQ(3.14, dVal);

    // check string field
    folly::StringPiece sVal;
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getString(4, sVal));
    EXPECT_EQ("hi", sVal.toString());
    sVal.clear();
    EXPECT_EQ(ResultType::SUCCEEDED, reader->getString("s_field", sVal));
    EXPECT_EQ("hi", sVal.toString());

    // check empty values
    std::vector<boost::any> emptyV;
    std::string emptyEncoded = codec.encode(emptyV);

    SchemaWriter emptyWriter;
    auto emptySchema = std::make_shared<ResultSchemaProvider>(emptyWriter.moveSchema());
    auto emptyReader = RowReaderWrapper::getRowReader(emptyEncoded, emptySchema);
    EXPECT_EQ(0, emptyReader->numFields());
}

TEST(NebulaCodec, decode) {
    std::string encoded;
    // Single byte header (Schema version is 0, no offset)
    encoded.append(1, 0x00);

    // bool column
    encoded.append(1, 0x01);

    // int column
    uint8_t buffer[10];
    size_t i_size = folly::encodeVarint(64, buffer);
    encoded.append(reinterpret_cast<char*>(buffer), i_size);

    // vid column
    int64_t vid = 0x1122334455667788L;
    encoded.append(reinterpret_cast<char*>(&vid), sizeof(int64_t));

    // float column
    float pi = 3.14F;
    encoded.append(reinterpret_cast<char*>(&pi), sizeof(float));

    // double column
    double e = 2.718;
    encoded.append(reinterpret_cast<char*>(&e), sizeof(double));

    // string column
    const char* str_value = "Hello World!";
    size_t s_size = folly::encodeVarint(strlen(str_value), buffer);
    encoded.append(reinterpret_cast<char*>(buffer), s_size);
    encoded.append(str_value, strlen(str_value));

    SchemaWriter schemaWriter;
    schemaWriter.appendCol("b_field", cpp2::SupportedType::BOOL);
    schemaWriter.appendCol("i_field", cpp2::SupportedType::INT);
    schemaWriter.appendCol("v_field", cpp2::SupportedType::VID);
    schemaWriter.appendCol("f_field", cpp2::SupportedType::FLOAT);
    schemaWriter.appendCol("d_field", cpp2::SupportedType::DOUBLE);
    schemaWriter.appendCol("s_field", cpp2::SupportedType::STRING);
    auto schema = std::make_shared<ResultSchemaProvider>(schemaWriter.moveSchema());

    dataman::NebulaCodecImpl codec;
    auto result = codec.decode(encoded, schema);

    EXPECT_TRUE(boost::any_cast<bool>(result.value()["b_field"]));
    EXPECT_EQ(boost::any_cast<int>(result.value()["i_field"]), 64);
    EXPECT_EQ(boost::any_cast<int64_t>(result.value()["v_field"]), 0x1122334455667788L);
    EXPECT_EQ(boost::any_cast<float>(result.value()["f_field"]), 3.14F);
    EXPECT_EQ(boost::any_cast<double>(result.value()["d_field"]), 2.718);
    EXPECT_EQ(boost::any_cast<std::string>(result.value()["s_field"]), "Hello World!");

    // check empty encoded string
    auto empty_encoded = codec.decode("", schema);
    ASSERT_FALSE(empty_encoded.ok());
    ASSERT_FALSE(empty_encoded.status().ok());
    ASSERT_EQ("encoded string is empty", empty_encoded.status().toString());

    // check empty schema
    auto empty_schema = codec.decode(encoded, nullptr);
    ASSERT_FALSE(empty_schema.ok());
    ASSERT_FALSE(empty_schema.status().ok());
    ASSERT_EQ("schema is not set", empty_schema.status().toString());
}
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
