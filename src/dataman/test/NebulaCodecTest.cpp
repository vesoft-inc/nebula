/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>

#include "dataman/RowReader.h"
#include "dataman/SchemaWriter.h"
#include "dataman/NebulaCodecImpl.h"

namespace nebula {

TEST(NebulaCodec, basic) {
  std::vector<boost::any> v;
  v.push_back(1);
  v.push_back(false);
  v.push_back(3.14F);
  v.push_back(3.14);
  v.push_back(std::string("hi"));

  EXPECT_EQ(boost::any_cast<int>(v[0]), 1);
  EXPECT_EQ(boost::any_cast<bool>(v[1]), false);
  EXPECT_EQ(boost::any_cast<float>(v[2]), 3.14F);
  EXPECT_EQ(boost::any_cast<double>(v[3]), 3.14);
  EXPECT_EQ(boost::any_cast<std::string>(v[4]), "hi");

  dataman::NebulaCodecImpl codec;
  std::string encoded = codec.encode(v);

  SchemaWriter schemaWriter;
  schemaWriter.appendCol("i_field", cpp2::SupportedType::INT);
  schemaWriter.appendCol("b_field", cpp2::SupportedType::BOOL);
  schemaWriter.appendCol("f_field", cpp2::SupportedType::FLOAT);
  schemaWriter.appendCol("d_field", cpp2::SupportedType::DOUBLE);
  schemaWriter.appendCol("s_field", cpp2::SupportedType::STRING);

  auto schema = std::make_shared<ResultSchemaProvider>(schemaWriter.moveSchema());
  auto reader = RowReader::getRowReader(encoded, schema);
  EXPECT_EQ(5, reader->numFields());

  // check int field
  int32_t iVal;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getInt(0, iVal));
  EXPECT_EQ(1, iVal);
  iVal = 0;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getInt("i_field", iVal));
  EXPECT_EQ(1, iVal);

  // check bool field
  bool bVal;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getBool(1, bVal));
  EXPECT_FALSE(bVal);
  bVal = true;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getBool("b_field", bVal));
  EXPECT_FALSE(bVal);

  // check float field
  float fVal;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getFloat(2, fVal));
  EXPECT_FLOAT_EQ(3.14, fVal);
  fVal = 0.0;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getFloat("f_field", fVal));
  EXPECT_FLOAT_EQ(3.14, fVal);

  // check double field
  double dVal;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getDouble(3, dVal));
  EXPECT_DOUBLE_EQ(3.14, dVal);
  dVal = 0.0;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getDouble("d_field", dVal));
  EXPECT_DOUBLE_EQ(3.14, dVal);

  // check string field
  folly::StringPiece sVal;
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getString(4, sVal));
  EXPECT_EQ("hi", sVal.toString());
  sVal.clear();
  EXPECT_EQ(ResultType::SUCCEEDED,
            reader->getString("s_field", sVal));
  EXPECT_EQ("hi", sVal.toString());
}
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
