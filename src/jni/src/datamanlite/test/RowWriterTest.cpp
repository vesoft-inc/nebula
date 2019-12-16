/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <type_traits>
#include "datamanlite/RowWriter.h"
#include "datamanlite/RowReader.h"
#include "datamanlite/NebulaSchemaProvider.h"

namespace nebula {
namespace dataman {
namespace codec {

TEST(RowWriter, withoutSchema) {
    RowWriter writer;
    writer << true << 10 << "Hello World!"
           << 3.1415926  // By default, this will be a double
           << static_cast<float>(3.1415926);

    std::string encoded = writer.encode();
    auto schema = std::make_shared<NebulaSchemaProvider>(0);
    schema->addField(Slice("c1"), ValueType::BOOL);
    schema->addField(Slice("c2"), ValueType::INT);
    schema->addField(Slice("c3"), ValueType::STRING);
    schema->addField(Slice("c4"), ValueType::DOUBLE);
    schema->addField(Slice("c5"), ValueType::FLOAT);

    auto reader = RowReader::getRowReader(Slice(encoded), schema);

    EXPECT_EQ(0x00000000, reader->schemaVer());
    EXPECT_EQ(5, reader->numFields());

    // Col 0
    bool bVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getBool(0, bVal));
    EXPECT_TRUE(bVal);

    // Col 1
    int32_t iVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getInt(1, iVal));
    EXPECT_EQ(10, iVal);

    // Col 2
    Slice sVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getString(2, sVal));
    EXPECT_EQ("Hello World!", sVal.toString());

    // Col 3
    double dVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getDouble(3, dVal));
    EXPECT_DOUBLE_EQ(3.1415926, dVal);

    // Col 4
    float fVal;
    EXPECT_EQ(ResultType::SUCCEEDED,
              reader->getFloat(4, fVal));
    EXPECT_FLOAT_EQ(3.1415926, fVal);
}


TEST(RowWriter, offsetsCreation) {
    RowWriter writer;
    // These fields should create two offsets
    for (int i = 0; i < 33; i++) {
        writer << i;
    }

    std::string encoded = writer.encode();
    auto schema = std::make_shared<NebulaSchemaProvider>(0);
    for (int i = 0; i < 33; i++) {
        schema->addField(Slice(folly::stringPrintf("col%d", i)), ValueType::INT);
    }
    auto reader = RowReader::getRowReader(Slice(encoded), schema);

    EXPECT_EQ(0x00000000, reader->schemaVer());
    EXPECT_EQ(33, reader->numFields());
    EXPECT_EQ(3, reader->blockOffsets_.size());
    EXPECT_EQ(0, reader->blockOffsets_[0].first);
    EXPECT_EQ(16, reader->blockOffsets_[1].first);
    EXPECT_EQ(32, reader->blockOffsets_[2].first);
}


}  // namespace codec
}  // namespace dataman
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

