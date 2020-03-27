/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "dataman/RowSetReader.h"
#include "dataman/RowSetWriter.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "dataman/SchemaWriter.h"

namespace nebula {

TEST(RowSetReaderWriter, allInts) {
    auto schema = std::make_shared<SchemaWriter>();
    for (int i = 0; i < 33; i++) {
        schema->appendCol(folly::stringPrintf("col%02d", i),
                          cpp2::SupportedType::INT);
    }

    RowSetWriter rsWriter(schema);
    for (int row = 0; row < 10; row++) {
        int32_t base = row * 100;
        RowWriter writer(schema);
        for (int col = 0; col < 33; col++) {
            writer << base + col + 1;
        }
        rsWriter.addRow(writer);
    }

    std::string data = std::move(rsWriter.data());
    RowSetReader rsReader(schema, data);
    auto it = rsReader.begin();
    int32_t base = 0;
    while (it) {
        auto fieldIt = it->begin();
        int32_t expected = base + 1;
        while (fieldIt) {
            int32_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getInt(v));
            EXPECT_EQ(expected, v);
            ++expected;
            ++fieldIt;
        }
        EXPECT_EQ(fieldIt, it->end());
        EXPECT_EQ(base + 34, expected);
        base += 100;
        ++it;
    }

    EXPECT_EQ(it, rsReader.end());
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

