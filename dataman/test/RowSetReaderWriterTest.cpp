/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "dataman/RowSetReader.h"
#include "dataman/RowSetWriter.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "dataman/SchemaWriter.h"

namespace vesoft {
namespace vgraph {

TEST(RowSetReaderWriter, allInts) {
    SchemaWriter schema;
    for (int i = 0; i < 33; i++) {
        schema.appendCol(folly::stringPrintf("col%02d", i),
                         storage::cpp2::SupportedType::INT);
    }

    RowSetWriter rsWriter(&schema);
    for (int row = 0; row < 10; row++ ) {
        int32_t base = row * 100;
        RowWriter writer(&schema);
        for (int col = 0; col < 33; col++) {
            writer << base + col + 1;
        }
        rsWriter.addRow(writer);
    }

    std::string data = std::move(rsWriter.data());
    RowSetReader rsReader(&schema, data);
    auto it = rsReader.begin();
    int32_t base = 0;
    while (bool(it)) {
        auto fieldIt = it->begin();
        int32_t expected = base + 1;
        while (bool(fieldIt)) {
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

}  // namespace vgraph
}  // namespace vesoft


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

