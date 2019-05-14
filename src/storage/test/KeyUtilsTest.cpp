/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/KeyUtils.h"


namespace nebula {
namespace storage {

TEST(KeyUtilsTest, SimpleTest) {
    PartitionID partId = 0L;
    VertexID srcId = 1001L, dstId = 2001L;
    EdgeType type = 101;
    EdgeRanking rank = 10L;
    EdgeVersion version = 20;
    auto edgeKey = KeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);

    CHECK(KeyUtils::isEdge(edgeKey));
    CHECK_EQ(srcId, KeyUtils::getSrcId(edgeKey));
    CHECK_EQ(dstId, KeyUtils::getDstId(edgeKey));
    CHECK_EQ(type, KeyUtils::getEdgeType(edgeKey));
    CHECK_EQ(rank, KeyUtils::getRank(edgeKey));
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


