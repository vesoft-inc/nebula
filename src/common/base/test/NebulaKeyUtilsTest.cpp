/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>

namespace nebula {

TEST(NebulaKeyUtilsTest, SimpleTest) {
    PartitionID partId = 0L;
    VertexID srcId = 1001L, dstId = 2001L;
    TagID tagId = 1001;
    TagVersion tagVersion = 20L;
    EdgeType type = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 20;

    auto vertexKey = NebulaKeyUtils::vertexKey(partId, srcId, tagId, tagVersion);
    CHECK(NebulaKeyUtils::isVertex(vertexKey));
    CHECK_EQ(tagId, NebulaKeyUtils::getTagId(vertexKey));

    auto edgeKey = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, edgeVersion);
    CHECK(NebulaKeyUtils::isEdge(edgeKey));
    CHECK_EQ(srcId, NebulaKeyUtils::getSrcId(edgeKey));
    CHECK_EQ(dstId, NebulaKeyUtils::getDstId(edgeKey));
    CHECK_EQ(type, NebulaKeyUtils::getEdgeType(edgeKey));
    CHECK_EQ(rank, NebulaKeyUtils::getRank(edgeKey));
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

