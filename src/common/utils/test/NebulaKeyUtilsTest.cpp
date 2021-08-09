/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>

namespace nebula {

class KeyUtilsTestBase : public ::testing::Test {
public:
    explicit KeyUtilsTestBase(size_t vIdLen)
        : vIdLen_(vIdLen) {}

    ~KeyUtilsTestBase() = default;

    void verifyVertex(PartitionID partId,
                      VertexID vId,
                      TagID tagId,
                      size_t actualSize) {
        auto vertexKey = NebulaKeyUtils::vertexKey(vIdLen_, partId, vId, tagId);
        ASSERT_EQ(vertexKey.size(), kVertexLen + vIdLen_);
        ASSERT_EQ(vertexKey.substr(0, sizeof(PartitionID) + vIdLen_ + sizeof(TagID)),
                NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId, tagId));
        ASSERT_EQ(vertexKey.substr(0, sizeof(PartitionID) + vIdLen_),
                NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId));
        ASSERT_TRUE(NebulaKeyUtils::isVertex(vIdLen_, vertexKey));
        ASSERT_FALSE(NebulaKeyUtils::isEdge(vIdLen_, vertexKey));
        ASSERT_EQ(partId, NebulaKeyUtils::getPart(vertexKey));
        ASSERT_EQ(tagId, NebulaKeyUtils::getTagId(vIdLen_, vertexKey));
        ASSERT_EQ(vId, NebulaKeyUtils::getVertexId(vIdLen_, vertexKey).subpiece(0, actualSize));
    }

    void verifyEdge(PartitionID partId,
                    VertexID srcId,
                    EdgeType type,
                    EdgeRanking rank,
                    VertexID dstId,
                    EdgeVerPlaceHolder edgeVersion,
                    size_t actualSize) {
        auto edgeKey = NebulaKeyUtils::edgeKey(vIdLen_, partId, srcId, type,
                                               rank, dstId, edgeVersion);
        ASSERT_EQ(edgeKey.size(), kEdgeLen + (vIdLen_ << 1));
        ASSERT_EQ(edgeKey.substr(0, sizeof(PartitionID) + vIdLen_ + sizeof(EdgeType)),
                  NebulaKeyUtils::edgePrefix(vIdLen_, partId, srcId, type));
        ASSERT_EQ(edgeKey.substr(0, sizeof(PartitionID) + vIdLen_),
                  NebulaKeyUtils::edgePrefix(vIdLen_, partId, srcId));
        ASSERT_EQ(edgeKey.substr(0, sizeof(PartitionID) + (vIdLen_ << 1)
                                 + sizeof(EdgeType) + sizeof(EdgeRanking)),
                  NebulaKeyUtils::edgePrefix(vIdLen_, partId, srcId, type, rank, dstId));
        ASSERT_TRUE(NebulaKeyUtils::isEdge(vIdLen_, edgeKey));
        ASSERT_FALSE(NebulaKeyUtils::isVertex(vIdLen_, edgeKey));
        ASSERT_EQ(partId, NebulaKeyUtils::getPart(edgeKey));
        ASSERT_EQ(srcId, NebulaKeyUtils::getSrcId(vIdLen_, edgeKey).subpiece(0, actualSize));
        ASSERT_EQ(dstId, NebulaKeyUtils::getDstId(vIdLen_, edgeKey).subpiece(0, actualSize));
        ASSERT_EQ(type, NebulaKeyUtils::getEdgeType(vIdLen_, edgeKey));
        ASSERT_EQ(rank, NebulaKeyUtils::getRank(vIdLen_, edgeKey));
    }

protected:
    size_t vIdLen_;
};

class V1Test : public KeyUtilsTestBase {
public:
    V1Test() : KeyUtilsTestBase(8) {}

    VertexID getStringId(int64_t vId) {
        std::string id;
        id.append(reinterpret_cast<const char*>(&vId), sizeof(int64_t));
        return id;
    }
};

class V2ShortTest : public KeyUtilsTestBase {
public:
    V2ShortTest() : KeyUtilsTestBase(10) {}
};

class V2LongTest : public KeyUtilsTestBase {
public:
    V2LongTest() : KeyUtilsTestBase(100) {}
};


TEST_F(V1Test, SimpleTest) {
    PartitionID partId = 123;
    VertexID vId = getStringId(1001L);
    TagID tagId = 2020;
    TagVersion tagVersion = folly::Random::rand64();
    verifyVertex(partId, vId, tagId, tagVersion);

    VertexID srcId = getStringId(1001L), dstId = getStringId(2001L);
    EdgeType type = 1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, sizeof(int64_t));
}

TEST_F(V1Test, NegativeEdgeTypeTest) {
    PartitionID partId = 123;
    VertexID vId = getStringId(1001L);
    TagID tagId = 2020;
    verifyVertex(partId, vId, tagId, sizeof(int64_t));

    VertexID srcId = getStringId(1001L), dstId = getStringId(2001L);
    EdgeType type = -1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, sizeof(int64_t));
}

TEST_F(V2ShortTest, SimpleTest) {
    PartitionID partId = 123;
    VertexID vId = "0123456789";
    TagID tagId = 2020;
    verifyVertex(partId, vId, tagId, 10);

    VertexID srcId = "0123456789", dstId = "9876543210";
    EdgeType type = 1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);
}

TEST_F(V2ShortTest, NegativeEdgeTypeTest) {
    PartitionID partId = 123;
    VertexID vId = "0123456789";
    TagID tagId = 2020;
    verifyVertex(partId, vId, tagId, 10);

    VertexID srcId = "0123456789", dstId = "9876543210";
    EdgeType type = -1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);
}

TEST_F(V2LongTest, SimpleTest) {
    PartitionID partId = 123;
    VertexID vId = "0123456789";
    TagID tagId = 2020;
    verifyVertex(partId, vId, tagId, 10);

    VertexID srcId = "0123456789", dstId = "9876543210";
    EdgeType type = 1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);
}

TEST_F(V2LongTest, NegativeEdgeTypeTest) {
    PartitionID partId = 123;
    VertexID vId = "0123456789";
    TagID tagId = 2020;
    verifyVertex(partId, vId, tagId, 10);

    VertexID srcId = "0123456789", dstId = "9876543210";
    EdgeType type = -1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);
}

TEST_F(V2LongTest, RankTest) {
    PartitionID partId = 123;
    VertexID srcId = "0123456789", dstId = "9876543210";
    EdgeType type = -1010;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);

    rank = 0L;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);

    rank = -1L;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);

    rank = 9223372036854775807L;
    verifyEdge(partId, srcId, type, rank, dstId, edgeVersion, 10);
}

TEST(KeyUtilsTest, MiscTest) {
    PartitionID partId = 123;
    auto commitKey = NebulaKeyUtils::systemCommitKey(partId);
    ASSERT_TRUE(NebulaKeyUtils::isSystemCommit(commitKey));
    auto partKey = NebulaKeyUtils::systemPartKey(partId);
    ASSERT_TRUE(NebulaKeyUtils::isSystemPart(partKey));
    auto systemPrefix = NebulaKeyUtils::systemPrefix();
    ASSERT_EQ(commitKey.find(systemPrefix), 0);
    ASSERT_EQ(partKey.find(systemPrefix), 0);
}



}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
