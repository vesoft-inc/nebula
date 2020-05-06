/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>

namespace nebula {

TEST(NebulaKeyUtilsTest, SimpleTest) {
    PartitionID partId = 15;
    VertexID srcId = 1001L, dstId = 2001L;
    TagID tagId = 1001;
    TagVersion tagVersion = 20L;
    EdgeType type = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 20;

    auto vertexKey = NebulaKeyUtils::vertexKey(partId, srcId, tagId, tagVersion);
    ASSERT_TRUE(NebulaKeyUtils::isVertex(vertexKey));
    ASSERT_EQ(partId, NebulaKeyUtils::getPart(vertexKey));
    ASSERT_EQ(tagId, NebulaKeyUtils::getTagId(vertexKey));
    ASSERT_EQ(srcId, NebulaKeyUtils::getVertexId(vertexKey));

    std::string invalidLenKey(12, 'A');
    ASSERT_EQ(12, invalidLenKey.size());
    ASSERT_FALSE(NebulaKeyUtils::isVertex(invalidLenKey));
    ASSERT_FALSE(NebulaKeyUtils::isEdge(invalidLenKey));

    auto edgeKey = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, edgeVersion);
    ASSERT_TRUE(NebulaKeyUtils::isEdge(edgeKey));
    ASSERT_EQ(partId, NebulaKeyUtils::getPart(edgeKey));
    ASSERT_EQ(srcId, NebulaKeyUtils::getSrcId(edgeKey));
    ASSERT_EQ(dstId, NebulaKeyUtils::getDstId(edgeKey));
    ASSERT_EQ(type, NebulaKeyUtils::getEdgeType(edgeKey));
    ASSERT_EQ(rank, NebulaKeyUtils::getRank(edgeKey));
    auto uuidKey = NebulaKeyUtils::uuidKey(partId, "nebula");
    ASSERT_TRUE(NebulaKeyUtils::isUUIDKey(uuidKey));
}

template<class T>
VariantType getVal(T v) {
    return v;
}

bool evalInt64(int64_t val) {
    auto v = getVal(val);
    auto str = NebulaKeyUtils::encodeVariant(v);
    auto res = NebulaKeyUtils::decodeVariant(str, nebula::cpp2::SupportedType::INT);
    if (!res.ok()) {
        return false;
    }

    EXPECT_EQ(VAR_INT64, res.value().which());
    EXPECT_EQ(v, res.value());
    return val == boost::get<int64_t>(res.value());
}

bool evalDouble(double val) {
    auto v = getVal(val);
    auto str = NebulaKeyUtils::encodeVariant(v);
    auto res = NebulaKeyUtils::decodeVariant(str, nebula::cpp2::SupportedType::DOUBLE);
    if (!res.ok()) {
        return false;
    }
    EXPECT_EQ(VAR_DOUBLE, res.value().which());
    EXPECT_EQ(v, res.value());
    return val == boost::get<double>(res.value());
}

TEST(NebulaKeyUtilsTest, encodeVariant) {
    EXPECT_TRUE(evalInt64(1));
    EXPECT_TRUE(evalInt64(0));
    EXPECT_TRUE(evalInt64(std::numeric_limits<int64_t>::max()));
    EXPECT_TRUE(evalInt64(std::numeric_limits<int64_t>::min()));
    EXPECT_TRUE(evalDouble(1.1));
    EXPECT_TRUE(evalDouble(0.0));
    EXPECT_TRUE(evalDouble(std::numeric_limits<double>::max()));
    EXPECT_TRUE(evalDouble(std::numeric_limits<double>::min()));
    EXPECT_TRUE(evalDouble(-std::numeric_limits<double>::max()));
    EXPECT_TRUE(evalDouble(-(0.000000001 - std::numeric_limits<double>::min())));
}

TEST(NebulaKeyUtilsTest, encodeDouble) {
    EXPECT_TRUE(evalDouble(100.5));
    EXPECT_TRUE(evalDouble(200.5));
    EXPECT_TRUE(evalDouble(300.5));
    EXPECT_TRUE(evalDouble(400.5));
    EXPECT_TRUE(evalDouble(500.5));
    EXPECT_TRUE(evalDouble(600.5));
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

