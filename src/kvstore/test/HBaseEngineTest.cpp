/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/HBaseEngine.h"
#include "kvstore/test/TestUtils.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include <gtest/gtest.h>

/**
 * TODO(zhangguoqing) Add a test runner to provide HBase/thrift2 service.
 * hbase/bin/hbase-daemon.sh start thrift2 -b 127.0.0.1 -p 9096
 * hbase(main):001:0> create 'Nebula_Graph_Space_0', 'cf'
 * */

namespace nebula {
namespace kvstore {

using nebula::storage::KeyUtils;

TEST(HBaseEngineTest, SimpleTest) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 1001L, dstId = 1010L;
    TagID tagId = 3001;
    TagVersion tagVersion = 0L;
    EdgeType edgeType = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 0L;
    auto schemaMan = TestUtils::mockSchemaMan();

    // Generate tag prop.
    auto vertexKey = KeyUtils::vertexKey(partId, srcId, tagId, tagVersion);
    auto tagScheam = schemaMan->getTagSchema(spaceId, tagId);
    RowWriter tagWriter(tagScheam);
    for (uint64_t numInt = 0; numInt < 3; numInt++) {
        tagWriter << numInt;
    }
    for (auto numString = 3; numString < 6; numString++) {
        tagWriter << folly::stringPrintf("tag_string_col_%d", numString);
    }
    auto tagValue = tagWriter.encode();
    // Generate edge prop.
    auto edgeKey = KeyUtils::edgeKey(partId, srcId, edgeType, rank, dstId, edgeVersion);
    auto edgeScheam = schemaMan->getEdgeSchema(spaceId, edgeType, edgeVersion);
    RowWriter edgeWriter(edgeScheam);
    for (int32_t iInt = 0; iInt < 10; iInt++) {
        edgeWriter << iInt;
    }
    for (int32_t iString = 10; iString < 20; iString++) {
        edgeWriter << folly::stringPrintf("string_col_%d", iString);
    }
    auto edgeValue = edgeWriter.encode();

    auto hbase_engine = std::make_unique<HBaseEngine>(
            spaceId, HostAddr(0, 9096), schemaMan.get());
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->put(vertexKey, tagValue));
    std::string retTagValue;
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->get(vertexKey, &retTagValue));
    EXPECT_EQ(tagValue, retTagValue);

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->put(edgeKey, edgeValue));
    std::string retEdgeValue;
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->get(edgeKey, &retEdgeValue));
    EXPECT_EQ(edgeValue, retEdgeValue);

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->remove(vertexKey));
    EXPECT_EQ(ResultCode::ERR_KEY_NOT_FOUND, hbase_engine->get(vertexKey, &retTagValue));
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->remove(edgeKey));
    EXPECT_EQ(ResultCode::ERR_KEY_NOT_FOUND, hbase_engine->get(edgeKey, &retEdgeValue));
}


TEST(HBaseEngineTest, MultiTest) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 1001L, dstId = 1010L;
    TagVersion tagVersion = 0L;
    EdgeType edgeType = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 0L;
    std::vector<std::string> vertexKeys;
    std::vector<std::string> edgeKeys;
    auto schemaMan = TestUtils::mockSchemaMan();

    // Generate tag prop.
    std::vector<KV> vertexData;
    for (auto tagId = 3001; tagId < 3010; tagId++) {
        auto vertexKey = KeyUtils::vertexKey(partId, srcId, tagId, tagVersion);
        vertexKeys.emplace_back(vertexKey);
        auto tagScheam = schemaMan->getTagSchema(spaceId, tagId);
        RowWriter tagWriter(tagScheam);
        for (uint64_t numInt = 0; numInt < 3; numInt++) {
            tagWriter << numInt;
        }
        for (auto numString = 3; numString < 6; numString++) {
            tagWriter << folly::stringPrintf("tag_string_col_%d", numString);
        }
        auto tagValue = tagWriter.encode();
        vertexData.emplace_back(vertexKey, tagValue);
    }
    // Generate edge prop.
    std::vector<KV> edgeData;
    auto edgeScheam = schemaMan->getEdgeSchema(spaceId, edgeType, edgeVersion);
    for (; srcId < dstId; srcId++) {
        auto edgeKey = KeyUtils::edgeKey(partId, srcId, edgeType, rank, dstId, edgeVersion);
        edgeKeys.emplace_back(edgeKey);
        RowWriter edgeWriter(edgeScheam);
        for (int32_t iInt = 0; iInt < 10; iInt++) {
            edgeWriter << iInt;
        }
        for (int32_t iString = 10; iString < 20; iString++) {
            edgeWriter << folly::stringPrintf("string_col_%d", iString);
        }
        auto edgeValue = edgeWriter.encode();
        edgeData.emplace_back(edgeKey, edgeValue);
    }

    auto hbase_engine = std::make_unique<HBaseEngine>(
            spaceId, HostAddr(0, 9096), schemaMan.get());
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiPut(vertexData));
    std::vector<std::string> retTagValues;
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiGet(vertexKeys, &retTagValues));
    EXPECT_EQ(9, retTagValues.size());
    for (size_t index = 0; index < retTagValues.size(); index++) {
        EXPECT_EQ(vertexData[index].second, retTagValues[index]);
    }

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiPut(edgeData));
    std::vector<std::string> retEdgeValues;
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiGet(edgeKeys, &retEdgeValues));
    EXPECT_EQ(9, retEdgeValues.size());
    for (size_t index = 0; index < retEdgeValues.size(); index++) {
        EXPECT_EQ(edgeData[index].second, retEdgeValues[index]);
    }

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiRemove(vertexKeys));
    retTagValues.clear();
    EXPECT_EQ(ResultCode::ERR_UNKNOWN, hbase_engine->multiGet(vertexKeys, &retTagValues));
    EXPECT_EQ(0, retTagValues.size());

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiRemove(edgeKeys));
    retEdgeValues.clear();
    EXPECT_EQ(ResultCode::ERR_UNKNOWN, hbase_engine->multiGet(edgeKeys, &retEdgeValues));
    EXPECT_EQ(0, retEdgeValues.size());
}


TEST(HBaseEngineTest, RangeTest) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 10L, dstId = 20L;
    EdgeType edgeType = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 0L;
    std::vector<std::string> edgeKeys;
    auto schemaMan = TestUtils::mockSchemaMan();

    // Generate edge prop.
    std::vector<KV> edgeData;
    auto edgeScheam = schemaMan->getEdgeSchema(spaceId, edgeType, edgeVersion);
    for (auto vertexId = srcId; vertexId < dstId; vertexId++) {
        auto edgeKey = KeyUtils::edgeKey(partId, vertexId, edgeType, rank, dstId, edgeVersion);
        edgeKeys.emplace_back(edgeKey);
        RowWriter edgeWriter(edgeScheam);
        for (int32_t iInt = 0; iInt < 10; iInt++) {
            edgeWriter << iInt;
        }
        for (int32_t iString = 10; iString < 20; iString++) {
            edgeWriter << folly::stringPrintf("string_col_%d", iString);
        }
        auto edgeValue = edgeWriter.encode();
        edgeData.emplace_back(edgeKey, edgeValue);
    }
    auto hbase_engine = std::make_unique<HBaseEngine>(
            spaceId, HostAddr(0, 9096), schemaMan.get());
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiPut(edgeData));

    auto checkRange = [&](VertexID start, VertexID end,
                          int32_t expectedFrom, int32_t expectedTotal) {
        LOG(INFO) << "start " << start << ", end " << end
                  << ", expectedFrom " << expectedFrom << ", expectedTotal " << expectedTotal;
        std::string s = KeyUtils::edgeKey(partId, start, edgeType, rank, dstId, edgeVersion);
        std::string e = KeyUtils::edgeKey(partId, end, edgeType, rank, dstId, edgeVersion);
        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->range(s, e, &iter));
        int num = 0;
        while (iter->valid()) {
            num++;
            std::string key(iter->key());
            EXPECT_EQ(edgeKeys[expectedFrom - srcId], key);
            std::string val(iter->val());
            EXPECT_EQ(edgeData[expectedFrom - srcId].second, val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(expectedTotal, num);
    };
    checkRange(10L, 20L, 10, 10);
    checkRange(1, 50, 10, 10);
    checkRange(15, 18, 15, 3);
    checkRange(15, 23, 15, 5);
    checkRange(1, 15, 10, 5);

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->removeRange(edgeKeys[0], edgeKeys[5]));
    std::vector<std::string> retEdgeValues;
    EXPECT_EQ(ResultCode::ERR_UNKNOWN, hbase_engine->multiGet(edgeKeys, &retEdgeValues));
    EXPECT_EQ(0, retEdgeValues.size());

    std::string start = KeyUtils::edgeKey(partId, srcId + 2, edgeType, rank, dstId, edgeVersion);
    std::string end = KeyUtils::edgeKey(partId, dstId, edgeType, rank, dstId, edgeVersion);
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->removeRange(start, end));
    retEdgeValues.clear();
    EXPECT_EQ(ResultCode::ERR_UNKNOWN, hbase_engine->multiGet(edgeKeys, &retEdgeValues));
    EXPECT_EQ(0, retEdgeValues.size());
}


TEST(HBaseEngineTest, PrefixTest) {
    LOG(INFO) << "Write data in batch and scan them...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 10L, dstId = 20L;
    EdgeType edgeType = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 0L;
    std::vector<std::string> edgeKeys;
    auto schemaMan = TestUtils::mockSchemaMan();

    // Generate edge prop.
    std::vector<KV> edgeData;
    auto edgeScheam = schemaMan->getEdgeSchema(spaceId, edgeType, edgeVersion);
    for (auto vertexId = srcId; vertexId < dstId; vertexId++) {
        auto edgeKey = KeyUtils::edgeKey(partId, srcId, edgeType, rank, vertexId, edgeVersion);
        edgeKeys.emplace_back(edgeKey);
        RowWriter edgeWriter(edgeScheam);
        for (int32_t iInt = 0; iInt < 10; iInt++) {
            edgeWriter << iInt;
        }
        for (int32_t iString = 10; iString < 20; iString++) {
            edgeWriter << folly::stringPrintf("string_col_%d", iString);
        }
        auto edgeValue = edgeWriter.encode();
        edgeData.emplace_back(edgeKey, edgeValue);
    }

    edgeScheam = schemaMan->getEdgeSchema(spaceId, edgeType + 1, edgeVersion);
    for (; edgeVersion < 10; edgeVersion++) {
        auto edgeKey = KeyUtils::edgeKey(partId, srcId, edgeType + 1, rank, dstId, edgeVersion);
        edgeKeys.emplace_back(edgeKey);
        RowWriter edgeWriter(edgeScheam);
        for (int32_t iInt = 0; iInt < 5; iInt++) {
            edgeWriter << iInt;
        }
        for (int32_t iString = 5; iString < 10; iString++) {
            edgeWriter << folly::stringPrintf("string_col_%d", iString);
        }
        auto edgeValue = edgeWriter.encode();
        edgeData.emplace_back(edgeKey, edgeValue);
    }
    auto hbase_engine = std::make_unique<HBaseEngine>(
            spaceId, HostAddr(0, 9096), schemaMan.get());
    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiPut(edgeData));

    auto checkPrefix = [&](const std::string& prefix,
                           int32_t expectedFrom, int32_t expectedTotal) {
        LOG(INFO) << "prefix " << prefix
                  << ", expectedFrom " << expectedFrom << ", expectedTotal " << expectedTotal;
        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->prefix(prefix, &iter));
        int num = 0;
        while (iter->valid()) {
            num++;
            std::string key(iter->key());
            EXPECT_EQ(edgeKeys[expectedFrom], key);
            std::string val(iter->val());
            EXPECT_EQ(edgeData[expectedFrom].second, val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(expectedTotal, num);
    };
    std::string prefix1 = KeyUtils::prefix(partId, srcId);
    checkPrefix(prefix1, 0, 20);
    std::string prefix2 = KeyUtils::prefix(partId, srcId, edgeType);
    checkPrefix(prefix2, 0, 10);
    std::string prefix3 = KeyUtils::prefix(partId, srcId, edgeType + 1, rank, dstId);
    checkPrefix(prefix3, 10, 10);

    EXPECT_EQ(ResultCode::SUCCEEDED, hbase_engine->multiRemove(edgeKeys));
    std::vector<std::string> retEdgeValues;
    EXPECT_EQ(ResultCode::ERR_UNKNOWN, hbase_engine->multiGet(edgeKeys, &retEdgeValues));
    EXPECT_EQ(0, retEdgeValues.size());
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

