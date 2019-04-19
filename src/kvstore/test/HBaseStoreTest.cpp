/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/test/TestUtils.h"
#include "kvstore/HBaseStore.h"
#include "kvstore/HBaseEngine.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "storage/KeyUtils.h"

/**
 * TODO(zhangguoqing) Add a test runner to provide HBase/thrift2 service.
 * hbase/bin/hbase-daemon.sh start thrift2 -b 127.0.0.1 -p 9096
 * hbase(main):001:0> create 'Nebula_Graph_Space_0', 'cf'
 * */

namespace nebula {
namespace kvstore {

using nebula::storage::KeyUtils;

TEST(HBaseStoreTest, SimpleTest) {
    auto partMan = std::make_unique<MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    // 0 => {0, 1, 2, 3, 4, 5}
    // 1 => {0, 1, 2, 3, 4, 5}
    // 2 => {0, 1, 2, 3, 4, 5}
    for (auto spaceId = 0; spaceId < 3; spaceId++) {
        for (auto partId = 0; partId < 6; partId++) {
            partMan->partsMap_[spaceId][partId] = PartMeta();
        }
    }
    LOG(INFO) << "Total space num " << partMan->partsMap_.size()
              << ", " << partMan->parts(HostAddr(0, 0)).size();
    auto schemaMan = TestUtils::mockSchemaMan();
    auto sm = schemaMan.get();
    CHECK_NOTNULL(sm);
    std::unique_ptr<HBaseStore> store;
    KVOptions options;
    options.hbaseServer_ = HostAddr(0, 9096);
    options.schemaMan_ = std::move(schemaMan);
    options.local_ = HostAddr(0, 0);
    options.partMan_ = std::move(partMan);

    store.reset(static_cast<HBaseStore*>(KVStore::instance(std::move(options), "hbase")));
    EXPECT_EQ(3, store->kvs_.size());
    EXPECT_EQ(6, store->kvs_[0]->parts_.size());
    EXPECT_EQ(6, store->kvs_[1]->parts_.size());
    EXPECT_EQ(6, store->kvs_[2]->parts_.size());

    store->asyncMultiPut(3, 0, {{"key", "val"}}, [](ResultCode code, HostAddr addr) {
        UNUSED(addr);
        EXPECT_EQ(ResultCode::ERR_SPACE_NOT_FOUND, code);
    });
    store->asyncMultiPut(1, 6, {{"key", "val"}}, [](ResultCode code, HostAddr addr) {
        UNUSED(addr);
        EXPECT_EQ(ResultCode::ERR_PART_NOT_FOUND, code);
    });

    LOG(INFO) << "Put some data then read them...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 10L, dstId = 20L;
    EdgeType edgeType = 101;
    EdgeRanking rank = 10L;
    EdgeVersion edgeVersion = 0L;
    std::vector<std::string> edgeKeys;
    std::vector<KV> edgeData;
    auto edgeScheam = sm->getEdgeSchema(spaceId, edgeType, edgeVersion);
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

    edgeScheam = sm->getEdgeSchema(spaceId, edgeType + 1, edgeVersion);
    for (; edgeVersion < 10L; edgeVersion++) {
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

    store->asyncMultiPut(spaceId, partId, edgeData, [](ResultCode code, HostAddr addr){
        UNUSED(addr);
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
    });

    auto checkPrefix = [&](const std::string& prefix,
                           int32_t expectedFrom, int32_t expectedTotal) {
        LOG(INFO) << "prefix " << prefix
                  << ", expectedFrom " << expectedFrom << ", expectedTotal " << expectedTotal;
        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(ResultCode::SUCCEEDED, store->prefix(spaceId, partId, prefix, &iter));
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

    store->asyncMultiRemove(spaceId, partId, edgeKeys, [](ResultCode code, HostAddr addr){
        UNUSED(addr);
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
    });
    std::vector<std::string> retEdgeValues;
    EXPECT_EQ(ResultCode::ERR_UNKNOWN, store->multiGet(spaceId, partId, edgeKeys, &retEdgeValues));
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


