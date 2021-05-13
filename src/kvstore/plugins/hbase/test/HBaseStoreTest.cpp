/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include "kvstore/plugins/hbase/HBaseStore.h"
#include "kvstore/plugins/hbase/test/TestUtils.h"
#include <gtest/gtest.h>

/**
 * TODO(zhangguoqing) Add a test runner to provide HBase/thrift2 service.
 * hbase/bin/hbase-daemon.sh start thrift2 -b 127.0.0.1 -p 9096
 * hbase(main):001:0> create 'Nebula_Graph_Space_0', 'cf'
 * */

namespace nebula {
namespace kvstore {

TEST(HBaseStoreTest, SimpleTest) {
    KVOptions options;
    auto schemaMan = TestUtils::mockSchemaMan();
    auto sm = schemaMan.get();
    CHECK_NOTNULL(sm);
    options.hbaseServer_ = HostAddr(0, 9096);
    options.schemaMan_ = schemaMan.get();
    auto hbaseStore = std::make_unique<HBaseStore>(std::move(options));
    hbaseStore->init();

    LOG(INFO) << "Put some data then read them...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 10L, dstId = 20L;
    EdgeType edgeType = 101;
    EdgeRanking rank = 10L;
    EdgeVerPlaceHolder edgeVersion = 1;
    std::vector<std::string> edgeKeys;
    std::vector<KV> edgeData;
    auto edgeScheam = sm->getEdgeSchema(spaceId, edgeType, edgeVersion);
    for (auto vertexId = srcId; vertexId < dstId; vertexId++) {
        auto edgeKey = NebulaKeyUtils::edgeKey(partId, srcId, edgeType,
                                               rank, vertexId);
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
        auto edgeKey = NebulaKeyUtils::edgeKey(partId, srcId, edgeType + 1,
                                               rank, dstId);
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

    hbaseStore->asyncMultiPut(spaceId, partId, edgeData, [](ResultCode code) {
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
    });

    std::vector<std::string> retEdgeValues;
    auto ret = hbaseStore->multiGet(spaceId, partId, edgeKeys, &retEdgeValues);
    EXPECT_EQ(ResultCode::SUCCEEDED, ret.first);
    EXPECT_EQ(20, retEdgeValues.size());

    auto checkPrefix = [&](const std::string& prefix,
                           int32_t expectedFrom,
                           int32_t expectedTotal) {
        LOG(INFO) << "prefix " << prefix << ", expectedFrom "
                  << expectedFrom << ", expectedTotal " << expectedTotal;
        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(ResultCode::SUCCEEDED, hbaseStore->prefix(spaceId, partId, prefix, &iter));
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
    std::string prefix1 = NebulaKeyUtils::vertexPrefix(partId, srcId);
    checkPrefix(prefix1, 0, 20);
    std::string prefix2 = NebulaKeyUtils::edgePrefix(partId, srcId, edgeType);
    checkPrefix(prefix2, 0, 10);
    std::string prefix3 = NebulaKeyUtils::prefix(partId, srcId, edgeType + 1, rank, dstId);
    checkPrefix(prefix3, 10, 10);

    hbaseStore->asyncRemovePrefix(spaceId, partId, prefix3, [](ResultCode code) {
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
    });

    hbaseStore->asyncMultiRemove(spaceId, partId, edgeKeys, [](ResultCode code) {
        EXPECT_EQ(ResultCode::SUCCEEDED, code);
    });

    retEdgeValues.clear();
    ret = hbaseStore->multiGet(spaceId, partId, edgeKeys, &retEdgeValues);
    EXPECT_EQ(ResultCode::E_UNKNOWN, ret.first);
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

