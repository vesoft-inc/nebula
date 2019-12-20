/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/query/QueryEdgePropsProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // Generate 7 edges for each source vertex id
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                auto key = NebulaKeyUtils::edgeKey(partId, vertexId, 101, dstId - 10001, dstId, 0);
                RowWriter writer(nullptr);
                for (int64_t numInt = 0; numInt < 10; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 10; numString < 20; numString++) {
                    writer << folly::stringPrintf("string_col_%d", numString);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(
            0, partId, std::move(data),
            [&](kvstore::ResultCode code) {
                EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
    }
}


void buildRequest(cpp2::EdgePropRequest& req) {
    req.set_space_id(0);
    decltype(req.parts) tmpEdges;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId =  partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                tmpEdges[partId].emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                              vertexId, 101, dstId - 10001, dstId);
            }
        }
    }
    req.set_parts(std::move(tmpEdges));
    req.set_edge_type(101);
    // Return edge props col_0, col_2, col_4 ... col_18
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 10; i++) {
        tmpColumns.emplace_back(TestUtils::edgePropDef(folly::stringPrintf("col_%d", i * 2), 101));
    }
    req.set_return_columns(std::move(tmpColumns));
}


void checkResponse(cpp2::EdgePropResponse& resp) {
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_EQ(14, resp.schema.columns.size());
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    LOG(INFO) << "Check edge props...";
    RowSetReader rsReader(provider, resp.data);
    auto it = rsReader.begin();
    int32_t rowNum = 0;
    while (static_cast<bool>(it)) {
        EXPECT_EQ(14, it->numFields());
       {
            // _src
            // We can't ensure the order, so just check the srcId range.
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getVid(0, v));
            CHECK_GE(30, v);
            CHECK_LE(0, v);
        }
        {
            // _rank
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getInt<int64_t>(1, v));
            CHECK_EQ(rowNum % 7, v);
        }
        {
            // _dst
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getVid(2, v));
            CHECK_EQ(10001 + rowNum % 7, v);
        }
        {
            // _dst
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getVid(3, v));
            CHECK_EQ(101, v);
        }
        // col_0, col_2 ... col_8
        for (auto i = 4; i < 9; i++) {
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getInt<int64_t>(i, v));
            CHECK_EQ((i - 4) * 2, v);
        }
        // col_10, col_12 ... col_18
        for (auto i = 9; i < 14; i++) {
            folly::StringPiece v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getString(i, v));
            CHECK_EQ(folly::stringPrintf("string_col_%d", (i - 9 + 5) * 2), v);
        }
        ++it;
        rowNum++;
    }
    EXPECT_EQ(it, rsReader.end());
    EXPECT_EQ(210, rowNum);
}


TEST(QueryEdgePropsTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryEdgePropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    LOG(INFO) << "Build EdgePropRequest...";
    cpp2::EdgePropRequest req;
    buildRequest(req);

    LOG(INFO) << "Test QueryEdgePropsRequest...";
    auto* processor = QueryEdgePropsProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    checkResponse(resp);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
