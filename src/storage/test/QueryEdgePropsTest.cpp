/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/QueryEdgePropsProcessor.h"
#include "storage/KeyUtils.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"
#include "meta/AdHocSchemaManager.h"

namespace nebula {
namespace storage {

TEST(QueryEdgePropsTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryEdgePropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    LOG(INFO) << "Prepare meta...";
    auto edgeSchema = TestUtils::genEdgeSchemaProvider(10, 10);
    meta::AdHocSchemaManager::addEdgeSchema(0, 101, edgeSchema);

    LOG(INFO) << "Prepare data...";
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // Generate 7 edges for each source vertex id
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId
                        << ", vertex " << vertexId
                        << ", dst " << dstId;
                auto key = KeyUtils::edgeKey(partId, vertexId, 101, dstId, dstId - 10001, 0);
                RowWriter writer(edgeSchema);
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
        kv->asyncMultiPut(
            0, partId, std::move(data),
            [&](kvstore::ResultCode code, HostAddr addr) {
                EXPECT_EQ(code, kvstore::ResultCode::SUCCESSED);
                UNUSED(addr);
            });
    }

    LOG(INFO) << "Build EdgePropRequest...";
    cpp2::EdgePropRequest req;
    req.set_space_id(0);
    decltype(req.parts) tmpEdges;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId =  partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                tmpEdges[partId].emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                              vertexId, 101, dstId, dstId - 10001);
            }
        }
    }
    req.set_parts(std::move(tmpEdges));
    req.set_edge_type(101);
    // Return edge props col_0, col_2, col_4 ... col_18
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 10; i++) {
        tmpColumns.emplace_back(TestUtils::propDef(cpp2::PropOwner::EDGE,
                                                   folly::stringPrintf("col_%d", i*2)));
    }
    req.set_return_columns(std::move(tmpColumns));
    LOG(INFO) << "Test QueryEdgePropsRequest...";
    auto* processor = QueryEdgePropsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_codes.size());

    EXPECT_EQ(10, resp.schema.columns.size());
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    LOG(INFO) << "Check edge props...";
    RowSetReader rsReader(provider, resp.data);
    auto it = rsReader.begin();
    int32_t rowNum = 0;
    while (static_cast<bool>(it)) {
        auto fieldIt = it->begin();
        int32_t i = 0;
        std::stringstream ss;
        while (static_cast<bool>(fieldIt)) {
            if (i < 5) {
                int64_t v;
                EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getInt<int64_t>(v));
                EXPECT_EQ(i*2, v);
                ss << v << ",";
            } else {
                folly::StringPiece v;
                EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getString(v));
                EXPECT_EQ(folly::stringPrintf("string_col_%d", i*2), v);
                ss << v << ",";
            }
            i++;
            ++fieldIt;
        }
        VLOG(3) << ss.str();
        EXPECT_EQ(fieldIt, it->end());
        EXPECT_EQ(10, i);
        ++it;
        rowNum++;
    }
    EXPECT_EQ(it, rsReader.end());
    EXPECT_EQ(210, rowNum);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


