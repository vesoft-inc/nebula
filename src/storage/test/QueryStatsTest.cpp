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
#include "storage/query/QueryStatsProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
                RowWriter writer;
                for (int64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
            // Generate 7 edges for each edgeType.
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
        kv->asyncMultiPut(0, partId, std::move(data), [&](kvstore::ResultCode code) {
            EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
        baton.wait();
    }
}


void buildRequest(cpp2::GetNeighborsRequest& req) {
    req.set_space_id(0);
    decltype(req.parts) tmpIds;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId =  partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            tmpIds[partId].emplace_back(vertexId);
        }
    }
    req.set_parts(std::move(tmpIds));
    std::vector<EdgeType> et = {101};
    req.set_edge_types(et);
    // Return tag props col_0, col_2, col_4
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 2; i++) {
        tmpColumns.emplace_back(
            TestUtils::vertexPropDef(folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2),
                                    cpp2::StatType::AVG, 3001 + i * 2));
    }
    // Return edge props col_0, col_2, col_4 ... col_18
    for (int i = 0; i < 5; i++) {
        tmpColumns.emplace_back(
            TestUtils::edgePropDef(folly::stringPrintf("col_%d", i * 2), cpp2::StatType::SUM, 101));
    }
    req.set_return_columns(std::move(tmpColumns));
}


void checkResponse(const cpp2::QueryStatsResponse& resp) {
    EXPECT_EQ(0, resp.result.failed_codes.size());

    EXPECT_EQ(7, resp.schema.columns.size());
    CHECK_GT(resp.data.size(), 0);
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    LOG(INFO) << "Check edge props...";

    std::vector<std::tuple<std::string, nebula::cpp2::SupportedType, int32_t>> expected;
    expected.emplace_back("tag_3001_col_0", nebula::cpp2::SupportedType::DOUBLE, 0);
    expected.emplace_back("tag_3003_col_2", nebula::cpp2::SupportedType::DOUBLE, 2);
    expected.emplace_back("col_0", nebula::cpp2::SupportedType::INT, 0);
    expected.emplace_back("col_2", nebula::cpp2::SupportedType::INT, 2);
    expected.emplace_back("col_4", nebula::cpp2::SupportedType::INT, 4);
    expected.emplace_back("col_6", nebula::cpp2::SupportedType::INT, 6);
    expected.emplace_back("col_8", nebula::cpp2::SupportedType::INT, 8);

    auto reader = RowReader::getRowReader(resp.data, provider);
    auto numFields = provider->getNumFields();
    for (size_t i = 0; i < numFields; i++) {
        const auto* name = provider->getFieldName(i);
        const auto& ftype = provider->getFieldType(i);
        EXPECT_EQ(name,  std::get<0>(expected[i]));
        EXPECT_TRUE(ftype.type == std::get<1>(expected[i]));
        switch (ftype.type) {
            case nebula::cpp2::SupportedType::INT: {
                int64_t v;
                auto ret = reader->getInt<int64_t>(i, v);
                EXPECT_EQ(ret, ResultType::SUCCEEDED);
                EXPECT_EQ(std::get<2>(expected[i]) * 210 , v);
                break;
            }
            case nebula::cpp2::SupportedType::DOUBLE: {
                float v;
                auto ret = reader->getFloat(i, v);
                EXPECT_EQ(ret, ResultType::SUCCEEDED);
                EXPECT_EQ(std::get<2>(expected[i]) , v);
                break;
            }
            default: {
                LOG(FATAL) << "Should not reach here!";
                break;
            }
        }
    }
}


TEST(QueryStatsTest, StatsSimpleTest) {
    fs::TempDir rootPath("/tmp/QueryStatsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    cpp2::GetNeighborsRequest req;
    buildRequest(req);

    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
    auto* processor = QueryStatsProcessor::instance(kv.get(), schemaMan.get(), nullptr,
                                                    executor.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

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
