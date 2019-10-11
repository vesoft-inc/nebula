/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/CompactionFilter.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
                RowWriter writer;
                for (uint64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
            // Generate 7 out-edges for each edgeType.
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                // Write multi versions,  we should get the latest version.
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, 101,
                                                       0, dstId,
                                                       std::numeric_limits<int>::max() - version);
                    RowWriter writer(nullptr);
                    for (uint64_t numInt = 0; numInt < 10; numInt++) {
                        writer << numInt;
                    }
                    for (auto numString = 10; numString < 20; numString++) {
                        writer << folly::stringPrintf("string_col_%d_%d", numString, version);
                    }
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }
            // Generate 5 in-edges for each edgeType, the edgeType is negative
            for (auto srcId = 20001; srcId <= 20005; srcId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", src " << srcId;
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, -101,
                                                       0, srcId,
                                                       std::numeric_limits<int>::max() - version);
                    data.emplace_back(std::move(key), "");
                }
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

TEST(NebulaCompactionFilterTest, InvalidSchemaAndMutliVersionsFilterTest) {
    fs::TempDir rootPath("/tmp/NebulaCompactionFilterTest.XXXXXX");
    auto schemaMan = TestUtils::mockSchemaMan();
    std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder(
                                    new StorageCompactionFilterFactoryBuilder(schemaMan.get()));
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path(),
                                                           6,
                                                           {0, 0},
                                                           nullptr,
                                                           false,
                                                           std::move(cffBuilder)));
    LOG(INFO) << "Write some data";
    mockData(kv.get());
    LOG(INFO) << "Let's delete one tag";
    auto* adhoc = static_cast<AdHocSchemaManager*>(schemaMan.get());
    adhoc->removeTagSchema(0, 3001);

    auto* ns = static_cast<kvstore::NebulaStore*>(kv.get());
    ns->compact(0);
    LOG(INFO) << "Finish compaction, check data...";

    auto checkTag = [&](PartitionID partId, VertexID vertexId, TagID tagId, int32_t expectedNum) {
        auto prefix = NebulaKeyUtils::vertexKey(partId,
                                                vertexId,
                                                tagId,
                                                0);
        std::unique_ptr<kvstore::KVIterator> iter;
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
        int32_t num = 0;
        while (iter->valid()) {
            iter->next();
            num++;
        }
        VLOG(3) << "Check tag " << partId << ":" << vertexId << ":" << tagId;
        ASSERT_EQ(expectedNum, num);
    };

    auto checkEdge = [&](PartitionID partId, VertexID vertexId, EdgeType edge,
                         EdgeRanking rank, VertexID dstId, int32_t expectedNum) {
        auto start = NebulaKeyUtils::edgeKey(partId,
                                             vertexId,
                                             edge,
                                             rank,
                                             dstId,
                                             0);
        auto end = NebulaKeyUtils::edgeKey(partId,
                                           vertexId,
                                           edge,
                                           rank,
                                           dstId,
                                           std::numeric_limits<int>::max());
        std::unique_ptr<kvstore::KVIterator> iter;
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, kv->range(0, partId, start, end, &iter));
        int32_t num = 0;
        while (iter->valid()) {
            iter->next();
            num++;
        }
        VLOG(3) << "Check edge " << partId << ":" << vertexId << ":"
                << edge << ":" << rank << ":" << dstId;
        ASSERT_EQ(expectedNum, num);
    };

    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            checkTag(partId, vertexId, 3001, 0);
            for (auto tagId = 3002; tagId < 3010; tagId++) {
                checkTag(partId, vertexId, tagId, 1);
            }
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                checkEdge(partId, vertexId, 101, 0, dstId, 1);
            }
            for (auto srcId = 20001; srcId <= 20005; srcId++) {
                checkEdge(partId, vertexId, -101, 0, srcId, 1);
            }
        }
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
