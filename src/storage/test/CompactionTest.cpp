/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "storage/test/TestUtils.h"
#include "storage/CommonUtils.h"
#include "storage/CompactionFilter.h"
#include "codec/RowWriterV2.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    for (PartitionID partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (VertexID vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (TagID tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
                auto val = TestUtils::encodeValue(partId, vertexId, tagId);
                data.emplace_back(std::move(key), std::move(val));
            }

            // Generate 7 out-edges for each edgeType.
            for (VertexID dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                for (EdgeVersion version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, 101, 0, dstId,
                                                       std::numeric_limits<int>::max() - version);
                    auto val = TestUtils::encodeValue(partId, vertexId, dstId, 101);
                    data.emplace_back(std::move(key), std::move(val));
                }
            }

            // Generate 5 in-edges for each edgeType, the edgeType is negative
            for (VertexID srcId = 20001; srcId <= 20005; srcId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", src " << srcId;
                for (EdgeVersion version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, -101, 0, srcId,
                                                       std::numeric_limits<int>::max() - version);
                    auto val = TestUtils::setupEncode(10, 20);
                    data.emplace_back(std::move(key), std::move(val));
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

void mockTTLDataExpired(kvstore::KVStore* kv) {
    for (PartitionID partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // one tag data, the record data will always expire
            auto tagId = 3001;
            auto tagkey = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
            RowWriter tagwriter;
            for (int64_t numInt = 0; numInt < 3; numInt++) {
                // all data expired, 1546272000 timestamp representation "2019-1-1 0:0:0"
                // With TTL, the record data will always expire
                tagwriter << numInt + 1546272000;
            }
            for (auto numString = 3; numString < 6; numString++) {
                tagwriter << folly::stringPrintf("string_col_%d", numString);
            }
            auto tagval = tagwriter.encode();
            data.emplace_back(std::move(tagkey), std::move(tagval));

            // one edge data, the record data will always expire
            auto edgeType = 101;
            auto edgekey = NebulaKeyUtils::edgeKey(partId, vertexId, edgeType, 0, 10001,
                                                   std::numeric_limits<int>::max() - 1);
            RowWriter edgewriter;
            for (int64_t numInt = 0; numInt < 10; numInt++) {
                // all data expired, 1546272000 timestamp representation "2019-1-1 0:0:0"
                // With TTL, the record data will always expire
                edgewriter << numInt + 1546272000;
            }
            for (auto numString = 10; numString < 20; numString++) {
                edgewriter << folly::stringPrintf("string_col_%d", numString);
            }
            auto edgeval = edgewriter.encode();
            data.emplace_back(std::move(edgekey), std::move(edgeval));
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

void mockTTLDataNotExpired(kvstore::KVStore* kv) {
    auto tagId = 3001;
    auto edgeType = 101;
    for (PartitionID partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // one tag data, the record data will never expire
            auto tagkey = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
            RowWriter tagwriter;
            for (int64_t numInt = 0; numInt < 3; numInt++) {
                // all data expired, 4102416000 timestamp representation "2100-1-1 0:0:0"
                // With TTL, the record data will never expire
                tagwriter << numInt + 4102416000;
            }
            for (auto numString = 3; numString < 6; numString++) {
                tagwriter << folly::stringPrintf("string_col_%d", numString);
            }
            auto tagval = tagwriter.encode();
            data.emplace_back(std::move(tagkey), std::move(tagval));

            // one edge data, the record data will never expire
            auto edgekey = NebulaKeyUtils::edgeKey(partId, vertexId, edgeType, 0, 10001,
                                                   std::numeric_limits<int>::max() - 1);
            RowWriter edgewriter;
            for (int64_t numInt = 0; numInt < 10; numInt++) {
                // all data expired, 4102416000 timestamp representation "2100-1-1 0:0:0"
                // With TTL, the record data will never expire
                edgewriter << numInt + 4102416000;
            }
            for (auto numString = 10; numString < 20; numString++) {
                edgewriter << folly::stringPrintf("string_col_%d", numString);
            }
            auto edgeval = edgewriter.encode();
            data.emplace_back(std::move(edgekey), std::move(edgeval));
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

void mockIndexData(kvstore::KVStore* kv) {
    auto tagId = 3001;
    auto edgeType = 101;
    IndexValues values;
    values.emplace_back(nebula::cpp2::SupportedType::STRING, "col1");
    for (PartitionID partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            auto tiKey = NebulaKeyUtils::vertexIndexKey(partId, tagId + 1000, vertexId, values);
            data.emplace_back(std::move(tiKey), "");
            /**
             * pseudo vertex index
             */
            auto ptiKey = NebulaKeyUtils::vertexIndexKey(partId, tagId + 1001, vertexId, values);
            data.emplace_back(std::move(ptiKey), "");

            auto eiKey = NebulaKeyUtils::edgeIndexKey(partId, edgeType + 100,
                                                      vertexId, 0, 10001, values);

            data.emplace_back(std::move(eiKey), "");
            /**
             * pseudo edge index
             */
            auto peiKey = NebulaKeyUtils::edgeIndexKey(partId, edgeType + 101,
                                                       vertexId, 0, 10001, values);

            data.emplace_back(std::move(peiKey), "");
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
                                    new StorageCompactionFilterFactoryBuilder(schemaMan.get(),
                                                                              nullptr));
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path(),
                                         6,
                                         {0, network::NetworkUtils::getAvailablePort()},
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

    for (PartitionID partId = 0; partId < 3; partId++) {
        for (VertexID vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            checkTag(partId, vertexId, 3001, 0);
            for (TagID tagId = 3002; tagId < 3010; tagId++) {
                checkTag(partId, vertexId, tagId, 1);
            }
            for (VertexID dstId = 10001; dstId <= 10007; dstId++) {
                checkEdge(partId, vertexId, 101, 0, dstId, 1);
            }
            for (VertexID srcId = 20001; srcId <= 20005; srcId++) {
                checkEdge(partId, vertexId, -101, 0, srcId, 1);
            }
        }
    }
}


TEST(NebulaCompactionFilterTest, TTLFilterDataExpiredTest) {
    fs::TempDir rootPath("/tmp/NebulaCompactionFilterTest.XXXXXX");
    auto schemaMan = TestUtils::mockSchemaWithTTLMan();
    std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder(
                                    new StorageCompactionFilterFactoryBuilder(schemaMan.get(),
                                                                              nullptr));
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path(),
                                         6,
                                         {0, network::NetworkUtils::getAvailablePort()},
                                         nullptr,
                                         false,
                                         std::move(cffBuilder)));
    LOG(INFO) << "Write some data";
    mockTTLDataExpired(kv.get());

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

    for (PartitionID partId = 0; partId < 3; partId++) {
        for (VertexID vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            checkTag(partId, vertexId, 3001, 0);
            checkEdge(partId, vertexId, 101, 0, 10001, 0);
        }
    }
}


TEST(NebulaCompactionFilterTest, TTLFilterDataNotExpiredTest) {
    fs::TempDir rootPath("/tmp/NebulaCompactionFilterTest.XXXXXX");
    auto schemaMan = TestUtils::mockSchemaWithTTLMan();
    std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder(
                                    new StorageCompactionFilterFactoryBuilder(schemaMan.get(),
                                                                              nullptr));
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path(),
                                         6,
                                         {0, network::NetworkUtils::getAvailablePort()},
                                         nullptr,
                                         false,
                                         std::move(cffBuilder)));
    LOG(INFO) << "Write some data";
    mockTTLDataNotExpired(kv.get());

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

    for (PartitionID partId = 0; partId < 3; partId++) {
        for (VertexID vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            checkTag(partId, vertexId, 3001, 1);
            checkEdge(partId, vertexId, 101, 0, 10001, 1);
        }
    }
}

TEST(NebulaCompactionFilterTest, DropIndexTest) {
    fs::TempDir rootPath("/tmp/DropIndexTest.XXXXXX");
    GraphSpaceID spaceId = 0;
    auto schemaMan = TestUtils::mockSchemaMan(spaceId);
    auto indexMan = TestUtils::mockIndexMan(spaceId, 3001, 3002, 101, 102);

    std::unique_ptr<kvstore::CompactionFilterFactoryBuilder> cffBuilder(
        new StorageCompactionFilterFactoryBuilder(schemaMan.get(),
                                                  indexMan.get()));
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path(),
                                         6,
                                         {0, network::NetworkUtils::getAvailablePort()},
                                         nullptr,
                                         false,
                                         std::move(cffBuilder)));
    LOG(INFO) << "Write some data";
    mockIndexData(kv.get());

    auto* ns = dynamic_cast<kvstore::NebulaStore*>(kv.get());
    ns->compact(spaceId);
    LOG(INFO) << "Finish compaction, check index data...";

    auto checkIndex = [&](PartitionID partId, IndexID indexId, int32_t expectedNum) {
        auto prefix = NebulaKeyUtils::indexPrefix(partId, indexId);
        std::unique_ptr<kvstore::KVIterator> iter;
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
        int32_t num = 0;
        while (iter->valid()) {
            iter->next();
            num++;
        }
        LOG(INFO) << "Check index " << partId << ":" << indexId;
        ASSERT_EQ(expectedNum, num);
    };

    for (PartitionID partId = 0; partId < 3; partId++) {
        /**
         * index exists
         */
        checkIndex(partId, 3001 + 1000, 10);
        checkIndex(partId, 101 + 100, 10);
        /**
         * index dropped
         */
        checkIndex(partId, 3001 + 1001, 0);
        checkIndex(partId, 101 + 101, 0);
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
