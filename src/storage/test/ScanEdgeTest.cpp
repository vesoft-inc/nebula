/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
#include "storage/query/ScanEdgeProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv, int32_t partCount = 10) {
    EdgeType edgeType = 101;
    for (auto partId = 0; partId < partCount; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // Generate 9 tag for each vertex
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
                RowWriter writer;
                for (uint64_t numInt = 0; numInt < 3; numInt++) {
                    writer << (vertexId + tagId + numInt);
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
            // Generate 100 edges for each source vertex id
            for (auto dstId = 10001; dstId <= 10100; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                int64_t ts = std::numeric_limits<int64_t>::max() -
                                  time::WallClock::fastNowInMicroSec();
                int64_t version = folly::Endian::big(ts);
                auto key = NebulaKeyUtils::edgeKey(partId, vertexId, edgeType, dstId - 10001,
                                                   dstId, version);
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
            // Generate 5 in-edges for each edgeType, the edgeType is negative
            for (auto srcId = 20001; srcId <= 20005; srcId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", src " << srcId;
                auto key = NebulaKeyUtils::edgeKey(partId, vertexId, -edgeType, 0, srcId, 0);
                data.emplace_back(std::move(key), "");
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


cpp2::ScanEdgeRequest buildRequest(PartitionID partId, const std::string& cursor,
                                   int32_t rowLimit, int64_t vertexId = -1,
                                   int64_t startTime = 0,
                                   int64_t endTime = std::numeric_limits<int64_t>::max()) {
    cpp2::ScanEdgeRequest req;
    req.set_space_id(0);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    if (vertexId != -1) {
        req.set_vertex_id(vertexId);
    }
    req.set_row_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    return req;
}


void checkResponse(PartitionID partId, cpp2::ScanEdgeResponse& resp, std::string& cursor,
                   int32_t& rowCount, int32_t expected, VertexID vertexId = -1) {
    EXPECT_EQ(0, resp.result.failed_codes.size());
    if (!resp.edge_schema.empty()) {
        EXPECT_EQ(1, resp.edge_schema.size());
        auto schema = resp.edge_schema.begin()->second;
        EXPECT_EQ(20, schema.columns.size());
        auto provider = std::make_shared<ResultSchemaProvider>(schema);
        int32_t rowNum = 0;
        for (const auto& row : resp.edge_data) {
            EXPECT_EQ(101, row.type);
            auto edgeKey = row.key;
            auto srcId = NebulaKeyUtils::getSrcId(edgeKey);
            if (vertexId != -1) {
                EXPECT_EQ(vertexId, srcId);
            }
            EXPECT_TRUE(partId * 10 <= srcId && srcId < (partId + 1) * 10);
            auto dstId = NebulaKeyUtils::getDstId(edgeKey);
            EXPECT_TRUE(10001 <= dstId && dstId <= 10100);
            auto edgeType = NebulaKeyUtils::getEdgeType(edgeKey);
            EXPECT_EQ(101, edgeType);
            auto rank = NebulaKeyUtils::getRank(edgeKey);
            EXPECT_EQ(rank, dstId - 10001);
            auto reader = RowReader::getRowReader(row.value, provider);
            for (int64_t i = 0; i < 10; i++) {
                int64_t value;
                reader->getInt(i, value);
                EXPECT_EQ(i, value);
            }
            for (auto i = 10; i < 20; i++) {
                folly::StringPiece value;
                reader->getString(i, value);
                EXPECT_EQ(folly::stringPrintf("string_col_%d", i), value);
            }
            rowNum++;
        }
        EXPECT_EQ(expected, rowNum);
        rowCount += rowNum;
    }
    cursor = resp.next_cursor;
}

TEST(ScanEdgeTest, RetrieveFirstBlockTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", 100);

    auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    std::string cursor = "";
    int32_t rowCount = 0, expectRowCount = 100;
    checkResponse(partId, resp, cursor, rowCount, expectRowCount);
}

TEST(ScanEdgeTest, RetrieveConsecutiveBlockTest1) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    int32_t rowCount = 0, rowLimit = 100, expectRowCount = 100;
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", rowLimit);

    while (true) {
        auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        checkResponse(partId, resp, cursor, rowCount, expectRowCount);
        if (cursor.empty()) {
            break;
        }

        req = buildRequest(partId, cursor, rowLimit);
    }
    EXPECT_EQ(rowCount, 1000);
}

TEST(ScanEdgeTest, RetrieveConsecutiveBlockTest2) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    // fetch 1000 rows in total within 11 blocks, first 10 blocks will get 99 rows,
    // last block will get 10 row, 99 * 10 + 10 = 1000
    int32_t rowCount = 0, rowLimit = 99, expectRowCount = 99;
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", rowLimit);
    int32_t batchCount = 0;

    while (true) {
        auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ++batchCount;
        if (batchCount == 11) {
            expectRowCount = 10;
        }

        checkResponse(partId, resp, cursor, rowCount, expectRowCount);
        if (cursor.empty()) {
            break;
        }
        req = buildRequest(partId, cursor, rowLimit);
    }
    EXPECT_EQ(rowCount, 1000);
}

TEST(ScanEdgeTest, RetrieveConsecutiveBlockTest3) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    // scan through all partition
    for (PartitionID partId = 0; partId  < 10; partId++) {
        std::string cursor = "";
        // fetch 1000 rows in total within 11 blocks, first 10 blocks will get 99 rows,
        // last block will get 10 row, 99 * 10 + 10 = 1000
        int32_t rowCount = 0, rowLimit = 99, expectRowCount = 99;
        auto req = buildRequest(partId, "", rowLimit);
        int32_t batchCount = 0;

        while (true) {
            auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ++batchCount;
            if (batchCount == 11) {
                expectRowCount = 10;
            }

            checkResponse(partId, resp, cursor, rowCount, expectRowCount);
            if (cursor.empty()) {
                break;
            }
            req = buildRequest(partId, cursor, rowLimit);
        }
        EXPECT_EQ(rowCount, 1000);
    }
}

TEST(ScanEdgeTest, RetrieveManyPartsTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 100);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    int32_t partCount = 100;
    mockData(kv.get(), partCount);

    // scan through all partition
    for (PartitionID partId = 0; partId  < partCount; partId++) {
        std::string cursor = "";
        int32_t rowCount = 0, rowLimit = 100, expectRowCount = 100;
        auto req = buildRequest(partId, "", rowLimit);

        while (true) {
            auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            checkResponse(partId, resp, cursor, rowCount, expectRowCount);
            if (cursor.empty()) {
                break;
            }

            req = buildRequest(partId, cursor, rowLimit);
        }
        EXPECT_EQ(rowCount, 1000);
    }
}

TEST(ScanEdgeTest, RetrieveSingleVertexTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    int32_t rowCount = 0, rowLimit = 50, expectRowCount = 50;
    PartitionID partId = 1;
    VertexID vertexId = 18;
    auto req = buildRequest(partId, "", rowLimit, vertexId);

    while (true) {
        auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        checkResponse(partId, resp, cursor, rowCount, expectRowCount, vertexId);
        if (cursor.empty()) {
            break;
        }

        req = buildRequest(partId, cursor, rowLimit, vertexId);
    }
    EXPECT_EQ(rowCount, 100);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
