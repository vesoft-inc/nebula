/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/test/TestUtils.h"
#include "storage/query/ScanEdgeProcessor.h"
#include "codec/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv, int32_t partCount = 10) {
    // we have 10srcId * 10dstId * 10part = 1000 edges of one edgeType,
    // and 10vId * 9tagId * 10part = 900 tags
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
            for (EdgeType edgeType = 101; edgeType <= 103; edgeType++) {
                // Generate 100 edges for each source vertex id
                for (auto dstId = 10001; dstId <= 10100; dstId++) {
                    VLOG(3) << "Write part " << partId << ", vertex "
                        << vertexId << ", dst " << dstId;
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
                    VLOG(3) << "Write part " << partId << ", vertex "
                        << vertexId << ", src " << srcId;
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
}

cpp2::ScanEdgeRequest buildRequest(PartitionID partId, const std::string& cursor,
                                   int32_t rowLimit,
                                   bool returnAllColumns = false,
                                   bool returnNoneColumns = false,
                                   std::unordered_set<EdgeType> edgeTypes = {101},
                                   int64_t startTime = 0,
                                   int64_t endTime = std::numeric_limits<int64_t>::max()) {
    cpp2::ScanEdgeRequest req;
    req.set_space_id(0);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    std::unordered_map<EdgeType, std::vector<cpp2::PropDef>> returnColumns;
    for (EdgeType edgeType : edgeTypes) {
        std::vector<cpp2::PropDef> props;
        if (returnNoneColumns) {
            returnColumns.emplace(edgeType, std::move(props));
            continue;
        }

        if (!returnAllColumns) {
            // return the even int/string properties in zip order
            for (int i = 0; i < 10; i += 2) {
                {
                    cpp2::PropDef prop;
                    prop.owner = cpp2::PropOwner::EDGE;
                    prop.id.set_edge_type(edgeType);
                    prop.name = folly::stringPrintf("col_%d", i);
                    props.emplace_back(std::move(prop));
                }
                {
                    cpp2::PropDef prop;
                    prop.owner = cpp2::PropOwner::EDGE;
                    prop.id.set_edge_type(edgeType);
                    prop.name = folly::stringPrintf("col_%d", i + 10);
                    props.emplace_back(std::move(prop));
                }
            }
        } else {
            // if return all columns we don't need to specify any propery
        }
        returnColumns.emplace(edgeType, std::move(props));
    }
    req.set_return_columns(std::move(returnColumns));
    req.set_all_columns(returnAllColumns);
    req.set_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    return req;
}


void checkResponse(PartitionID partId, cpp2::ScanEdgeResponse& resp, std::string& cursor,
                   int32_t& totalRowCount, int32_t expectedRowNum, int32_t expectedColumnsNum,
                   bool returnAllColumns = false,
                   bool returnNoneColumns = false,
                   std::unordered_set<EdgeType> edgeTypes = {101}) {
    EXPECT_EQ(0, resp.result.failed_codes.size());
    for (const auto& schemaIter : resp.edge_schema) {
        EXPECT_EQ(expectedColumnsNum, schemaIter.second.columns.size());
    }

    if (!resp.edge_data.empty()) {
        int32_t rowNum = 0;
        for (const auto& scanEdge : resp.edge_data) {
            auto srcId = scanEdge.src;
            EXPECT_TRUE(partId * 10 <= srcId && srcId < (partId + 1) * 10);
            auto edgeType = scanEdge.type;
            EXPECT_TRUE(edgeTypes.count(edgeType));
            auto dstId = scanEdge.dst;
            EXPECT_TRUE(10001 <= dstId && dstId <= 10100);

            if (returnNoneColumns) {
                EXPECT_TRUE(scanEdge.value.empty());
            } else {
                auto schemaIter = resp.edge_schema.find(edgeType);
                EXPECT_TRUE(schemaIter != resp.edge_schema.end());
                auto provider = std::make_shared<ResultSchemaProvider>(schemaIter->second);
                auto reader = RowReaderWrapper::getRowReader(scanEdge.value, provider);

                if (!returnAllColumns) {
                    for (int64_t i = 0; i < 10; i += 2) {
                        int64_t value;
                        reader->getInt(i, value);
                        EXPECT_EQ(i, value);
                    }
                    for (auto i = 1; i < 10; i += 2) {
                        folly::StringPiece value;
                        reader->getString(i, value);
                        EXPECT_EQ(folly::stringPrintf("string_col_%d", i + 10 - 1), value);
                    }
                } else {
                    for (int64_t i = 0; i < 10; i += 1) {
                        int64_t value;
                        reader->getInt(i, value);
                        EXPECT_EQ(i, value);
                    }
                    for (auto i = 10; i < 20; i += 1) {
                        folly::StringPiece value;
                        reader->getString(i, value);
                        EXPECT_EQ(folly::stringPrintf("string_col_%d", i), value);
                    }
                }
            }
            rowNum++;
        }
        EXPECT_EQ(expectedRowNum, rowNum);
        totalRowCount += rowNum;
    }

    if (resp.has_next) {
        EXPECT_TRUE(!resp.next_cursor.empty());
        cursor = resp.next_cursor;
    } else {
        EXPECT_TRUE(resp.next_cursor.empty());
        cursor = "";
        return;
    }
}

TEST(ScanEdgeTest, RetrieveAllPropertyTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", 100, true);

    auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    std::string cursor = "";
    int32_t rowCount = 0, expectRowCount = 100;
    checkResponse(partId, resp, cursor, rowCount, expectRowCount, 20, true);
    EXPECT_EQ(rowCount, 100);
}

TEST(ScanEdgeTest, RetrieveSomePropertyTest) {
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
    checkResponse(partId, resp, cursor, rowCount, expectRowCount, 10);
    EXPECT_EQ(rowCount, 100);
}

TEST(ScanEdgeTest, RetrieveNonePropertyTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", 100, false, true);

    auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    std::string cursor = "";
    int32_t rowCount = 0, expectRowCount = 100;
    checkResponse(partId, resp, cursor, rowCount, expectRowCount, 0, false, true);
    EXPECT_EQ(rowCount, 100);
}

// Retrive edges of all property in a part
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
    auto req = buildRequest(partId, "", rowLimit, true);
    int32_t batchCount = 0;

    while (true) {
        auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ++batchCount;

        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 20, true);
        if (cursor.empty()) {
            break;
        }

        req = buildRequest(partId, cursor, rowLimit, true);
    }
    EXPECT_EQ(rowCount, 1000);
}

// Retrive edges of some property in a part
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

        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 10);
        if (cursor.empty()) {
            break;
        }
        req = buildRequest(partId, cursor, rowLimit);
    }
    EXPECT_EQ(rowCount, 1000);
}

// Retrive edges of none property in a part
TEST(ScanEdgeTest, RetrieveConsecutiveBlockTest3) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    // fetch 1000 rows one by one
    int32_t rowCount = 0, rowLimit = 1, expectRowCount = 1;
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", rowLimit, false, true);
    int32_t batchCount = 0;

    while (true) {
        auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ++batchCount;

        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 0, false, true);
        if (cursor.empty()) {
            break;
        }
        req = buildRequest(partId, cursor, rowLimit, false, true);
    }
    EXPECT_EQ(rowCount, 1000);
}

// Retrive multiple edges of all property in a part
TEST(ScanEdgeTest, RetrieveMultiEdgeTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    int32_t rowCount = 0, rowLimit = 100, expectRowCount = 100;
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", rowLimit, true, false, {101, 103});
    int32_t batchCount = 0;

    while (true) {
        auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ++batchCount;

        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 20, true, false,
                      {101, 103});
        if (cursor.empty()) {
            break;
        }

        req = buildRequest(partId, cursor, rowLimit, true, false, {101, 103});
    }
    EXPECT_EQ(rowCount, 2000);
}

TEST(ScanEdgeTest, RetrieveManyPartsTest) {
    fs::TempDir rootPath("/tmp/ScanEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    int32_t partCount = 10;
    mockData(kv.get(), partCount);

    // scan through all partition
    int32_t totalRowCount = 0;
    for (PartitionID partId = 0; partId  < partCount; partId++) {
        std::string cursor = "";
        int32_t rowCount = 0, rowLimit = 100, expectRowCount = 100;
        auto req = buildRequest(partId, "", rowLimit);

        while (true) {
            auto* processor = ScanEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            checkResponse(partId, resp, cursor, rowCount, expectRowCount, 10);
            if (cursor.empty()) {
                break;
            }

            req = buildRequest(partId, cursor, rowLimit);
        }
        EXPECT_EQ(rowCount, 1000);
        totalRowCount += rowCount;
    }
    EXPECT_EQ(totalRowCount, 10000);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
