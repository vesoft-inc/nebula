/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/query/ScanVertexProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

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
                int64_t ts = std::numeric_limits<int64_t>::max() -
                                  time::WallClock::fastNowInMicroSec();
                int64_t version = folly::Endian::big(ts);
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
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


cpp2::ScanVertexRequest buildRequest(PartitionID partId, const std::string& cursor,
                                     int32_t rowLimit,
                                     bool returnAllColumns = false,
                                     bool returnNoneColumns = false,
                                     std::unordered_set<TagID> tagIds = {3001},
                                     int64_t startTime = 0,
                                     int64_t endTime = std::numeric_limits<int64_t>::max()) {
    cpp2::ScanVertexRequest req;
    req.set_space_id(0);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    std::unordered_map<EdgeType, std::vector<cpp2::PropDef>> returnColumns;
    for (TagID tagId : tagIds) {
        std::vector<cpp2::PropDef> props;
        if (returnNoneColumns) {
            returnColumns.emplace(tagId, std::move(props));
            continue;
        }

        if (!returnAllColumns) {
            // return col_4 and col_1
            {
                cpp2::PropDef prop;
                prop.owner = cpp2::PropOwner::SOURCE;
                prop.id.set_tag_id(tagId);
                prop.name = folly::stringPrintf("tag_%d_col_%d", tagId, 4);
                props.emplace_back(std::move(prop));
            }
            {
                cpp2::PropDef prop;
                prop.owner = cpp2::PropOwner::SOURCE;
                prop.id.set_tag_id(tagId);
                prop.name = folly::stringPrintf("tag_%d_col_%d", tagId, 1);
                props.emplace_back(std::move(prop));
            }
        } else {
            // if return all columns we don't need to specify any propery
        }
        returnColumns.emplace(tagId, std::move(props));
    }
    req.set_return_columns(std::move(returnColumns));
    req.set_all_columns(returnAllColumns);
    req.set_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    return req;
}


void checkResponse(PartitionID partId, cpp2::ScanVertexResponse& resp, std::string& cursor,
                   int32_t& totalRowCount, int32_t expectedRowNum, int32_t expectedColumnsNum,
                   bool returnAllColumns = false,
                   bool returnNoneColumns = false,
                   std::unordered_set<TagID> tagIds = {3001}) {
    EXPECT_EQ(0, resp.result.failed_codes.size());
    for (const auto& schemaIter : resp.vertex_schema) {
        EXPECT_EQ(expectedColumnsNum, schemaIter.second.columns.size());
    }

    if (!resp.vertex_data.empty()) {
        int32_t rowNum = 0;
        for (const auto& scanVertex : resp.vertex_data) {
            auto vertexId = scanVertex.vertexId;
            EXPECT_TRUE(partId * 10 <= vertexId && vertexId < (partId + 1) * 10);
            auto tagId = scanVertex.tagId;
            EXPECT_TRUE(tagIds.count(tagId));

            if (returnNoneColumns) {
                EXPECT_TRUE(scanVertex.value.empty());
            } else {
                auto schemaIter = resp.vertex_schema.find(tagId);
                EXPECT_TRUE(schemaIter != resp.vertex_schema.end());
                auto provider = std::make_shared<ResultSchemaProvider>(schemaIter->second);
                auto reader = RowReader::getRowReader(scanVertex.value, provider);

                if (!returnAllColumns) {
                    {
                        folly::StringPiece value;
                        reader->getString(0, value);
                        EXPECT_EQ(folly::stringPrintf("tag_string_col_%d", 4), value);
                    }
                    {
                        int64_t value;
                        reader->getInt(1, value);
                        EXPECT_EQ(vertexId + tagId + 1, value);
                    }
                } else {
                    for (uint64_t i = 0; i < 3; i++) {
                        int64_t value;
                        reader->getInt(i, value);
                        EXPECT_EQ(vertexId + tagId + i, value);
                    }
                    for (auto i = 3; i < 6; i++) {
                        folly::StringPiece value;
                        reader->getString(i, value);
                        EXPECT_EQ(folly::stringPrintf("tag_string_col_%d", i), value);
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

// Retrieve all tags 3001 with all property
TEST(ScanVertexTest, RetrieveAllPropertyTest) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", 10, true);

    auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    std::string cursor = "";
    int32_t rowCount = 0, expectRowCount = 10;
    checkResponse(partId, resp, cursor, rowCount, expectRowCount, 6, true);
    EXPECT_EQ(rowCount, 10);
}

// Retrieve all tags 3001 with some property
TEST(ScanVertexTest, RetrieveSomePropertyTest) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", 10);

    auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    std::string cursor = "";
    int32_t rowCount = 0, expectRowCount = 10;
    checkResponse(partId, resp, cursor, rowCount, expectRowCount, 2);
    EXPECT_EQ(rowCount, 10);
}

// Retrieve all tags 3001 with none property
TEST(ScanVertexTest, RetrieveNonePropertyTest) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    PartitionID partId = 1;
    auto req = buildRequest(partId, "", 10, false, true);

    auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    std::string cursor = "";
    int32_t rowCount = 0, expectRowCount = 10;
    checkResponse(partId, resp, cursor, rowCount, expectRowCount, 0, false, true);
    EXPECT_EQ(rowCount, 10);
}

// Retrive vertices of all property in a part
TEST(ScanVertexTest, RetrieveConsecutiveBlockTest1) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    int32_t rowCount = 0, rowLimit = 10, expectRowCount = 10;
    PartitionID partId = 1;
    std::unordered_set<TagID> tagIds;
    for (TagID tagId = 3001; tagId < 3010; tagId++) {
        tagIds.emplace(tagId);
    }
    auto req = buildRequest(partId, "", rowLimit, true, false, tagIds);

    while (true) {
        auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 6, true, false, tagIds);
        if (cursor.empty()) {
            break;
        }

        req = buildRequest(partId, cursor, rowLimit, true, false, tagIds);
    }
    EXPECT_EQ(rowCount, 90);
}

// Retrive vertices of some property in a part
TEST(ScanVertexTest, RetrieveConsecutiveBlockTest2) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    // fetch 90 rows in total within 9 blocks, first 8 blocks will get 8 * 11 = 88 rows,
    // last block will get 2 row, 11 * 8 + 2 = 90
    int32_t rowCount = 0, rowLimit = 11, expectRowCount = 11;
    PartitionID partId = 1;
    std::unordered_set<TagID> tagIds;
    for (TagID tagId = 3001; tagId < 3010; tagId++) {
        tagIds.emplace(tagId);
    }
    auto req = buildRequest(partId, "", rowLimit, false, false, tagIds);
    int32_t batchCount = 0;

    while (true) {
        auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ++batchCount;
        if (batchCount == 9) {
            expectRowCount = 2;
        }

        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 2, false, false, tagIds);
        if (cursor.empty()) {
            break;
        }
        req = buildRequest(partId, cursor, rowLimit, false, false, tagIds);
    }
    EXPECT_EQ(rowCount, 90);
}

// Retrive vertices of none property in a part
TEST(ScanVertexTest, RetrieveConsecutiveBlockTest3) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());

    std::string cursor = "";
    // fetch 900 rows one by one
    int32_t rowCount = 0, rowLimit = 1, expectRowCount = 1;
    PartitionID partId = 1;
    std::unordered_set<TagID> tagIds;
    for (TagID tagId = 3001; tagId < 3010; tagId++) {
        tagIds.emplace(tagId);
    }
    auto req = buildRequest(partId, "", rowLimit, false, true, tagIds);
    int32_t batchCount = 0;

    while (true) {
        auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ++batchCount;

        checkResponse(partId, resp, cursor, rowCount, expectRowCount, 0, false, true, tagIds);
        if (cursor.empty()) {
            break;
        }
        req = buildRequest(partId, cursor, rowLimit, false, true, tagIds);
    }
    EXPECT_EQ(rowCount, 90);
}

TEST(ScanVertexTest, RetrieveManyPartsTest) {
    fs::TempDir rootPath("/tmp/ScanVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get());
    // 10vId * 5tag * 10part = 500 rows
    std::unordered_set<TagID> tagIds;
    for (TagID tagId = 3001; tagId < 3010; tagId += 2) {
        tagIds.emplace(tagId);
    }

    // scan through all partition
    int32_t totalRowCount = 0;
    for (PartitionID partId = 0; partId < 10; partId++) {
        std::string cursor = "";
        int32_t rowCount = 0, rowLimit = 10, expectRowCount = 10;
        auto req = buildRequest(partId, "", rowLimit, false, false, tagIds);
        int32_t batchCount = 0;

        while (true) {
            auto* processor = ScanVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ++batchCount;

            checkResponse(partId, resp, cursor, rowCount, expectRowCount, 2, false, false, tagIds);
            if (cursor.empty()) {
                break;
            }
            req = buildRequest(partId, cursor, rowLimit, false, false, tagIds);
        }
        EXPECT_EQ(rowCount, 50);
        totalRowCount += rowCount;
    }
    EXPECT_EQ(totalRowCount, 500);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
