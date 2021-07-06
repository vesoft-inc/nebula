/* Copyright (c) 2020 vesoft inc. All rights reserved.
  *
  * This source code is licensed under Apache 2.0 License,
  * attached with Common Clause Condition 1.0, found in the LICENSES directory.
  */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "common/fs/TempDir.h"
#include "utils/IndexKeyUtils.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "storage/index/LookupProcessor.h"
#include "codec/test/RowWriterV1.h"
#include "codec/RowWriterV2.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/test/QueryTestUtils.h"

using nebula::storage::cpp2::NewVertex;
using nebula::cpp2::PartitionID;
using nebula::cpp2::TagID;
using nebula::storage::cpp2::IndexColumnHint;

namespace nebula {
namespace storage {

ObjectPool objPool;
auto pool = &objPool;

class LookupIndexTest : public ::testing::TestWithParam<bool> {
public:
    void SetUp() override {
        FLAGS_query_concurrently = GetParam();
    }

    void TearDown() override {
    }
};

TEST_P(LookupIndexTest, LookupIndexTestV1) {
    fs::TempDir rootPath("/tmp/LookupIndexTestV1.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    // setup v1 data and v2 data
    {
        int64_t vid1 = 1, vid2 = 2;
        std::vector<nebula::kvstore::KV> keyValues;
        // setup V1 row
        auto vId1 = reinterpret_cast<const char*>(&vid1);
        auto schemaV1 = env->schemaMan_->getTagSchema(1, 3, 0);
        RowWriterV1 writer(schemaV1.get());
        writer << true << 1L << 1.1F << 1.1F << "row1";
        writer.encode();
        auto key = NebulaKeyUtils::vertexKey(vIdLen.value(), 1, vId1, 3);
        keyValues.emplace_back(std::move(key), writer.encode());

        // setup V2 row
        auto vId2 = reinterpret_cast<const char*>(&vid2);
        const Date date = {2020, 2, 20};
        const DateTime dt = {2020, 2, 20, 10, 30, 45, 0};
        auto schemaV2 = env->schemaMan_->getTagSchema(1, 3, 1);
        RowWriterV2 writer2(schemaV2.get());
        writer2.setValue("col_bool", true);
        writer2.setValue("col_int", 1L);
        writer2.setValue("col_float", 1.1F);
        writer2.setValue("col_double", 1.1F);
        writer2.setValue("col_str", "row1");
        writer2.setValue("col_int8", 8);
        writer2.setValue("col_int16", 16);
        writer2.setValue("col_int32", 32);
        writer2.setValue("col_timestamp", 1L);
        writer2.setValue("col_date", date);
        writer2.setValue("col_datetime", dt);
        writer2.finish();
        key = NebulaKeyUtils::vertexKey(vIdLen.value(), 1, vId2, 3);
        keyValues.emplace_back(std::move(key), writer2.moveEncodedStr());

        // setup index key

        std::string indexVal1;
        indexVal1.append(IndexKeyUtils::encodeValue(true));
        indexVal1.append(IndexKeyUtils::encodeValue(1L));
        indexVal1.append(IndexKeyUtils::encodeValue(1.1F));
        indexVal1.append(IndexKeyUtils::encodeValue(1.1F));
        indexVal1.append(IndexKeyUtils::encodeValue("row1"));
        std::string indexVal2 = indexVal1;

        key = IndexKeyUtils::vertexIndexKey(vIdLen.value(), 1, 3, vId1, std::move(indexVal1));
        keyValues.emplace_back(std::move(key), "");

        key = IndexKeyUtils::vertexIndexKey(vIdLen.value(), 1, 3, vId2, std::move(indexVal2));
        keyValues.emplace_back(std::move(key), "");

        folly::Baton<true, std::atomic> baton;
        env->kvstore_->asyncMultiPut(spaceId, 1, std::move(keyValues),
                                     [&](nebula::cpp2::ErrorCode code) {
                                         EXPECT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                                         baton.post();
                                     });
        baton.wait();
    }
    /**
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(1);
        indices.set_tag_or_edge_id(3);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back("col_bool");
        returnCols.emplace_back("col_int");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(true));
        columnHint.set_column_name("col_bool");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(3);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               "1.col_bool",
                                               "1.col_int"};
        decltype(resp.get_data()->rows) expectRows;

        int64_t vid1 = 1, vid2 = 2;
        std::string vId1, vId2;
        vId1.append(reinterpret_cast<const char*>(&vid1), sizeof(int64_t));
        vId2.append(reinterpret_cast<const char*>(&vid2), sizeof(int64_t));
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(true));
        row1.emplace_back(Value(1L));
        expectRows.emplace_back(Row(row1));

        Row row2;
        row2.emplace_back(Value(vId2));
        row2.emplace_back(Value(true));
        row2.emplace_back(Value(1L));
        expectRows.emplace_back(Row(row2));

        // TODO (sky) : normalize for int type of vertex id.
        // QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST_P(LookupIndexTest, SimpleTagIndexTest) {
    fs::TempDir rootPath("/tmp/SimpleVertexIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, where player.name_ == "Rudy Gay"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back(kTag);
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag),
                                               "1.age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId;
        vId.append(name.data(), name.size());
        Row row1;
        row1.emplace_back(Value(vId));
        row1.emplace_back(Value(1L));
        row1.emplace_back(Value(34L));
        expectRows.emplace_back(Row(row1));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
    /**
     * two IndexQueryContext, where player.name_ == "Rudy Gay" OR player.name_ == "Kobe Bryant"
     * lookup plan should be :
     *                    +--------+---------+
     *                    |       Plan       |
     *                    +--------+---------+
     *                             |
     *                    +--------+---------+
     *                    |    DeDupNode     |
     *                    +--------+---------+
     *                       |            |
     *   +----------+-----------+     +----------+-----------+
     *   +   IndexOutputNode    +     +   IndexOutputNode    +
     *   +----------+-----------+     +----------+-----------+
     *              |                            |
     *   +----------+-----------+     +----------+-----------+
     *   +    IndexScanNode     +     +    IndexScanNode     +
     *   +----------+-----------+     +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back(kTag);
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        // player.name_ == "Rudy Gay"
        cpp2::IndexColumnHint columnHint1;
        std::string name1 = "Rudy Gay";
        columnHint1.set_begin_value(Value(name1));
        columnHint1.set_column_name("name");
        columnHint1.set_scan_type(cpp2::ScanType::PREFIX);
        // player.name_ == "Kobe Bryant"
        cpp2::IndexColumnHint columnHint2;
        std::string name2 = "Kobe Bryant";
        columnHint2.set_begin_value(Value(name2));
        columnHint2.set_column_name("name");
        columnHint2.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        std::vector<IndexColumnHint> columnHints2;
        columnHints2.emplace_back(std::move(columnHint2));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints1));
        context1.set_filter("");
        context1.set_index_id(1);
        cpp2::IndexQueryContext context2;
        context2.set_column_hints(std::move(columnHints2));
        context2.set_filter("");
        context2.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        contexts.emplace_back(std::move(context2));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag),
                                               "1.age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId1, vId2;
        vId1.append(name1.data(), name1.size());
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(1L));
        row1.emplace_back(Value(34L));
        expectRows.emplace_back(Row(row1));
        vId2.append(name2.data(), name2.size());
        Row row2;
        row2.emplace_back(Value(vId2));
        row2.emplace_back(Value(1L));
        row2.emplace_back(Value(41L));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST_P(LookupIndexTest, SimpleEdgeIndexTest) {
    fs::TempDir rootPath("/tmp/SimpleEdgeIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, where teammates.player1 == "Tony Parker"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kSrc);
        returnCols.emplace_back(kType);
        returnCols.emplace_back(kRank);
        returnCols.emplace_back(kDst);
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                               std::string("102.").append(kType),
                                               std::string("102.").append(kRank),
                                               std::string("102.").append(kDst),
                                               "102.teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string srcId, dstId;
        srcId.append(tony.data(), tony.size());
        dstId.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(srcId));
        row1.emplace_back(Value(102L));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(dstId));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(dstId));
        row2.emplace_back(Value(102L));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(srcId));
        row2.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
    /**
     * two IndexQueryContext
     * where teammates.player1 == "Tony Parker" OR teammates.player1 == "Yao Ming"
     * lookup plan should be :
     *                    +--------+---------+
     *                    |       Plan       |
     *                    +--------+---------+
     *                             |
     *                    +--------+---------+
     *                    |    DeDupNode     |
     *                    +--------+---------+
     *                       |            |
     *   +----------+-----------+     +----------+-----------+
     *   +   IndexOutputNode    +     +   IndexOutputNode    +
     *   +----------+-----------+     +----------+-----------+
     *              |                            |
     *   +----------+-----------+     +----------+-----------+
     *   +    IndexScanNode     +     +    IndexScanNode     +
     *   +----------+-----------+     +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::string yao = "Yao Ming";
        std::string tracy = "Tracy McGrady";
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kSrc);
        returnCols.emplace_back(kType);
        returnCols.emplace_back(kRank);
        returnCols.emplace_back(kDst);
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        // teammates.player1 == "Tony Parker"
        cpp2::IndexColumnHint columnHint1;
        columnHint1.set_begin_value(Value(tony));
        columnHint1.set_column_name("player1");
        columnHint1.set_scan_type(cpp2::ScanType::PREFIX);
        // teammates.player1 == "Yao Ming"
        cpp2::IndexColumnHint columnHint2;
        columnHint2.set_begin_value(Value(yao));
        columnHint2.set_column_name("player1");
        columnHint2.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        std::vector<IndexColumnHint> columnHints2;
        columnHints2.emplace_back(std::move(columnHint2));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints1));
        context1.set_filter("");
        context1.set_index_id(102);
        cpp2::IndexQueryContext context2;
        context2.set_column_hints(std::move(columnHints2));
        context2.set_filter("");
        context2.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        contexts.emplace_back(std::move(context2));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                               std::string("102.").append(kType),
                                               std::string("102.").append(kRank),
                                               std::string("102.").append(kDst),
                                               "102.teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId1, vId2, vId3, vId4;
        vId1.append(tony.data(), tony.size());
        vId2.append(manu.data(), manu.size());
        vId3.append(yao.data(), yao.size());
        vId4.append(tracy.data(), tracy.size());
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(102L));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(vId2));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(vId2));
        row2.emplace_back(Value(102L));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(vId1));
        row2.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row2));
        Row row3;
        row3.emplace_back(Value(vId3));
        row3.emplace_back(Value(102L));
        row3.emplace_back(Value(2004L));
        row3.emplace_back(Value(vId4));
        row3.emplace_back(Value("Rockets"));
        expectRows.emplace_back(Row(row3));
        Row row4;
        row4.emplace_back(Value(vId4));
        row4.emplace_back(Value(102L));
        row4.emplace_back(Value(2004L));
        row4.emplace_back(Value(vId3));
        row4.emplace_back(Value("Rockets"));
        expectRows.emplace_back(Row(row4));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST_P(LookupIndexTest, TagIndexFilterTest) {
    fs::TempDir rootPath("/tmp/TagIndexFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, where player.name == "Rudy Gay" AND player.age == 34
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +  IndexFilterNode     +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back(kTag);
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        const auto& expr =
            *RelationalExpression::makeEQ(pool,
                                          TagPropertyExpression::make(pool, "player", "age"),
                                          ConstantExpression::make(pool, Value(34L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag),
                                               "1.age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId;
        vId.append(name.data(), name.size());
        Row row1;
        row1.emplace_back(Value(vId));
        row1.emplace_back(Value(1L));
        row1.emplace_back(Value(34L));
        expectRows.emplace_back(Row(row1));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
    /**
     * one IndexQueryContext, where player.name == "Rudy Gay" AND player.name > 34
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +  IndexFilterNode     +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
     **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back(kTag);
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        const auto& expr =
            *RelationalExpression::makeGT(pool,
                                          TagPropertyExpression::make(pool, "player", "age"),
                                          ConstantExpression::make(pool, Value(34L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag),
                                               "1.age"};
        QueryTestUtils::checkResponse(resp, expectCols, {});
    }
}

TEST_P(LookupIndexTest, EdgeIndexFilterTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext
     * where teammates.player1 == "Tony Parker" AND teammates.teamName == "Spurs"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +  IndexFilterNode     +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kSrc);
        returnCols.emplace_back(kType);
        returnCols.emplace_back(kRank);
        returnCols.emplace_back(kDst);
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        // const auto& expr = *RelationalExpression::makeEQ(
        //     pool,
        //     EdgePropertyExpression::make(pool, "Teammate", "teamName"),
        //     ConstantExpression::make(pool, Value("Spurs")));
        context1.set_filter("");
        context1.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                               std::string("102.").append(kType),
                                               std::string("102.").append(kRank),
                                               std::string("102.").append(kDst),
                                               "102.teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string srcId, dstId;
        srcId.append(tony.data(), tony.size());
        dstId.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(srcId));
        row1.emplace_back(Value(102L));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(dstId));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(dstId));
        row2.emplace_back(Value(102L));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(srcId));
        row2.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }

    /**
     * one IndexQueryContext
     * where teammates.player1 == "Tony Parker" AND teammates.teamName != "Spurs"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +  IndexFilterNode     +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
     **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kSrc);
        returnCols.emplace_back(kType);
        returnCols.emplace_back(kRank);
        returnCols.emplace_back(kDst);
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        const auto& expr = *RelationalExpression::makeNE(
            pool,
            EdgePropertyExpression::make(pool, "Teammate", "teamName"),
            ConstantExpression::make(pool, Value("Spurs")));
        context1.set_filter(expr.encode());
        context1.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                               std::string("102.").append(kType),
                                               std::string("102.").append(kRank),
                                               std::string("102.").append(kDst),
                                               "102.teamName"};
        QueryTestUtils::checkResponse(resp, expectCols, {});
    }
}

TEST_P(LookupIndexTest, TagIndexWithDataTest) {
    fs::TempDir rootPath("/tmp/TagIndexWithDataTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, where player.name == "Rudy Gay" yield player.games
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +  IndexVertexNode     +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back(kTag);
        returnCols.emplace_back("games");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag),
                                               "1.games"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId;
        vId.append(name.data(), name.size());
        Row row1;
        row1.emplace_back(Value(vId));
        row1.emplace_back(Value(1L));
        row1.emplace_back(Value(939L));
        expectRows.emplace_back(Row(row1));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST_P(LookupIndexTest, EdgeIndexWithDataTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexWithDataTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, where teammates.player1 == "Tony Parker"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +   IndexEdgeNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kSrc);
        returnCols.emplace_back(kType);
        returnCols.emplace_back(kRank);
        returnCols.emplace_back(kDst);
        returnCols.emplace_back("startYear");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                               std::string("102.").append(kType),
                                               std::string("102.").append(kRank),
                                               std::string("102.").append(kDst),
                                               "102.startYear"};
        decltype(resp.get_data()->rows) expectRows;

        std::string srcId, dstId;
        srcId.append(tony.data(), tony.size());
        dstId.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(srcId));
        row1.emplace_back(Value(102L));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(dstId));
        row1.emplace_back(Value(2002L));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(dstId));
        row2.emplace_back(Value(102L));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(srcId));
        row2.emplace_back(Value(2002L));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

// Tag has prop, statistics vertices
TEST_P(LookupIndexTest, TagWithPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/TagWithPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true, false));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        cpp2::IndexQueryContext context1;
        context1.set_filter("");
        context1.set_index_id(4);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        req.set_return_columns({kVid, kTag});

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag)};
        decltype(resp.get_data()->rows) expectRows;

        auto playerVerticeId = mock::MockData::mockPlayerVerticeIds();
        for (auto& vId : playerVerticeId) {
            Row row;
            row.emplace_back(vId);
            row.emplace_back(1);
            expectRows.emplace_back(std::move(row));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

// Tag no prop, statistics vertices
TEST_P(LookupIndexTest, TagWithoutPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/TagWithoutPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, false);
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true, false, false));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        cpp2::IndexQueryContext context1;
        context1.set_filter("");
        context1.set_index_id(4);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        req.set_return_columns({kVid, kTag});

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag)};
        decltype(resp.get_data()->rows) expectRows;

        auto playerVerticeId = mock::MockData::mockPlayerVerticeIds();
        for (auto& vId : playerVerticeId) {
            Row row;
            row.emplace_back(vId);
            row.emplace_back(1);
            expectRows.emplace_back(std::move(row));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}


// Edge has prop, statistics edges
TEST_P(LookupIndexTest, EdgeWithPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/EdgeWithPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true, false));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(101);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        cpp2::IndexQueryContext context1;
        context1.set_filter("");
        context1.set_index_id(103);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        req.set_return_columns({kSrc, kType, kRank, kDst});

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("101.").append(kSrc),
                                               std::string("101.").append(kType),
                                               std::string("101.").append(kRank),
                                               std::string("101.").append(kDst)};
        decltype(resp.get_data()->rows) expectRows;

        auto serveEdgeKey = mock::MockData::mockEdgeKeys();

        // Only positive edgeTypes are counted
        for (auto & edgekey : serveEdgeKey) {
            if (edgekey.type_ < 0) {
                continue;
            }
            Row row;
            row.emplace_back(edgekey.srcId_);
            row.emplace_back(edgekey.type_);
            row.emplace_back(edgekey.rank_);
            row.emplace_back(edgekey.dstId_);
            expectRows.emplace_back(std::move(row));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

// Edge no prop, statistics edges
TEST_P(LookupIndexTest, EdgeWithoutPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/EdgeWithoutPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, false);
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, false, false, false));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true, false, false));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |    DeDupNode     |
     *              +--------+---------+
     *                       |
     *            +----------+-----------+
     *            +   IndexOutputNode    +
     *            +----------+-----------+
     *                       |
     *            +----------+-----------+
     *            +    IndexScanNode     +
     *            +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(101);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        cpp2::IndexQueryContext context1;
        context1.set_filter("");
        context1.set_index_id(103);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        req.set_return_columns({kSrc, kType, kRank, kDst});

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("101.").append(kSrc),
                                               std::string("101.").append(kType),
                                               std::string("101.").append(kRank),
                                               std::string("101.").append(kDst)};
        decltype(resp.get_data()->rows) expectRows;

        auto serveEdgeKey = mock::MockData::mockEdgeKeys();

        // Only positive edgeTypes are counted
        for (auto & edgekey : serveEdgeKey) {
            if (edgekey.type_ < 0) {
                continue;
            }
            Row row;
            row.emplace_back(edgekey.srcId_);
            row.emplace_back(edgekey.type_);
            row.emplace_back(edgekey.rank_);
            row.emplace_back(edgekey.dstId_);
            expectRows.emplace_back(std::move(row));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST_P(LookupIndexTest, NullableInIndexAndFilterTest) {
    GraphSpaceID spaceId = 1;
    TagID tagId = 111;
    IndexID indexId = 222;
    fs::TempDir rootPath("/tmp/VerticesValueTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    {
        auto* schemaMan = reinterpret_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        schema->addField("col1", meta::cpp2::PropertyType::INT64, 0, true);
        schema->addField("col2", meta::cpp2::PropertyType::STRING, 0, true);
        schema->addField("col3", meta::cpp2::PropertyType::INT64, 0, true);
        schema->addField("col4", meta::cpp2::PropertyType::STRING, 0, true);
        schemaMan->addTagSchema(spaceId, tagId, schema);
    }
    {
        auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
        std::vector<nebula::meta::cpp2::ColumnDef> cols;

        meta::cpp2::ColumnDef col1;
        col1.name = "col1";
        col1.type.set_type(meta::cpp2::PropertyType::INT64);
        col1.set_nullable(true);
        cols.emplace_back(std::move(col1));

        meta::cpp2::ColumnDef col2;
        col2.name = "col2";
        col2.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
        col2.type.set_type_length(20);
        col2.set_nullable(true);
        cols.emplace_back(std::move(col2));

        indexMan->addTagIndex(spaceId, tagId, indexId, std::move(cols));
    }
    auto genVid = [vIdLen] (std::string& vId) -> std::string {
        return vId.append(vIdLen - sizeof(vId), '\0');
    };

    // insert data
    {
        PartitionID partId = 1;
        cpp2::AddVerticesRequest req;
        req.set_space_id(spaceId);
        req.set_if_not_exists(true);
        std::unordered_map<TagID, std::vector<std::string>> propNames;
        propNames[tagId] = {"col1", "col2", "col3", "col4"};
        req.set_prop_names(std::move(propNames));
        {
            // all not null
            VertexID vId = "1_a_1_a";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(1));
            props.emplace_back(Value("aaa"));
            props.emplace_back(Value(1));
            props.emplace_back(Value("aaa"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // string null
            VertexID vId = "string_null";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(2));
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(Value(2));
            props.emplace_back(NullType::__NULL__);
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // int null
            VertexID vId = "int_null";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(Value("bbb"));
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(Value("bbb"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // half_null
            VertexID vId = "3_c_null_null";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(3));
            props.emplace_back(Value("ccc"));
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // half_null
            VertexID vId = "3_c_3_c";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(3));
            props.emplace_back(Value("ccc"));
            props.emplace_back(Value(3));
            props.emplace_back(Value("ccc"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // all null
            VertexID vId = "all_null";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }

        auto* processor = AddVerticesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    cpp2::LookupIndexRequest req;
    req.set_space_id(spaceId);
    req.set_parts({1, 2, 3, 4, 5, 6});
    req.set_return_columns({kVid});
    {
        LOG(INFO) << "lookup on tag where tag.col1 == 0";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_column_name("col1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        columnHint.set_begin_value(0);

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints({columnHint});

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        ASSERT_EQ(0, resp.get_data()->size());
    }
    {
        LOG(INFO) << "lookup on tag where tag.col1 == 1";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_column_name("col1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        columnHint.set_begin_value(1);

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints({columnHint});

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"1_a_1_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col1 == 1 and tag.col2 == \"aaa\"";
        std::vector<cpp2::IndexColumnHint> columnHints;
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col1");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value(1);
            columnHints.emplace_back(std::move(columnHint));
        }
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col2");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value("aaa");
            columnHints.emplace_back(std::move(columnHint));
        }

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"1_a_1_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col1 < 2";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_column_name("col1");
        columnHint.set_scan_type(cpp2::ScanType::RANGE);
        columnHint.set_begin_value(std::numeric_limits<int64_t>::min());
        columnHint.set_end_value(2);

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints({columnHint});

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"1_a_1_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col1 >= 2";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_column_name("col1");
        columnHint.set_scan_type(cpp2::ScanType::RANGE);
        columnHint.set_begin_value(2);
        columnHint.set_end_value(std::numeric_limits<int64_t>::max());

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints({columnHint});

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"string_null"}));
        expected.rows.emplace_back(nebula::Row({"3_c_3_c"}));
        expected.rows.emplace_back(nebula::Row({"3_c_null_null"}));
        QueryTestUtils::checkResponse(resp, expected.colNames, expected.rows);
    }
    {
        LOG(INFO) << "lookup on tag where tag.col1 > 2";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_column_name("col1");
        columnHint.set_scan_type(cpp2::ScanType::RANGE);
        columnHint.set_begin_value(3);
        columnHint.set_end_value(std::numeric_limits<int64_t>::max());

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints({columnHint});

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"3_c_3_c"}));
        expected.rows.emplace_back(nebula::Row({"3_c_null_null"}));
        QueryTestUtils::checkResponse(resp, expected.colNames, expected.rows);
    }
    {
        // col3 and col4 is out of index, need to pass as filter
        LOG(INFO) << "lookup on tag where tag.col1 == 3 and tag.col2 == \"ccc\" and col3 > 1";
        std::vector<cpp2::IndexColumnHint> columnHints;
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col1");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value(3);
            columnHints.emplace_back(std::move(columnHint));
        }
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col2");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value("ccc");
            columnHints.emplace_back(std::move(columnHint));
        }

        const auto& expr = *RelationalExpression::makeGT(pool,
                                                  TagPropertyExpression::make(pool, "111", "col3"),
                                                  ConstantExpression::make(pool, Value(1)));

        cpp2::IndexQueryContext context;
        context.set_filter(expr.encode());
        context.set_index_id(222);
        context.set_column_hints(columnHints);

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        // Because col3 is out of index, so col3 > 1 will be used as filter
        expected.rows.emplace_back(nebula::Row({"3_c_3_c"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        // col3 and col4 is out of index, need to pass as filter
        LOG(INFO) << "lookup on tag where tag.col1 == 3 and tag.col2 == \"ccc\" and col3 < 5";
        std::vector<cpp2::IndexColumnHint> columnHints;
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col1");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value(3);
            columnHints.emplace_back(std::move(columnHint));
        }
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col2");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value("ccc");
            columnHints.emplace_back(std::move(columnHint));
        }
        const auto& expr = *RelationalExpression::makeLT(pool,
                                                  TagPropertyExpression::make(pool, "111", "col3"),
                                                  ConstantExpression::make(pool, Value(5)));

        cpp2::IndexQueryContext context;
        context.set_filter(expr.encode());
        context.set_index_id(222);
        context.set_column_hints(columnHints);

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"3_c_3_c"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        // col3 and col4 is out of index, need to pass as filter
        LOG(INFO) << "lookup on tag where "
                     "tag.col1 == 3 and tag.col2 == \"ccc\" and tag.col3 > 1 and tag.col4 > \"a\"";
        std::vector<cpp2::IndexColumnHint> columnHints;
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col1");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value(3);
            columnHints.emplace_back(std::move(columnHint));
        }
        {
            cpp2::IndexColumnHint columnHint;
            columnHint.set_column_name("col2");
            columnHint.set_scan_type(cpp2::ScanType::PREFIX);
            columnHint.set_begin_value("ccc");
            columnHints.emplace_back(std::move(columnHint));
        }
        const auto& expr = *nebula::LogicalExpression::makeAnd(
            pool,
            RelationalExpression::makeGT(pool,
                                         TagPropertyExpression::make(pool, "111", "col3"),
                                         ConstantExpression::make(pool, Value(1))),
            RelationalExpression::makeGT(pool,
                                         TagPropertyExpression::make(pool, "111", "col4"),
                                         ConstantExpression::make(pool, Value("a"))));

        cpp2::IndexQueryContext context;
        context.set_filter(expr.encode());
        context.set_index_id(222);
        context.set_column_hints(columnHints);

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"3_c_3_c"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
}

TEST_P(LookupIndexTest, NullablePropertyTest) {
    GraphSpaceID spaceId = 1;
    TagID tagId = 111;
    IndexID indexId = 222;
    fs::TempDir rootPath("/tmp/VerticesValueTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
    size_t strColLen = 20;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    {
        auto* schemaMan = reinterpret_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        schema->addField("col_bool", meta::cpp2::PropertyType::BOOL, 0, true);
        schema->addField("col_int", meta::cpp2::PropertyType::INT64, 0, true);
        schema->addField("col_double", meta::cpp2::PropertyType::DOUBLE, 0, true);
        schema->addField("col_str", meta::cpp2::PropertyType::STRING, strColLen, true);
        schemaMan->addTagSchema(spaceId, tagId, schema);
    }
    auto nullColumnDef = [strColLen](const std::string& name, const meta::cpp2::PropertyType type) {
        meta::cpp2::ColumnDef col;
        col.name = name;
        col.type.set_type(type);
        col.set_nullable(true);
        if (type == meta::cpp2::PropertyType::FIXED_STRING) {
            col.type.set_type_length(strColLen);
        }
        return col;
    };
    {
        auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
        std::vector<nebula::meta::cpp2::ColumnDef> cols;
        cols.emplace_back(nullColumnDef("col_bool", meta::cpp2::PropertyType::BOOL));
        cols.emplace_back(nullColumnDef("col_int", meta::cpp2::PropertyType::INT64));
        cols.emplace_back(nullColumnDef("col_double", meta::cpp2::PropertyType::DOUBLE));
        cols.emplace_back(nullColumnDef("col_str", meta::cpp2::PropertyType::FIXED_STRING));
        indexMan->addTagIndex(spaceId, tagId, indexId, std::move(cols));
    }
    auto genVid = [vIdLen] (std::string& vId) -> std::string {
        return vId.append(vIdLen - sizeof(vId), '\0');
    };
    // insert data
    {
        PartitionID partId = 1;
        cpp2::AddVerticesRequest req;
        req.set_space_id(spaceId);
        req.set_if_not_exists(true);
        std::unordered_map<TagID, std::vector<std::string>> propNames;
        propNames[tagId] = {"col_bool", "col_int", "col_double", "col_str"};
        req.set_prop_names(std::move(propNames));
        {
            // all not null
            VertexID vId = "true_1_1.0_a";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(true));
            props.emplace_back(Value(1));
            props.emplace_back(Value(1.0f));
            props.emplace_back(Value("aaa"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // bool null
            VertexID vId = "null_2_2.0_b";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(Value(2));
            props.emplace_back(Value(2.0f));
            props.emplace_back(Value("bbb"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // int null
            VertexID vId = "false_null_3.0_c";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(false));
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(Value(3.0f));
            props.emplace_back(Value("ccc"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // double null
            VertexID vId = "true_4_null_d";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(true));
            props.emplace_back(Value(4));
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(Value("ddd"));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // string null
            VertexID vId = "false_5_5.0_null";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(Value(false));
            props.emplace_back(Value(5));
            props.emplace_back(Value(5.0f));
            props.emplace_back(NullType::__NULL__);
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }
        {
            // all null
            VertexID vId = "all_null";
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId);
            std::vector<Value> props;
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            props.emplace_back(NullType::__NULL__);
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(genVid(vId));
            newVertex.set_tags(std::move(newTags));
            (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
        }

        auto* processor = AddVerticesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }

    auto columnHint = [] (const std::string& col, const cpp2::ScanType& scanType,
                          const Value& begin, const Value& end) {
        cpp2::IndexColumnHint hint;
        hint.set_column_name(col);
        hint.set_scan_type(scanType);
        hint.set_begin_value(begin);
        hint.set_end_value(end);
        return hint;
    };

    cpp2::LookupIndexRequest req;
    req.set_space_id(spaceId);
    req.set_parts({1, 2, 3, 4, 5, 6});
    req.set_return_columns({kVid});

    // bool range scan will be forbiden in query engine, so only test preix for bool
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        expected.rows.emplace_back(nebula::Row({"true_4_null_d"}));
        QueryTestUtils::checkResponse(resp, expected.colNames, expected.rows);
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int == 1";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int > 2";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(columnHint("col_bool",
                                            cpp2::ScanType::PREFIX,
                                            Value(true),
                                            Value()));
        columnHints.emplace_back(columnHint("col_int",
                                            cpp2::ScanType::RANGE,
                                            Value(3),
                                            Value(std::numeric_limits<int64_t>::max())));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_4_null_d"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int < 4";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(columnHint("col_bool",
                                            cpp2::ScanType::PREFIX,
                                            Value(true),
                                            Value()));
        columnHints.emplace_back(columnHint("col_int",
                                            cpp2::ScanType::RANGE,
                                            Value(std::numeric_limits<int64_t>::min()),
                                            Value(4)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int >= 0 and "
                     "tag.col_int < 3";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::RANGE, Value(0), Value(3)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == false and tag.col_int >= 0 and "
                     "tag.col_int < 10";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::RANGE, Value(0), Value(10)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"false_5_5.0_null"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int == 1 and "
                     "tag.col_double < 2.0";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(columnHint("col_bool",
                                            cpp2::ScanType::PREFIX,
                                            Value(true),
                                            Value()));
        columnHints.emplace_back(columnHint("col_int",
                                            cpp2::ScanType::PREFIX,
                                            Value(1),
                                            Value()));
        columnHints.emplace_back(columnHint("col_double",
                                            cpp2::ScanType::RANGE,
                                            Value(-std::numeric_limits<double>::max()),
                                            Value(2.0)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == false and tag.col_int == 5 and "
                     "tag.col_double > 0.0";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(columnHint("col_bool",
                                            cpp2::ScanType::PREFIX,
                                            Value(false),
                                            Value()));
        columnHints.emplace_back(columnHint("col_int",
                                            cpp2::ScanType::PREFIX,
                                            Value(5),
                                            Value()));
        columnHints.emplace_back(columnHint("col_double",
                                            cpp2::ScanType::RANGE,
                                            Value(0.0 + kEpsilon),
                                            Value(std::numeric_limits<double>::max())));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"false_5_5.0_null"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == false and tag.col_int == 5 and "
                     "tag.col_double >= 0.0 and tag.col_double < 10.0";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(5), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::RANGE, Value(0.0), Value(10.0)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"false_5_5.0_null"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int == 1 and "
                     "tag.col_double == 1.0 and tag.col_str == \"aaa\"";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(1.0), Value()));
        columnHints.emplace_back(
            columnHint("col_str", cpp2::ScanType::PREFIX, Value("aaa"), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int == 1 and "
                     "tag.col_double == 1.0 and tag.col_str > \"a\" and tag.col_str <= \"c\"";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(1.0), Value()));
        std::string begin = std::string("a").append(18, 0x00).append(1, 0x01);
        std::string end = std::string("c").append(18, 0x00).append(1, 0x01);
        columnHints.emplace_back(
            columnHint("col_str", cpp2::ScanType::RANGE, Value(begin), Value(end)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int == 1 and "
                     "tag.col_double == 1.0 and tag.col_str >= \"a\" and tag.col_str < \"c\"";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(1.0), Value()));
        columnHints.emplace_back(
            columnHint("col_str", cpp2::ScanType::RANGE, Value("a"), Value("c")));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_1_1.0_a"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == null";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"null_2_2.0_b"}));
        expected.rows.emplace_back(nebula::Row({"all_null"}));
        QueryTestUtils::checkResponse(resp, expected.colNames, expected.rows);
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == null";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"all_null"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == null and "
                     "tag.col_double == null";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"all_null"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == null and "
                     "tag.col_double == null and tag.col_str == null";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_str", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"all_null"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == true and tag.col_int == 4 and "
                     "tag.col_double == null and tag.col_str == ddd";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(4), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_str", cpp2::ScanType::PREFIX, Value("ddd"), Value()));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"true_4_null_d"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == false and tag.col_int == null and "
                     "tag.col_double == 3.0 and tag.col_str < \"d\"";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_double", cpp2::ScanType::PREFIX, Value(3.0), Value()));
        columnHints.emplace_back(columnHint(
            "col_str", cpp2::ScanType::RANGE, Value(std::string(strColLen, '\0')), Value("d")));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"false_null_3.0_c"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == null and "
                     "tag.col_double >= 0.0";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(columnHint("col_double",
                                            cpp2::ScanType::RANGE,
                                            Value(0.0),
                                            Value(std::numeric_limits<double>::max())));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        ASSERT_EQ(expected, *(resp.get_data()));
    }
    {
        LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int >= 0 and "
                     "tag.col_int < 10";
        std::vector<cpp2::IndexColumnHint> columnHints;
        columnHints.emplace_back(
            columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
        columnHints.emplace_back(
            columnHint("col_int", cpp2::ScanType::RANGE, Value(0), Value(10)));

        cpp2::IndexQueryContext context;
        context.set_filter("");
        context.set_index_id(222);
        context.set_column_hints(std::move(columnHints));

        cpp2::IndexSpec indices;
        indices.set_tag_or_edge_id(111);
        indices.set_is_edge(false);
        indices.set_contexts({context});
        req.set_indices(std::move(indices));

        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        nebula::DataSet expected({std::string("111.").append(kVid)});
        expected.rows.emplace_back(nebula::Row({"null_2_2.0_b"}));
        ASSERT_EQ(expected, *(resp.get_data()));
    }
}


TEST_P(LookupIndexTest, DeDupTagIndexTest) {
    fs::TempDir rootPath("/tmp/DeDupTagIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    /**
     * two IndexQueryContext, where player.name_ == "Rudy Gay" OR player.name_ == "Rudy Gay"
     * lookup plan should be :
     *                    +--------+---------+
     *                    |       Plan       |
     *                    +--------+---------+
     *                             |
     *                    +--------+---------+
     *                    |    DeDupNode     |
     *                    +--------+---------+
     *                       |            |
     *   +----------+-----------+     +----------+-----------+
     *   +   IndexOutputNode    +     +   IndexOutputNode    +
     *   +----------+-----------+     +----------+-----------+
     *              |                            |
     *   +----------+-----------+     +----------+-----------+
     *   +    IndexScanNode     +     +    IndexScanNode     +
     *   +----------+-----------+     +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kVid);
        returnCols.emplace_back(kTag);
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        // player.name_ == "Rudy Gay"
        cpp2::IndexColumnHint columnHint1;
        std::string name1 = "Rudy Gay";
        columnHint1.set_begin_value(Value(name1));
        columnHint1.set_column_name("name");
        columnHint1.set_scan_type(cpp2::ScanType::PREFIX);
        // player.name_ == "Rudy Gay"
        cpp2::IndexColumnHint columnHint2;
        std::string name2 = "Rudy Gay";
        columnHint2.set_begin_value(Value(name2));
        columnHint2.set_column_name("name");
        columnHint2.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        std::vector<IndexColumnHint> columnHints2;
        columnHints2.emplace_back(std::move(columnHint2));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints1));
        context1.set_filter("");
        context1.set_index_id(1);
        cpp2::IndexQueryContext context2;
        context2.set_column_hints(std::move(columnHints2));
        context2.set_filter("");
        context2.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        contexts.emplace_back(std::move(context2));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                               std::string("1.").append(kTag),
                                               "1.age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId1, vId2;
        vId1.append(name1.data(), name1.size());
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(1L));
        row1.emplace_back(Value(34L));
        expectRows.emplace_back(Row(row1));
        vId2.append(name2.data(), name2.size());
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST_P(LookupIndexTest, DedupEdgeIndexTest) {
    fs::TempDir rootPath("/tmp/DedupEdgeIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, true));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    /**
     * two IndexQueryContext
     * where teammates.player1 == "Tony Parker" OR teammates.player1 == "Tony Parker"
     * lookup plan should be :
     *                    +--------+---------+
     *                    |       Plan       |
     *                    +--------+---------+
     *                             |
     *                    +--------+---------+
     *                    |    DeDupNode     |
     *                    +--------+---------+
     *                       |            |
     *   +----------+-----------+     +----------+-----------+
     *   +   IndexOutputNode    +     +   IndexOutputNode    +
     *   +----------+-----------+     +----------+-----------+
     *              |                            |
     *   +----------+-----------+     +----------+-----------+
     *   +    IndexScanNode     +     +    IndexScanNode     +
     *   +----------+-----------+     +----------+-----------+
    **/
    {
        auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
        cpp2::LookupIndexRequest req;
        nebula::storage::cpp2::IndexSpec indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        std::vector<PartitionID> parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::vector<std::string> returnCols;
        returnCols.emplace_back(kSrc);
        returnCols.emplace_back(kType);
        returnCols.emplace_back(kRank);
        returnCols.emplace_back(kDst);
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        // teammates.player1 == "Tony Parker"
        cpp2::IndexColumnHint columnHint1;
        columnHint1.set_begin_value(Value(tony));
        columnHint1.set_column_name("player1");
        columnHint1.set_scan_type(cpp2::ScanType::PREFIX);
        cpp2::IndexColumnHint columnHint2;
        columnHint2.set_begin_value(Value(tony));
        columnHint2.set_column_name("player1");
        columnHint2.set_scan_type(cpp2::ScanType::PREFIX);
        std::vector<IndexColumnHint> columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        std::vector<IndexColumnHint> columnHints2;
        columnHints2.emplace_back(std::move(columnHint2));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints1));
        context1.set_filter("");
        context1.set_index_id(102);
        cpp2::IndexQueryContext context2;
        context2.set_column_hints(std::move(columnHints2));
        context2.set_filter("");
        context2.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        contexts.emplace_back(std::move(context2));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                               std::string("102.").append(kType),
                                               std::string("102.").append(kRank),
                                               std::string("102.").append(kDst),
                                               "102.teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId1, vId2;
        vId1.append(tony.data(), tony.size());
        vId2.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(102L));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(vId2));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(vId2));
        row2.emplace_back(Value(102L));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(vId1));
        row2.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

INSTANTIATE_TEST_CASE_P(
    Lookup_concurrently,
    LookupIndexTest,
    ::testing::Values(false, true));

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
