/* Copyright (c) 2020 vesoft inc. All rights reserved.
  *
  * This source code is licensed under Apache 2.0 License,
  * attached with Common Clause Condition 1.0, found in the LICENSES directory.
  */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "storage/index/LookupProcessor.h"
#include "codec/test/RowWriterV1.h"
#include "codec/RowWriterV2.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

TEST(LookupIndexTest, LookupIndexTestV1) {
    fs::TempDir rootPath("/tmp/LookupIndexTestV1.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
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
        auto key = NebulaKeyUtils::vertexKey(vIdLen.value(), 1, vId1, 3, 0);
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
        key = NebulaKeyUtils::vertexKey(vIdLen.value(), 1, vId2, 3, 0);
        keyValues.emplace_back(std::move(key), writer2.moveEncodedStr());

        // setup index key

        std::vector<Value> values;
        values.emplace_back(Value(true));
        values.emplace_back(Value(1L));
        values.emplace_back(Value(1.1F));
        values.emplace_back(Value(1.1F));
        values.emplace_back(Value("row1"));

        key = IndexKeyUtils::vertexIndexKey(vIdLen.value(), 1, 3, vId1, values);
        keyValues.emplace_back(std::move(key), "");

        key = IndexKeyUtils::vertexIndexKey(vIdLen.value(), 1, 3, vId2, values);
        keyValues.emplace_back(std::move(key), "");

        folly::Baton<true, std::atomic> baton;
        env->kvstore_->asyncMultiPut(spaceId, 1, std::move(keyValues),
                                     [&](kvstore::ResultCode code) {
                                         EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
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
     *              |  AggregateNode   |
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
        decltype(req.indices) indices;
        req.set_space_id(1);
        indices.set_tag_or_edge_id(3);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("col_bool");
        returnCols.emplace_back("col_int");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(true));
        columnHint.set_column_name("col_bool");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(3);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_vid", "col_bool", "col_int"};
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

TEST(LookupIndexTest, SimpleTagIndexTest) {
    fs::TempDir rootPath("/tmp/SimpleVertexIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));

    /**
     * one IndexQueryContext, where player.name_ == "Rudy Gay"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
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
        std::vector<std::string> expectCols = {"_vid", "age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId;
        vId.append(name.data(), name.size());
        Row row1;
        row1.emplace_back(Value(vId));
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
     *                    |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
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
        decltype(indices.contexts[0].column_hints) columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        decltype(indices.contexts[0].column_hints) columnHints2;
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
        std::vector<std::string> expectCols = {"_vid", "age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId1, vId2;
        vId1.append(name1.data(), name1.size());
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(34L));
        expectRows.emplace_back(Row(row1));
        vId2.append(name2.data(), name2.size());
        Row row2;
        row2.emplace_back(Value(vId2));
        row2.emplace_back(Value(41L));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST(LookupIndexTest, SimpleEdgeIndexTest) {
    fs::TempDir rootPath("/tmp/SimpleEdgeIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true));

    /**
     * one IndexQueryContext, where teammates.player1 == "Tony Parker"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
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
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst", "teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string srcId, dstId;
        srcId.append(tony.data(), tony.size());
        dstId.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(srcId));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(dstId));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(dstId));
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
     *                    |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        std::string yao = "Yao Ming";
        std::string tracy = "Tracy McGrady";
        decltype(req.return_columns) returnCols;
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
        decltype(indices.contexts[0].column_hints) columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        decltype(indices.contexts[0].column_hints) columnHints2;
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
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst", "teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId1, vId2, vId3, vId4;
        vId1.append(tony.data(), tony.size());
        vId2.append(manu.data(), manu.size());
        vId3.append(yao.data(), yao.size());
        vId4.append(tracy.data(), tracy.size());
        Row row1;
        row1.emplace_back(Value(vId1));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(vId2));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(vId2));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(vId1));
        row2.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row2));
        Row row3;
        row3.emplace_back(Value(vId3));
        row3.emplace_back(Value(2004L));
        row3.emplace_back(Value(vId4));
        row3.emplace_back(Value("Rockets"));
        expectRows.emplace_back(Row(row3));
        Row row4;
        row4.emplace_back(Value(vId4));
        row4.emplace_back(Value(2004L));
        row4.emplace_back(Value(vId3));
        row4.emplace_back(Value("Rockets"));
        expectRows.emplace_back(Row(row4));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST(LookupIndexTest, TagIndexFilterTest) {
    fs::TempDir rootPath("/tmp/TagIndexFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));

    /**
     * one IndexQueryContext, where player.name == "Rudy Gay" AND player.age == 34
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelEQ,
            new TagPropertyExpression(
                new std::string(folly::to<std::string>("player")),
                new std::string("age")),
            new ConstantExpression(Value(34L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_vid", "age"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId;
        vId.append(name.data(), name.size());
        Row row1;
        row1.emplace_back(Value(vId));
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
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("age");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelGT,
            new TagPropertyExpression(
                new std::string(folly::to<std::string>("player")),
                new std::string("age")),
            new ConstantExpression(Value(34L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_vid", "age"};
        QueryTestUtils::checkResponse(resp, expectCols, {});
    }
}

TEST(LookupIndexTest, EdgeIndexFilterTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true));

    /**
     * one IndexQueryContext
     * where teammates.player1 == "Tony Parker" AND teammates.teamName == "Spurs"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelGT,
            new EdgePropertyExpression(
                new std::string(folly::to<std::string>("Teammate")),
                new std::string("teamName")),
            new ConstantExpression(Value("Spurs")));
        context1.set_filter("");
        context1.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst", "teamName"};
        decltype(resp.get_data()->rows) expectRows;

        std::string srcId, dstId;
        srcId.append(tony.data(), tony.size());
        dstId.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(srcId));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(dstId));
        row1.emplace_back(Value("Spurs"));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(dstId));
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
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("teamName");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelNE,
            new EdgePropertyExpression(
                new std::string(folly::to<std::string>("Teammate")),
                new std::string("teamName")),
            new ConstantExpression(Value("Spurs")));
        context1.set_filter(expr.encode());
        context1.set_index_id(102);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst", "teamName"};
        QueryTestUtils::checkResponse(resp, expectCols, {});
    }
}

TEST(LookupIndexTest, TagIndexWithDataTest) {
    fs::TempDir rootPath("/tmp/TagIndexWithDataTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));

    /**
     * one IndexQueryContext, where player.name == "Rudy Gay" yield player.games
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("games");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
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
        std::vector<std::string> expectCols = {"_vid", "games"};
        decltype(resp.get_data()->rows) expectRows;

        std::string vId;
        vId.append(name.data(), name.size());
        Row row1;
        row1.emplace_back(Value(vId));
        row1.emplace_back(Value(939L));
        expectRows.emplace_back(Row(row1));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

TEST(LookupIndexTest, EdgeIndexWithDataTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexWithDataTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true));

    /**
     * one IndexQueryContext, where teammates.player1 == "Tony Parker"
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));
        std::string tony = "Tony Parker";
        std::string manu = "Manu Ginobili";
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("startYear");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
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
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst", "startYear"};
        decltype(resp.get_data()->rows) expectRows;

        std::string srcId, dstId;
        srcId.append(tony.data(), tony.size());
        dstId.append(manu.data(), manu.size());
        Row row1;
        row1.emplace_back(Value(srcId));
        row1.emplace_back(Value(2002L));
        row1.emplace_back(Value(dstId));
        row1.emplace_back(Value(2002L));
        expectRows.emplace_back(Row(row1));
        Row row2;
        row2.emplace_back(Value(dstId));
        row2.emplace_back(Value(2002L));
        row2.emplace_back(Value(srcId));
        row2.emplace_back(Value(2002L));
        expectRows.emplace_back(Row(row2));
        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

// Tag has prop, statistics vertices
TEST(LookupIndexTest, TagWithPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/TagWithPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true, false));

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
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

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_vid"};
        decltype(resp.get_data()->rows) expectRows;

        auto playerVerticeId = mock::MockData::mockPlayerVerticeIds();
        for (auto& vId : playerVerticeId) {
            Row row1;
            row1.emplace_back(Value(vId));
            expectRows.emplace_back(Row(row1));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

// Tag no prop, statistics vertices
TEST(LookupIndexTest, TagWithoutPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/TagWithoutPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, false);
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true, false, false));

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);
        decltype(req.parts) parts;
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

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_vid"};
        decltype(resp.get_data()->rows) expectRows;

        auto playerVerticeId = mock::MockData::mockPlayerVerticeIds();
        for (auto& vId : playerVerticeId) {
            Row row1;
            row1.emplace_back(Value(vId));
            expectRows.emplace_back(Row(row1));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}


// Edge has prop, statistics edges
TEST(LookupIndexTest, EdgeWithPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/EdgeWithPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true, false));

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
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

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst"};
        decltype(resp.get_data()->rows) expectRows;

        auto serveEdgeKey = mock::MockData::mockEdgeKeys();

        // Only positive edgeTypes are counted
        for (auto & edgekey : serveEdgeKey) {
            if (edgekey.type_ < 0) {
                continue;
            }
            Row row1;
            row1.emplace_back(Value(edgekey.srcId_));
            row1.emplace_back(Value(edgekey.rank_));
            row1.emplace_back(Value(edgekey.dstId_));
            expectRows.emplace_back(Row(row1));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
    }
}

// Edge no prop, statistics edges
TEST(LookupIndexTest, EdgeWithoutPropStatisVerticesIndexTest) {
    fs::TempDir rootPath("/tmp/EdgeWithoutPropStatisVerticesIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0), 1, false);
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, false, false, false));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true, false, false));

    /**
     * one IndexQueryContext, only has index_id, filter and column_hints are empty
     * lookup plan should be :
     *              +--------+---------+
     *              |       Plan       |
     *              +--------+---------+
     *                       |
     *              +--------+---------+
     *              |  AggregateNode   |
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
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(102);
        indices.set_is_edge(true);
        decltype(req.parts) parts;
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

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        std::vector<std::string> expectCols = {"_src", "_ranking", "_dst"};
        decltype(resp.get_data()->rows) expectRows;

        auto serveEdgeKey = mock::MockData::mockEdgeKeys();

        // Only positive edgeTypes are counted
        for (auto & edgekey : serveEdgeKey) {
            if (edgekey.type_ < 0) {
                continue;
            }
            Row row1;
            row1.emplace_back(Value(edgekey.srcId_));
            row1.emplace_back(Value(edgekey.rank_));
            row1.emplace_back(Value(edgekey.dstId_));
            expectRows.emplace_back(Row(row1));
        }

        QueryTestUtils::checkResponse(resp, expectCols, expectRows);
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
