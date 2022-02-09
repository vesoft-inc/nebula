/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "codec/RowWriterV2.h"
#include "codec/test/RowWriterV1.h"
#include "common/base/Base.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/fs/TempDir.h"
#include "common/utils/IndexKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/index/LookupProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/test/QueryTestUtils.h"

using nebula::cpp2::PartitionID;
using nebula::cpp2::PropertyType;
using nebula::cpp2::TagID;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::NewVertex;

namespace nebula {
namespace storage {

ObjectPool objPool;
auto pool = &objPool;

class LookupIndexTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() override {
    FLAGS_query_concurrently = GetParam();
  }

  void TearDown() override {}
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
    auto key = NebulaKeyUtils::tagKey(vIdLen.value(), 1, vId1, 3);
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
    key = NebulaKeyUtils::tagKey(vIdLen.value(), 1, vId2, 3);
    keyValues.emplace_back(std::move(key), writer2.moveEncodedStr());

    // setup index key

    std::string indexVal1;
    indexVal1.append(IndexKeyUtils::encodeValue(true));
    indexVal1.append(IndexKeyUtils::encodeValue(1L));
    indexVal1.append(IndexKeyUtils::encodeValue(1.1F));
    indexVal1.append(IndexKeyUtils::encodeValue(1.1F));
    indexVal1.append(IndexKeyUtils::encodeValue("row1"));
    std::string indexVal2 = indexVal1;

    auto keys = IndexKeyUtils::vertexIndexKeys(vIdLen.value(), 1, 3, vId1, {std::move(indexVal1)});
    for (auto& k : keys) {
      keyValues.emplace_back(std::move(k), "");
    }

    keys = IndexKeyUtils::vertexIndexKeys(vIdLen.value(), 1, 3, vId2, {std::move(indexVal2)});
    for (auto& k : keys) {
      keyValues.emplace_back(std::move(k), "");
    }

    folly::Baton<true, std::atomic> baton;
    env->kvstore_->asyncMultiPut(
        spaceId, 1, std::move(keyValues), [&](nebula::cpp2::ErrorCode code) {
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
    req.space_id_ref() = 1;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 3;
    indices.schema_id_ref() = schemaId;
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = std::move(parts);
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back("col_bool");
    returnCols.emplace_back("col_int");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = (Value(true));
    columnHint.column_name_ref() = ("col_bool");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (3);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), "1.col_bool", "1.col_int"};
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("age");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    std::string name = "Rudy Gay";
    columnHint.begin_value_ref() = (Value(name));
    columnHint.column_name_ref() = ("name");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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
   * two IndexQueryContext, where player.name_ == "Rudy Gay" OR player.name_ ==
   *"Kobe Bryant" lookup plan should be :
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("age");
    req.return_columns_ref() = (std::move(returnCols));
    // player.name_ == "Rudy Gay"
    cpp2::IndexColumnHint columnHint1;
    std::string name1 = "Rudy Gay";
    columnHint1.begin_value_ref() = (Value(name1));
    columnHint1.column_name_ref() = ("name");
    columnHint1.scan_type_ref() = (cpp2::ScanType::PREFIX);
    // player.name_ == "Kobe Bryant"
    cpp2::IndexColumnHint columnHint2;
    std::string name2 = "Kobe Bryant";
    columnHint2.begin_value_ref() = (Value(name2));
    columnHint2.column_name_ref() = ("name");
    columnHint2.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints1;
    columnHints1.emplace_back(std::move(columnHint1));
    std::vector<IndexColumnHint> columnHints2;
    columnHints2.emplace_back(std::move(columnHint2));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints1));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (1);
    cpp2::IndexQueryContext context2;
    context2.column_hints_ref() = (std::move(columnHints2));
    context2.filter_ref() = ("");
    context2.index_id_ref() = (1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    contexts.emplace_back(std::move(context2));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (102);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::string tony = "Tony Parker";
    std::string manu = "Manu Ginobili";
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kSrc);
    returnCols.emplace_back(kType);
    returnCols.emplace_back(kRank);
    returnCols.emplace_back(kDst);
    returnCols.emplace_back("teamName");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = (Value(tony));
    columnHint.column_name_ref() = ("player1");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (102);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
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
    req.return_columns_ref() = (std::move(returnCols));
    // teammates.player1 == "Tony Parker"
    cpp2::IndexColumnHint columnHint1;
    columnHint1.begin_value_ref() = (Value(tony));
    columnHint1.column_name_ref() = ("player1");
    columnHint1.scan_type_ref() = (cpp2::ScanType::PREFIX);
    // teammates.player1 == "Yao Ming"
    cpp2::IndexColumnHint columnHint2;
    columnHint2.begin_value_ref() = (Value(yao));
    columnHint2.column_name_ref() = ("player1");
    columnHint2.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints1;
    columnHints1.emplace_back(std::move(columnHint1));
    std::vector<IndexColumnHint> columnHints2;
    columnHints2.emplace_back(std::move(columnHint2));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints1));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (102);
    cpp2::IndexQueryContext context2;
    context2.column_hints_ref() = (std::move(columnHints2));
    context2.filter_ref() = ("");
    context2.index_id_ref() = (102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    contexts.emplace_back(std::move(context2));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("age");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    std::string name = "Rudy Gay";
    columnHint.begin_value_ref() = (Value(name));
    columnHint.column_name_ref() = ("name");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    const auto& expr =
        *RelationalExpression::makeEQ(pool,
                                      TagPropertyExpression::make(pool, "player", "age"),
                                      ConstantExpression::make(pool, Value(34L)));
    context1.filter_ref() = (expr.encode());
    context1.index_id_ref() = (1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("age");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    std::string name = "Rudy Gay";
    columnHint.begin_value_ref() = (Value(name));
    columnHint.column_name_ref() = ("name");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    const auto& expr =
        *RelationalExpression::makeGT(pool,
                                      TagPropertyExpression::make(pool, "player", "age"),
                                      ConstantExpression::make(pool, Value(34L)));
    context1.filter_ref() = (expr.encode());
    context1.index_id_ref() = (1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (102);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::string tony = "Tony Parker";
    std::string manu = "Manu Ginobili";
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kSrc);
    returnCols.emplace_back(kType);
    returnCols.emplace_back(kRank);
    returnCols.emplace_back(kDst);
    returnCols.emplace_back("teamName");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = (Value(tony));
    columnHint.column_name_ref() = ("player1");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    // const auto& expr = *RelationalExpression::makeEQ(
    //     pool,
    //     EdgePropertyExpression::make(pool, "Teammate", "teamName"),
    //     ConstantExpression::make(pool, Value("Spurs")));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (102);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::string tony = "Tony Parker";
    std::string manu = "Manu Ginobili";
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kSrc);
    returnCols.emplace_back(kType);
    returnCols.emplace_back(kRank);
    returnCols.emplace_back(kDst);
    returnCols.emplace_back("teamName");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = (Value(tony));
    columnHint.column_name_ref() = ("player1");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    const auto& expr =
        *RelationalExpression::makeNE(pool,
                                      EdgePropertyExpression::make(pool, "Teammate", "teamName"),
                                      ConstantExpression::make(pool, Value("Spurs")));
    context1.filter_ref() = (expr.encode());
    context1.index_id_ref() = (102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("games");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    std::string name = "Rudy Gay";
    columnHint.begin_value_ref() = (Value(name));
    columnHint.column_name_ref() = ("name");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), std::string("1.").append(kTag), "1.games"};
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (102);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::string tony = "Tony Parker";
    std::string manu = "Manu Ginobili";
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kSrc);
    returnCols.emplace_back(kType);
    returnCols.emplace_back(kRank);
    returnCols.emplace_back(kDst);
    returnCols.emplace_back("startYear");
    req.return_columns_ref() = (std::move(returnCols));
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = (Value(tony));
    columnHint.column_name_ref() = ("player1");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
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
TEST_P(LookupIndexTest, TagWithPropStatsVerticesIndexTest) {
  fs::TempDir rootPath("/tmp/TagWithPropStatsVerticesIndexTest.XXXXXX");
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    cpp2::IndexQueryContext context1;
    context1.filter_ref() = ("");
    context1.index_id_ref() = (4);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    req.return_columns_ref() = {kVid, kTag};

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
TEST_P(LookupIndexTest, TagWithoutPropStatsVerticesIndexTest) {
  fs::TempDir rootPath("/tmp/TagWithoutPropStatsVerticesIndexTest.XXXXXX");
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    cpp2::IndexQueryContext context1;
    context1.filter_ref() = ("");
    context1.index_id_ref() = (4);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    req.return_columns_ref() = {kVid, kTag};

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
TEST_P(LookupIndexTest, EdgeWithPropStatsVerticesIndexTest) {
  fs::TempDir rootPath("/tmp/EdgeWithPropStatsVerticesIndexTest.XXXXXX");
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (101);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    cpp2::IndexQueryContext context1;
    context1.filter_ref() = ("");
    context1.index_id_ref() = (103);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    req.return_columns_ref() = {kSrc, kType, kRank, kDst};

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
    for (auto& edgekey : serveEdgeKey) {
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
TEST_P(LookupIndexTest, EdgeWithoutPropStatsVerticesIndexTest) {
  fs::TempDir rootPath("/tmp/EdgeWithoutPropStatsVerticesIndexTest.XXXXXX");
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (101);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    cpp2::IndexQueryContext context1;
    context1.filter_ref() = ("");
    context1.index_id_ref() = (103);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    req.return_columns_ref() = {kSrc, kType, kRank, kDst};

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
    for (auto& edgekey : serveEdgeKey) {
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
    schema->addField("col1", PropertyType::INT64, 0, true);
    schema->addField("col2", PropertyType::STRING, 0, true);
    schema->addField("col3", PropertyType::INT64, 0, true);
    schema->addField("col4", PropertyType::STRING, 0, true);
    schemaMan->addTagSchema(spaceId, tagId, schema);
  }
  {
    auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
    std::vector<nebula::meta::cpp2::ColumnDef> cols;

    meta::cpp2::ColumnDef col1;
    col1.name = "col1";
    col1.type.type_ref() = (PropertyType::INT64);
    col1.nullable_ref() = (true);
    cols.emplace_back(std::move(col1));

    meta::cpp2::ColumnDef col2;
    col2.name = "col2";
    col2.type.type_ref() = (PropertyType::FIXED_STRING);
    col2.type.type_length_ref() = (20);
    col2.nullable_ref() = (true);
    cols.emplace_back(std::move(col2));

    indexMan->addTagIndex(spaceId, tagId, indexId, std::move(cols));
  }
  auto genVid = [vIdLen](std::string& vId) -> std::string {
    return vId.append(vIdLen - sizeof(vId), '\0');
  };

  // insert data
  {
    PartitionID partId = 1;
    cpp2::AddVerticesRequest req;
    req.space_id_ref() = (spaceId);
    req.if_not_exists_ref() = (true);
    std::unordered_map<TagID, std::vector<std::string>> propNames;
    propNames[tagId] = {"col1", "col2", "col3", "col4"};
    req.prop_names_ref() = (std::move(propNames));
    {
      // all not null
      VertexID vId = "1_a_1_a";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(1));
      props.emplace_back(Value("aaa"));
      props.emplace_back(Value(1));
      props.emplace_back(Value("aaa"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = genVid(vId);
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // string null
      VertexID vId = "string_null";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(2));
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(Value(2));
      props.emplace_back(NullType::__NULL__);
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // int null
      VertexID vId = "int_null";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(Value("bbb"));
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(Value("bbb"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // half_null
      VertexID vId = "3_c_null_null";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(3));
      props.emplace_back(Value("ccc"));
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // half_null
      VertexID vId = "3_c_3_c";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(3));
      props.emplace_back(Value("ccc"));
      props.emplace_back(Value(3));
      props.emplace_back(Value("ccc"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // all null
      VertexID vId = "all_null";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }

    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }
  cpp2::LookupIndexRequest req;
  req.space_id_ref() = (spaceId);
  req.parts_ref() = {1, 2, 3, 4, 5, 6};
  req.return_columns_ref() = {kVid};
  {
    LOG(INFO) << "lookup on tag where tag.col1 == 0";
    cpp2::IndexColumnHint columnHint;
    columnHint.column_name_ref() = ("col1");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    columnHint.begin_value_ref() = (0);

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = {columnHint};

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHint.column_name_ref() = ("col1");
    columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
    columnHint.begin_value_ref() = (1);

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = {columnHint};

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
      columnHint.column_name_ref() = ("col1");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = (1);
      columnHints.emplace_back(std::move(columnHint));
    }
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col2");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = ("aaa");
      columnHints.emplace_back(std::move(columnHint));
    }

    cpp2::IndexQueryContext context;
    context.filter_ref() = "";
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHint.column_name_ref() = ("col1");
    columnHint.scan_type_ref() = (cpp2::ScanType::RANGE);
    columnHint.begin_value_ref() = (std::numeric_limits<int64_t>::min());
    columnHint.end_value_ref() = (2);

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = {columnHint};

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHint.column_name_ref() = ("col1");
    columnHint.scan_type_ref() = (cpp2::ScanType::RANGE);
    columnHint.begin_value_ref() = (2);
    columnHint.end_value_ref() = (std::numeric_limits<int64_t>::max());

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = {columnHint};

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHint.column_name_ref() = ("col1");
    columnHint.scan_type_ref() = (cpp2::ScanType::RANGE);
    columnHint.begin_value_ref() = (3);
    columnHint.end_value_ref() = (std::numeric_limits<int64_t>::max());

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = {columnHint};

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    LOG(INFO) << "lookup on tag where tag.col1 == 3 and tag.col2 == \"ccc\" "
                 "and col3 > 1";
    std::vector<cpp2::IndexColumnHint> columnHints;
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col1");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = (3);
      columnHints.emplace_back(std::move(columnHint));
    }
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col2");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = ("ccc");
      columnHints.emplace_back(std::move(columnHint));
    }

    const auto& expr =
        *RelationalExpression::makeGT(pool,
                                      TagPropertyExpression::make(pool, "111", "col3"),
                                      ConstantExpression::make(pool, Value(1)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = (expr.encode());
    context.index_id_ref() = (222);
    context.column_hints_ref() = (columnHints);

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    LOG(INFO) << "lookup on tag where tag.col1 == 3 and tag.col2 == \"ccc\" "
                 "and col3 < 5";
    std::vector<cpp2::IndexColumnHint> columnHints;
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col1");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = (3);
      columnHints.emplace_back(std::move(columnHint));
    }
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col2");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = ("ccc");
      columnHints.emplace_back(std::move(columnHint));
    }
    const auto& expr =
        *RelationalExpression::makeLT(pool,
                                      TagPropertyExpression::make(pool, "111", "col3"),
                                      ConstantExpression::make(pool, Value(5)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = (expr.encode());
    context.index_id_ref() = (222);
    context.column_hints_ref() = (columnHints);

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
                 "tag.col1 == 3 and tag.col2 == \"ccc\" and tag.col3 > 1 and "
                 "tag.col4 > \"a\"";
    std::vector<cpp2::IndexColumnHint> columnHints;
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col1");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = (3);
      columnHints.emplace_back(std::move(columnHint));
    }
    {
      cpp2::IndexColumnHint columnHint;
      columnHint.column_name_ref() = ("col2");
      columnHint.scan_type_ref() = (cpp2::ScanType::PREFIX);
      columnHint.begin_value_ref() = ("ccc");
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
    context.filter_ref() = (expr.encode());
    context.index_id_ref() = (222);
    context.column_hints_ref() = (columnHints);

    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    cpp2::IndexSpec indices;
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    schema->addField("col_bool", PropertyType::BOOL, 0, true);
    schema->addField("col_int", PropertyType::INT64, 0, true);
    schema->addField("col_double", PropertyType::DOUBLE, 0, true);
    schema->addField("col_str", PropertyType::STRING, strColLen, true);
    schemaMan->addTagSchema(spaceId, tagId, schema);
  }
  auto nullColumnDef = [strColLen](const std::string& name, const PropertyType type) {
    meta::cpp2::ColumnDef col;
    col.name = name;
    col.type.type_ref() = (type);
    col.nullable_ref() = (true);
    if (type == PropertyType::FIXED_STRING) {
      col.type.type_length_ref() = (strColLen);
    }
    return col;
  };
  {
    auto* indexMan = reinterpret_cast<mock::AdHocIndexManager*>(env->indexMan_);
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    cols.emplace_back(nullColumnDef("col_bool", PropertyType::BOOL));
    cols.emplace_back(nullColumnDef("col_int", PropertyType::INT64));
    cols.emplace_back(nullColumnDef("col_double", PropertyType::DOUBLE));
    cols.emplace_back(nullColumnDef("col_str", PropertyType::FIXED_STRING));
    indexMan->addTagIndex(spaceId, tagId, indexId, std::move(cols));
  }
  auto genVid = [vIdLen](std::string& vId) -> std::string {
    return vId.append(vIdLen - sizeof(vId), '\0');
  };
  // insert data
  {
    PartitionID partId = 1;
    cpp2::AddVerticesRequest req;
    req.space_id_ref() = (spaceId);
    req.if_not_exists_ref() = (true);
    std::unordered_map<TagID, std::vector<std::string>> propNames;
    propNames[tagId] = {"col_bool", "col_int", "col_double", "col_str"};
    req.prop_names_ref() = (std::move(propNames));
    {
      // all not null
      VertexID vId = "true_1_1.0_a";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(true));
      props.emplace_back(Value(1));
      props.emplace_back(Value(1.0f));
      props.emplace_back(Value("aaa"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // bool null
      VertexID vId = "null_2_2.0_b";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(Value(2));
      props.emplace_back(Value(2.0f));
      props.emplace_back(Value("bbb"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // int null
      VertexID vId = "false_null_3.0_c";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(false));
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(Value(3.0f));
      props.emplace_back(Value("ccc"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // double null
      VertexID vId = "true_4_null_d";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(true));
      props.emplace_back(Value(4));
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(Value("ddd"));
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // string null
      VertexID vId = "false_5_5.0_null";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(Value(false));
      props.emplace_back(Value(5));
      props.emplace_back(Value(5.0f));
      props.emplace_back(NullType::__NULL__);
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    {
      // all null
      VertexID vId = "all_null";
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      newTag.tag_id_ref() = (tagId);
      std::vector<Value> props;
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      props.emplace_back(NullType::__NULL__);
      newTag.props_ref() = (std::move(props));
      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));
      newVertex.id_ref() = (genVid(vId));
      newVertex.tags_ref() = (std::move(newTags));
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }

    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

  auto columnHint = [](const std::string& col,
                       const cpp2::ScanType& scanType,
                       const Value& begin,
                       const Value& end) {
    cpp2::IndexColumnHint hint;
    hint.column_name_ref() = (col);
    hint.scan_type_ref() = (scanType);
    hint.begin_value_ref() = (begin);
    hint.end_value_ref() = (end);
    return hint;
  };

  cpp2::LookupIndexRequest req;
  req.space_id_ref() = (spaceId);
  req.parts_ref() = {1, 2, 3, 4, 5, 6};
  req.return_columns_ref() = {kVid};

  // bool range scan will be forbidden in query engine, so only test prefix for
  // bool
  {
    LOG(INFO) << "lookup on tag where tag.col_bool == true";
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint(
        "col_int", cpp2::ScanType::RANGE, Value(3), Value(std::numeric_limits<int64_t>::max())));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint(
        "col_int", cpp2::ScanType::RANGE, Value(std::numeric_limits<int64_t>::min()), Value(4)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::RANGE, Value(0), Value(3)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::RANGE, Value(0), Value(10)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
    columnHints.emplace_back(columnHint("col_double",
                                        cpp2::ScanType::RANGE,
                                        Value(-std::numeric_limits<double>::max()),
                                        Value(2.0)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(5), Value()));
    columnHints.emplace_back(columnHint("col_double",
                                        cpp2::ScanType::RANGE,
                                        Value(0.0 + kEpsilon),
                                        Value(std::numeric_limits<double>::max())));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(5), Value()));
    columnHints.emplace_back(
        columnHint("col_double", cpp2::ScanType::RANGE, Value(0.0), Value(10.0)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
    columnHints.emplace_back(columnHint("col_double", cpp2::ScanType::PREFIX, Value(1.0), Value()));
    columnHints.emplace_back(columnHint("col_str", cpp2::ScanType::PREFIX, Value("aaa"), Value()));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
                 "tag.col_double == 1.0 and tag.col_str > \"a\" and tag.col_str <= "
                 "\"c\"";
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
    columnHints.emplace_back(columnHint("col_double", cpp2::ScanType::PREFIX, Value(1.0), Value()));
    std::string begin = std::string("a").append(18, 0x00).append(1, 0x01);
    std::string end = std::string("c").append(18, 0x00).append(1, 0x01);
    columnHints.emplace_back(
        columnHint("col_str", cpp2::ScanType::RANGE, Value(begin), Value(end)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
                 "tag.col_double == 1.0 and tag.col_str >= \"a\" and tag.col_str < "
                 "\"c\"";
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(1), Value()));
    columnHints.emplace_back(columnHint("col_double", cpp2::ScanType::PREFIX, Value(1.0), Value()));
    columnHints.emplace_back(columnHint("col_str", cpp2::ScanType::RANGE, Value("a"), Value("c")));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == "
                 "null and "
                 "tag.col_double == null";
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(
        columnHint("col_bool", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
    columnHints.emplace_back(
        columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
    columnHints.emplace_back(
        columnHint("col_double", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == "
                 "null and "
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
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(true), Value()));
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::PREFIX, Value(4), Value()));
    columnHints.emplace_back(
        columnHint("col_double", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
    columnHints.emplace_back(columnHint("col_str", cpp2::ScanType::PREFIX, Value("ddd"), Value()));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    LOG(INFO) << "lookup on tag where tag.col_bool == false and tag.col_int == "
                 "null and "
                 "tag.col_double == 3.0 and tag.col_str < \"d\"";
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(columnHint("col_bool", cpp2::ScanType::PREFIX, Value(false), Value()));
    columnHints.emplace_back(
        columnHint("col_int", cpp2::ScanType::PREFIX, Value(NullType::__NULL__), Value()));
    columnHints.emplace_back(columnHint("col_double", cpp2::ScanType::PREFIX, Value(3.0), Value()));
    columnHints.emplace_back(columnHint(
        "col_str", cpp2::ScanType::RANGE, Value(std::string(strColLen, '\0')), Value("d")));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    LOG(INFO) << "lookup on tag where tag.col_bool == null and tag.col_int == "
                 "null and "
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
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
    columnHints.emplace_back(columnHint("col_int", cpp2::ScanType::RANGE, Value(0), Value(10)));

    cpp2::IndexQueryContext context;
    context.filter_ref() = ("");
    context.index_id_ref() = (222);
    context.column_hints_ref() = (std::move(columnHints));

    cpp2::IndexSpec indices;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (111);
    indices.schema_id_ref() = (schemaId);
    indices.contexts_ref() = {context};
    req.indices_ref() = (std::move(indices));

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
   * two IndexQueryContext, where player.name_ == "Rudy Gay" OR player.name_ ==
   *"Rudy Gay" lookup plan should be :
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = (1);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kVid);
    returnCols.emplace_back(kTag);
    returnCols.emplace_back("age");
    req.return_columns_ref() = (std::move(returnCols));
    // player.name_ == "Rudy Gay"
    cpp2::IndexColumnHint columnHint1;
    std::string name1 = "Rudy Gay";
    columnHint1.begin_value_ref() = (Value(name1));
    columnHint1.column_name_ref() = ("name");
    columnHint1.scan_type_ref() = (cpp2::ScanType::PREFIX);
    // player.name_ == "Rudy Gay"
    cpp2::IndexColumnHint columnHint2;
    std::string name2 = "Rudy Gay";
    columnHint2.begin_value_ref() = (Value(name2));
    columnHint2.column_name_ref() = ("name");
    columnHint2.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints1;
    columnHints1.emplace_back(std::move(columnHint1));
    std::vector<IndexColumnHint> columnHints2;
    columnHints2.emplace_back(std::move(columnHint2));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints1));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (1);
    cpp2::IndexQueryContext context2;
    context2.column_hints_ref() = (std::move(columnHints2));
    context2.filter_ref() = ("");
    context2.index_id_ref() = (1);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    contexts.emplace_back(std::move(context2));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    std::vector<std::string> expectCols = {
        std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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
   * where teammates.player1 == "Tony Parker" OR teammates.player1 == "Tony
   *Parker" lookup plan should be :
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
    req.space_id_ref() = (spaceId);
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = (102);
    indices.schema_id_ref() = (schemaId);
    std::vector<PartitionID> parts;
    for (int32_t p = 1; p <= totalParts; p++) {
      parts.emplace_back(p);
    }
    req.parts_ref() = (std::move(parts));
    std::string tony = "Tony Parker";
    std::string manu = "Manu Ginobili";
    std::vector<std::string> returnCols;
    returnCols.emplace_back(kSrc);
    returnCols.emplace_back(kType);
    returnCols.emplace_back(kRank);
    returnCols.emplace_back(kDst);
    returnCols.emplace_back("teamName");
    req.return_columns_ref() = (std::move(returnCols));
    // teammates.player1 == "Tony Parker"
    cpp2::IndexColumnHint columnHint1;
    columnHint1.begin_value_ref() = (Value(tony));
    columnHint1.column_name_ref() = ("player1");
    columnHint1.scan_type_ref() = (cpp2::ScanType::PREFIX);
    cpp2::IndexColumnHint columnHint2;
    columnHint2.begin_value_ref() = (Value(tony));
    columnHint2.column_name_ref() = ("player1");
    columnHint2.scan_type_ref() = (cpp2::ScanType::PREFIX);
    std::vector<IndexColumnHint> columnHints1;
    columnHints1.emplace_back(std::move(columnHint1));
    std::vector<IndexColumnHint> columnHints2;
    columnHints2.emplace_back(std::move(columnHint2));
    cpp2::IndexQueryContext context1;
    context1.column_hints_ref() = (std::move(columnHints1));
    context1.filter_ref() = ("");
    context1.index_id_ref() = (102);
    cpp2::IndexQueryContext context2;
    context2.column_hints_ref() = (std::move(columnHints2));
    context2.filter_ref() = ("");
    context2.index_id_ref() = (102);
    decltype(indices.contexts) contexts;
    contexts.emplace_back(std::move(context1));
    contexts.emplace_back(std::move(context2));
    indices.contexts_ref() = (std::move(contexts));
    req.indices_ref() = (std::move(indices));
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

// test aggregate in tag, like sum(age) as "total age"
TEST_P(LookupIndexTest, AggregateTagIndexTest) {
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

  auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());

  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.tag_id_ref() = 1;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= totalParts; p++) {
    parts.emplace_back(p);
  }

  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  returnCols.emplace_back("age");
  req.return_columns_ref() = std::move(returnCols);

  // player.name_ == "Rudy Gay"
  cpp2::IndexColumnHint columnHint1;
  std::string name1 = "Rudy Gay";
  columnHint1.begin_value_ref() = Value(name1);
  columnHint1.column_name_ref() = "name";
  columnHint1.scan_type_ref() = cpp2::ScanType::PREFIX;

  // player.name_ == "Kobe Bryant"
  cpp2::IndexColumnHint columnHint2;
  std::string name2 = "Kobe Bryant";
  columnHint2.begin_value_ref() = Value(name2);
  columnHint2.column_name_ref() = "name";
  columnHint2.scan_type_ref() = cpp2::ScanType::PREFIX;
  std::vector<IndexColumnHint> columnHints1;
  columnHints1.emplace_back(std::move(columnHint1));
  std::vector<IndexColumnHint> columnHints2;
  columnHints2.emplace_back(std::move(columnHint2));

  cpp2::IndexQueryContext context1;
  context1.column_hints_ref() = std::move(columnHints1);
  context1.filter_ref() = "";
  context1.index_id_ref() = 1;
  std::vector<cpp2::StatProp> statProps;
  cpp2::IndexQueryContext context2;
  context2.column_hints_ref() = std::move(columnHints2);
  context2.filter_ref() = "";
  context2.index_id_ref() = 1;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  contexts.emplace_back(std::move(context2));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  cpp2::StatProp statProp;
  statProp.alias_ref() = "total age";
  const auto& exp = *TagPropertyExpression::make(pool, folly::to<std::string>(1), "age");
  statProp.prop_ref() = Expression::encode(exp);
  statProp.stat_ref() = cpp2::StatType::SUM;
  statProps.emplace_back(std::move(statProp));
  req.stat_columns_ref() = std::move(statProps);

  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();

  std::vector<std::string> expectCols = {
      std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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

  std::vector<std::string> expectStatColumns;
  nebula::Row expectStatRow;
  expectStatColumns.emplace_back("total age");
  expectStatRow.values.push_back(Value(75L));
  QueryTestUtils::checkStatResponse(resp, expectStatColumns, expectStatRow);
}

// test aggregate in tag, like sum(age) as "total age", and age not in returnColumns
TEST_P(LookupIndexTest, AggregateTagPropNotInReturnColumnsTest) {
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

  auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());

  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.tag_id_ref() = 1;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= totalParts; p++) {
    parts.emplace_back(p);
  }

  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  req.return_columns_ref() = std::move(returnCols);

  // player.name_ == "Rudy Gay"
  cpp2::IndexColumnHint columnHint1;
  std::string name1 = "Rudy Gay";
  columnHint1.begin_value_ref() = Value(name1);
  columnHint1.column_name_ref() = "name";
  columnHint1.scan_type_ref() = cpp2::ScanType::PREFIX;

  // player.name_ == "Kobe Bryant"
  cpp2::IndexColumnHint columnHint2;
  std::string name2 = "Kobe Bryant";
  columnHint2.begin_value_ref() = Value(name2);
  columnHint2.column_name_ref() = "name";
  columnHint2.scan_type_ref() = cpp2::ScanType::PREFIX;
  std::vector<IndexColumnHint> columnHints1;
  columnHints1.emplace_back(std::move(columnHint1));
  std::vector<IndexColumnHint> columnHints2;
  columnHints2.emplace_back(std::move(columnHint2));

  cpp2::IndexQueryContext context1;
  context1.column_hints_ref() = std::move(columnHints1);
  context1.filter_ref() = "";
  context1.index_id_ref() = 1;
  std::vector<cpp2::StatProp> statProps;
  cpp2::IndexQueryContext context2;
  context2.column_hints_ref() = std::move(columnHints2);
  context2.filter_ref() = "";
  context2.index_id_ref() = 1;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  contexts.emplace_back(std::move(context2));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  cpp2::StatProp statProp;
  statProp.alias_ref() = "total age";
  const auto& exp = *TagPropertyExpression::make(pool, folly::to<std::string>(1), "age");
  statProp.prop_ref() = Expression::encode(exp);
  statProp.stat_ref() = cpp2::StatType::SUM;
  statProps.emplace_back(std::move(statProp));
  req.stat_columns_ref() = std::move(statProps);

  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();

  std::vector<std::string> expectCols = {std::string("1.").append(kVid),
                                         std::string("1.").append(kTag)};
  decltype(resp.get_data()->rows) expectRows;

  std::string vId1, vId2;
  vId1.append(name1.data(), name1.size());
  Row row1;
  row1.emplace_back(Value(vId1));
  row1.emplace_back(Value(1L));
  expectRows.emplace_back(Row(row1));

  vId2.append(name2.data(), name2.size());
  Row row2;
  row2.emplace_back(Value(vId2));
  row2.emplace_back(Value(1L));
  expectRows.emplace_back(Row(row2));
  QueryTestUtils::checkResponse(resp, expectCols, expectRows);

  std::vector<std::string> expectStatColumns;
  nebula::Row expectStatRow;
  expectStatColumns.emplace_back("total age");
  expectStatRow.values.push_back(Value(75L));
  QueryTestUtils::checkStatResponse(resp, expectStatColumns, expectStatRow);
}

// test multi aggregate in tag, like sum(age) as "total age", max(age) as "max age",
//    max(kVid) as "max kVid", min(age) as "min age"
TEST_P(LookupIndexTest, AggregateTagPropsTest) {
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

  auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());

  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.tag_id_ref() = 1;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= totalParts; p++) {
    parts.emplace_back(p);
  }

  req.parts_ref() = std::move(parts);
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  returnCols.emplace_back(kTag);
  returnCols.emplace_back("age");
  req.return_columns_ref() = std::move(returnCols);

  // player.name_ == "Rudy Gay"
  cpp2::IndexColumnHint columnHint1;
  std::string name1 = "Rudy Gay";
  columnHint1.begin_value_ref() = Value(name1);
  columnHint1.column_name_ref() = "name";
  columnHint1.scan_type_ref() = cpp2::ScanType::PREFIX;

  // player.name_ == "Kobe Bryant"
  cpp2::IndexColumnHint columnHint2;
  std::string name2 = "Kobe Bryant";
  columnHint2.begin_value_ref() = Value(name2);
  columnHint2.column_name_ref() = "name";
  columnHint2.scan_type_ref() = cpp2::ScanType::PREFIX;
  std::vector<IndexColumnHint> columnHints1;
  columnHints1.emplace_back(std::move(columnHint1));
  std::vector<IndexColumnHint> columnHints2;
  columnHints2.emplace_back(std::move(columnHint2));

  cpp2::IndexQueryContext context1;
  context1.column_hints_ref() = std::move(columnHints1);
  context1.filter_ref() = "";
  context1.index_id_ref() = 1;
  std::vector<cpp2::StatProp> statProps;
  cpp2::IndexQueryContext context2;
  context2.column_hints_ref() = std::move(columnHints2);
  context2.filter_ref() = "";
  context2.index_id_ref() = 1;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  contexts.emplace_back(std::move(context2));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  cpp2::StatProp statProp1;
  statProp1.alias_ref() = "total age";
  const auto& exp1 = *TagPropertyExpression::make(pool, folly::to<std::string>(1), "age");
  statProp1.prop_ref() = Expression::encode(exp1);
  statProp1.stat_ref() = cpp2::StatType::SUM;
  statProps.emplace_back(statProp1);

  cpp2::StatProp statProp2;
  statProp2.alias_ref() = "max age";
  const auto& exp2 = *TagPropertyExpression::make(pool, folly::to<std::string>(1), "age");
  statProp2.prop_ref() = Expression::encode(exp2);
  statProp2.stat_ref() = cpp2::StatType::MAX;
  statProps.emplace_back(statProp2);

  cpp2::StatProp statProp3;
  statProp3.alias_ref() = "max kVid";
  const auto& exp3 = *TagPropertyExpression::make(pool, folly::to<std::string>(1), kVid);
  statProp3.prop_ref() = Expression::encode(exp3);
  statProp3.stat_ref() = cpp2::StatType::MAX;
  statProps.emplace_back(statProp3);

  cpp2::StatProp statProp4;
  statProp4.alias_ref() = "min age";
  const auto& exp4 = *TagPropertyExpression::make(pool, folly::to<std::string>(1), "age");
  statProp4.prop_ref() = Expression::encode(exp4);
  statProp4.stat_ref() = cpp2::StatType::MIN;
  statProps.emplace_back(statProp4);

  req.stat_columns_ref() = std::move(statProps);

  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();

  std::vector<std::string> expectCols = {
      std::string("1.").append(kVid), std::string("1.").append(kTag), "1.age"};
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

  std::vector<std::string> expectStatColumns;
  expectStatColumns.emplace_back("total age");
  expectStatColumns.emplace_back("max age");
  expectStatColumns.emplace_back("max kVid");
  expectStatColumns.emplace_back("min age");

  nebula::Row expectStatRow;
  expectStatRow.values.push_back(Value(75L));
  expectStatRow.values.push_back(Value(41L));
  expectStatRow.values.push_back(Value(vId1));
  expectStatRow.values.push_back(Value(34L));

  QueryTestUtils::checkStatResponse(resp, expectStatColumns, expectStatRow);
}

TEST_P(LookupIndexTest, AggregateEdgeIndexTest) {
  fs::TempDir rootPath("/tmp/AggregateEdgeIndexTest.XXXXXX");
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

  auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.edge_type_ref() = 102;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= totalParts; p++) {
    parts.emplace_back(p);
  }
  req.parts_ref() = std::move(parts);

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
  returnCols.emplace_back("startYear");
  req.return_columns_ref() = std::move(returnCols);

  // teammates.player1 == "Tony Parker"
  cpp2::IndexColumnHint columnHint1;
  columnHint1.begin_value_ref() = Value(tony);
  columnHint1.column_name_ref() = "player1";
  columnHint1.scan_type_ref() = cpp2::ScanType::PREFIX;
  // teammates.player1 == "Yao Ming"
  cpp2::IndexColumnHint columnHint2;
  columnHint2.begin_value_ref() = Value(yao);
  columnHint2.column_name_ref() = "player1";
  columnHint2.scan_type_ref() = cpp2::ScanType::PREFIX;
  std::vector<IndexColumnHint> columnHints1;
  columnHints1.emplace_back(std::move(columnHint1));
  std::vector<IndexColumnHint> columnHints2;
  columnHints2.emplace_back(std::move(columnHint2));

  cpp2::IndexQueryContext context1;
  context1.column_hints_ref() = std::move(columnHints1);
  context1.filter_ref() = "";
  context1.index_id_ref() = 102;
  cpp2::IndexQueryContext context2;
  context2.column_hints_ref() = std::move(columnHints2);
  context2.filter_ref() = "";
  context2.index_id_ref() = 102;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  contexts.emplace_back(std::move(context2));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  std::vector<cpp2::StatProp> statProps;
  cpp2::StatProp statProp1;
  statProp1.alias_ref() = "total startYear";
  const auto& exp1 = *EdgePropertyExpression::make(pool, folly::to<std::string>(102), "startYear");
  statProp1.prop_ref() = Expression::encode(exp1);
  statProp1.stat_ref() = cpp2::StatType::SUM;
  statProps.emplace_back(std::move(statProp1));

  cpp2::StatProp statProp2;
  statProp2.alias_ref() = "max kDst";
  const auto& exp2 = *EdgePropertyExpression::make(pool, folly::to<std::string>(102), kDst);
  statProp2.prop_ref() = Expression::encode(exp2);
  statProp2.stat_ref() = cpp2::StatType::MAX;
  statProps.emplace_back(std::move(statProp2));
  req.stat_columns_ref() = std::move(statProps);

  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                         std::string("102.").append(kType),
                                         std::string("102.").append(kRank),
                                         std::string("102.").append(kDst),
                                         "102.teamName",
                                         "102.startYear"};
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
  row1.emplace_back(Value(2002));
  expectRows.emplace_back(Row(row1));

  Row row2;
  row2.emplace_back(Value(vId2));
  row2.emplace_back(Value(102L));
  row2.emplace_back(Value(2002L));
  row2.emplace_back(Value(vId1));
  row2.emplace_back(Value("Spurs"));
  row2.emplace_back(Value(2002));
  expectRows.emplace_back(Row(row2));

  Row row3;
  row3.emplace_back(Value(vId3));
  row3.emplace_back(Value(102L));
  row3.emplace_back(Value(2004L));
  row3.emplace_back(Value(vId4));
  row3.emplace_back(Value("Rockets"));
  row3.emplace_back(Value(2004L));
  expectRows.emplace_back(Row(row3));

  Row row4;
  row4.emplace_back(Value(vId4));
  row4.emplace_back(Value(102L));
  row4.emplace_back(Value(2004L));
  row4.emplace_back(Value(vId3));
  row4.emplace_back(Value("Rockets"));
  row4.emplace_back(Value(2004L));
  expectRows.emplace_back(Row(row4));
  QueryTestUtils::checkResponse(resp, expectCols, expectRows);

  std::vector<std::string> expectStatColumns;
  expectStatColumns.emplace_back("total startYear");
  expectStatColumns.emplace_back("max kDst");

  nebula::Row expectStatRow;
  expectStatRow.values.push_back(Value(8012L));
  expectStatRow.values.push_back(Value(vId3));
  QueryTestUtils::checkStatResponse(resp, expectStatColumns, expectStatRow);
}

TEST_P(LookupIndexTest, AggregateEdgePropNotInReturnColumnsTest) {
  fs::TempDir rootPath("/tmp/AggregateEdgeIndexTest.XXXXXX");
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

  auto* processor = LookupProcessor::instance(env, nullptr, threadPool.get());
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.edge_type_ref() = 102;
  indices.schema_id_ref() = schemaId;
  std::vector<PartitionID> parts;
  for (int32_t p = 1; p <= totalParts; p++) {
    parts.emplace_back(p);
  }
  req.parts_ref() = std::move(parts);

  std::string tony = "Tony Parker";
  std::string manu = "Manu Ginobili";
  std::string yao = "Yao Ming";
  std::string tracy = "Tracy McGrady";
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kSrc);
  returnCols.emplace_back(kType);
  returnCols.emplace_back(kRank);
  returnCols.emplace_back(kDst);
  req.return_columns_ref() = std::move(returnCols);

  // teammates.player1 == "Tony Parker"
  cpp2::IndexColumnHint columnHint1;
  columnHint1.begin_value_ref() = Value(tony);
  columnHint1.column_name_ref() = "player1";
  columnHint1.scan_type_ref() = cpp2::ScanType::PREFIX;
  // teammates.player1 == "Yao Ming"
  cpp2::IndexColumnHint columnHint2;
  columnHint2.begin_value_ref() = Value(yao);
  columnHint2.column_name_ref() = "player1";
  columnHint2.scan_type_ref() = cpp2::ScanType::PREFIX;
  std::vector<IndexColumnHint> columnHints1;
  columnHints1.emplace_back(std::move(columnHint1));
  std::vector<IndexColumnHint> columnHints2;
  columnHints2.emplace_back(std::move(columnHint2));

  cpp2::IndexQueryContext context1;
  context1.column_hints_ref() = std::move(columnHints1);
  context1.filter_ref() = "";
  context1.index_id_ref() = 102;
  cpp2::IndexQueryContext context2;
  context2.column_hints_ref() = std::move(columnHints2);
  context2.filter_ref() = "";
  context2.index_id_ref() = 102;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  contexts.emplace_back(std::move(context2));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  std::vector<cpp2::StatProp> statProps;
  cpp2::StatProp statProp1;
  statProp1.alias_ref() = "total startYear";
  const auto& exp1 = *EdgePropertyExpression::make(pool, folly::to<std::string>(102), "startYear");
  statProp1.prop_ref() = Expression::encode(exp1);
  statProp1.stat_ref() = cpp2::StatType::SUM;
  statProps.emplace_back(std::move(statProp1));

  cpp2::StatProp statProp2;
  statProp2.alias_ref() = "count teamName";
  const auto& exp2 = *EdgePropertyExpression::make(pool, folly::to<std::string>(102), "teamName");
  statProp2.prop_ref() = Expression::encode(exp2);
  statProp2.stat_ref() = cpp2::StatType::COUNT;
  statProps.emplace_back(std::move(statProp2));
  req.stat_columns_ref() = std::move(statProps);

  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  std::vector<std::string> expectCols = {std::string("102.").append(kSrc),
                                         std::string("102.").append(kType),
                                         std::string("102.").append(kRank),
                                         std::string("102.").append(kDst)};
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
  expectRows.emplace_back(Row(row1));

  Row row2;
  row2.emplace_back(Value(vId2));
  row2.emplace_back(Value(102L));
  row2.emplace_back(Value(2002L));
  row2.emplace_back(Value(vId1));
  expectRows.emplace_back(Row(row2));

  Row row3;
  row3.emplace_back(Value(vId3));
  row3.emplace_back(Value(102L));
  row3.emplace_back(Value(2004L));
  row3.emplace_back(Value(vId4));
  expectRows.emplace_back(Row(row3));

  Row row4;
  row4.emplace_back(Value(vId4));
  row4.emplace_back(Value(102L));
  row4.emplace_back(Value(2004L));
  row4.emplace_back(Value(vId3));
  expectRows.emplace_back(Row(row4));
  QueryTestUtils::checkResponse(resp, expectCols, expectRows);

  std::vector<std::string> expectStatColumns;
  expectStatColumns.emplace_back("total startYear");
  expectStatColumns.emplace_back("count teamName");

  nebula::Row expectStatRow;
  expectStatRow.values.push_back(Value(8012L));
  expectStatRow.values.push_back(Value(4L));
  QueryTestUtils::checkStatResponse(resp, expectStatColumns, expectStatRow);
}

INSTANTIATE_TEST_SUITE_P(Lookup_concurrently, LookupIndexTest, ::testing::Values(false, true));

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
