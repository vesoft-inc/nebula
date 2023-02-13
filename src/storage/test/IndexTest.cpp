/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>
#include <gtest/gtest.h>
#include <s2/base/integral_types.h>

#include <limits>
#include <string>

#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"
#include "common/base/ObjectPool.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/geo/GeoIndex.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVIterator.h"
#include "storage/exec/IndexDedupNode.h"
#include "storage/exec/IndexEdgeScanNode.h"
#include "storage/exec/IndexLimitNode.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexProjectionNode.h"
#include "storage/exec/IndexSelectionNode.h"
#include "storage/exec/IndexVertexScanNode.h"
#include "storage/test/IndexTestUtil.h"
namespace nebula {
namespace storage {
namespace {
int schemaVer = 2;
using std::string_literals::operator""s;
}  // namespace
/**
 * IndexScanTest
 *
 * Test:
 * 1. Vertex/Edge
 * 2. back to table or not
 * 3. different value type
 *    a. int/float/bool/fix_string/time/date/datetime/geography
 *    b. compound index
 * 4. range/prefix
 *    a. prefix(equal)
 *    b. range with begin is include/exclude/-INF
 *    c. range with end id include/exclude/+INF
 * 5. nullable
 * 6. multiPart
 * Case:
 * ┌────────────┬───────────┬───────────────┬─────────────────────────┬─────────┐
 * │ section1   │ name      │    case       │     description         │  NOTICE │
 * ├────────────┼───────────┼───────────────┼─────────────────────────┼─────────┤
 * | Base       | Base      |               |                         |         |
 * |            ├───────────┼───────────────┼─────────────────────────┼─────────┤
 * |            | Vertex    | IndexOnly     |                         |         |
 * |            | Edge      | BackToTable   |                         |         |
 * |            ├───────────┼───────────────┼─────────────────────────┼─────────┤
 * |            | MultiPart |               |                         |         |
 * ├────────────┼───────────┼───────────────┼─────────────────────────┼─────────┤
 * | Value Type | Int       | Truncate      | Test different interval |         |
 * |            | Float     | NoTruncate    | with each type of Value |         |
 * |            | Bool      | INCLUDE_BEGIN |                         |         |
 * |            | String    | INCLUDE_END   |                         |         |
 * |            | FixString | EXCLUDE_BEGIN |                         |         |
 * |            | Time      | EXCLUDE_END   |                         |         |
 * |            | Date      | POSITIVE_INF  |                         |         |
 * |            | DateTime  | NEGATIVE_INF  |                         |         |
 * |            | Compound  |               |                         |         |
 * |            | Nullable  |               |                         |         |
 * |            | Geography |               |                         |         |
 * └────────────┴───────────┴───────────────┴─────────────────────────┴─────────┘
 */
class IndexScanTestHelper {
 public:
  void setIndex(IndexVertexScanNode* node, std::shared_ptr<::nebula::meta::cpp2::IndexItem> index) {
    node->getIndex = [index](std::shared_ptr<::nebula::meta::cpp2::IndexItem>& ret) {
      ret = index;
      return ::nebula::cpp2::ErrorCode::SUCCEEDED;
    };
  }

  void setIndex(IndexEdgeScanNode* node, std::shared_ptr<::nebula::meta::cpp2::IndexItem> index) {
    node->getIndex = [index](std::shared_ptr<::nebula::meta::cpp2::IndexItem>& ret) {
      ret = index;
      return ::nebula::cpp2::ErrorCode::SUCCEEDED;
    };
  }

  void setTag(IndexVertexScanNode* node,
              std::shared_ptr<::nebula::meta::NebulaSchemaProvider> schema) {
    node->getTag = [schema](IndexVertexScanNode::TagSchemas& tag) {
      tag = std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>{schema};
      return ::nebula::cpp2::ErrorCode::SUCCEEDED;
    };
  }

  void setEdge(IndexEdgeScanNode* node,
               std::shared_ptr<::nebula::meta::NebulaSchemaProvider> schema) {
    node->getEdge = [schema](IndexEdgeScanNode::EdgeSchemas& edge) {
      edge = std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>{schema};
      return ::nebula::cpp2::ErrorCode::SUCCEEDED;
    };
  }

  void setFatal(IndexScanNode* node, bool value) {
    node->fatalOnBaseNotFound_ = value;
  }
};

class IndexScanTest : public ::testing::Test {
 protected:
  using Schema = ::nebula::meta::NebulaSchemaProvider;
  using IndexItem = ::nebula::meta::cpp2::IndexItem;
  using ColumnHint = ::nebula::storage::cpp2::IndexColumnHint;

  static ColumnHint makeColumnHint(const std::string& name, const Value& value) {
    ColumnHint hint;
    hint.column_name_ref() = name;
    hint.begin_value_ref() = value;
    hint.scan_type_ref() = cpp2::ScanType::PREFIX;
    return hint;
  }

  template <bool includeBegin, bool includeEnd>
  static ColumnHint makeColumnHint(const std::string& name, const Value& begin, const Value& end) {
    ColumnHint hint;
    hint.column_name_ref() = name;
    hint.scan_type_ref() = cpp2::ScanType::RANGE;
    hint.begin_value_ref() = begin;
    hint.end_value_ref() = end;
    hint.include_begin_ref() = includeBegin;
    hint.include_end_ref() = includeEnd;
    return hint;
  }

  template <bool include>
  static ColumnHint makeBeginColumnHint(const std::string& name, const Value& begin) {
    ColumnHint hint;
    hint.column_name_ref() = name;
    hint.scan_type_ref() = cpp2::ScanType::RANGE;
    hint.begin_value_ref() = begin;
    hint.include_begin_ref() = include;
    return hint;
  }

  template <bool include>
  static ColumnHint makeEndColumnHint(const std::string& name, const Value& end) {
    ColumnHint hint;
    hint.column_name_ref() = name;
    hint.scan_type_ref() = cpp2::ScanType::RANGE;
    hint.end_value_ref() = end;
    hint.include_end_ref() = include;
    return hint;
  }

  static std::vector<std::map<std::string, std::string>> encodeTag(
      const std::vector<Row>& rows,
      TagID tagId,
      std::shared_ptr<Schema> schema,
      std::vector<std::shared_ptr<IndexItem>> indices) {
    std::vector<std::map<std::string, std::string>> ret(indices.size() + 1);
    for (size_t i = 0; i < rows.size(); i++) {
      auto key = NebulaKeyUtils::tagKey(8, 0, std::to_string(i), tagId);
      RowWriterV2 writer(schema.get());
      for (size_t j = 0; j < rows[i].size(); j++) {
        writer.setValue(j, rows[i][j]);
      }
      writer.finish();
      auto value = writer.moveEncodedStr();
      CHECK(ret[0].insert({key, value}).second);
      RowReaderWrapper reader(schema.get(), folly::StringPiece(value), schemaVer);
      auto ttlProperty = CommonUtils::ttlValue(schema.get(), reader.get());
      auto ttlValue = ttlProperty.ok() ? IndexKeyUtils::indexVal(ttlProperty.value()) : "";
      for (size_t j = 0; j < indices.size(); j++) {
        auto& index = indices[j];
        auto indexValue = IndexKeyUtils::collectIndexValues(&reader, index.get()).value();
        auto indexKeys = IndexKeyUtils::vertexIndexKeys(
            8, 0, index->get_index_id(), std::to_string(i), std::move(indexValue));
        for (auto& indexKey : indexKeys) {
          CHECK(ret[j + 1].insert({indexKey, ttlValue}).second);
        }
      }
    }
    return ret;
  }

  static std::vector<std::map<std::string, std::string>> encodeEdge(
      const std::vector<Row>& rows,
      EdgeType edgeType,
      std::shared_ptr<Schema> schema,
      std::vector<std::shared_ptr<IndexItem>> indices) {
    std::vector<std::map<std::string, std::string>> ret(indices.size() + 1);
    for (size_t i = 0; i < rows.size(); i++) {
      auto key = NebulaKeyUtils::edgeKey(8, 0, std::to_string(i), edgeType, i, std::to_string(i));
      RowWriterV2 writer(schema.get());
      for (size_t j = 0; j < rows[i].size(); j++) {
        writer.setValue(j, rows[i][j]);
      }
      writer.finish();
      auto value = writer.moveEncodedStr();
      CHECK(ret[0].insert({key, value}).second);
      RowReaderWrapper reader(schema.get(), folly::StringPiece(value), schemaVer);
      for (size_t j = 0; j < indices.size(); j++) {
        auto& index = indices[j];
        auto indexValue = IndexKeyUtils::collectIndexValues(&reader, index.get()).value();
        auto indexKeys = IndexKeyUtils::edgeIndexKeys(8,
                                                      0,
                                                      index->get_index_id(),
                                                      std::to_string(i),
                                                      i,
                                                      std::to_string(i),
                                                      std::move(indexValue));
        for (auto& indexKey : indexKeys) {
          CHECK(ret[j + 1].insert({indexKey, ""}).second);
        }
      }
    }
    return ret;
  }

  static PlanContext* getPlanContext() {
    static std::unique_ptr<PlanContext> ctx = std::make_unique<PlanContext>(nullptr, 0, 8, false);
    return ctx.get();
  }

  static std::unique_ptr<RuntimeContext> makeContext(TagID tagId, EdgeType edgeType) {
    auto ctx = std::make_unique<RuntimeContext>(getPlanContext());
    ctx->tagId_ = tagId;
    ctx->edgeType_ = edgeType;
    return ctx;
  }
};

class IndexScanScalarType : public IndexScanTest {
 protected:
  void checkTag(MockKVStore* kvstore,
                std::shared_ptr<Schema> schema,
                std::shared_ptr<IndexItem> index,
                const std::vector<ColumnHint>& columnHints,
                const std::vector<Row>& expect,
                const std::string& case_,
                bool setFatal = false) {
    bool hasNullableCol = schema->hasNullableCol();
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), 0, columnHints, kvstore, hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), index);
    helper.setTag(scanNode.get(), schema);
    if (setFatal) {
      helper.setFatal(scanNode.get(), true);
    }
    InitContext initCtx;
    initCtx.requiredColumns = {kVid};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    EXPECT_EQ(result, expect) << "Fail at case " << case_;
  }

  void checkEdge(MockKVStore* kvstore,
                 std::shared_ptr<Schema> schema,
                 std::shared_ptr<IndexItem> index,
                 const std::vector<ColumnHint>& columnHints,
                 const std::vector<Row>& expect,
                 const std::string& case_) {
    bool hasNullableCol = schema->hasNullableCol();
    auto context = makeContext(0, 1);
    auto scanNode =
        std::make_unique<IndexEdgeScanNode>(context.get(), 0, columnHints, kvstore, hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), index);
    helper.setEdge(scanNode.get(), schema);
    helper.setFatal(scanNode.get(), true);
    InitContext initCtx;
    initCtx.requiredColumns = {kSrc};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    EXPECT_EQ(result, expect) << "Fail at case " << case_;
  }

  template <typename... Args>
  std::vector<Row> expect(Args... vidList) {
    std::vector<Row> ret;
    std::vector<Value> value;
    (value.push_back(std::to_string(vidList)), ...);
    for (auto& v : value) {
      Row row;
      row.emplace_back(v);
      ret.emplace_back(std::move(row));
    }
    return ret;
  }
};

class IndexScanStringType : public IndexScanTest {
 protected:
  void checkString(MockKVStore* kvstore,
                   std::shared_ptr<Schema> schema,
                   std::shared_ptr<IndexItem> index,
                   const std::vector<ColumnHint>& columnHints,
                   const std::vector<std::string>& acquiredColumns,
                   const std::vector<Row>& expect,
                   const std::string& case_) {
    bool hasNullableCol = schema->hasNullableCol();
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), 0, columnHints, kvstore, hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), index);
    helper.setTag(scanNode.get(), schema);
    helper.setFatal(scanNode.get(), true);
    InitContext initCtx;
    initCtx.requiredColumns.insert(acquiredColumns.begin(), acquiredColumns.end());
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    std::vector<Row> actual(result.size());
    for (size_t j = 0; j < acquiredColumns.size(); j++) {
      int p = initCtx.retColMap[acquiredColumns[j]];
      for (size_t i = 0; i < result.size(); i++) {
        actual[i].emplace_back(result[i][p]);
      }
    }
    EXPECT_EQ(actual, expect) << "Fail at case " << case_;
  }

  std::vector<Row> expect(const std::vector<Row>& rows,
                          const std::vector<int64_t>& vidList,
                          const std::vector<size_t>& columns) {
    std::vector<Row> ret;
    for (size_t i = 0; i < vidList.size(); i++) {
      Row row;
      row.emplace_back(Value(std::to_string(vidList[i])));
      for (size_t j = 0; j < columns.size(); j++) {
        row.emplace_back(rows[vidList[i]][columns[j]]);
      }
      ret.emplace_back(std::move(row));
    }
    return ret;
  }
};

TEST_F(IndexScanTest, Base) {
  auto rows = R"(
    int | int
    1   | 2
    1   | 3
  )"_row;
  auto schema = R"(
    a   | int  ||false
    b   | int  ||false
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (i1,2):a
    (i2,3):b
  )"_index(schema);
  bool hasNullableCol = schema->hasNullableCol();
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  {  // Case 1
    std::vector<ColumnHint> columnHints{
        makeColumnHint("a", Value(1))  // a=1
    };
    IndexID indexId = 2;
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), indexId, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), indices[0]);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "a"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    auto expect = R"(
      string | int
      0   | 1
      1   | 1
    )"_row;
    std::vector<std::string> colOrder = {kVid, "a"};
    ASSERT_EQ(result.size(), expect.size());
    for (size_t i = 0; i < result.size(); i++) {
      ASSERT_EQ(result[i].size(), expect[i].size());
      for (size_t j = 0; j < expect[i].size(); j++) {
        ASSERT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }  // End of Case 1
  {  // Case 2
    std::vector<ColumnHint> columnHints{
        makeColumnHint("b", Value(3))  // b=3
    };
    IndexID indexId = 3;
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), indexId, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), indices[1]);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "b"};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    auto expect = R"(
      string | int
      1   | 3
    )"_row;
    std::vector<std::string> colOrder = {kVid, "b"};
    ASSERT_EQ(result.size(), expect.size());
    for (size_t i = 0; i < result.size(); i++) {
      ASSERT_EQ(result[i].size(), expect[i].size());
      for (size_t j = 0; j < expect[i].size(); j++) {
        ASSERT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }  // End of Case 2
}

TEST_F(IndexScanTest, Vertex) {
  auto rows = R"(
    int | int
    1   | 2
    1   | 3
  )"_row;
  auto schema = R"(
    a   | int | | false
    b   | int | | false
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (i1,2):a
  )"_index(schema);
  bool hasNullableCol = schema->hasNullableCol();
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  std::vector<ColumnHint> columnHints{
      makeColumnHint("a", Value(1))  // a=1
  };
  IndexID indexId = 0;
  auto context = makeContext(1, 0);
  {  // Case 1: IndexOnly
    // Only put index key-values into kvstore
    for (auto& item : kv[1]) {
      kvstore->put(item.first, item.second);
    }
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), indexId, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), indices[0]);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "a"};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    auto expect = R"(
      string | int
      0   | 1
      1   | 1
    )"_row;
    std::vector<std::string> colOrder = {kVid, "a"};
    ASSERT_EQ(result.size(), expect.size());
    for (size_t i = 0; i < result.size(); i++) {
      ASSERT_EQ(result[i].size(), expect[i].size());
      for (size_t j = 0; j < expect[i].size(); j++) {
        ASSERT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }  // End of Case 1
  {  // Case 2: Access base data
    // Put base data key-values into kvstore
    for (auto& item : kv[0]) {
      kvstore->put(item.first, item.second);
    }
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), indexId, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), indices[0]);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "b"};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    auto expect = R"(
      string | int
      0   | 2
      1   | 3
    )"_row;
    std::vector<std::string> colOrder = {kVid, "b"};
    ASSERT_EQ(result.size(), expect.size());
    for (size_t i = 0; i < result.size(); i++) {
      ASSERT_EQ(result[i].size(), expect[i].size());
      VLOG(1) << result[i];
      for (size_t j = 0; j < expect[i].size(); j++) {
        ASSERT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }  // End of Case 2
}

TEST_F(IndexScanTest, Edge) {
  auto rows = R"(
    int | int | int
    5   | 2   | 1
    10  | 3   | 2
    20  | 3   | 3
  )"_row;
  auto schema = R"(
    a   | int | | false
    b   | int | | false
    c   | int | | false
  )"_schema;
  auto indices = R"(
    EDGE(e,1)
    (i1,2):b,c
  )"_index(schema);
  bool hasNullableCol = schema->hasNullableCol();
  auto kv = encodeEdge(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  std::vector<ColumnHint> columnHints{
      makeColumnHint("b", Value(3)),  // b=3
  };
  IndexID indexId = 0;
  auto context = makeContext(0, 1);
  {  // Case 1: IndexOnly
    for (auto& item : kv[1]) {
      kvstore->put(item.first, item.second);
    }
    auto scanNode = std::make_unique<IndexEdgeScanNode>(
        context.get(), indexId, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), indices[0]);
    helper.setEdge(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kSrc, kRank, kDst, "c"};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    auto expect = R"(
      string | int  | string | int
      1      | 1    | 1      | 2
      2      | 2    | 2      | 3
    )"_row;
    std::vector<std::string> colOrder = {kSrc, kRank, kDst, "c"};
    ASSERT_EQ(result.size(), expect.size());
    for (size_t i = 0; i < result.size(); i++) {
      ASSERT_EQ(result[i].size(), expect[i].size());
      for (size_t j = 0; j < expect[i].size(); j++) {
        EXPECT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }  // End of Case 1
  {  // Case 2: Access base data
    for (auto& item : kv[0]) {
      kvstore->put(item.first, item.second);
    }
    auto scanNode = std::make_unique<IndexEdgeScanNode>(
        context.get(), indexId, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), indices[0]);
    helper.setEdge(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kSrc, kRank, kDst, "a"};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    auto expect = R"(
      string | int  | string | int
      1      | 1    | 1      | 10
      2      | 2    | 2      | 20
    )"_row;
    std::vector<std::string> colOrder = {kSrc, kRank, kDst, "a"};
    ASSERT_EQ(result.size(), expect.size());
    for (size_t i = 0; i < result.size(); i++) {
      ASSERT_EQ(result[i].size(), expect[i].size());
      for (size_t j = 0; j < expect[i].size(); j++) {
        EXPECT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }
}

TEST_F(IndexScanScalarType, Int) {
  auto rows = R"(
    int | int                  | int
    1   | -1                   | -10
    2   | 1                    | -9223372036854775808
    3   | 0                    | -1
    4   | 9223372036854775807  | 0
    5   | -9223372036854775808 | 9223372036854775807
    6   | <null>               | 0
  )"_row;
  auto schema = R"(
    a   | int | | false
    b   | int | | true
    c   | int | | false
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (i1,2):a
    (i2,3):b
    (i3,4):c
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  const int64_t MAX = 0x7fffffffffffffff;
  const int64_t MIN = -MAX - 1;
  /* Case 1: Prefix */
  {
    std::vector<ColumnHint> columnHints = {makeColumnHint("a", 1)};                     // a=1;
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0), "case1.1");     //
    columnHints = {makeColumnHint("b", MAX)};                                           // b=MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3), "case1.2");     //
    columnHints = {makeColumnHint("b", MIN)};                                           // b=MIN
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4), "case1.3");     //
    columnHints = {makeColumnHint("c", 0)};                                             // c=0
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(3, 5), "case1.4");  //
  }  // End of Case 1
  /* Case 2: [x, INF) */
  {
    std::vector<ColumnHint> columnHints = {makeBeginColumnHint<true>("a", -1)};  // Case2.1: a >= -1
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case2.1");  //
    columnHints = {makeBeginColumnHint<true>("a", 4)};  // Case2.2: a>=4
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5), "case2.2");  //
    columnHints = {makeBeginColumnHint<true>("a", 7)};                        // Case2.3: a>=7
    checkTag(kvstore.get(), schema, indices[0], columnHints, {}, "case2.3");  //
    columnHints = {makeBeginColumnHint<true>("b", MIN)};                      // Case2.4: b>=INT_MIN
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 0, 2, 1, 3), "case2.4");  //
    columnHints = {makeBeginColumnHint<true>("b", MAX)};  // Case2.5: b>=INT_MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3), "case2.5");  //
    columnHints = {makeBeginColumnHint<true>("b", 0)};  // Case2.6: b>=0
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(2, 1, 3), "case2.6");  //
    columnHints = {makeBeginColumnHint<true>("c", MIN)};  // Case2.7: c>=INT_MIN
    checkTag(
        kvstore.get(), schema, indices[2], columnHints, expect(1, 0, 2, 3, 5, 4), "case2.7");  //
    columnHints = {makeBeginColumnHint<true>("c", MAX)};  // Case2.8: c>=INT_MAX
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(4), "case2.8");  //
    columnHints = {makeBeginColumnHint<true>("c", 0)};  // Case2.9: c>=0
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(3, 5, 4), "case2.9");  //
  }  // End of Case 2
  /* Case 3: [x, y) */
  {
    std::vector<ColumnHint> columnHints;                       //
    columnHints = {makeColumnHint<true, false>("a", -1, 10)};  // Case3.1: -1<=a<10
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case3.1");  //
    columnHints = {makeColumnHint<true, false>("a", -100, 4)};  // Case3.2: -100<=a<4
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2), "case3.2");  //
    columnHints = {makeColumnHint<true, false>("a", 4, 100)};  // Case3.3: 4<=a<100
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5), "case3.3");  //
    columnHints = {makeColumnHint<true, false>("a", 2, 5)};  // Case3.4: 2<=a<5
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3), "case3.4");  //
    columnHints = {makeColumnHint<true, false>("a", -100, 0)};                // Case3.5: -100<=a<0
    checkTag(kvstore.get(), schema, indices[0], columnHints, {}, "case3.5");  //
    columnHints = {makeColumnHint<true, false>("a", 10, 100)};                // Case3.6: 10<=a<100
    checkTag(kvstore.get(), schema, indices[0], columnHints, {}, "case3.6");  //
    columnHints = {makeColumnHint<true, false>("b", MIN, MAX)};               // Case3.7: MIN<=b<MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 0, 2, 1), "case3.7");  //
    columnHints = {makeColumnHint<true, false>("c", MIN, MAX)};  // Case3.8: MIN<=c<MAX
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(1, 0, 2, 3, 5), "case3.8");  //
  }  // End of Case 3
  /* Case 4: (x, INF) */
  {
    std::vector<ColumnHint> columnHints;                 //
    columnHints = {makeBeginColumnHint<false>("a", 3)};  // Case 4.1: a>3
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5), "case4.1");  //
    columnHints = {makeBeginColumnHint<false>("b", MIN)};  // Case 4.2: b>MIN
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 2, 1, 3), "case4.2");  //
    columnHints = {makeBeginColumnHint<false>("b", MAX)};                     // Case4.3: b>MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, {}, "case4.3");  //
    columnHints = {makeBeginColumnHint<false>("c", MIN)};                     // Case4.4: c>MIN
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(0, 2, 3, 5, 4), "case4.4");  //
    columnHints = {makeBeginColumnHint<false>("c", MAX - 1)};  // Case4.5: c>MAX-1
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(4), "case4.4");  //
  }  // End of Case 4
  /* Case 5: (x, y] */
  {
    std::vector<ColumnHint> columnHints;                     //
    columnHints = {makeColumnHint<false, true>("a", 1, 6)};  // Case5.1: 1<a<=6
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 4, 5), "case5.1");  //
    columnHints = {makeColumnHint<false, true>("a", 0, 3)};  // Case5.2: 0<a<=3
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2), "case5.2");  //
    columnHints = {makeColumnHint<false, true>("b", MIN, MIN)};               // Case5.3: MIN<b<=MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, {}, "case5.3");  //
    columnHints = {makeColumnHint<false, true>("b", MAX, MAX)};               // Case5.4: MAX<b<=MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, {}, "case5.4");  //
    columnHints = {makeColumnHint<false, true>("b", 0, MAX)};                 // Case5.5: 0<b<=MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 3), "case5.5");  //
    columnHints = {makeColumnHint<false, true>("c", -1, MAX)};  // Case5.6: -1<c<=MAX
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(3, 5, 4), "case5.6");  //
  }  // End of Case 5
  /* Case 6: (-INF,y]*/
  {
    std::vector<ColumnHint> columnHints;              //
    columnHints = {makeEndColumnHint<true>("a", 4)};  // Case6.1: a<=4
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3), "case6.1");  //
    columnHints = {makeEndColumnHint<true>("a", 1)};  // Case6.2: a<=1
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0), "case6.2");  //
    columnHints = {makeEndColumnHint<true>("b", MIN)};  // Case6.3: b<=MIN
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4), "case6.3");  //
    columnHints = {makeEndColumnHint<true>("b", MAX)};  // Case6.4: b<=MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 0, 2, 1, 3), "casae6.4");  //
    columnHints = {makeEndColumnHint<true>("c", MIN)};  // Case6.5: c<=MIN
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(1), "case6.5");  //
    columnHints = {makeEndColumnHint<true>("c", MAX)};  // Case6.6: c<=MAX
    checkTag(
        kvstore.get(), schema, indices[2], columnHints, expect(1, 0, 2, 3, 5, 4), "case6.6");  //
  }  // End of Case 6
  /* Case 7: (-INF, y) */
  {
    std::vector<ColumnHint> columnHints;               //
    columnHints = {makeEndColumnHint<false>("a", 4)};  // Case7.1: a<4
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2), "case7.1");  //
    columnHints = {makeEndColumnHint<false>("a", 1)};                         // Case7.2: a<1
    checkTag(kvstore.get(), schema, indices[0], columnHints, {}, "case7.2");  //
    columnHints = {makeEndColumnHint<false>("b", MIN)};                       // Case7.3: b<MIN
    checkTag(kvstore.get(), schema, indices[1], columnHints, {}, "case7.3");  //
    columnHints = {makeEndColumnHint<false>("b", MAX)};                       // Case7.4: b<MAX
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 0, 2, 1), "casae7.4");  //
    columnHints = {makeEndColumnHint<false>("c", MIN)};                       // Case7.5: c<MIN
    checkTag(kvstore.get(), schema, indices[2], columnHints, {}, "case7.5");  //
    columnHints = {makeEndColumnHint<false>("c", MAX)};                       // Case7.6: c<MAX
    checkTag(kvstore.get(), schema, indices[2], columnHints, expect(1, 0, 2, 3, 5), "case7.6");  //
  }  // End of Case 7
}

TEST_F(IndexScanScalarType, Float) {
  /**
   * Some special values
   *  Value                     | Desc | IEEE 754                 |  Our encode
   * 0.0                                0000 0000 0000 0000       |
   * -0.0                               8000 0000 0000 0000
   * +INF                               7FF0 0000 0000 0000       FFF0 0000 0000 0000
   * -INF                               FFF0 0000 0000 0000
   * 4.9406564584124654e-324            0000 0000 0000 0001   8000 0000 0000 0001
   * 2.2250738585072009e-308            000F FFFF FFFF FFFF   800F FFFF FFFF FFFF
   * 2.2250738585072014e−308            0010 0000 0000 0000   8010 0000 0000 0000
   * 1.7976931348623157e+308            7FEF FFFF FFFF FFFF   FFEF FFFF FFFF FFFF
   * Min subnormal positive double
   * reference: https://en.wikipedia.org/wiki/Double-precision_floating-point_format
   *
   * WARNING: There are some wrong in encode Double type.
   *          https://github.com/vesoft-inc/nebula/issues/3034
   */
#define FIX_ISSUE_3034 1
  const double MIN_SV = 4.9406564584124654e-324;  // MIN_SUBNORMAL_VALUE
  const double MAX_SV = 2.2250738585072009e-308;  // MAX_SUBNORMAL_VALUE
  const double MIN_NV = 2.2250738585072014e-308;  // MIN_NORMAL_VALUE
  const double MAX_NV = 1.7976931348623157e+308;  // MAX_NORMAL_VALUE
  const double INF = std::numeric_limits<double>::infinity();
  auto rows = R"(
float     | float                     | float                   | int
-100.0    | 0.0                       | <null>                  | 0
-20.0     | -0.0                      | <null>                  | 1
-5.0      | <INF>                     | 1.7976931348623157e+308 | 2
-0.0      | <-INF>                    | 1.7976931348623157e+308 | 3
0.0       | <NaN>                     | <INF>                   | 4
1.234e10  | <-NaN>                    | <INF>                   | 5
5.0       | 4.9406564584124654e-324   | <-INF>                  | 6
20.0      | -4.9406564584124654e-324  | <-INF>                  | 7
100.0     | 2.2250738585072009e-308   | <NaN>                   | 8
1.2345e10 | -2.2250738585072009e-308  | <NaN>                   | 9
-7e-10    | 2.2250738585072014e-308   | <-NaN>                  | 10
7e10      | -2.2250738585072014e-308  | <-NaN>                  | 11
-7e10     | 1.7976931348623157e+308   | -0.0                    | 12
7e-10     | -1.7976931348623157e+308  | 0.0                     | 13
  )"_row;
  auto schema = R"(
    a | double | | false
    b | double | | false
    c | double | | true
  )"_schema;
  auto indices = R"(
    EDGE(e,1)
    (i1,2):a
    (i2,3):b
    (i3,4):c
  )"_index(schema);
  auto kv = encodeEdge(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  /* Case 1: prefix */ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", 1000.0), {}, "case1.1");  // Case1.1: a=1000.0
    checkEdge(kvstore.get(),
              schema,
              indices[0],
              hint("a", 0.0),
              expect(3, 4),
              "case1.2");  // Case1.2: a=0.0
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", MAX_NV),
              expect(12),
              "case1.3");  // Case1.3: b=MAX_NV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", MIN_NV),
              expect(10),
              "case1.4");  // Case1.4: b=MIN_NV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", MAX_SV),
              expect(8),
              "case1.5");  // Case1.5: b=MAX_SV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", MIN_SV),
              expect(6),
              "case1.6");  // Case1.6: b=MIN_SV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", -MAX_NV),
              expect(13),
              "case1.7");  // Case1.7: b=-MAX_NV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", -MIN_NV),
              expect(11),
              "case1.8");  // Case1.8: b=-MIN_NV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", -MAX_SV),
              expect(9),
              "case1.9");  // Case1.9: b=-MAX_SV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", -MIN_SV),
              expect(7),
              "case1.10");  // Case1.10 b=-MIN_SV
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", 0.0),
              expect(0, 1),
              "case1.11");  // Case1.11: b=0.0
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", -0.0),
              expect(0, 1),
              "case1.12");  // Case1.12: b=-0.0
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", INF),
              expect(2),
              "case1.13");  // Case1.13: b=<INF>
    checkEdge(kvstore.get(),
              schema,
              indices[1],
              hint("b", -INF),
              expect(3),
              "case1.14");  // Case1.14: b=<-INF>
    checkEdge(kvstore.get(),
              schema,
              indices[2],
              hint("c", INF),
              expect(4, 5),
              "case1.15");  // Case1.15: c=<INF>
  }                         // End of Case 1
  //                   0   1  2  3  4   5  6  7   8  9  10 11 12 13
  auto aOrder = expect(12, 0, 1, 2, 10, 3, 4, 13, 6, 7, 8, 5, 9, 11);
  auto bOrder = expect(3, 13, 11, 9, 7, 0, 1, 6, 8, 10, 12, 2);
  auto cOrder = expect(6, 7, 12, 13, 2, 3, 4, 5);
  /* Case 2: [x, INF) */ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<true>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t start) {
      return decltype(all){all.begin() + start, all.end()};
    };
    checkEdge(kvstore.get(),
              schema,
              indices[0],
              hint("a", -100.0),
              slice(aOrder, 1),
              "case2.1");  // Case 2.1: a>=-100
    checkEdge(kvstore.get(),
              schema,
              indices[0],
              hint("a", 0.0),
              slice(aOrder, 5),
              "case2.2");  // Case 2.2: a>=0.0
    // Case 2.3~2.14: a>={each of $val}
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case2.{}", i + 3);
      auto offset = i;
      if (val[i] == 0 && val[i - 1] == 0) {
        offset--;
      }
      checkEdge(kvstore.get(), schema, indices[1], hint("b", val[i]), slice(bOrder, offset), case_);
    }
    checkEdge(kvstore.get(), schema, indices[2], hint("c", -INF), slice(cOrder, 0), "case2.15");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", 0.0), slice(cOrder, 2), "case2.16");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", MAX_NV), slice(cOrder, 4), "case2.17");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", INF), slice(cOrder, 6), "case2.18");
  }
  /* Case 3: [x, y)*/ {
    auto hint = [](const char* name, double left, double right) {
      return std::vector<ColumnHint>{makeColumnHint<true, false>(name, left, right)};
    };
    auto slice = [](decltype(aOrder) all, size_t start, size_t end) {
      return decltype(all){all.begin() + start, all.begin() + std::min(end, all.size())};
    };
    checkEdge(kvstore.get(),
              schema,
              indices[0],
              hint("a", -100.0, -0.0),
              slice(aOrder, 1, 5),
              "case3.1");  // Case3.1:-100<=a<0
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", 10, 1e9), slice(aOrder, 9, 11), "case3.2");
    checkEdge(kvstore.get(), schema, indices[0], hint("a", 1, 2), {}, "case3.3");
    checkEdge(kvstore.get(), schema, indices[0], hint("a", -INF, INF), aOrder, "case3.4");
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", -INF, 0), slice(aOrder, 0, 5), "case3.5");
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", 0, INF), slice(aOrder, 5, 14), "case3.6");
    // Case 3.7~3.18: b<{each of $val}
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case3.{}", i + 7);
      auto offset = i;
      if (val[i] == 0 && val[i - 1] == 0) {
        offset--;
      }
      checkEdge(kvstore.get(),
                schema,
                indices[1],
                hint("b", -INF, val[i]),
                slice(bOrder, 0, offset),
                case_);
    }
    checkEdge(
        kvstore.get(), schema, indices[2], hint("c", -INF, INF), slice(cOrder, 0, 6), "case3.19");
  }
  /* Case 4: (x, INF)*/ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<false>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t start) {
      return decltype(all){all.begin() + start, all.end()};
    };
    checkEdge(kvstore.get(), schema, indices[0], hint("a", 100), slice(aOrder, 11), "case4.1");
    checkEdge(kvstore.get(), schema, indices[1], hint("b", INF), {}, "case4.2");
    int64_t x;
    ::memcpy(&x, &INF, 8);
    // int64_t x = *reinterpret_cast<const int64_t*>(&INF);
    x--;
    double y;
    ::memcpy(&y, &x, 8);
    // double y = *reinterpret_cast<double*>(&x);
    checkEdge(kvstore.get(), schema, indices[1], hint("b", y), slice(bOrder, 11), "case4.3");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", INF), {}, "case4.4");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", y), slice(cOrder, 6), "case4.5");
  } /* Case 5: (x, y]*/
  {
    auto hint = [](const char* name, double left, double right) {
      return std::vector<ColumnHint>{makeColumnHint<false, true>(name, left, right)};
    };
    auto slice = [](decltype(aOrder) all, size_t start, size_t end) {
      return decltype(all){all.begin() + start, all.begin() + end};
    };
    checkEdge(kvstore.get(),
              schema,
              indices[0],
              hint("a", -100.0, -0.0),
              slice(aOrder, 2, 7),
              "case5.1");  // Case3.1:-100<=a<0
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", 10, 1e9), slice(aOrder, 9, 11), "case5.2");
    checkEdge(kvstore.get(), schema, indices[0], hint("a", 1, 2), {}, "case5.3");
    checkEdge(kvstore.get(), schema, indices[0], hint("a", -INF, INF), aOrder, "case5.4");
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", -INF, 0), slice(aOrder, 0, 7), "case5.5");
    checkEdge(
        kvstore.get(), schema, indices[0], hint("a", 0, INF), slice(aOrder, 7, 14), "case5.6");
    // Case 5.7~5.18: b>{each of $val}
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case5.{}", i + 7);
      auto offset = i + 1;
      if (val[i] == 0 && val[i + 1] == 0) {
        offset++;
      }
      checkEdge(kvstore.get(),
                schema,
                indices[1],
                hint("b", val[i], INF),
                slice(bOrder, offset, bOrder.size()),
                case_);
    }
    checkEdge(
        kvstore.get(), schema, indices[2], hint("c", -INF, INF), slice(cOrder, 2, 8), "case5.19");
  } /* Case 6: (-INF, y]*/
  {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeEndColumnHint<true>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t end) {
      return decltype(all){all.begin(), all.begin() + end};
    };
    checkEdge(kvstore.get(), schema, indices[0], hint("a", 0), slice(aOrder, 7), "case6.1");
    checkEdge(kvstore.get(), schema, indices[0], hint("a", -0.0), slice(aOrder, 7), "case6.2");
    checkEdge(kvstore.get(), schema, indices[0], hint("a", -100.0), slice(aOrder, 2), "case6.3");
    // Case 6.4~6.15
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case6.{}", i + 3);
      auto offset = i + 1;
      if (val[i] == 0 && val[i + 1] == 0) {
        offset++;
      }
      checkEdge(kvstore.get(), schema, indices[1], hint("b", val[i]), slice(bOrder, offset), case_);
    }
    checkEdge(kvstore.get(), schema, indices[2], hint("c", INF), cOrder, "case6.16");
  }
  /* Case 7: (-INF, y)*/ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeEndColumnHint<false>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t end) {
      return decltype(all){all.begin(), all.begin() + end};
    };
    checkEdge(kvstore.get(), schema, indices[0], hint("a", 100), slice(aOrder, 10), "case7.1");
    checkEdge(kvstore.get(), schema, indices[1], hint("b", -INF), {}, "case7.2");
    int64_t x;
    ::memcpy(&x, &INF, 8);
    // int64_t x = *reinterpret_cast<const int64_t*>(&INF);
    x--;
    double y;
    ::memcpy(&y, &x, 8);
    // double y = *reinterpret_cast<double*>(&x);
    checkEdge(kvstore.get(), schema, indices[1], hint("b", -y), slice(bOrder, 1), "case7.3");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", -INF), {}, "case7.4");
    checkEdge(kvstore.get(), schema, indices[2], hint("c", -y), slice(cOrder, 2), "case7.5");
  }
}

TEST_F(IndexScanScalarType, Bool) {
  auto rows = R"(
    bool  | bool
    true  | true
    true  | false
    false | <null>
    false | false
    true  | <null>
  )"_row;
  auto schema = R"(
    a | bool  | |
    b | bool  | | true
  )"_schema;
  auto indices = R"(
    TAG(t,2)
    (i1,2):a
    (i2,3):b
  )"_index(schema);
  auto kv = encodeTag(rows, 2, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  /* Case 1: Prefix */ {
    checkTag(
        kvstore.get(), schema, indices[0], {makeColumnHint("a", true)}, expect(0, 1, 4), "case1.1");
    checkTag(
        kvstore.get(), schema, indices[0], {makeColumnHint("a", false)}, expect(2, 3), "case1.2");
    checkTag(kvstore.get(), schema, indices[1], {makeColumnHint("b", true)}, expect(0), "case1.3");
    checkTag(
        kvstore.get(), schema, indices[1], {makeColumnHint("b", false)}, expect(1, 3), "case1.4");
  }
  /* Case 2: [x,INF) */ {
    checkTag(kvstore.get(),
             schema,
             indices[0],
             {makeBeginColumnHint<true>("a", false)},
             expect(2, 3, 0, 1, 4),
             "case2.1");
    checkTag(kvstore.get(),
             schema,
             indices[0],
             {makeBeginColumnHint<true>("a", true)},
             expect(0, 1, 4),
             "case2.2");
    checkTag(kvstore.get(),
             schema,
             indices[1],
             {makeBeginColumnHint<true>("b", true)},
             expect(0),
             "case2.3");
  }
}

TEST_F(IndexScanStringType, String1) {
  /**
   * data and query both without truncate
   * That means ScanNode only access Index Key-Values
   */
  auto rows =
      " string    | string    | string                       | int \n"
      " 123456789 | abcdefghi | \xFF\xFF\xFF\xFF\xFF\xFF\xFF | 0   \n"
      " 123456789 | <null>    |                              | 1   \n"
      " 12345678  | <null>    | \x01                         | 2   \n"
      " 123456788 | \xFF\xFF  |                              | 3   \n"
      " 12345678: | aacd      | \xFF\xFF\xFF\xFF\xFF\xFF\xFE | 4   \n"
      " a1234     | accd      | \x00\x01                     | 5   \n"
      "           |           | <null>                       | 6   \n"
      ""_row;
  //                        0  1  2  3  4  5  6  7
  std::vector<int64_t> a = {6, 2, 3, 0, 1, 4, 5};
  std::vector<int64_t> b = {6, 4, 0, 5, 3};
  std::vector<int64_t> c = {1, 3, 5, 2, 4, 0};

  auto schema = R"(
    a | string  | 10  | false
    b | string  | 10  | true
    c | string  | 10  | true
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (ia,2): a(10)
    (ib,3): b(10)
    (ic,4): c(10)
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (size_t i = 0; i < kv.size(); i++) {
    for (auto& item : kv[i]) {
      kvstore->put(item.first, item.second);
    }
  }
  /* Case 1: prefix */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "123456789"),
                {kVid, "a"},
                expect(rows, {0, 1}, {0}),
                "case1.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "12345678"),
                {kVid, "a"},
                expect(rows, {2}, {0}),
                "case1.2");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", ""),
                {kVid, "a"},
                expect(rows, {6}, {0}),
                "case1.3");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFF"),
                {kVid, "b"},
                expect(rows, {3}, {1}),
                "case1.4");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", ""),
                {kVid, "b"},
                expect(rows, {6}, {1}),
                "case1.5");
    auto columnHint = hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE");
    checkString(kvstore.get(),
                schema,
                indices[2],
                columnHint,
                {kVid, "c"},
                expect(rows, {4}, {2}),
                "case1.6");
  }

  /* Case 2: [x, INF)*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<true>(name, value)};
    };
    auto slice = [](decltype(a) all, int begin) {
      return decltype(all){all.begin() + begin, all.end()};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "12345678"),
                {kVid, "a"},
                expect(rows, slice(a, 1), {0}),
                "case2.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "123456780"),
                {kVid, "a"},
                expect(rows, slice(a, 2), {0}),
                "case2.2");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", ""),
                {kVid, "a"},
                expect(rows, a, {0}),
                "case2.3");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", ""),
                {kVid, "b"},
                expect(rows, b, {1}),
                "case2.4");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "abc"),
                {kVid, "b"},
                expect(rows, slice(b, 2), {1}),
                "case2.5");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "aac"),
                {kVid, "b"},
                expect(rows, slice(b, 1), {1}),
                "case2.6");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "aacd\x01"),
                {kVid, "b"},
                expect(rows, slice(b, 2), {1}),
                "case2.7");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFF"),
                {kVid, "b"},
                expect(rows, slice(b, 4), {1}),
                "case2.8");
    checkString(
        kvstore.get(), schema, indices[1], hint("b", "\xFF\xFF\x01"), {kVid, "b"}, {}, "case2.9");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", ""),
                {kVid, "c"},
                expect(rows, c, {2}),
                "case2.10");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\x01"),
                {kVid, "c"},
                expect(rows, slice(c, 3), {2}),
                "case2.11");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "c"},
                expect(rows, slice(c, 5), {2}),
                "case2.12");
  }
  /* Case 3: (x,y) */ {
    auto hint = [](const char* name, const std::string& begin, const std::string& end) {
      return std::vector<ColumnHint>{makeColumnHint<false, false>(name, begin, end)};
    };
    auto slice = [](decltype(a) all, int begin, int end) {
      return decltype(all){all.begin() + begin, all.begin() + end};
    };
    auto columnHint = hint("a", "12345678", "123456789");
    checkString(kvstore.get(),
                schema,
                indices[0],
                columnHint,
                {kVid, "a"},
                expect(rows, slice(a, 2, 3), {0}),
                "case3.1");
    checkString(
        kvstore.get(), schema, indices[0], hint("a", "", "123456"), {kVid, "a"}, {}, "case3.2");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "", "\xFF"),
                {kVid, "b"},
                expect(rows, slice(b, 1, 4), {1}),
                "case3.3");
    columnHint = hint("b", "aaccd", "\xFF\xFF");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, slice(b, 1, 4), {1}),
                "case3.4");
    columnHint = hint("b", "\xFF", "\xFF\xFF\x01");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, slice(b, 4, 5), {1}),
                "case3.5");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "", "\x01"),
                {kVid, "c"},
                expect(rows, slice(c, 2, 3), {2}),
                "case3.6");
    columnHint = hint("c", "\x00\x00\x01"s, "\x01\x01");
    checkString(kvstore.get(),
                schema,
                indices[2],
                columnHint,
                {kVid, "c"},
                expect(rows, slice(c, 2, 4), {2}),
                "case3.7");
    columnHint = hint("c", "\x00\x01"s, "\x01\x01");
    checkString(kvstore.get(),
                schema,
                indices[2],
                columnHint,
                {kVid, "c"},
                expect(rows, slice(c, 3, 4), {2}),
                "case3.8");
  }
  /* Case 4: (INF,y]*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeEndColumnHint<true>(name, value)};
    };
    auto slice = [](decltype(a) all, int end) {
      return decltype(all){all.begin(), all.begin() + end};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "123456789"),
                {kVid, "a"},
                expect(rows, slice(a, 5), {0}),
                "case4.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", ""),
                {kVid, "a"},
                expect(rows, slice(a, 1), {0}),
                "case4.2");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "\xFF"),
                {kVid, "a"},
                expect(rows, slice(a, 7), {0}),
                "case4.3");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFF"),
                {kVid, "b"},
                expect(rows, slice(b, 5), {1}),
                "case4.4");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFE"),
                {kVid, "b"},
                expect(rows, slice(b, 4), {1}),
                "case4.5");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\x00\x00\x01"s),
                {kVid, "c"},
                expect(rows, slice(c, 2), {2}),
                "case4.6");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\x00\x01"s),
                {kVid, "c"},
                expect(rows, slice(c, 3), {2}),
                "case4.7");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\x01"),
                {kVid, "c"},
                expect(rows, slice(c, 4), {2}),
                "case4.8");
    auto columnHint = hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
    checkString(kvstore.get(),
                schema,
                indices[2],
                columnHint,
                {kVid, "c"},
                expect(rows, c, {2}),
                "case4.9");
  }
}

TEST_F(IndexScanStringType, String2) {
  /**
   * data with truncate
   * query without truncate
   * That means ScanNode need to access base data only when require indexed column
   */
  auto rows =
      " string  | string | string                       | int \n"
      " 123456  | ABCDE2 | <null>                       | 0   \n"
      " 1234567 | ABCDE1 | \xFF\xFF\xFF\xFF\xFF         | 1   \n"
      " 1234567 | ABCDE  | \xFF\xFF\xFF\xFF\xFF\x00\x01 | 2   \n"
      " 123457  | ABCDF  | \xFF\xFF\xFF\xFF\xFF         | 3   \n"
      ""_row;
  auto schema = R"(
    c1 | string  | | false
    c2 | string  | | true
    c3 | string  | | true
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (i1,2):c1(5)
    (i2,3):c2(5)
    (i3,4):c3(5)
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (size_t i = 0; i < kv.size(); i++) {
    for (auto& item : kv[i]) {
      kvstore->put(item.first, item.second);
    }
  }
  /* Case 1: Prefix */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    checkString(kvstore.get(), schema, indices[0], hint("c1", "1234"), {kVid, "c1"}, {}, "case1.1");
    checkString(
        kvstore.get(), schema, indices[0], hint("c1", "12345"), {kVid, "c1"}, {}, "case1.2");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("c2", "ABCDE"),
                {kVid, "c2"},
                expect(rows, {2}, {1}),
                "case1.3");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c3", "\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "c3"},
                expect(rows, {1, 3}, {2}),
                "case1.4");
  }
  /* Case 2: (x, INF)*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<false>(name, value)};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("c1", "12345"),
                {kVid, "c1"},
                expect(rows, {0, 1, 2, 3}, {0}),
                "case2.1");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("c2", "ABCDE"),
                {kVid, "c2"},
                expect(rows, {0, 1, 3}, {1}),
                "case2.2");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c3", "\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "c3"},
                expect(rows, {2}, {2}),
                "case2.3");
  }
  /* Case 3: [x, y] */ {
    auto hint = [](const char* name, const std::string& begin, const std::string& end) {
      return std::vector<ColumnHint>{makeColumnHint<true, true>(name, begin, end)};
    };
    auto columnHint = hint("c1", "12345", "12346");
    checkString(kvstore.get(),
                schema,
                indices[0],
                columnHint,
                {kVid, "c1"},
                expect(rows, {0, 1, 2, 3}, {0}),
                "case3.1");
    columnHint = hint("c1", "12345", "12345");
    checkString(kvstore.get(), schema, indices[0], columnHint, {kVid, "c1"}, {}, "case3.2");
    columnHint = hint("c2", "ABCDE", "ABCDF");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "c2"},
                expect(rows, {0, 1, 2, 3}, {1}),
                "case3.3");
    columnHint = hint("c2", "ABCDE", "ABCDE");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "c2"},
                expect(rows, {2}, {1}),
                "case3.4");
    columnHint = hint("c3", "\xFF\xFF\xFF\xFF\xFF", "\xFF\xFF\xFF\xFF\xFF");
    checkString(kvstore.get(),
                schema,
                indices[2],
                columnHint,
                {kVid, "c3"},
                expect(rows, {1, 3}, {2}),
                "case3.5");
  }
  /* Case 4: (INF,y)*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeEndColumnHint<false>(name, value)};
    };
    auto columnHint = hint("c1", "12345");
    checkString(kvstore.get(), schema, indices[0], columnHint, {kVid, "c1"}, {}, "case4.1");
    columnHint = hint("c2", "ABCDE");
    checkString(kvstore.get(), schema, indices[1], columnHint, {kVid, "c2"}, {}, "case4.2");
    columnHint = hint("c2", "ABCDF");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "c2"},
                expect(rows, {0, 1, 2}, {1}),
                "case4.3");
    columnHint = hint("c3", " \xFF\xFF\xFF\xFF\xFF");
    checkString(kvstore.get(), schema, indices[2], columnHint, {kVid, "c3"}, {}, "case4.4");
  }
}

TEST_F(IndexScanStringType, String3) {
  /**
   * data without truncate
   * query with truncate
   * That means ScanNode only access Index Key-Values
   */
  auto rows =
      " string  | string  | string                | int \n"
      " abcde   | 98765   | <null>                | 0   \n"
      " abcda   | 12345   | \xFF\xFF\xFF\xFF\xFF  | 1   \n"
      " abcda   | 98766   |                       | 2   \n"
      "         | <null>  |                       | 3   \n"
      ""_row;
  auto schema = R"(
    a | string  | | false
    b | string  | | true
    c | string  | | true
  )"_schema;
  auto indices = R"(
    TAG(t,0)
    (ia,1): a(6)
    (ib,2): b(6)
    (ic,3): c(6)
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (size_t i = 0; i < kv.size(); i++) {
    for (auto& item : kv[i]) {
      kvstore->put(item.first, item.second);
    }
  }
  /* Case 1: Prefix */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    checkString(
        kvstore.get(), schema, indices[0], hint("a", "abcde  "), {kVid, "a"}, {}, "case1.1");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "c"},
                {},
                "case1.2");
  }
  /* Case 2: [x, INF)*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<true>(name, value)};
    };
    checkString(kvstore.get(), schema, indices[0], hint("a", "abcdef"), {kVid, "a"}, {}, "case2.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcda  "),
                {kVid, "a"},
                expect(rows, {0}, {0}),
                "case2.2");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "987654  "),
                {kVid, "b"},
                expect(rows, {2}, {1}),
                "case2.3");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "c"},
                {},
                "case2.4");
  }
  /* Case 3: (x, y]*/ {
    auto hint = [](const char* name, const std::string& begin, const std::string& end) {
      return std::vector<ColumnHint>{makeColumnHint<false, true>(name, begin, end)};
    };
    auto columnHint = hint("a", "abcda  ", "abcde  ");
    checkString(kvstore.get(),
                schema,
                indices[0],
                columnHint,
                {kVid, "a"},
                expect(rows, {0}, {0}),
                "case3.1");
    columnHint = hint("b", "98765  ", "98766  ");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, {2}, {1}),
                "case3.2");
    columnHint = hint("c", "\xFF\xFF\xFF\xFF\xFE  ", "\xFF\xFF\xFF\xFF\xFF  ");
    checkString(kvstore.get(),
                schema,
                indices[2],
                columnHint,
                {kVid, "c"},
                expect(rows, {1}, {2}),
                "case3.3");
  }
  /* Case 4: (INF,y)*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeEndColumnHint<false>(name, value)};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde  "),
                {kVid, "a"},
                expect(rows, {3, 1, 2, 0}, {0}),
                "case4.1");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "98764  "),
                {kVid, "b"},
                expect(rows, {1}, {1}),
                "case4.2");
    checkString(kvstore.get(),
                schema,
                indices[2],
                hint("c", "\xFF\xFF\xFF\xFF\xFF  "),
                {kVid, "c"},
                expect(rows, {2, 3, 1}, {2}),
                "case4.3");
  }
}

TEST_F(IndexScanStringType, String4) {
  /**
   * data with truncate
   * query with truncate
   * That means ScanNode always need to access base data.
   */
  auto rows =
      " string    | string                      | int \n"
      " abcde1    | 987654                      | 0   \n"
      " abcdd     | 98765                       | 1   \n"
      " abcdf     | 12345                       | 2   \n"
      " abcde     | \xFF\xFF\xFF\xFF\xFF\xFF    | 3   \n"
      " abcde12   | <null>                      | 4   \n"
      " abcde123  | \xFF\xFF\xFF\xFF\xFF        | 5   \n"
      " abcde1234 | \xFF\xFF\xFF\xFF\xFF\xFF\x01| 6   \n"
      " abcde1234 | <null>                      | 7   \n"
      ""_row;
  auto schema = R"(
    a | string  | | false
    b | string  | | true
  )"_schema;
  auto indices = R"(
    TAG(t,0)
    (ia,1): a(5)
    (ib,2): b(5)
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (size_t i = 0; i < kv.size(); i++) {
    for (auto& item : kv[i]) {
      kvstore->put(item.first, item.second);
    }
  }
  /* Case 1: Prefix */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde"),
                {kVid, "a"},
                expect(rows, {3}, {0}),
                "case1.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde1234"),
                {kVid, "a"},
                expect(rows, {6, 7}, {0}),
                "case1.2");
    checkString(kvstore.get(), schema, indices[0], hint("a", "abcde2"), {kVid, "a"}, {}, "case1.3");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "b"},
                expect(rows, {5}, {1}),
                "case1.4");
  }
  /* Case 2: (x, INF) */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<false>(name, value)};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde"),
                {kVid, "a"},
                expect(rows, {0, 4, 5, 6, 7, 2}, {0}),
                "case2.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde12"),
                {kVid, "a"},
                expect(rows, {5, 6, 7, 2}, {0}),
                "case2.2");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde12345"),
                {kVid, "a"},
                expect(rows, {2}, {0}),
                "case2.3");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcdd"),
                {kVid, "a"},
                expect(rows, {0, 3, 4, 5, 6, 7, 2}, {0}),
                "case2.4");
    auto columnHint = hint("b", "\xFF\xFF\xFF\xFF\xFF");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, {3, 6}, {1}),
                "case2.5");
    columnHint = hint("b", "\xFF\xFF\xFF\xFF\xFF\x01");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, {3, 6}, {1}),
                "case2.6");
    columnHint = hint("b", "\xFF\xFF\xFF\xFF\xFF\xFF");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, {6}, {1}),
                "case2.7");
  }
  /* Case 3: [x,y) */ {
    auto hint = [](const char* name, const std::string& begin, const std::string& end) {
      return std::vector<ColumnHint>{makeColumnHint<true, false>(name, begin, end)};
    };
    auto columnHint = hint("a", "abcdd123", "abcde1234");
    checkString(kvstore.get(),
                schema,
                indices[0],
                columnHint,
                {kVid, "a"},
                expect(rows, {0, 3, 4, 5}, {0}),
                "case3.1");
    columnHint = hint("a", "abcde1", "abcdf");
    checkString(kvstore.get(),
                schema,
                indices[0],
                columnHint,
                {kVid, "a"},
                expect(rows, {0, 4, 5, 6, 7}, {0}),
                "case3.2");
    columnHint = hint("a", "abcde12345", "abcde123456");
    checkString(kvstore.get(), schema, indices[0], columnHint, {kVid, "a"}, {}, "case3.3");
    columnHint = hint("a", "abcde1234", "abcde12345");
    checkString(kvstore.get(),
                schema,
                indices[0],
                columnHint,
                {kVid, "a"},
                expect(rows, {6, 7}, {0}),
                "case3.4");
    columnHint = hint("b", "\xFF\xFF\xFF\xFF\xFF", "\xFF\xFF\xFF\xFF\xFF\x00\x01"s);
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, {5}, {1}),
                "case3.5");
    columnHint = hint("b", "\xFF\xFF\xFF\xFF\xFF\x01", "\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
    checkString(kvstore.get(),
                schema,
                indices[1],
                columnHint,
                {kVid, "b"},
                expect(rows, {3, 6}, {1}),
                "case3.6");
  }
  /* Case 4: (INF,y] */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeEndColumnHint<true>(name, value)};
    };
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde123"),
                {kVid, "a"},
                expect(rows, {1, 0, 3, 4, 5}, {0}),
                "case4.1");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde"),
                {kVid, "a"},
                expect(rows, {1, 3}, {0}),
                "case4.2");
    checkString(kvstore.get(),
                schema,
                indices[0],
                hint("a", "abcde1234"),
                {kVid, "a"},
                expect(rows, {1, 0, 3, 4, 5, 6, 7}, {0}),
                "case4.3");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "b"},
                expect(rows, {2, 0, 1, 5}, {1}),
                "case4.4");
    checkString(kvstore.get(),
                schema,
                indices[1],
                hint("b", "\xFF\xFF\xFF\xFF\xFF\xFF"),
                {kVid, "b"},
                expect(rows, {2, 0, 1, 3, 5}, {1}),
                "case4.5");
  }
}

TEST_F(IndexScanScalarType, FixedString) {
  auto rows = R"(
    fixed_string | fixed_string
    aaa          | aaa
    aaaaa        | aaaaa
    aaaaaaa      | aaaaaaa
    abc          | abc
    abcd         | abcd
    abcde        | <null>
    abcdef       | abcdef
    abcdefg      | abcdefg
    abcdf        | abcdf
    abd          | abd
    zoo          | zoo
  )"_row;
  auto schema = R"(
    a | fixed_string | 5 | false
    b | fixed_string | 5 | true
  )"_schema;
  // Since it is a index on fixed_string property, index field length is same as length of
  // fixed_string property
  auto indices = R"(
    TAG(t,0)
    (ia,1): a(5)
    (ib,2): b(5)
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  // Case 1: fixed string in [x, x]
  std::vector<ColumnHint> columnHints;
  {
    columnHints = {makeColumnHint("a", Value("abc"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3), "case1.1");
    columnHints = {makeColumnHint("b", Value("abc"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3), "case1.2");
    columnHints = {makeColumnHint("a", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case1.3");
    columnHints = {makeColumnHint("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(6, 7), "case1.4");
  }
  // Case 2: fixed string in [x, INF)
  {
    columnHints = {makeBeginColumnHint<true>("a", Value("abc"))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6, 7, 8, 9, 10), "case2.1");
    columnHints = {makeBeginColumnHint<true>("b", Value("abc"))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(3, 4, 6, 7, 8, 9, 10), "case2.2");
    columnHints = {makeBeginColumnHint<true>("a", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7, 8, 9, 10), "case2.3");
    columnHints = {makeBeginColumnHint<true>("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(6, 7, 8, 9, 10), "case2.4");
    columnHints = {makeBeginColumnHint<true>("a", Value("well"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(10), "case2.5");
    columnHints = {makeBeginColumnHint<true>("b", Value("well"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(10), "case2.6");
  }
  // Case 3: fixed string in (x, INF)
  {
    columnHints = {makeBeginColumnHint<false>("a", Value("abc"))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7, 8, 9, 10), "case3.1");
    columnHints = {makeBeginColumnHint<false>("b", Value("abc"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 6, 7, 8, 9, 10), "case3.2");
    columnHints = {makeBeginColumnHint<false>("a", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8, 9, 10), "case3.3");
    columnHints = {makeBeginColumnHint<false>("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(8, 9, 10), "case3.4");
    columnHints = {makeBeginColumnHint<false>("a", Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(10), "case3.5");
    columnHints = {makeBeginColumnHint<false>("b", Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(10), "case3.6");
  }
  // Case 4: fixed string in [x, y)
  {
    columnHints = {makeColumnHint<true, false>("a", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 4), "case4.1");
    columnHints = {makeColumnHint<true, false>("b", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2, 3, 4), "case4.2");
    columnHints = {makeColumnHint<true, false>("a", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6, 7, 8), "case4.3");
    columnHints = {makeColumnHint<true, false>("b", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 4, 6, 7, 8), "case4.4");
    columnHints = {makeColumnHint<true, false>("a", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7, 8), "case4.5");
    columnHints = {makeColumnHint<true, false>("b", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(6, 7, 8), "case4.6");
  }
  // Case 5: fixed string in [x, y]
  {
    columnHints = {makeColumnHint<true, true>("a", Value("aaaaa"), Value("abcde"))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 4, 5, 6, 7), "case5.1");
    columnHints = {makeColumnHint<true, true>("b", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2, 3, 4, 6, 7), "case5.2");
    columnHints = {makeColumnHint<true, true>("a", Value("abc"), Value("abd"))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6, 7, 8, 9), "case5.3");
    columnHints = {makeColumnHint<true, true>("b", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 4, 6, 7, 8, 9), "case5.4");
    columnHints = {makeColumnHint<true, true>("a", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7, 8, 9), "case5.5");
    columnHints = {makeColumnHint<true, true>("b", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(6, 7, 8, 9), "case5.6");
  }
  // Case 6: fixed string in (x, y]
  {
    columnHints = {makeColumnHint<false, true>("a", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6, 7), "case6.1");
    columnHints = {makeColumnHint<false, true>("b", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 4, 6, 7), "case6.2");
    columnHints = {makeColumnHint<false, true>("a", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7, 8, 9), "case6.3");
    columnHints = {makeColumnHint<false, true>("b", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 6, 7, 8, 9), "case6.4");
    columnHints = {makeColumnHint<false, true>("a", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8, 9), "case6.5");
    columnHints = {makeColumnHint<false, true>("b", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(8, 9), "case6.6");
  }
  // Case 7: fixed string in (x, y)
  {
    columnHints = {makeColumnHint<false, false>("a", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4), "case7.1");
    columnHints = {makeColumnHint<false, false>("b", Value("aaaaa"), Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 4), "case7.2");
    columnHints = {makeColumnHint<false, false>("a", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7, 8), "case7.3");
    columnHints = {makeColumnHint<false, false>("b", Value("abc"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 6, 7, 8), "case7.4");
    columnHints = {makeColumnHint<false, false>("a", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8), "case7.5");
    columnHints = {makeColumnHint<false, false>("b", Value("abcde"), Value("abd"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(8), "case7.6");
  }
  // Case 8: fixed string in (-INF, y]
  {
    columnHints = {makeEndColumnHint<true>("a", Value("aaaaa"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2), "case8.1");
    columnHints = {makeEndColumnHint<true>("b", Value("aaaaa"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2), "case8.2");
    columnHints = {makeEndColumnHint<true>("a", Value("abcde"))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case8.3");
    columnHints = {makeEndColumnHint<true>("b", Value("abcde"))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 6, 7), "case8.4");
    columnHints = {makeEndColumnHint<true>("a", Value("abd"))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
             "case8.5");
    columnHints = {makeEndColumnHint<true>("b", Value("abd"))};
    checkTag(kvstore.get(),
             schema,
             indices[1],
             columnHints,
             expect(0, 1, 2, 3, 4, 6, 7, 8, 9),
             "case8.6");
  }
  // Case 9: fixed string in (-INF, y)
  {
    columnHints = {makeEndColumnHint<false>("a", Value("aaaaa"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0), "case9.1");
    columnHints = {makeEndColumnHint<false>("b", Value("aaaaa"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0), "case9.2");
    columnHints = {makeEndColumnHint<false>("a", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4), "case9.3");
    columnHints = {makeEndColumnHint<false>("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4), "case9.4");
    columnHints = {makeEndColumnHint<false>("a", Value("abd"))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8),
             "case9.5");
    columnHints = {makeEndColumnHint<false>("b", Value("abd"))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 6, 7, 8), "case9.6");
  }
}

TEST_F(IndexScanScalarType, CompoundFixedString) {
  auto rows = R"(
    fixed_string | fixed_string
    aaa          | aaa
    abc          | aaa
    abcde        | abcde
    abcde        | <null>
    abcde        | abcdef
    abcde        | abcdefg
    abcde        | abcdf
    abcde        | abd
    abd          | abd
  )"_row;
  auto schema = R"(
    a | fixed_string | 5 | false
    b | fixed_string | 5 | true
  )"_schema;
  // Since it is a index on fixed_string property, index field length is same as length of
  // fixed_string property
  auto indices = R"(
    TAG(t,1)
    (iab,2): a(5), b(5)
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  std::vector<ColumnHint> columnHints;
  // prefix
  {
    columnHints = {makeColumnHint("a", Value("abcde"))};
    // null is ordered at last
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5, 6, 7, 3), "case1.1");
  }
  // prefix + prefix
  {
    columnHints = {makeColumnHint("a", Value("abcde")), makeColumnHint("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5), "case2.1");
    columnHints = {makeColumnHint("a", Value("abcde")), makeColumnHint("b", Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(7), "case2.2");
  }
  // prefix + range
  {
    // where a = "abcde" and b < "abd"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeEndColumnHint<false>("b", Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5, 6), "case3.1");
    // where a = "abcde" and b <= "abd"
    columnHints = {makeColumnHint("a", Value("abcde")), makeEndColumnHint<true>("b", Value("abd"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5, 6, 7), "case3.2");
    // where a = "abcde" and b > "abcde"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeBeginColumnHint<false>("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(6, 7), "case3.3");
    // where a = "abcde" and b >= "abcde"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeBeginColumnHint<true>("b", Value("abcde"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5, 6, 7), "case3.4");
    // where a = "abcde" and "abcde" < b < "abcdf"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeColumnHint<false, false>("b", Value("abcde"), Value("abcdf"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(), "case3.5");
    // where a = "abcde" and "abcde" <= b < "abcdf"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeColumnHint<true, false>("b", Value("abcde"), Value("abcdf"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5), "case3.6");
    // where a = "abcde" and "abcde" < b <= "abcdf"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeColumnHint<false, true>("b", Value("abcde"), Value("abcdf"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(6), "case3.7");
    // where a = "abcde" and "abcde" <= b <= "abcdf"
    columnHints = {makeColumnHint("a", Value("abcde")),
                   makeColumnHint<true, true>("b", Value("abcde"), Value("abcdf"))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5, 6), "case3.8");
  }
}

TEST_F(IndexScanScalarType, Nullable) {
  std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema;
  auto kvstore = std::make_unique<MockKVStore>();
  auto hint = [](const std::string& name) {
    return std::vector{makeColumnHint(name, Value::kNullValue)};
  };
  /* Case 1: Int*/ {
    auto rows = R"(
      int                   | int
      0                     | 0
      9223372036854775807   | <null>
      9223372036854775807   | <null>
      -9223372036854775807  | 9223372036854775807
    )"_row;
    schema = R"(
      a | int | | false
      b | int | | true
    )"_schema;
    auto indices = R"(
      TAG(t,1)
      (ia,2):a
      (ib,3):b
      (iba,4):b,a
    )"_index(schema);
    auto kv = encodeTag(rows, 1, schema, indices);
    kvstore = std::make_unique<MockKVStore>();
    for (auto& iter : kv) {
      for (auto& item : iter) {
        kvstore->put(item.first, item.second);
      }
    }
    checkTag(kvstore.get(), schema, indices[0], hint("a"), {}, "case1.1");
    checkTag(kvstore.get(), schema, indices[1], hint("b"), expect(1, 2), "case1.2");
    checkTag(kvstore.get(), schema, indices[2], hint("b"), expect(1, 2), "case1.3");
  }
  /* Case 2: Float */ {
    auto rows = R"(
      float                     | float
      1.7976931348623157e+308   | <null>
      0                         | <NaN>
      <INF>                     | <null>
      <NaN>                     | <-NaN>
    )"_row;
    schema = R"(
      a | double | | false
      b | double | | true
    )"_schema;
    auto indices = R"(
      TAG(t,1)
      (ia,2):a
      (ib,3):b
      (iba,4):b,a
    )"_index(schema);
    auto kv = encodeTag(rows, 1, schema, indices);
    kvstore = std::make_unique<MockKVStore>();
    for (auto& iter : kv) {
      for (auto& item : iter) {
        kvstore->put(item.first, item.second);
      }
    }
    checkTag(kvstore.get(), schema, indices[0], hint("a"), {}, "case2.1");
    checkTag(kvstore.get(), schema, indices[1], hint("b"), expect(0, 2), "case2.2");
    checkTag(kvstore.get(), schema, indices[2], hint("b"), expect(0, 2), "case2.3");
  }
  /* Case 3: String */ {
    auto rows = R"(
      string        | string
      \xFF\xFF\xFF  | <null>
      123           | 456
      \xFF\xFF\x01  | \xFF\xFF\xFF
      \xFF\xFF\x01  | <null>
    )"_row;
    schema = R"(
      a | string  | | false
      b | string  | | true
    )"_schema;
    auto indices = R"(
      TAG(t,1)
      (ia,2):a(3)
      (ib,3):b(3)
      (iba,4):b(3),a(3)
    )"_index(schema);
    auto kv = encodeTag(rows, 1, schema, indices);
    kvstore = std::make_unique<MockKVStore>();
    for (auto& iter : kv) {
      for (auto& item : iter) {
        kvstore->put(item.first, item.second);
      }
    }
    checkTag(kvstore.get(), schema, indices[0], hint("a"), {}, "case3.1");
    checkTag(kvstore.get(), schema, indices[1], hint("b"), expect(0, 3), "case3.2");
    checkTag(kvstore.get(), schema, indices[2], hint("b"), expect(0, 3), "case3.3");
  }
  /* Case 4: Bool */ {
    auto rows = R"(
      bool  | bool
      false | <null>
      true  | <null>
      true  | true
      true  | false
      false | true
      false | false
    )"_row;
    schema = R"(
      a | bool | | false
      b | bool | | true
    )"_schema;
    auto indices = R"(
      TAG(t,1)
      (ia,2):a
      (ib,3):b
      (iba,4):b,a
    )"_index(schema);
    auto kv = encodeTag(rows, 1, schema, indices);
    kvstore = std::make_unique<MockKVStore>();
    for (auto& iter : kv) {
      for (auto& item : iter) {
        kvstore->put(item.first, item.second);
      }
    }
    checkTag(kvstore.get(), schema, indices[0], hint("a"), {}, "case4.1");
    checkTag(kvstore.get(), schema, indices[1], hint("b"), expect(0, 1), "case4.2");
    checkTag(kvstore.get(), schema, indices[2], hint("b"), expect(0, 1), "case4.3");
    checkTag(kvstore.get(),
             schema,
             indices[0],
             std::vector{makeColumnHint("a", Value(true))},
             expect(1, 2, 3),
             "case4.4");
    checkTag(kvstore.get(),
             schema,
             indices[1],
             std::vector{makeColumnHint("b", Value(true))},
             expect(2, 4),
             "case4.5");
  }
}

TEST_F(IndexScanScalarType, TTL) {
  auto rows = R"(
    int
    1
    100
    <now>
    <null>
  )"_row;
  auto schema = R"(
    a   | int | | true
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (i1,2):a
  )"_index(schema);

  int64_t ttlDuration = 1;
  // set a ttl property to schema
  meta::cpp2::SchemaProp schemaProp;
  schemaProp.ttl_duration_ref() = ttlDuration;
  schemaProp.ttl_col() = "a";
  schema->setProp(std::move(schemaProp));

  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  std::vector<ColumnHint> columnHints;
  columnHints = {makeBeginColumnHint<false>("a", Value(100))};
  checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2), "ttl_not_expired");
  sleep(ttlDuration + 1);
  columnHints = {makeBeginColumnHint<false>("a", Value(100))};
  checkTag(kvstore.get(), schema, indices[0], columnHints, expect(), "ttl_expired");
}

TEST_F(IndexScanScalarType, Time) {
  auto rows = R"(
    time          | time
    0:0:0.0       | 0:0:0.0
    0:0:0.1       | 0:0:0.1
    0:0:0.0123    | 0:0:0.0123
    0:0:1.0       | 0:0:1.0
    0:0:1.000001  | 0:0:1.000001
    0:0:59.0      | 0:0:59.0
    0:1:0.0       | 0:1:0.0
    0:15:20.0     | 0:15:20.0
    0:30:05.0     | <null>
    0:59:59.0     | 0:59:59.0
    0:59:59.9999  | 0:59:59.9999
    1:0:0.0       | 1:0:0.0
    1:1:1.1       | 1:1:1.1
    9:15:0.0      | <null>
    11:59:59.0    | 11:59:59.0
    12:0:0.0      | 12:0:0.0
    18:30:0.0     | <null>
    20:21:22.23   | 20:21:22.23
    23:59:59.0    | 23:59:59.0
    23:59:59.0001 | 23:59:59.0001
    23:59:59.9999 | 23:59:59.9999
  )"_row;
  auto schema = R"(
    a   | time | | false
    b   | time | | true
  )"_schema;
  auto indices = R"(
    EDGE(t,1)
    (i1,2):a
    (i2,3):b
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  // Case 1: t in [x, x]
  std::vector<ColumnHint> columnHints;
  {
    columnHints = {makeColumnHint("a", Value(Time(12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(15), "case1.1");
    columnHints = {makeColumnHint("b", Value(Time(12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(15), "case1.2");
    columnHints = {makeColumnHint("a", Value(Time(0, 59, 59, 9999)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(10), "case1.3");
    columnHints = {makeColumnHint("b", Value(Time(0, 59, 59, 9999)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(10), "case1.4");
  }
  // Case 2: t in [x, INF)
  {
    columnHints = {makeBeginColumnHint<true>("a", Value(Time(18, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(16, 17, 18, 19, 20), "case2.1");
    columnHints = {makeBeginColumnHint<true>("b", Value(Time(18, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(17, 18, 19, 20), "case2.2");
    columnHints = {makeBeginColumnHint<true>("a", Value(Time(23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(18, 19, 20), "case2.3");
    columnHints = {makeBeginColumnHint<true>("b", Value(Time(23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(18, 19, 20), "case2.4");
    columnHints = {makeBeginColumnHint<true>("a", Value(Time(23, 59, 59, 5)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(20), "case2.5");
    columnHints = {makeBeginColumnHint<true>("b", Value(Time(23, 59, 59, 5)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(20), "case2.6");
  }
  // Case 3: t in (x, INF)
  {
    columnHints = {makeBeginColumnHint<false>("a", Value(Time(18, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(16, 17, 18, 19, 20), "case3.1");
    columnHints = {makeBeginColumnHint<false>("b", Value(Time(18, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(17, 18, 19, 20), "case3.2");
    columnHints = {makeBeginColumnHint<false>("a", Value(Time(23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(19, 20), "case3.3");
    columnHints = {makeBeginColumnHint<false>("b", Value(Time(23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(19, 20), "case3.4");
    columnHints = {makeBeginColumnHint<false>("a", Value(Time(23, 59, 59, 5)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(20), "case3.5");
    columnHints = {makeBeginColumnHint<false>("b", Value(Time(23, 59, 59, 5)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(20), "case3.6");
  }
  // Case 4: t in [x, y)
  {
    columnHints = {
        makeColumnHint<true, false>("a", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(11), "case4.1");
    columnHints = {
        makeColumnHint<true, false>("b", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(11), "case4.2");
    columnHints = {
        makeColumnHint<true, false>("a", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case4.3");
    columnHints = {
        makeColumnHint<true, false>("b", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5), "case4.4");
    columnHints = {
        makeColumnHint<true, false>("a", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
             "case4.5");
    columnHints = {
        makeColumnHint<true, false>("b", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[1],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 9, 10),
             "case6.6");
  }
  // Case 5: t in [x, y]
  {
    columnHints = {
        makeColumnHint<true, true>("a", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(11, 12), "case5.1");
    columnHints = {
        makeColumnHint<true, true>("b", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(11, 12), "case5.2");
    columnHints = {
        makeColumnHint<true, true>("a", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case5.3");
    columnHints = {
        makeColumnHint<true, true>("b", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case5.4");
    columnHints = {
        makeColumnHint<true, true>("a", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
             "case5.5");
    columnHints = {
        makeColumnHint<true, true>("b", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[1],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11),
             "case5.6");
  }
  // Case 6: t in (x, y]
  {
    columnHints = {
        makeColumnHint<false, true>("a", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(12), "case6.1");
    columnHints = {
        makeColumnHint<false, true>("b", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(12), "case6.2");
    columnHints = {
        makeColumnHint<false, true>("a", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 4, 5, 6), "case6.3");
    columnHints = {
        makeColumnHint<false, true>("b", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2, 3, 4, 5, 6), "case6.4");
    columnHints = {
        makeColumnHint<false, true>("a", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
             "case6.5");
    columnHints = {
        makeColumnHint<false, true>("b", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[1],
             columnHints,
             expect(1, 2, 3, 4, 5, 6, 7, 9, 10, 11),
             "case6.6");
  }
  // Case 7: t in (x, y)
  {
    columnHints = {
        makeColumnHint<false, false>("a", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, {}, "case7.1");
    columnHints = {
        makeColumnHint<false, false>("b", Value(Time(1, 0, 0, 0)), Value(Time(1, 1, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, {}, "case7.2");
    columnHints = {
        makeColumnHint<false, false>("a", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 4, 5), "case7.3");
    columnHints = {
        makeColumnHint<false, false>("b", Value(Time(0, 0, 0, 0)), Value(Time(0, 1, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2, 3, 4, 5), "case7.4");
    columnHints = {
        makeColumnHint<false, false>("a", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
             "case7.5");
    columnHints = {
        makeColumnHint<false, false>("b", Value(Time(0, 0, 0, 0)), Value(Time(1, 0, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[1],
             columnHints,
             expect(1, 2, 3, 4, 5, 6, 7, 9, 10),
             "case7.6");
  }
  // Case 8: t in (-INF, y]
  {
    columnHints = {makeEndColumnHint<true>("a", Value(Time(0, 2, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case8.1");
    columnHints = {makeEndColumnHint<true>("b", Value(Time(0, 2, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case8.2");
    columnHints = {makeEndColumnHint<true>("a", Value(Time(0, 30, 5, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8),
             "case8.3");
    columnHints = {makeEndColumnHint<true>("b", Value(Time(0, 30, 5, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case8.4");
    columnHints = {makeEndColumnHint<true>("a", Value(Time(0, 45, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8),
             "case8.5");
    columnHints = {makeEndColumnHint<true>("b", Value(Time(0, 45, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case8.6");
  }
  // Case 9: t in (-INF, y)
  {
    columnHints = {makeEndColumnHint<false>("a", Value(Time(0, 2, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case9.1");
    columnHints = {makeEndColumnHint<false>("b", Value(Time(0, 2, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case9.2");
    columnHints = {makeEndColumnHint<false>("a", Value(Time(0, 30, 5, 0)))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case9.3");
    columnHints = {makeEndColumnHint<false>("b", Value(Time(0, 30, 5, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case9.4");
    columnHints = {makeEndColumnHint<false>("a", Value(Time(0, 45, 0, 0)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8),
             "case9.5");
    columnHints = {makeEndColumnHint<false>("b", Value(Time(0, 45, 0, 0)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case9.6");
  }
}

TEST_F(IndexScanScalarType, CompoundTime) {
  auto rows = R"(
    time          | time
    0:0:0.0       | 0:0:0.0
    1:0:0.0       | 0:0:0.0
    1:0:0.0       | 10:0:0.0
    1:0:0.0       | 10:10:0.0
    1:0:0.0       | <null>
    1:0:0.0       | 20:0:0.0
    1:0:0.0       | 23:59:59.0
    2:0:0.0       | 0:0:0.0
  )"_row;
  auto schema = R"(
    a   | time | | false
    b   | time | | true
  )"_schema;
  auto indices = R"(
    EDGE(t,1)
    (i3,4):a,b
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  std::vector<ColumnHint> columnHints;
  // prefix
  {
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0)))};
    // null is ordered at last
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 5, 6, 4), "case1.1");
  }
  // prefix + prefix
  {
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0))),
                   makeColumnHint("b", Value(Time(10, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2), "case2.1");
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0))),
                   makeColumnHint("b", Value(Time(20, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5), "case2.2");
  }
  // prefix + range
  {
    // where a = 1:0:0.0 and b < 12:0:0.0
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0))),
                   makeEndColumnHint<false>("b", Value(Time(12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3), "case3.1");
    // where a = 1:0:0.0 and b <= 20:0:0.0
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0))),
                   makeEndColumnHint<true>("b", Value(Time(20, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 5), "case3.2");
    // where a = 1:0:0.0 and b > 12:0:0.0
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0))),
                   makeBeginColumnHint<false>("b", Value(Time(12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6), "case3.3");
    // where a = 1:0:0.0 and b >= 10:0:0.0
    columnHints = {makeColumnHint("a", Value(Time(1, 0, 0, 0))),
                   makeBeginColumnHint<true>("b", Value(Time(10, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 3, 5, 6), "case3.4");
    // where a = 1:0:0.0 and 10:0:0.0 < b < 20:0:0.0
    columnHints = {
        makeColumnHint("a", Value(Time(1, 0, 0, 0))),
        makeColumnHint<false, false>("b", Value(Time(10, 0, 0, 0)), Value(Time(20, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3), "case3.5");
    // where a = 1:0:0.0 and 10:0:0.0 <= b < 20:0:0.0
    columnHints = {
        makeColumnHint("a", Value(Time(1, 0, 0, 0))),
        makeColumnHint<true, false>("b", Value(Time(10, 0, 0, 0)), Value(Time(20, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 3), "case3.6");
    // where a = 1:0:0.0 and 10:0:0.0 < b <= 20:0:0.0
    columnHints = {
        makeColumnHint("a", Value(Time(1, 0, 0, 0))),
        makeColumnHint<false, true>("b", Value(Time(10, 0, 0, 0)), Value(Time(20, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 5), "case3.7");
    // where a = 1:0:0.0 and 10:0:0.0 <= b <= 20:0:0.0
    columnHints = {
        makeColumnHint("a", Value(Time(1, 0, 0, 0))),
        makeColumnHint<true, true>("b", Value(Time(10, 0, 0, 0)), Value(Time(20, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 3, 5), "case3.8");
  }
}

TEST_F(IndexScanScalarType, Date) {
  auto rows = R"(
    date       | date
    1970-1-1   | 1970-1-1
    1997-7-1   | 1997-7-1
    1999-12-20 | 1999-12-20
    1999-12-31 | 1999-12-31
    2000-1-1   | 2000-1-1
    2020-1-31  | 2020-1-31
    2020-2-1   | 2020-2-1
    2020-2-2   | <null>
    2022-04-28 | 2022-04-28
    2022-11-22 | 2022-11-22
  )"_row;
  auto schema = R"(
    a   | date | | false
    b   | date | | true
  )"_schema;
  auto indices = R"(
    EDGE(t,1)
    (i1,2):a
    (i2,3):b
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  // Case 1: date in [x, x]
  std::vector<ColumnHint> columnHints;
  {
    columnHints = {makeColumnHint("a", Value(Date(1999, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3), "case1.1");
    columnHints = {makeColumnHint("b", Value(Date(1999, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3), "case1.2");
    columnHints = {makeColumnHint("a", Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(7), "case1.3");
    columnHints = {makeColumnHint("b", Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, {}, "case1.4");
  }
  // Case 2: date in [x, INF)
  {
    columnHints = {makeBeginColumnHint<true>("a", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7, 8, 9), "case2.1");
    columnHints = {makeBeginColumnHint<true>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 5, 6, 8, 9), "case2.2");
    columnHints = {makeBeginColumnHint<true>("a", Value(Date(2022, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8, 9), "case2.3");
    columnHints = {makeBeginColumnHint<true>("b", Value(Date(2022, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(8, 9), "case2.4");
    columnHints = {makeBeginColumnHint<true>("a", Value(Date(2022, 4, 28)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8, 9), "case2.5");
    columnHints = {makeBeginColumnHint<true>("b", Value(Date(2022, 4, 28)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(8, 9), "case2.6");
  }
  // Case 3: date in (x, INF)
  {
    columnHints = {makeBeginColumnHint<false>("a", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7, 8, 9), "case3.1");
    columnHints = {makeBeginColumnHint<false>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6, 8, 9), "case3.2");
    columnHints = {makeBeginColumnHint<false>("a", Value(Date(2022, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8, 9), "case3.3");
    columnHints = {makeBeginColumnHint<false>("b", Value(Date(2022, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(8, 9), "case3.4");
    columnHints = {makeBeginColumnHint<false>("a", Value(Date(2022, 4, 28)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(9), "case3.5");
    columnHints = {makeBeginColumnHint<false>("b", Value(Date(2022, 4, 28)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(9), "case3.6");
  }
  // Case 4: date in [x, y)
  {
    columnHints = {
        makeColumnHint<true, false>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6), "case4.1");
    columnHints = {
        makeColumnHint<true, false>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case4.2");
    columnHints = {
        makeColumnHint<true, false>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case4.3");
    columnHints = {
        makeColumnHint<true, false>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case4.4");
    columnHints = {
        makeColumnHint<true, false>("a", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7), "case4.5");
    columnHints = {
        makeColumnHint<true, false>("b", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 5, 6), "case4.6");
  }
  // Case 5: date in [x, y]
  {
    columnHints = {
        makeColumnHint<true, true>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case5.1");
    columnHints = {
        makeColumnHint<true, true>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case5.2");
    columnHints = {
        makeColumnHint<true, true>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case5.3");
    columnHints = {
        makeColumnHint<true, true>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case5.4");
    columnHints = {
        makeColumnHint<true, true>("a", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7), "case5.5");
    columnHints = {
        makeColumnHint<true, true>("b", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(4, 5, 6), "case5.6");
  }
  // Case 6: date in (x, y]
  {
    columnHints = {
        makeColumnHint<false, true>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case6.1");
    columnHints = {
        makeColumnHint<false, true>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case6.2");
    columnHints = {
        makeColumnHint<false, true>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case6.3");
    columnHints = {
        makeColumnHint<false, true>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case6.4");
    columnHints = {
        makeColumnHint<false, true>("a", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case6.5");
    columnHints = {
        makeColumnHint<false, true>("b", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case6.6");
  }
  // Case 7: date in (x, y)
  {
    columnHints = {
        makeColumnHint<false, false>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6), "case7.1");
    columnHints = {
        makeColumnHint<false, false>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 2, 2)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case7.2");
    columnHints = {
        makeColumnHint<false, false>("a", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case7.3");
    columnHints = {
        makeColumnHint<false, false>("b", Value(Date(2020, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case7.4");
    columnHints = {
        makeColumnHint<false, false>("a", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5, 6, 7), "case7.5");
    columnHints = {
        makeColumnHint<false, false>("b", Value(Date(2000, 1, 1)), Value(Date(2020, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case7.6");
  }
  // Case 8: date in (-INF, y]
  {
    columnHints = {makeEndColumnHint<true>("a", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4), "case8.1");
    columnHints = {makeEndColumnHint<true>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4), "case8.2");
    columnHints = {makeEndColumnHint<true>("a", Value(Date(2022, 4, 28)))};
    checkTag(kvstore.get(),
             schema,
             indices[0],
             columnHints,
             expect(0, 1, 2, 3, 4, 5, 6, 7, 8),
             "case8.5");
    columnHints = {makeEndColumnHint<true>("b", Value(Date(2022, 4, 28)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 8), "case8.6");
  }
  // Case 9: date in (-INF, y)
  {
    columnHints = {makeEndColumnHint<false>("a", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3), "case9.1");
    columnHints = {makeEndColumnHint<false>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3), "case9.2");
    columnHints = {makeEndColumnHint<false>("a", Value(Date(2022, 4, 28)))};
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(0, 1, 2, 3, 4, 5, 6, 7), "case9.5");
    columnHints = {makeEndColumnHint<false>("b", Value(Date(2022, 4, 28)))};
    checkTag(
        kvstore.get(), schema, indices[1], columnHints, expect(0, 1, 2, 3, 4, 5, 6), "case9.6");
  }
}

TEST_F(IndexScanScalarType, CompoundDate) {
  auto rows = R"(
    date       | date
    1970-1-1   | 1970-1-1
    2000-1-1   | 1970-1-1
    2000-1-1   | 1999-12-31
    2000-1-1   | 2000-1-1
    2000-1-1   | 2000-1-2
    2000-1-1   | <null>
    2000-1-1   | 2000-2-28
    2000-1-1   | 2000-12-31
    2000-1-1   | 2022-11-22
    2022-11-22 | 2022-11-22
  )"_row;
  auto schema = R"(
    a   | date | | false
    b   | date | | true
  )"_schema;
  auto indices = R"(
    EDGE(t,1)
    (i3,4):a,b
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  std::vector<ColumnHint> columnHints;
  // prefix
  {
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1)))};
    // null is ordered at last
    checkTag(
        kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3, 4, 6, 7, 8, 5), "case1.1");
  }
  // prefix + prefix
  {
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1))),
                   makeColumnHint("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3), "case2.1");
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1))),
                   makeColumnHint("b", Value(Date(2022, 11, 22)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(8), "case2.2");
  }
  // prefix + range
  {
    // where a = 2000-01-01 and b < 2000-01-01
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1))),
                   makeEndColumnHint<false>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2), "case3.1");
    // where a = 2000-01-01 and b < 2000-01-01
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1))),
                   makeEndColumnHint<true>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3), "case3.2");
    // where a = 2000-01-01 and b > 2000-01-01
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1))),
                   makeBeginColumnHint<false>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 6, 7, 8), "case3.3");
    // where a = 2000-01-01 and b >= 2000-01-01
    columnHints = {makeColumnHint("a", Value(Date(2000, 1, 1))),
                   makeBeginColumnHint<true>("b", Value(Date(2000, 1, 1)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 6, 7, 8), "case3.4");
    // where a = 2000-01-01 and 2000-01-01 < b < 2000-12-31
    columnHints = {
        makeColumnHint("a", Value(Date(2000, 1, 1))),
        makeColumnHint<false, false>("b", Value(Date(2000, 1, 1)), Value(Date(2000, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 6), "case3.5");
    // where a = 2000-01-01 and 2000-01-01 <= b < 2000-12-31
    columnHints = {
        makeColumnHint("a", Value(Date(2000, 1, 1))),
        makeColumnHint<true, false>("b", Value(Date(2000, 1, 1)), Value(Date(2000, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 6), "case3.6");
    // where a = 2000-01-01 and 2000-01-01 < b <= 2000-12-31
    columnHints = {
        makeColumnHint("a", Value(Date(2000, 1, 1))),
        makeColumnHint<false, true>("b", Value(Date(2000, 1, 1)), Value(Date(2000, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 6, 7), "case3.7");
    // where a = 2000-01-01 and 2000-01-01 <= b <= 2000-12-31
    columnHints = {
        makeColumnHint("a", Value(Date(2000, 1, 1))),
        makeColumnHint<true, true>("b", Value(Date(2000, 1, 1)), Value(Date(2000, 12, 31)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 6, 7), "case3.8");
  }
}

TEST_F(IndexScanScalarType, DateTime) {
  auto rows = R"(
    datetime                  | datetime
    1970-1-1T0:0:0.0          | 1970-1-1T0:0:0.0
    1999-12-31T23:59:59.0     | 1999-12-31T23:59:59.0
    1999-12-31T23:59:59.999   | 1999-12-31T23:59:59.999
    2000-1-1T0:0:0.0          | 2000-1-1T0:0:0.0
    2000-1-1T11:59:59.0       | <null>
    2000-1-1T12:0:0.0         | 2000-1-1T12:0:0.0
    2022-11-22T18:34:0.0      | 2022-11-22T18:34:0.0
    2023-1-1T0:0:0.0          | 2023-1-1T0:0:0.0
  )"_row;
  auto schema = R"(
    a   | datetime | | false
    b   | datetime | | true
  )"_schema;
  auto indices = R"(
    EDGE(t,1)
    (i1,2):a
    (i2,3):b
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  // Case 1: dt in [x, x]
  std::vector<ColumnHint> columnHints;
  {
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3), "case1.1");
    columnHints = {makeColumnHint("b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3), "case1.2");
  }
  // Case 2: dt in [x, INF)
  {
    columnHints = {makeBeginColumnHint<true>("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6, 7), "case2.1");
    columnHints = {makeBeginColumnHint<true>("b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 5, 6, 7), "case2.2");
    columnHints = {makeBeginColumnHint<true>("a", Value(DateTime(2000, 1, 1, 10, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7), "case2.3");
    columnHints = {makeBeginColumnHint<true>("b", Value(DateTime(2000, 1, 1, 10, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6, 7), "case2.4");
  }
  // Case 3: dt in (x, INF)
  {
    columnHints = {makeBeginColumnHint<false>("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7), "case3.1");
    columnHints = {makeBeginColumnHint<false>("b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6, 7), "case3.2");
    columnHints = {makeBeginColumnHint<false>("a", Value(DateTime(2000, 1, 1, 10, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7), "case3.3");
    columnHints = {makeBeginColumnHint<false>("b", Value(DateTime(2000, 1, 1, 10, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6, 7), "case3.4");
  }
  // Case 4: dt in [x, y)
  {
    columnHints = {makeColumnHint<true, false>(
        "a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6), "case4.1");
    columnHints = {makeColumnHint<true, false>(
        "b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 5, 6), "case4.2");
    columnHints = {makeColumnHint<true, false>(
        "a", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2), "case4.3");
    columnHints = {makeColumnHint<true, false>(
        "b", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2), "case4.4");
  }
  // Case 5: dt in [x, y]
  {
    columnHints = {makeColumnHint<true, true>(
        "a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(3, 4, 5, 6, 7), "case5.1");
    columnHints = {makeColumnHint<true, true>(
        "b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(3, 5, 6, 7), "case5.2");
    columnHints = {makeColumnHint<true, true>(
        "a", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3), "case5.3");
    columnHints = {makeColumnHint<true, true>(
        "b", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2, 3), "case5.4");
  }
  // Case 6: dt in (x, y]
  {
    columnHints = {makeColumnHint<false, true>(
        "a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6, 7), "case6.1");
    columnHints = {makeColumnHint<false, true>(
        "b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6, 7), "case6.2");
    columnHints = {makeColumnHint<false, true>(
        "a", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 3), "case6.3");
    columnHints = {makeColumnHint<false, true>(
        "b", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2, 3), "case6.4");
  }
  // Case 7: dt in (x, y)
  {
    columnHints = {makeColumnHint<false, false>(
        "a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6), "case7.1");
    columnHints = {makeColumnHint<false, false>(
        "b", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)), Value(DateTime(2023, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(5, 6), "case7.2");
    columnHints = {makeColumnHint<false, false>(
        "a", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2), "case7.3");
    columnHints = {makeColumnHint<false, false>(
        "b", Value(DateTime(1999, 1, 1, 0, 0, 0, 0)), Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(1, 2), "case7.4");
  }
  // Case 8: dt in (-INF, y]
  {
    columnHints = {makeEndColumnHint<true>("a", Value(DateTime(1999, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1), "case8.1");
    columnHints = {makeEndColumnHint<true>("b", Value(DateTime(1999, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1), "case8.1");
    columnHints = {makeEndColumnHint<true>("a", Value(DateTime(1999, 12, 31, 23, 59, 59, 9)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1), "case8.3");
    columnHints = {makeEndColumnHint<true>("b", Value(DateTime(1999, 12, 31, 23, 59, 59, 9)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1), "case8.4");
  }
  // Case 9: dt in (-INF, y)
  {
    columnHints = {makeEndColumnHint<false>("a", Value(DateTime(1999, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0), "case9.1");
    columnHints = {makeEndColumnHint<false>("b", Value(DateTime(1999, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0), "case9.1");
    columnHints = {makeEndColumnHint<false>("a", Value(DateTime(1999, 12, 31, 23, 59, 59, 9)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(0, 1), "case9.3");
    columnHints = {makeEndColumnHint<false>("b", Value(DateTime(1999, 12, 31, 23, 59, 59, 9)))};
    checkTag(kvstore.get(), schema, indices[1], columnHints, expect(0, 1), "case9.4");
  }
}

TEST_F(IndexScanScalarType, CompoundDateTime) {
  auto rows = R"(
    datetime                  | datetime
    1970-1-1T0:0:0.0          | 1970-1-1T0:0:0.0
    2000-1-1T0:0:0.0          | 2000-1-1T0:0:0.0
    2000-1-1T0:0:0.0          | 2000-1-1T12:0:0.0
    2000-1-1T0:0:0.0          | <null>
    2000-1-1T0:0:0.0          | 2000-1-1T23:59:59.0
    2000-1-1T0:0:0.0          | 2000-12-31T23:59:59.0
    2000-1-1T0:0:0.0          | 2022-1-1T0:0:0.0
    2022-11-22T18:34:0.0      | 2022-11-22T18:34:0.0
  )"_row;
  auto schema = R"(
    a   | datetime | | false
    b   | datetime | | true
  )"_schema;
  auto indices = R"(
    EDGE(t,1)
    (i3,4):a,b
  )"_index(schema);
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  std::vector<ColumnHint> columnHints;
  // prefix
  {
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0)))};
    // null is ordered at last
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 4, 5, 6, 3), "case1.1");
  }
  // prefix + prefix
  {
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeColumnHint("b", Value(DateTime(2000, 1, 1, 12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2), "case2.1");
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeColumnHint("b", Value(DateTime(2000, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(5), "case2.2");
  }
  // prefix + range
  {
    // where a = 2000-01-01T0:0:0.0 and b < 2000-01-01T23:59:59.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeEndColumnHint<false>("b", Value(DateTime(2000, 1, 1, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2), "case3.1");
    // where a = 2000-01-01T0:0:0.0 and b <= 2000-01-01T23:59:59.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeEndColumnHint<true>("b", Value(DateTime(2000, 1, 1, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(1, 2, 4), "case3.2");
    // where a = 2000-01-01T0:0:0.0 and b > 2000-01-01T12:0:0.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeBeginColumnHint<false>("b", Value(DateTime(2000, 1, 1, 12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5, 6), "case3.3");
    // where a = 2000-01-01T0:0:0.0 and b >= 2000-01-01T12:0:0.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeBeginColumnHint<true>("b", Value(DateTime(2000, 1, 1, 12, 0, 0, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5, 6), "case3.4");
    // where a = 2000-01-01T0:0:0.0 and 2000-1-1T12:0:0.0 < b < 2020-12-31T23:59:59.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeColumnHint<false, false>("b",
                                                Value(DateTime(2000, 1, 1, 12, 0, 0, 0)),
                                                Value(DateTime(2000, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4), "case3.5");
    // where a = 2000-01-01T0:0:0.0 and 2000-1-1T12:0:0.0 <= b < 2020-12-31T23:59:59.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeColumnHint<true, false>("b",
                                               Value(DateTime(2000, 1, 1, 12, 0, 0, 0)),
                                               Value(DateTime(2000, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4), "case3.6");
    // where a = 2000-01-01T0:0:0.0 and 2000-1-1T12:0:0.0 < b <= 2020-12-31T23:59:59.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeColumnHint<false, true>("b",
                                               Value(DateTime(2000, 1, 1, 12, 0, 0, 0)),
                                               Value(DateTime(2000, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(4, 5), "case3.7");
    // where a = 2000-01-01T0:0:0.0 and 2000-1-1T12:0:0.0 <= b <= 2020-12-31T23:59:59.0
    columnHints = {makeColumnHint("a", Value(DateTime(2000, 1, 1, 0, 0, 0, 0))),
                   makeColumnHint<true, true>("b",
                                              Value(DateTime(2000, 1, 1, 12, 0, 0, 0)),
                                              Value(DateTime(2000, 12, 31, 23, 59, 59, 0)))};
    checkTag(kvstore.get(), schema, indices[0], columnHints, expect(2, 4, 5), "case3.8");
  }
}

TEST_F(IndexScanScalarType, Geography) {
  auto rows = R"(
    geography
    POINT(108.1 32.5)
    POINT(103.8 32.3)
    LINESTRING(68.9 48.9,76.1 35.5,125.7 28.2)
    POLYGON((91.2 38.6,99.7 41.9,111.2 38.9,115.6 33.2,109.5 29.0,105.8 24.1,102.9 30.5,93.0 28.1,95.4 32.8,86.1 33.6,85.3 38.8,91.2 38.6))
    POLYGON((88.9 42.2,92.5 39.5,104.2 39.6,115.5 36.5,114.4 24.4,98.9 20.3,109.5 31.3,91.4 25.6,95.4 33.1,88.9 42.2))
  )"_row;
  /* Format: WKT:[CellID...]. The expected result comes from BigQuery.
  POINT(108.1 32.5): [0x368981adc0392fe3]
  POINT(103.8 32.3): [0x36f0b347378c3683]
  LINESTRING(68.9 48.9,76.1 35.5,125.7 28.2):
    [0x3440000000000000,0x34ff000000000000,0x3640000000000000,0x3684000000000000,
     0x37c0000000000000,0x3840000000000000,0x38c0000000000000,0x4240000000000000]
  POLYGON((91.2 38.6,99.7 41.9,111.2 38.9,115.6 33.2,109.5 29.0,
           105.8 24.1,102.9 30.5,93.0 28.1,95.4 32.8,86.1 33.6,85.3 38.8,91.2 38.6)):
    [0x342b000000000000,0x35d0000000000000,0x3700000000000000,0x3830000000000000,
     0x39d0000000000000]
  POLYGON((88.9 42.2,92.5 39.5,104.2 39.6,115.5 36.5,114.4 24.4,98.9 20.3,
           109.5 31.3,91.4 25.6,95.4 33.1,88.9 42.2)):
    [0x30d4000000000000,0x3130000000000000,0x341c000000000000,0x3430000000000000,
     0x35d4000000000000,0x35dc000000000000,0x3700000000000000,0x381c000000000000]
  */
  auto schema = R"(
    geo   | geography | | false
  )"_schema;
  auto indices = R"(
    TAG(t,1)
    (i1,2):geo
  )"_index(schema);
  bool hasNullableCol = schema->hasNullableCol();
  auto kv = encodeTag(rows, 1, schema, indices);
  auto kvstore = std::make_unique<MockKVStore>();
  for (auto& iter : kv) {
    for (auto& item : iter) {
      kvstore->put(item.first, item.second);
    }
  }
  auto actual = [&](std::shared_ptr<IndexItem> index,
                    const std::vector<ColumnHint>& columnHints) -> auto {
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(
        context.get(), 0, columnHints, kvstore.get(), hasNullableCol);
    IndexScanTestHelper helper;
    helper.setIndex(scanNode.get(), index);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid};
    scanNode->init(initCtx);
    scanNode->execute(0);

    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    return result;
  };
  auto encodeCellId = [](int64_t i) -> std::string {
    // First, reinterpret the int64_t as uint64_t
    uint64_t u = static_cast<uint64_t>(i);
    // Then, encode the uint64_t as string
    return IndexKeyUtils::encodeUint64(u);
  };
  // For the Geography type, there are only two cases: prefix and [x, y].
  /* Case 1: Prefix */
  // Explicitly specify the index column hints
  {
    std::vector<ColumnHint> columnHints;
    columnHints = {makeColumnHint("geo", encodeCellId(0x368981adc0392fe3))};
    EXPECT_EQ(expect(0), actual(indices[0], columnHints));
    columnHints = {makeColumnHint("geo", encodeCellId(0x36f0b347378c3683))};
    EXPECT_EQ(expect(1), actual(indices[0], columnHints));
    columnHints = {makeColumnHint("geo", encodeCellId(0x381c000000000000))};
    EXPECT_EQ(expect(4), actual(indices[0], columnHints));
    columnHints = {makeColumnHint("geo", encodeCellId(0x3440000000000000))};
    EXPECT_EQ(expect(2), actual(indices[0], columnHints));
    columnHints = {makeColumnHint("geo", encodeCellId(0x4240000000000000))};
    EXPECT_EQ(expect(2), actual(indices[0], columnHints));
    columnHints = {makeColumnHint("geo", encodeCellId(0x342b000000000000))};
    EXPECT_EQ(expect(3), actual(indices[0], columnHints));
    columnHints = {makeColumnHint("geo", encodeCellId(0x3700000000000000))};
    EXPECT_EQ(expect(3, 4), actual(indices[0], columnHints));
  }
  // Let GeoIndex generates the index column hints
  {
      // TODO(jie)
  } /* Case 2: [x, y] */
  // Explicitly specify the index column hints
  {
    auto hint = [&encodeCellId](const char* name, int64_t begin, int64_t end) {
      return std::vector<ColumnHint>{
          makeColumnHint<true, true>(name, encodeCellId(begin), encodeCellId(end))};
    };
    auto columnHint = hint("geo", 0x36f0b347378c3683, 0x36f0b347378c3683);
    EXPECT_EQ(expect(1), actual(indices[0], columnHint));
    columnHint = hint("geo", 0x35d0000000000000, 0x35dfffffffffffff);
    EXPECT_EQ(expect(3, 4), actual(indices[0], columnHint));
    columnHint = hint("geo", 0x3600000000000000, 0x38ffffffffffffff);
    EXPECT_EQ(expect(2, 0, 1, 3, 4), actual(indices[0], columnHint));
    columnHint = hint("geo", 0x36f0b00000000000, 0x3700000000000000);
    EXPECT_EQ(expect(1, 3, 4), actual(indices[0], columnHint));
  }
  // Let GeoIndex generates the index column hints
  {
    // TODO(jie)
  }
}

TEST_F(IndexScanTest, Compound) {
  // TODO(hs.zhang): add unittest
}

class IndexTest : public ::testing::Test {
 protected:
  static PlanContext* getPlanContext() {
    static std::unique_ptr<PlanContext> ctx = std::make_unique<PlanContext>(nullptr, 0, 8, false);
    return ctx.get();
  }
  static std::unique_ptr<RuntimeContext> makeContext() {
    auto ctx = std::make_unique<RuntimeContext>(getPlanContext());
    ctx->tagId_ = 0;
    ctx->edgeType_ = 0;
    return ctx;
  }
  static std::vector<Row> collectResult(IndexNode* node) {
    std::vector<Row> result;
    InitContext initCtx;
    node->init(initCtx);
    while (true) {
      auto res = node->next();
      ASSERT(res.success());
      if (!res.hasData()) {
        break;
      }
      result.emplace_back(std::move(res).row());
    }
    return result;
  }
  static std::vector<Row> pick(const std::vector<Row>& rows, const std::vector<size_t>& indices) {
    std::vector<Row> ret;
    for (auto i : indices) {
      ret.push_back(rows[i]);
    }
    return ret;
  }
  ::nebula::ObjectPool pool;
};

TEST_F(IndexTest, Selection) {
  const auto rows = R"(
    int     | int
    1       | 2
    <null>  | <null>
    8       | 10
    8       | 10
  )"_row;
  size_t currentOffset = 0;
  auto ctx = makeContext();
  auto expr = RelationalExpression::makeGE(&pool,
                                           TagPropertyExpression::make(&pool, "", "a"),
                                           ConstantExpression::make(&pool, Value(5)));

  auto selection = std::make_unique<IndexSelectionNode>(ctx.get(), expr);
  auto mockChild = std::make_unique<MockIndexNode>(ctx.get());
  mockChild->executeFunc = [](PartitionID) { return ::nebula::cpp2::ErrorCode::SUCCEEDED; };
  mockChild->nextFunc = [&rows, &currentOffset]() -> IndexNode::Result {
    if (currentOffset < rows.size()) {
      auto row = rows[currentOffset++];
      return IndexNode::Result(std::move(row));
    } else {
      return IndexNode::Result();
    }
  };
  mockChild->initFunc = [](InitContext& initCtx) -> ::nebula::cpp2::ErrorCode {
    initCtx.returnColumns = {"a", "b"};
    initCtx.retColMap = {{"a", 0}, {"b", 1}};
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  selection->addChild(std::move(mockChild));
  ASSERT_EQ(collectResult(selection.get()), pick(rows, {2, 3}));
}

TEST_F(IndexTest, Projection) {
  const auto rows = R"(
    int | int | int
    1   | 2   | 3
    4   | 5   | 6
    7   | 8   |9
  )"_row;
  size_t currentOffset = 0;
  auto ctx = makeContext();
  auto projection =
      std::make_unique<IndexProjectionNode>(ctx.get(), std::vector<std::string>{"c", "a", "b"});
  auto mockChild = std::make_unique<MockIndexNode>(ctx.get());
  mockChild->executeFunc = [](PartitionID) { return ::nebula::cpp2::ErrorCode::SUCCEEDED; };
  mockChild->nextFunc = [&rows, &currentOffset]() -> IndexNode::Result {
    if (currentOffset < rows.size()) {
      auto row = rows[currentOffset++];
      return IndexNode::Result(std::move(row));
    } else {
      return IndexNode::Result();
    }
  };
  mockChild->initFunc = [](InitContext& initCtx) -> ::nebula::cpp2::ErrorCode {
    initCtx.returnColumns = {"a", "b", "c"};
    initCtx.retColMap = {{"a", 0}, {"b", 1}, {"c", 2}};
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  projection->addChild(std::move(mockChild));
  auto expect = R"(
    int | int | int
    3   | 1   | 2
    6   | 4   | 5
    9   | 7   | 8
  )"_row;
  ASSERT_EQ(collectResult(projection.get()), expect);
}

TEST_F(IndexTest, Limit) {
  auto genRows = [](int start, int end) {
    std::vector<Row> ret;
    for (int i = start; i < end; i++) {
      Row row;
      row.emplace_back(Value(i));
      row.emplace_back(Value(i * i));
      row.emplace_back(Value(i * i * i));
      ret.emplace_back(std::move(row));
    }
    return ret;
  };
  auto rows = genRows(0, 1000);
  size_t currentOffset = 0;
  auto ctx = makeContext();
  auto limit = std::make_unique<IndexLimitNode>(ctx.get(), 10);
  auto mockChild = std::make_unique<MockIndexNode>(ctx.get());
  mockChild->executeFunc = [](PartitionID) { return ::nebula::cpp2::ErrorCode::SUCCEEDED; };
  mockChild->nextFunc = [&rows, &currentOffset]() -> IndexNode::Result {
    if (currentOffset < rows.size()) {
      auto row = rows[currentOffset++];
      return IndexNode::Result(std::move(row));
    } else {
      return IndexNode::Result();
    }
  };
  mockChild->initFunc = [](InitContext&) -> ::nebula::cpp2::ErrorCode {
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  limit->addChild(std::move(mockChild));
  ASSERT_EQ(collectResult(limit.get()), genRows(0, 10));
}

TEST_F(IndexTest, Dedup) {
  auto rows1 = R"(
    int | int
    1   | 2
    1   | 3
    2   | 2
  )"_row;
  auto rows2 = R"(
    int | int
    1   | 4
    2   | 3
    1   | 5
    3   | 6
  )"_row;
  size_t offset1 = 0, offset2 = 0;
  auto ctx = makeContext();
  auto dedup = std::make_unique<IndexDedupNode>(ctx.get(), std::vector<std::string>{"a"});
  auto child1 = std::make_unique<MockIndexNode>(ctx.get());
  child1->executeFunc = [](PartitionID) { return ::nebula::cpp2::ErrorCode::SUCCEEDED; };
  child1->nextFunc = [&rows1, &offset1]() -> IndexNode::Result {
    if (offset1 < rows1.size()) {
      auto row = rows1[offset1++];
      return IndexNode::Result(std::move(row));
    } else {
      return IndexNode::Result();
    }
  };
  child1->initFunc = [](InitContext& initCtx) -> ::nebula::cpp2::ErrorCode {
    initCtx.returnColumns = {"a", "b"};
    initCtx.retColMap = {{"a", 0}, {"b", 1}};
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  auto child2 = std::make_unique<MockIndexNode>(ctx.get());
  child2->executeFunc = [](PartitionID) { return ::nebula::cpp2::ErrorCode::SUCCEEDED; };
  child2->nextFunc = [&rows2, &offset2]() -> IndexNode::Result {
    if (offset2 < rows2.size()) {
      auto row = rows2[offset2++];
      return IndexNode::Result(std::move(row));
    } else {
      return IndexNode::Result();
    }
  };
  child2->initFunc = [](InitContext& initCtx) -> ::nebula::cpp2::ErrorCode {
    initCtx.returnColumns = {"a", "b"};
    initCtx.retColMap = {{"a", 0}, {"b", 1}};
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  dedup->addChild(std::move(child1));
  dedup->addChild(std::move(child2));
  auto expect = R"(
    int | int
    1   | 2
    2   | 2
    3   | 6
  )"_row;
  ASSERT_EQ(collectResult(dedup.get()), expect);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
