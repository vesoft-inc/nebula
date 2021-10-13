/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include <limits>

#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVIterator.h"
#include "storage/exec/IndexDedupNode.h"
#include "storage/exec/IndexEdgeScanNode.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexProjectionNode.h"
#include "storage/exec/IndexSelectionNode.h"
#include "storage/exec/IndexVertexScanNode.h"
#include "storage/test/IndexTestUtil.h"
namespace nebula {
namespace storage {
namespace {
int schemaVer = 2;
}
/**
 * IndexScanTest
 *
 * Test:
 * 1. Vertex/Edge
 * 2. back to table or not
 * 3. different value type
 *    a. int/float/bool/fix_string/time/date/datetime
 *    b. compound index
 * 4. range/prefix
 *    a. prefix(equal)
 *    b. range with begin is include/exclude/-INF
 *    c. range with end id include/exclude/+INF
 * 5. nullable
 * Case:
 * ┌──────────┬─────────────────────────────────────────┐
 * │ section1 │ name │ case │ description  │  NOTICE  │
 *   Base     | Base |
 *            | Vertex| IndexOnly
 *            |       | BackToTable
 *            | Edge  | indexOnly
 *                    | BackToTable
 *   Value Type | Int     |  PrefixWithTruncate
 *              | Float   |  PrefixWithoutTruncate
 *              | Bool    |  INCLUDE_INF
 *              | String  |  INCLUDE_EXCLUDE
 *              | Time    |  EXCLUDE_INF
 *              | Date    |  EXCLUDE_INCLUDE
 *              | DateTime|  INF_INCLUDE
 *              | Compound|  INF_EXCLUDE
 *              | Nullable|
 *
 * ┌┬┐├┼┤└┴┘┌─┐│┼│└─┘
 *
 */
struct IndexScanTestHelper {
  void setKVStore(IndexScanNode* node, kvstore::KVStore* store) { node->kvstore_ = store; }
  void setIndex(IndexVertexScanNode* node, std::shared_ptr<::nebula::meta::cpp2::IndexItem> index) {
    node->getIndex = [index]() { return index; };
  }
  void setIndex(IndexEdgeScanNode* node, std::shared_ptr<::nebula::meta::cpp2::IndexItem> index) {
    node->getIndex = [index]() { return index; };
  }
  void setTag(IndexVertexScanNode* node,
              std::shared_ptr<::nebula::meta::NebulaSchemaProvider> schema) {
    node->getTag = [schema]() { return schema; };
  }
  void setEdge(IndexEdgeScanNode* node,
               std::shared_ptr<::nebula::meta::NebulaSchemaProvider> schema) {
    node->getEdge = [schema]() { return schema; };
  }
  void setFatal(IndexScanNode* node, bool value) { node->fatalOnBaseNotFound_ = value; }
};
class IndexScanTest : public ::testing::Test {
 protected:
  using Schema = ::nebula::meta::NebulaSchemaProvider;
  using IndexItem = ::nebula::meta::cpp2::IndexItem;
  using ColumnHint = ::nebula::storage::cpp2::IndexColumnHint;
  static ColumnHint makeColumnHint(const std::string& name, const Value& value) {
    ColumnHint hint;
    hint.set_column_name(name);
    hint.set_begin_value(value);
    hint.set_scan_type(cpp2::ScanType::PREFIX);
    return hint;
  }
  template <bool includeBegin, bool includeEnd>
  static ColumnHint makeColumnHint(const std::string& name, const Value& begin, const Value& end) {
    ColumnHint hint;
    hint.set_column_name(name);
    hint.set_scan_type(cpp2::ScanType::RANGE);
    hint.set_begin_value(begin);
    hint.set_end_value(end);
    hint.set_include_begin(includeBegin);
    hint.set_include_end(includeEnd);
    return hint;
  }
  template <bool include>
  static ColumnHint makeBeginColumnHint(const std::string& name, const Value& begin) {
    ColumnHint hint;
    hint.set_column_name(name);
    hint.set_scan_type(cpp2::ScanType::RANGE);
    hint.set_begin_value(begin);
    hint.set_include_begin(include);
    return hint;
  }
  template <bool include>
  static ColumnHint makeEndColumnHint(const std::string& name, const Value& end) {
    ColumnHint hint;
    hint.set_column_name(name);
    hint.set_scan_type(cpp2::ScanType::RANGE);
    hint.set_end_value(end);
    hint.set_include_end(include);
    return hint;
  }
  static std::vector<std::map<std::string, std::string>> encodeTag(
      const std::vector<Row>& rows,
      TagID tagId,
      std::shared_ptr<Schema> schema,
      std::vector<std::shared_ptr<IndexItem>> indices) {
    std::vector<std::map<std::string, std::string>> ret(indices.size() + 1);
    for (size_t i = 0; i < rows.size(); i++) {
      auto key = NebulaKeyUtils::vertexKey(8, 0, std::to_string(i), tagId);
      RowWriterV2 writer(schema.get());
      for (size_t j = 0; j < rows[i].size(); j++) {
        writer.setValue(j, rows[i][j]);
      }
      writer.finish();
      auto value = writer.moveEncodedStr();
      assert(ret[0].insert({key, value}).second);
      RowReaderWrapper reader(schema.get(), folly::StringPiece(value), schemaVer);
      for (size_t j = 0; j < indices.size(); j++) {
        auto& index = indices[j];
        auto indexValue = IndexKeyUtils::collectIndexValues(&reader, index->get_fields()).value();
        DVLOG(1) << folly::hexDump(indexValue.data(), indexValue.size());
        auto indexKey = IndexKeyUtils::vertexIndexKey(
            8, 0, index->get_index_id(), std::to_string(i), std::move(indexValue));
        assert(ret[j + 1].insert({indexKey, ""}).second);
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
      DVLOG(1) << '\n' << folly::hexDump(key.data(), key.size());
      RowWriterV2 writer(schema.get());
      for (size_t j = 0; j < rows[i].size(); j++) {
        writer.setValue(j, rows[i][j]);
      }
      writer.finish();
      auto value = writer.moveEncodedStr();
      assert(ret[0].insert({key, value}).second);
      RowReaderWrapper reader(schema.get(), folly::StringPiece(value), schemaVer);
      for (size_t j = 0; j < indices.size(); j++) {
        auto& index = indices[j];
        auto indexValue = IndexKeyUtils::collectIndexValues(&reader, index->get_fields()).value();
        auto indexKey = IndexKeyUtils::edgeIndexKey(8,
                                                    0,
                                                    index->get_index_id(),
                                                    std::to_string(i),
                                                    i,
                                                    std::to_string(i),
                                                    std::move(indexValue));
        DVLOG(1) << '\n' << folly::hexDump(indexKey.data(), indexKey.size());
        assert(ret[j + 1].insert({indexKey, ""}).second);
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
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), indexId, columnHints);
    scanNode->kvstore_ = kvstore.get();
    scanNode->getIndex = [index = indices[0]]() { return index; };
    scanNode->getTag = [schema]() { return schema; };
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "a"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
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
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), indexId, columnHints);
    DVLOG(1) << kvstore.get();
    scanNode->kvstore_ = kvstore.get();
    scanNode->getIndex = [index = indices[1]]() { return index; };
    scanNode->getTag = [schema]() { return schema; };
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "b"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
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
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), indexId, columnHints);
    scanNode->kvstore_ = kvstore.get();
    scanNode->getIndex = [index = indices[0]]() { return index; };
    scanNode->getTag = [schema]() { return schema; };
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "a"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
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
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), indexId, columnHints);
    scanNode->kvstore_ = kvstore.get();
    scanNode->getIndex = [index = indices[0]]() { return index; };
    scanNode->getTag = [schema]() { return schema; };
    InitContext initCtx;
    initCtx.requiredColumns = {kVid, "b"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
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
    auto scanNode = std::make_unique<IndexEdgeScanNode>(context.get(), indexId, columnHints);
    scanNode->kvstore_ = kvstore.get();
    scanNode->getIndex = [index = indices[0]]() { return index; };
    scanNode->getEdge = [schema]() { return schema; };
    InitContext initCtx;
    initCtx.requiredColumns = {kSrc, kRank, kDst, "c"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
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
      DVLOG(1) << result[i];
      for (size_t j = 0; j < expect[i].size(); j++) {
        EXPECT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }  // End of Case 1
  {  // Case 2: Access base data
    for (auto& item : kv[0]) {
      kvstore->put(item.first, item.second);
    }
    auto scanNode = std::make_unique<IndexEdgeScanNode>(context.get(), indexId, columnHints);
    scanNode->kvstore_ = kvstore.get();
    scanNode->kvstore_ = kvstore.get();
    scanNode->getIndex = [index = indices[0]]() { return index; };
    scanNode->getEdge = [schema]() { return schema; };
    InitContext initCtx;
    initCtx.requiredColumns = {kSrc, kRank, kDst, "a"};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
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
      DVLOG(1) << result[i];
      for (size_t j = 0; j < expect[i].size(); j++) {
        EXPECT_EQ(expect[i][j], result[i][initCtx.retColMap[colOrder[j]]]);
      }
    }
  }
}
TEST_F(IndexScanTest, Int) {
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
  auto check = [&](std::shared_ptr<IndexItem> index,
                   const std::vector<ColumnHint>& columnHints,
                   const std::vector<Row>& expect,
                   const std::string& case_) {
    DVLOG(1) << "Start case " << case_;
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), 0, columnHints);
    IndexScanTestHelper helper;
    helper.setKVStore(scanNode.get(), kvstore.get());
    helper.setIndex(scanNode.get(), index);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
    }
    EXPECT_EQ(result, expect) << "Fail at case " << case_;
  };
  auto expect = [](auto... vidList) {
    std::vector<Row> ret;
    std::vector<Value> value;
    (value.push_back(std::to_string(vidList)), ...);
    for (auto& v : value) {
      Row row;
      row.emplace_back(v);
      ret.emplace_back(std::move(row));
    }
    return ret;
  };
  const int64_t MAX = 0x7fffffffffffffff;
  const int64_t MIN = -MAX - 1;
  /* Case 1: Prefix */
  {
    std::vector<ColumnHint> columnHints = {makeColumnHint("a", 1)};  // a=1;
    check(indices[0], columnHints, expect(0), "case1.1");            //
    columnHints = {makeColumnHint("b", MAX)};                        // b=MAX
    check(indices[1], columnHints, expect(3), "case1.2");            //
    columnHints = {makeColumnHint("b", MIN)};                        // b=MIN
    check(indices[1], columnHints, expect(4), "case1.3");            //
    columnHints = {makeColumnHint("c", 0)};                          // c=0
    check(indices[2], columnHints, expect(3, 5), "case1.4");         //
  }                                                                  // End of Case 1
  /* Case 2: [x, INF) */
  {
    std::vector<ColumnHint> columnHints = {makeBeginColumnHint<true>("a", -1)};  // Case2.1: a >= -1
    check(indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case2.1");         //
    columnHints = {makeBeginColumnHint<true>("a", 4)};                           // Case2.2: a>=4
    check(indices[0], columnHints, expect(3, 4, 5), "case2.2");                  //
    columnHints = {makeBeginColumnHint<true>("a", 7)};                           // Case2.3: a>=7
    check(indices[0], columnHints, {}, "case2.3");                               //
    columnHints = {makeBeginColumnHint<true>("b", MIN)};                  // Case2.4: b>=INT_MIN
    check(indices[1], columnHints, expect(4, 0, 2, 1, 3), "case2.4");     //
    columnHints = {makeBeginColumnHint<true>("b", MAX)};                  // Case2.5: b>=INT_MAX
    check(indices[1], columnHints, expect(3), "case2.5");                 //
    columnHints = {makeBeginColumnHint<true>("b", 0)};                    // Case2.6: b>=0
    check(indices[1], columnHints, expect(2, 1, 3), "case2.6");           //
    columnHints = {makeBeginColumnHint<true>("c", MIN)};                  // Case2.7: c>=INT_MIN
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5, 4), "case2.7");  //
    columnHints = {makeBeginColumnHint<true>("c", MAX)};                  // Case2.8: c>=INT_MAX
    check(indices[2], columnHints, expect(4), "case2.8");                 //
    columnHints = {makeBeginColumnHint<true>("c", 0)};                    // Case2.9: c>=0
    check(indices[2], columnHints, expect(3, 5, 4), "case2.9");           //
  }                                                                       // End of Case 2
  /* Case 3: [x, y) */
  {
    std::vector<ColumnHint> columnHints;                                  //
    columnHints = {makeColumnHint<true, false>("a", -1, 10)};             // Case3.1: -1<=a<10
    check(indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case3.1");  //
    columnHints = {makeColumnHint<true, false>("a", -100, 4)};            // Case3.2: -100<=a<4
    check(indices[0], columnHints, expect(0, 1, 2), "case3.2");           //
    columnHints = {makeColumnHint<true, false>("a", 4, 100)};             // Case3.3: 4<=a<100
    check(indices[0], columnHints, expect(3, 4, 5), "case3.3");           //
    columnHints = {makeColumnHint<true, false>("a", 2, 5)};               // Case3.4: 2<=a<5
    check(indices[0], columnHints, expect(1, 2, 3), "case3.4");           //
    columnHints = {makeColumnHint<true, false>("a", -100, 0)};            // Case3.5: -100<=a<0
    check(indices[0], columnHints, {}, "case3.5");                        //
    columnHints = {makeColumnHint<true, false>("a", 10, 100)};            // Case3.6: 10<=a<100
    check(indices[0], columnHints, {}, "case3.6");                        //
    columnHints = {makeColumnHint<true, false>("b", MIN, MAX)};           // Case3.7: MIN<=b<MAX
    check(indices[1], columnHints, expect(4, 0, 2, 1), "case3.7");        //
    columnHints = {makeColumnHint<true, false>("c", MIN, MAX)};           // Case3.8: MIN<=c<MAX
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5), "case3.8");     //
  }                                                                       // End of Case 3
  /* Case 4: (x, INF) */
  {
    std::vector<ColumnHint> columnHints;                               //
    columnHints = {makeBeginColumnHint<false>("a", 3)};                // Case 4.1: a>3
    check(indices[0], columnHints, expect(3, 4, 5), "case4.1");        //
    columnHints = {makeBeginColumnHint<false>("b", MIN)};              // Case 4.2: b>MIN
    check(indices[1], columnHints, expect(0, 2, 1, 3), "case4.2");     //
    columnHints = {makeBeginColumnHint<false>("b", MAX)};              // Case4.3: b>MAX
    check(indices[1], columnHints, {}, "case4.3");                     //
    columnHints = {makeBeginColumnHint<false>("c", MIN)};              // Case4.4: c>MIN
    check(indices[2], columnHints, expect(0, 2, 3, 5, 4), "case4.4");  //
    columnHints = {makeBeginColumnHint<false>("c", MAX - 1)};          // Case4.5: c>MAX-1
    check(indices[2], columnHints, expect(4), "case4.4");              //
  }                                                                    // End of Case 4
  /* Case 5: (x, y] */
  {
    std::vector<ColumnHint> columnHints;                               //
    columnHints = {makeColumnHint<false, true>("a", 1, 6)};            // Case5.1: 1<a<=6
    check(indices[0], columnHints, expect(1, 2, 3, 4, 5), "case5.1");  //
    columnHints = {makeColumnHint<false, true>("a", 0, 3)};            // Case5.2: 0<a<=3
    check(indices[0], columnHints, expect(0, 1, 2), "case5.2");        //
    columnHints = {makeColumnHint<false, true>("b", MIN, MIN)};        // Case5.3: MIN<b<=MAX
    check(indices[1], columnHints, {}, "case5.3");                     //
    columnHints = {makeColumnHint<false, true>("b", MAX, MAX)};        // Case5.4: MAX<b<=MAX
    check(indices[1], columnHints, {}, "case5.4");                     //
    columnHints = {makeColumnHint<false, true>("b", 0, MAX)};          // Case5.5: 0<b<=MAX
    check(indices[1], columnHints, expect(1, 3), "case5.5");           //
    columnHints = {makeColumnHint<false, true>("c", -1, MAX)};         // Case5.6: -1<c<=MAX
    check(indices[2], columnHints, expect(3, 5, 4), "case5.6");        //
  }                                                                    // End of Case 5
  /* Case 6: (-INF,y]*/
  {
    std::vector<ColumnHint> columnHints;                                  //
    columnHints = {makeEndColumnHint<true>("a", 4)};                      // Case6.1: a<=4
    check(indices[0], columnHints, expect(0, 1, 2, 3), "case6.1");        //
    columnHints = {makeEndColumnHint<true>("a", 1)};                      // Case6.2: a<=1
    check(indices[0], columnHints, expect(0), "case6.2");                 //
    columnHints = {makeEndColumnHint<true>("b", MIN)};                    // Case6.3: b<=MIN
    check(indices[1], columnHints, expect(4), "case6.3");                 //
    columnHints = {makeEndColumnHint<true>("b", MAX)};                    // Case6.4: b<=MAX
    check(indices[1], columnHints, expect(4, 0, 2, 1, 3), "casae6.4");    //
    columnHints = {makeEndColumnHint<true>("c", MIN)};                    // Case6.5: c<=MIN
    check(indices[2], columnHints, expect(1), "case6.5");                 //
    columnHints = {makeEndColumnHint<true>("c", MAX)};                    // Case6.6: c<=MAX
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5, 4), "case6.6");  //
  }                                                                       // End of Case 6
  /* Case 7: (-INF, y) */
  {
    std::vector<ColumnHint> columnHints;                               //
    columnHints = {makeEndColumnHint<false>("a", 4)};                  // Case7.1: a<4
    check(indices[0], columnHints, expect(0, 1, 2), "case7.1");        //
    columnHints = {makeEndColumnHint<false>("a", 1)};                  // Case7.2: a<1
    check(indices[0], columnHints, {}, "case7.2");                     //
    columnHints = {makeEndColumnHint<false>("b", MIN)};                // Case7.3: b<MIN
    check(indices[1], columnHints, {}, "case7.3");                     //
    columnHints = {makeEndColumnHint<false>("b", MAX)};                // Case7.4: b<MAX
    check(indices[1], columnHints, expect(4, 0, 2, 1), "casae7.4");    //
    columnHints = {makeEndColumnHint<false>("c", MIN)};                // Case7.5: c<MIN
    check(indices[2], columnHints, {}, "case7.5");                     //
    columnHints = {makeEndColumnHint<false>("c", MAX)};                // Case7.6: c<MAX
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5), "case7.6");  //
  }                                                                    // End of Case 7
}
TEST_F(IndexScanTest, Float) {
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
  auto check = [&](std::shared_ptr<IndexItem> index,
                   const std::vector<ColumnHint>& columnHints,
                   const std::vector<Row>& expect,
                   const std::string& case_) {
    DVLOG(1) << "Start case " << case_;
    auto context = makeContext(0, 1);
    auto scanNode = std::make_unique<IndexEdgeScanNode>(context.get(), 0, columnHints);
    IndexScanTestHelper helper;
    helper.setKVStore(scanNode.get(), kvstore.get());
    helper.setIndex(scanNode.get(), index);
    helper.setEdge(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kSrc};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
    }
    EXPECT_EQ(result, expect) << "Fail at case " << case_;
    DVLOG(1) << "End case " << case_;
  };
  auto expect = [](auto... vidList) {
    std::vector<Row> ret;
    std::vector<Value> value;
    (value.push_back(std::to_string(vidList)), ...);
    for (auto& v : value) {
      Row row;
      row.emplace_back(v);
      ret.emplace_back(std::move(row));
    }
    return ret;
  };
  /* Case 1: prefix */ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    check(indices[0], hint("a", 1000.0), {}, "case1.1");           // Case1.1: a=1000.0
    check(indices[0], hint("a", 0.0), expect(3, 4), "case1.2");    // Case1.2: a=0.0
    check(indices[1], hint("b", MAX_NV), expect(12), "case1.3");   // Case1.3: b=MAX_NV
    check(indices[1], hint("b", MIN_NV), expect(10), "case1.4");   // Case1.4: b=MIN_NV
    check(indices[1], hint("b", MAX_SV), expect(8), "case1.5");    // Case1.5: b=MAX_SV
    check(indices[1], hint("b", MIN_SV), expect(6), "case1.6");    // Case1.6: b=MIN_SV
    check(indices[1], hint("b", -MAX_NV), expect(13), "case1.7");  // Case1.7: b=-MAX_NV
    check(indices[1], hint("b", -MIN_NV), expect(11), "case1.8");  // Case1.8: b=-MIN_NV
    check(indices[1], hint("b", -MAX_SV), expect(9), "case1.9");   // Case1.9: b=-MAX_SV
    check(indices[1], hint("b", -MIN_SV), expect(7), "case1.10");  // Case1.10 b=-MIN_SV
    check(indices[1], hint("b", 0.0), expect(0, 1), "case1.11");   // Case1.11: b=0.0
    check(indices[1], hint("b", -0.0), expect(0, 1), "case1.12");  // Case1.12: b=-0.0
    check(indices[1], hint("b", INF), expect(2), "case1.13");      // Case1.13: b=<INF>
    check(indices[1], hint("b", -INF), expect(3), "case1.14");     // Case1.14: b=<-INF>
    check(indices[2], hint("c", INF), expect(4, 5), "case1.15");   // Case1.15: c=<INF>
  }                                                                // End of Case 1
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
    check(indices[0], hint("a", -100.0), slice(aOrder, 1), "case2.1");  // Case 2.1: a>=-100
    check(indices[0], hint("a", 0.0), slice(aOrder, 5), "case2.2");     // Case 2.2: a>=0.0
    // Case 2.3~2.14: a>={each of $val}
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case2.{}", i + 3);
      auto offset = i;
      if (val[i] == 0 && val[i - 1] == 0) {
        offset--;
      }
      check(indices[1], hint("b", val[i]), slice(bOrder, offset), case_);
    }
    check(indices[2], hint("c", -INF), slice(cOrder, 0), "case2.15");
    check(indices[2], hint("c", 0.0), slice(cOrder, 2), "case2.16");
    check(indices[2], hint("c", MAX_NV), slice(cOrder, 4), "case2.17");
    check(indices[2], hint("c", INF), slice(cOrder, 6), "case2.18");
  }
  /* Case 3: [x, y)*/ {
    auto hint = [](const char* name, double left, double right) {
      return std::vector<ColumnHint>{makeColumnHint<true, false>(name, left, right)};
    };
    auto slice = [](decltype(aOrder) all, size_t start, size_t end) {
      return decltype(all){all.begin() + start, all.begin() + std::min(end, all.size())};
    };
    check(
        indices[0], hint("a", -100.0, -0.0), slice(aOrder, 1, 5), "case3.1");  // Case3.1:-100<=a<0
    check(indices[0], hint("a", 10, 1e9), slice(aOrder, 9, 11), "case3.2");
    check(indices[0], hint("a", 1, 2), {}, "case3.3");
    check(indices[0], hint("a", -INF, INF), aOrder, "case3.4");
    check(indices[0], hint("a", -INF, 0), slice(aOrder, 0, 5), "case3.5");
    check(indices[0], hint("a", 0, INF), slice(aOrder, 5, 14), "case3.6");
    // Case 3.7~3.18: b<{each of $val}
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case3.{}", i + 7);
      auto offset = i;
      if (val[i] == 0 && val[i - 1] == 0) {
        offset--;
      }
      check(indices[1], hint("b", -INF, val[i]), slice(bOrder, 0, offset), case_);
    }
    check(indices[2], hint("c", -INF, INF), slice(cOrder, 0, 6), "case3.19");
  }
  /* Case 4: (x, INF)*/ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<false>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t start) {
      return decltype(all){all.begin() + start, all.end()};
    };
    check(indices[0], hint("a", 100), slice(aOrder, 11), "case4.1");
    check(indices[1], hint("b", INF), {}, "case4.2");
    int64_t x = *reinterpret_cast<const int64_t*>(&INF);
    x--;
    double y = *reinterpret_cast<double*>(&x);
    check(indices[1], hint("b", y), slice(bOrder, 11), "case4.3");
    check(indices[2], hint("c", INF), {}, "case4.4");
    check(indices[2], hint("c", y), slice(cOrder, 6), "case4.5");
  } /* Case 5: (x, y]*/
  {
    auto hint = [](const char* name, double left, double right) {
      return std::vector<ColumnHint>{makeColumnHint<false, true>(name, left, right)};
    };
    auto slice = [](decltype(aOrder) all, size_t start, size_t end) {
      return decltype(all){all.begin() + start, all.begin() + end};
    };
    check(
        indices[0], hint("a", -100.0, -0.0), slice(aOrder, 2, 7), "case5.1");  // Case3.1:-100<=a<0
    check(indices[0], hint("a", 10, 1e9), slice(aOrder, 9, 11), "case5.2");
    check(indices[0], hint("a", 1, 2), {}, "case5.3");
    check(indices[0], hint("a", -INF, INF), aOrder, "case5.4");
    check(indices[0], hint("a", -INF, 0), slice(aOrder, 0, 7), "case5.5");
    check(indices[0], hint("a", 0, INF), slice(aOrder, 7, 14), "case5.6");
    // Case 5.7~5.18: b>{each of $val}
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case5.{}", i + 7);
      auto offset = i + 1;
      if (val[i] == 0 && val[i + 1] == 0) {
        offset++;
      }
      check(indices[1], hint("b", val[i], INF), slice(bOrder, offset, bOrder.size()), case_);
    }
    check(indices[2], hint("c", -INF, INF), slice(cOrder, 2, 8), "case5.19");
  } /* Case 6: (-INF, y]*/
  {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeEndColumnHint<true>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t end) {
      return decltype(all){all.begin(), all.begin() + end};
    };
    check(indices[0], hint("a", 0), slice(aOrder, 7), "case6.1");
    check(indices[0], hint("a", -0.0), slice(aOrder, 7), "case6.2");
    check(indices[0], hint("a", -100.0), slice(aOrder, 2), "case6.3");
    // Case 6.4~6.15
    std::vector<double> val{
        -INF, -MAX_NV, -MIN_NV, -MAX_SV, -MIN_SV, -0.0, 0.0, MIN_SV, MAX_SV, MIN_NV, MAX_NV, INF};
    for (size_t i = 0; i < val.size(); i++) {
      std::string case_ = fmt::format("case6.{}", i + 3);
      auto offset = i + 1;
      if (val[i] == 0 && val[i + 1] == 0) {
        offset++;
      }
      check(indices[1], hint("b", val[i]), slice(bOrder, offset), case_);
    }
    check(indices[2], hint("c", INF), cOrder, "case6.16");
  }
  /* Case 7: (-INF, y)*/ {
    auto hint = [](const char* name, double value) {
      return std::vector<ColumnHint>{makeEndColumnHint<false>(name, value)};
    };
    auto slice = [](decltype(aOrder) all, size_t end) {
      return decltype(all){all.begin(), all.begin() + end};
    };
    check(indices[0], hint("a", 100), slice(aOrder, 10), "case7.1");
    check(indices[1], hint("b", -INF), {}, "case7.2");
    int64_t x = *reinterpret_cast<const int64_t*>(&INF);
    x--;
    double y = *reinterpret_cast<double*>(&x);
    check(indices[1], hint("b", -y), slice(bOrder, 1), "case7.3");
    check(indices[2], hint("c", -INF), {}, "case7.4");
    check(indices[2], hint("c", -y), slice(cOrder, 2), "case7.5");
  }
}
TEST_F(IndexScanTest, Bool) {
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
  auto check = [&](std::shared_ptr<IndexItem> index,
                   const std::vector<ColumnHint>& columnHints,
                   const std::vector<Row>& expect,
                   const std::string& case_) {
    DVLOG(1) << "Start case " << case_;
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), 0, columnHints);
    IndexScanTestHelper helper;
    helper.setKVStore(scanNode.get(), kvstore.get());
    helper.setIndex(scanNode.get(), index);
    helper.setTag(scanNode.get(), schema);
    InitContext initCtx;
    initCtx.requiredColumns = {kVid};
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
    }
    EXPECT_EQ(result, expect) << "Fail at case " << case_;
    DVLOG(1) << "End case " << case_;
  };
  auto expect = [](auto... vidList) {
    std::vector<Row> ret;
    std::vector<Value> value;
    (value.push_back(std::to_string(vidList)), ...);
    for (auto& v : value) {
      Row row;
      row.emplace_back(v);
      ret.emplace_back(std::move(row));
    }
    return ret;
  };
  /* Case 1: Prefix */ {
    check(indices[0], {makeColumnHint("a", true)}, expect(0, 1, 4), "case1.1");
    check(indices[0], {makeColumnHint("a", false)}, expect(2, 3), "case1.2");
    check(indices[1], {makeColumnHint("b", true)}, expect(0), "case1.3");
    check(indices[1], {makeColumnHint("b", false)}, expect(1, 3), "case1.4");
  }
  /* Case 2: [x,INF) */ {
    check(indices[0], {makeBeginColumnHint<true>("a", false)}, expect(2, 3, 0, 1, 4), "case2.1");
    check(indices[0], {makeBeginColumnHint<true>("a", true)}, expect(0, 1, 4), "case2.2");
    check(indices[1], {makeBeginColumnHint<true>("b", true)}, expect(0), "case2.3");
  }
}
TEST_F(IndexScanTest, String1) {
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
  auto check = [&](std::shared_ptr<IndexItem> index,
                   const std::vector<ColumnHint>& columnHints,
                   const std::vector<std::string>& acquiredColumns,
                   const std::vector<Row>& expect,
                   const std::string& case_) {
    DVLOG(1) << "Start case " << case_;
    auto context = makeContext(1, 0);
    auto scanNode = std::make_unique<IndexVertexScanNode>(context.get(), 0, columnHints);
    IndexScanTestHelper helper;
    helper.setKVStore(scanNode.get(), kvstore.get());
    helper.setIndex(scanNode.get(), index);
    helper.setTag(scanNode.get(), schema);
    helper.setFatal(scanNode.get(), true);
    InitContext initCtx;
    initCtx.requiredColumns.insert(acquiredColumns.begin(), acquiredColumns.end());
    scanNode->init(initCtx);
    scanNode->execute(0);
    bool hasNext = false;
    std::vector<Row> result;
    while (true) {
      auto res = scanNode->next(hasNext);
      ASSERT(::nebula::ok(res));
      if (!hasNext) {
        break;
      }
      result.emplace_back(::nebula::value(std::move(res)));
    }
    std::vector<Row> result2(result.size());
    for (size_t j = 0; j < acquiredColumns.size(); j++) {
      int p = initCtx.retColMap[acquiredColumns[j]];
      for (size_t i = 0; i < result.size(); i++) {
        result2[i].emplace_back(result[i][p]);
      }
    }
    result = result2;
    EXPECT_EQ(result, expect) << "Fail at case " << case_;
  };
  auto expect = [&rows](const std::vector<int64_t>& vidList, const std::vector<size_t>& columns) {
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
  };
  /* Case 1: prefix */ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeColumnHint(name, value)};
    };
    check(indices[0], hint("a", "123456789"), {kVid, "a"}, expect({0, 1}, {0}), "case1.1");
    check(indices[0], hint("a", "12345678"), {kVid, "a"}, expect({2}, {0}), "case1.2");
    check(indices[0], hint("a", ""), {kVid, "a"}, expect({6}, {0}), "case1.3");
    check(indices[1], hint("b", "\xFF\xFF"), {kVid, "b"}, expect({3}, {1}), "case1.4");
    check(indices[1], hint("b", ""), {kVid, "b"}, expect({6}, {1}), "case1.5");
    auto columnHint = hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE");
    check(indices[2], columnHint, {kVid, "c"}, expect({4}, {2}), "case1.6");
  }
  //                        0  1  2  3  4  5  6  7
  std::vector<int64_t> a = {6, 2, 3, 0, 1, 4, 5};
  std::vector<int64_t> b = {6, 4, 0, 5, 3};
  std::vector<int64_t> c = {1, 3, 5, 2, 4, 0};
  auto slice = [](decltype(a) all, int begin) {
    return decltype(all){all.begin() + begin, all.end()};
  };
  /* Case 2: [x, INF)*/ {
    auto hint = [](const char* name, const std::string& value) {
      return std::vector<ColumnHint>{makeBeginColumnHint<true>(name, value)};
    };
    check(indices[0], hint("a", "12345678"), {kVid, "a"}, expect(slice(a, 1), {0}), "case2.1");
    check(indices[0], hint("a", "123456780"), {kVid, "a"}, expect(slice(a, 2), {0}), "case2.2");
    check(indices[0], hint("a", ""), {kVid, "a"}, expect(a, {0}), "case2.3");
    check(indices[1], hint("b", ""), {kVid, "b"}, expect(b, {1}), "case2.4");
    check(indices[1], hint("b", "abc"), {kVid, "b"}, expect(slice(b, 2), {1}), "case2.5");
    check(indices[1], hint("b", "aac"), {kVid, "b"}, expect(slice(b, 1), {1}), "case2.6");
    check(indices[1], hint("b", "aacd\x01"), {kVid, "b"}, expect(slice(b, 2), {1}), "case2.7");
    check(indices[1], hint("b", "\xFF\xFF"), {kVid, "b"}, expect(slice(b, 4), {1}), "case2.8");
    check(indices[1], hint("b", "\xFF\xFF\x01"), {kVid, "b"}, {}, "case2.9");
    check(indices[2], hint("c", ""), {kVid, "c"}, expect(c, {2}), "case2.10");
    check(indices[2], hint("c", "\x01"), {kVid, "c"}, expect(slice(c, 3), {2}), "case2.11");
    check(indices[2],
          hint("c", "\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
          {kVid, "c"},
          expect(slice(c, 5), {2}),
          "case2.12");
  } /* Case 3: (x,y) */
  // {

  // } /* Case 4: (INF,y]*/ {}
}
TEST_F(IndexScanTest, String2) {
  /**
   * data with truncate
   * query without truncate
   */
}
TEST_F(IndexScanTest, String3) {
  /**
   * data without truncate
   * query with truncate
   */
}
TEST_F(IndexScanTest, String4) {
  /**
   * data with truncate
   * query with truncate
   */
}
TEST_F(IndexScanTest, String5) {
  /**
   * mix
   */
}
TEST_F(IndexScanTest, Time) {}
TEST_F(IndexScanTest, Date) {}
TEST_F(IndexScanTest, DateTime) {}
TEST_F(IndexScanTest, Compound) {}
TEST_F(IndexScanTest, Nullable) {}

}  // namespace storage
}  // namespace nebula
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
