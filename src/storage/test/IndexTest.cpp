/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

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
    DVLOG(2) << "Start case " << case_;
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
  {                                                                              // Case 1: prefix
    std::vector<ColumnHint> columnHints = {makeColumnHint("a", 1)};              // a=1;
    check(indices[0], columnHints, expect(0), "case1.1");                        //
    columnHints = {makeColumnHint("b", 0x7fffffffffffffff)};                     // b=MAX
    check(indices[1], columnHints, expect(3), "case1.2");                        //
    columnHints = {makeColumnHint("b", -0x7fffffffffffffff - 1)};                // b=MIN
    check(indices[1], columnHints, expect(4), "case1.3");                        //
    columnHints = {makeColumnHint("c", 0)};                                      // c=0
    check(indices[2], columnHints, expect(3, 5), "case1.4");                     //
  }                                                                              // End of Case 1
  {                                                                              // Case 2: [x, INF)
    std::vector<ColumnHint> columnHints = {makeBeginColumnHint<true>("a", -1)};  // Case2.1: a >= -1
    check(indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case2.1");         //
    columnHints = {makeBeginColumnHint<true>("a", 4)};                           // Case2.2: a>=4
    check(indices[0], columnHints, expect(3, 4, 5), "case2.2");                  //
    columnHints = {makeBeginColumnHint<true>("a", 7)};                           // Case2.3: a>=7
    check(indices[0], columnHints, {}, "case2.3");                               //
    columnHints = {makeBeginColumnHint<true>("b", -0x7fffffffffffffff - 1)};  // Case2.4: b>=INT_MIN
    check(indices[1], columnHints, expect(4, 0, 2, 1, 3), "case2.4");         //
    columnHints = {makeBeginColumnHint<true>("b", 0x7fffffffffffffff)};       // Case2.5: b>=INT_MAX
    check(indices[1], columnHints, expect(3), "case2.5");                     //
    columnHints = {makeBeginColumnHint<true>("b", 0)};                        // Case2.6: b>=0
    check(indices[1], columnHints, expect(2, 1, 3), "case2.6");               //
    columnHints = {makeBeginColumnHint<true>("c", -0x7fffffffffffffff - 1)};  // Case2.7: c>=INT_MIN
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5, 4), "case2.7");      //
    columnHints = {makeBeginColumnHint<true>("c", 0x7fffffffffffffff)};       // Case2.8: c>=INT_MAX
    check(indices[2], columnHints, expect(4), "case2.8");                     //
    columnHints = {makeBeginColumnHint<true>("c", 0)};                        // Case2.9: c>=0
    check(indices[2], columnHints, expect(3, 5, 4), "case2.9");               //
  }                                                                           // End of Case 2
  {                                                                           // Case 3: [x, y)
    std::vector<ColumnHint> columnHints;                                      //
    columnHints = {makeColumnHint<true, false>("a", -1, 10)};                 // Case3.1: a >= -1
    check(indices[0], columnHints, expect(0, 1, 2, 3, 4, 5), "case3.1");
    columnHints = {makeColumnHint<true, false>("a", -100, 4)};
    check(indices[0], columnHints, expect(0, 1, 2), "case3.2");
    columnHints = {makeColumnHint<true, false>("a", 4, 100)};
    check(indices[0], columnHints, expect(3, 4, 5), "case3.3");
    columnHints = {makeColumnHint<true, false>("a", 2, 5)};
    check(indices[0], columnHints, expect(1, 2, 3), "case3.4");
    columnHints = {makeColumnHint<true, false>("a", -100, 0)};
    check(indices[0], columnHints, {}, "case3.5");
    columnHints = {makeColumnHint<true, false>("a", 10, 100)};
    check(indices[0], columnHints, {}, "case3.6");
    columnHints = {makeColumnHint<true, false>("b", -0x7fffffffffffffff - 1, 0x7fffffffffffffff)};
    check(indices[1], columnHints, expect(4, 0, 2, 1), "case3.7");
    columnHints = {makeColumnHint<true, false>("c", -0x7fffffffffffffff - 1, 0x7fffffffffffffff)};
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5), "case3.8");
  }  // End of Case 3
  {  // Case 4: (x, INF)
    std::vector<ColumnHint> columnHints;
    columnHints = {makeBeginColumnHint<false>("a", 3)};
    check(indices[0], columnHints, expect(3, 4, 5), "case4.1");
    columnHints = {makeBeginColumnHint<false>("b", MIN)};
    check(indices[1], columnHints, expect(0, 2, 1, 3), "case4.2");
    columnHints = {makeBeginColumnHint<false>("b", MAX)};
    check(indices[1], columnHints, {}, "case4.3");
    columnHints = {makeBeginColumnHint<false>("c", MIN)};
    check(indices[2], columnHints, expect(0, 2, 3, 5, 4), "case4.4");
    columnHints = {makeBeginColumnHint<false>("c", MAX - 1)};
    check(indices[2], columnHints, expect(4), "case4.4");
  }  // End of Case 4
  {  // Case 5: (x, y]
    std::vector<ColumnHint> columnHints;
    columnHints = {makeColumnHint<false, true>("a", 1, 6)};
    check(indices[0], columnHints, expect(1, 2, 3, 4, 5), "case5.1");
    columnHints = {makeColumnHint<false, true>("a", 0, 3)};
    check(indices[0], columnHints, expect(0, 1, 2), "case5.2");
    columnHints = {makeColumnHint<false, true>("b", MIN, MIN)};
    check(indices[1], columnHints, {}, "case5.3");
    columnHints = {makeColumnHint<false, true>("b", MAX, MAX)};
    check(indices[1], columnHints, {}, "case5.4");
    columnHints = {makeColumnHint<false, true>("b", 0, MAX)};
    check(indices[1], columnHints, expect(1, 3), "case5.5");
    columnHints = {makeColumnHint<false, true>("c", -1, MAX)};
    check(indices[2], columnHints, expect(3, 5, 4), "case5.6");
  }  // End of Case 5
  {  // Case 6: (-INF, y]
    std::vector<ColumnHint> columnHints;
    columnHints = {makeEndColumnHint<true>("a", 4)};
    check(indices[0], columnHints, expect(0, 1, 2, 3), "case6.1");
    columnHints = {makeEndColumnHint<true>("a", 1)};
    check(indices[0], columnHints, expect(0), "case6.2");
    columnHints = {makeEndColumnHint<true>("b", MIN)};
    check(indices[1], columnHints, expect(4), "case6.3");
    columnHints = {makeEndColumnHint<true>("b", MAX)};
    check(indices[1], columnHints, expect(4, 0, 2, 1, 3), "casae6.4");
    columnHints = {makeEndColumnHint<true>("c", MIN)};
    check(indices[2], columnHints, expect(1), "case6.5");
    columnHints = {makeEndColumnHint<true>("c", MAX)};
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5, 4), "case6.6");
  }  // End of Case 6
  {  // Case 7: (-INF, y)
    std::vector<ColumnHint> columnHints;
    columnHints = {makeEndColumnHint<false>("a", 4)};
    check(indices[0], columnHints, expect(0, 1, 2), "case7.1");
    columnHints = {makeEndColumnHint<false>("a", 1)};
    check(indices[0], columnHints, {}, "case7.2");
    columnHints = {makeEndColumnHint<false>("b", MIN)};
    check(indices[1], columnHints, {}, "case7.3");
    columnHints = {makeEndColumnHint<false>("b", MAX)};
    check(indices[1], columnHints, expect(4, 0, 2, 1), "casae7.4");
    columnHints = {makeEndColumnHint<false>("c", MIN)};
    check(indices[2], columnHints, {}, "case7.5");
    columnHints = {makeEndColumnHint<false>("c", MAX)};
    check(indices[2], columnHints, expect(1, 0, 2, 3, 5), "case7.6");
  }  // End of Case 7
}
TEST_F(IndexScanTest, Float) {}
TEST_F(IndexScanTest, Bool) {}
TEST_F(IndexScanTest, String1) {}
TEST_F(IndexScanTest, String2) {}
TEST_F(IndexScanTest, String3) {}
TEST_F(IndexScanTest, String4) {}
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
