/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

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
/**
 * _KSrc,
 * "",123,456,"abc",
 *
 *
 *
 *
 *
 */

class MockKVStore : public ::nebula::kvstore::KVEngine {};
class IndexMockNode : public IndexNode {
 public:
 private:
};

/**
 *
 */
class IndexScanTest : public ::testing::Test {
 protected:
 private:
  static std::unique_ptr<MockKVStore> kvstore_;
};
TEST_F(IndexScanTest, VertexIndexOnlyScan) {
  std::vector<Row> rows = R"(
    int | int | int
    1   | 2   | 3
    4   | 5   | 6
  )"_row;
  auto schema = R"(
    a   | int ||
    b   | int ||
    c   | int ||
  )"_schema;
  auto index = R"(
    TAG(a,1)
    (i1,2):a
    (i2,3):b
  )"_index(schema);
  std::vector<std::string> keyValues = encode(rows, schema, index);
}
// TEST_F(IndexScanTest, VertexIndexScan)
// TEST_F(IndexScanTest, EdgeBase){

// };
// TEST_F(IndexScanTest, Prefix1){

// };
// TEST_F(IndexScanTest, Prefix2){

// };
// TEST_F(IndexScanTest, Range1){

// };
// TEST_F(IndexScanTest, Range2){

// };
// TEST_F(IndexScanTest, Range3){

// };
// TEST_F(IndexScanTest, Range4){

// };
// class IndexTest : public ::testing::Test {
//  protected:
//  private:
// };
// TEST_F(IndexTest, VertexScan1) {}
// TEST_F(IndexTest, VertexScan2) {}
// TEST_F(IndexTest, VertexScan3){

// };
// TEST_F(IndexTest, VertexScan4){

// };
}  // namespace storage
}  // namespace nebula
