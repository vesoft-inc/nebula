/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXVERTEXSCANNODE_H
#define STORAGE_EXEC_INDEXVERTEXSCANNODE_H

#include <folly/Range.h>       // for StringPiece
#include <gtest/gtest_prod.h>  // for FRIEND_TEST

#include <functional>  // for function
#include <memory>      // for shared_ptr, unique_ptr
#include <string>      // for string
#include <utility>     // for pair
#include <vector>      // for vector

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"         // for Row
#include "common/utils/Types.h"               // for IndexID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for IndexItem
#include "storage/exec/IndexNode.h"           // for Map, IndexNode (ptr only)
#include "storage/exec/IndexScanNode.h"       // for IndexScanNode
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace meta {
class NebulaSchemaProvider;
}  // namespace meta
namespace storage {
namespace cpp2 {
class IndexColumnHint;
}  // namespace cpp2
struct RuntimeContext;
}  // namespace storage
struct Value;

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore
namespace meta {
class NebulaSchemaProvider;
}  // namespace meta
struct Value;

namespace storage {
namespace cpp2 {
class IndexColumnHint;
}  // namespace cpp2
struct RuntimeContext;

/**
 * IndexVertexScanNode
 *
 * reference: IndexScanNode
 */
class IndexVertexScanNode final : public IndexScanNode {
 public:
  IndexVertexScanNode(const IndexVertexScanNode& node);
  IndexVertexScanNode(RuntimeContext* context,
                      IndexID indexId,
                      const std::vector<cpp2::IndexColumnHint>& columnHint,
                      ::nebula::kvstore::KVStore* kvstore);
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;

 private:
  nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                      std::pair<std::string, std::string>& kv) override;
  Row decodeFromIndex(folly::StringPiece key) override;
  Map<std::string, Value> decodeFromBase(const std::string& key, const std::string& value) override;

  using TagSchemas = std::vector<std::shared_ptr<const nebula::meta::NebulaSchemaProvider>>;
  const TagSchemas& getSchema() override {
    return tag_;
  }
  TagSchemas tag_;
  using IndexItem = ::nebula::meta::cpp2::IndexItem;
  // Convenient for testing
  std::function<::nebula::cpp2::ErrorCode(std::shared_ptr<IndexItem>&)> getIndex;
  std::function<::nebula::cpp2::ErrorCode(TagSchemas&)> getTag;

  FRIEND_TEST(IndexScanTest, VertexIndexOnlyScan);
  FRIEND_TEST(IndexScanTest, VertexBase);
  FRIEND_TEST(IndexScanTest, Prefix1);
  FRIEND_TEST(IndexScanTest, Prefix2);
  FRIEND_TEST(IndexScanTest, Base);
  FRIEND_TEST(IndexScanTest, Vertex);
  friend class IndexScanTestHelper;
};
}  // namespace storage
}  // namespace nebula
#endif
