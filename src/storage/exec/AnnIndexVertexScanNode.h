/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_ANNINDEXVERTEXSCANNODE_H
#define STORAGE_EXEC_ANNINDEXVERTEXSCANNODE_H

#include <gtest/gtest_prod.h>

#include <functional>
#include <memory>
#include <unordered_map>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/thrift/ThriftTypes.h"
#include "storage/exec/IndexScanNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

/**
 * AnnIndexVertexScanNode
 *
 * reference: AnnIndexScanNode
 */
class AnnIndexVertexScanNode final : public AnnIndexScanNode {
 public:
  AnnIndexVertexScanNode(const AnnIndexVertexScanNode& node);
  AnnIndexVertexScanNode(RuntimeContext* context,
                         IndexID indexId,
                         ::nebula::kvstore::KVStore* kvstore,
                         bool hasNullableCol,
                         int64_t limit,
                         int64_t param,
                         const Value& queryVector);
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;

 private:
  StatusOr<Map<std::string, Value>> getVidByVectorId(VectorID vectorId) override;

  using TagSchemas = std::vector<std::shared_ptr<const nebula::meta::NebulaSchemaProvider>>;
  const std::unordered_map<TagID, TagSchemas>& getSchemas() {
    return tags_;
  }

  std::unordered_map<TagID, TagSchemas> tags_;
  using AnnIndexItem = ::nebula::meta::cpp2::AnnIndexItem;
  // Convenient for testing
  std::function<::nebula::cpp2::ErrorCode(std::shared_ptr<AnnIndexItem>&)> getIndex;
  std::function<::nebula::cpp2::ErrorCode(std::unordered_map<TagID, TagSchemas>&)> getTags;

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
