/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/base/Base.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/exec/IndexScanNode2.h"
#include "storage/exec/QueryUtils.h"
#include "storage/exec/StorageIterator.h"
namespace nebula {
namespace storage {

class IndexEdgeScanNode : public IndexScanNode {
 public:
  IndexEdgeScanNode(const IndexEdgeScanNode& node);
  IndexEdgeScanNode(RuntimeContext* context,
                    IndexID indexId,
                    const std::vector<cpp2::IndexColumnHint>& columnHint,
                    ::nebula::kvstore::KVStore* kvstore);
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;

 private:
  Row decodeFromIndex(folly::StringPiece key) override;
  nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                      std::pair<std::string, std::string>& kv) override;
  Map<std::string, Value> decodeFromBase(const std::string& key, const std::string& value) override;
  const meta::SchemaProviderIf* getSchema() override;

  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> edge_;

  // Convenient for testing
  std::function<StatusOr<std::shared_ptr<::nebula::meta::cpp2::IndexItem>>()> getIndex;
  std::function<std::shared_ptr<const nebula::meta::NebulaSchemaProvider>()> getEdge;

  FRIEND_TEST(IndexScanTest, Edge);
  friend class IndexScanTestHelper;
};
}  // namespace storage
}  // namespace nebula
