/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once

#include "common/base/Base.h"
#include "storage/exec/IndexScanNode2.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"
namespace nebula {
namespace storage {
class IndexVertexScanNode final : public IndexScanNode {
 public:
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  IndexVertexScanNode(RuntimeContext* context,
                      IndexID indexId,
                      const std::vector<cpp2::IndexColumnHint>& clolumnHint);

 private:
  nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                      std::pair<std::string, std::string>& kv) override;
  Row decodeFromIndex(folly::StringPiece key) override;
  Map<std::string, Value> decodeFromBase(const std::string& key, const std::string& value) override;
  const meta::SchemaProviderIf* getSchema() override { return tag_.get(); }

  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> tag_;
};
}  // namespace storage
}  // namespace nebula
