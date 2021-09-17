/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/utils/NebulaKeyUtils.h"
#include "folly/Likely.h"
#include "storage/exec/IndexScanNode2.h"
#include "storage/exec/QueryUtils.h"
namespace nebula {
namespace storage {

class IndexEdgeScanNode : public IndexScanNode {
 public:
  static ErrorOr<IndexEdgeScanNode> make(RuntimeContext* context,
                                         IndexID indexId,
                                         const std::vector<cpp2::IndexColumnHint>& columnHint);

 private:
  IndexEdgeScanNode(RuntimeContext* context,
                    IndexID indexId,
                    const std::vector<cpp2::IndexColumnHint>& columnHint)
      : IndexScanNode(context, indexId, columnHint) {}
  Row decodeFromIndex(folly::StringPiece key) override;
  nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                      std::pair<std::string, std::string>& kv) override;
  Map<std::string, Value> decodeFromBase(const std::string& key, const std::string& value) override;
  const meta::SchemaProviderIf* getSchema() override;

 private:
  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> edge_;
};
}  // namespace storage
}  // namespace nebula
