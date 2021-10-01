/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXVERTEXNODE_H_
#define STORAGE_EXEC_INDEXVERTEXNODE_H_

#include "common/base/Base.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexScanNode.h"

namespace nebula {
namespace storage {

template <typename T>
class IndexVertexNode final : public IndexNode<T> {
 public:
  using RelNode<T>::doExecute;

  IndexVertexNode(RuntimeContext* context,
                  IndexScanNode<T>* indexScanNode,
                  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
                  const std::string& schemaName,
                  int64_t limit = -1)
      : IndexNode<T>(context, "IndexVertexNode"),
        indexScanNode_(indexScanNode),
        schemas_(schemas),
        schemaName_(schemaName),
        limit_(limit) {}

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    auto schema = this->schema();
    auto ttlProp = CommonUtils::ttlProps(schema);

    data_.clear();
    std::vector<VertexID> vids;

    for (auto iter = indexScanNode_->iterator(); iter && iter->valid(); iter->next()) {
      if (this->isPlanKilled()) {
        return nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED;
      }
      if (!this->isTTLExpired(iter->val(), ttlProp, schema)) {
        auto* viter = static_cast<VertexIndexIterator*>(iter);
        vids.emplace_back(viter->vId());
      }
    }
    int64_t count = 0;
    for (const auto& vId : vids) {
      std::string val;
      auto key = this->vertexKey(partId, vId);
      ret = this->get(partId, key, &val);
      if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
        data_.emplace_back(std::move(key), std::move(val));
      } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        continue;
      } else {
        return ret;
      }
      if (limit_ > 0 && ++count >= limit_) {
        break;
      }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  std::vector<kvstore::KV> moveData() { return std::move(data_); }

  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& getSchemas() {
    return schemas_;
  }

  const std::string& getSchemaName() { return schemaName_; }

 private:
  IndexScanNode<T>* indexScanNode_;
  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas_;
  const std::string& schemaName_;
  int64_t limit_;
  std::vector<kvstore::KV> data_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_EXEC_INDEXVERTEXNODE_H_
