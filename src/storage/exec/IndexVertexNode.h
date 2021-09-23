/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXVERTEXNODE_H_
#define STORAGE_EXEC_INDEXVERTEXNODE_H_

#include "common/base/Base.h"
#include "storage/exec/IndexScanNode.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template <typename T>
class IndexVertexNode final : public RelNode<T> {
 public:
  using RelNode<T>::doExecute;

  IndexVertexNode(RuntimeContext* context,
                  IndexScanNode<T>* indexScanNode,
                  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
                  const std::string& schemaName,
                  int64_t limit = -1)
      : context_(context),
        indexScanNode_(indexScanNode),
        schemas_(schemas),
        schemaName_(schemaName),
        limit_(limit) {
    RelNode<T>::name_ = "IndexVertexNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    auto ttlProp = CommonUtils::ttlProps(context_->tagSchema_);

    data_.clear();
    std::vector<VertexID> vids;
    auto* iter = static_cast<VertexIndexIterator*>(indexScanNode_->iterator());

    while (iter && iter->valid()) {
      if (context_->isPlanKilled()) {
        return nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED;
      }
      if (!iter->val().empty() && ttlProp.first) {
        auto v = IndexKeyUtils::parseIndexTTL(iter->val());
        if (CommonUtils::checkDataExpiredForTTL(
                context_->tagSchema_, std::move(v), ttlProp.second.second, ttlProp.second.first)) {
          iter->next();
          continue;
        }
      }
      vids.emplace_back(iter->vId());
      iter->next();
    }
    int64_t count = 0;
    for (const auto& vId : vids) {
      VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << context_->tagId_;
      auto key = NebulaKeyUtils::vertexKey(context_->vIdLen(), partId, vId, context_->tagId_);
      std::string val;
      ret = context_->env()->kvstore_->get(context_->spaceId(), partId, key, &val);
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
  RuntimeContext* context_;
  IndexScanNode<T>* indexScanNode_;
  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas_;
  const std::string& schemaName_;
  int64_t limit_;
  std::vector<kvstore::KV> data_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_EXEC_INDEXVERTEXNODE_H_
