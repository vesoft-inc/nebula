/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/exec/RelNode.h"

namespace nebula {

namespace meta::cpp2 {
class IndexItem;
}

namespace storage {

/**
 * This class as the base class of index related rel-nodes is aimed to reuse some common codes.
 */
template <typename T>
class IndexNode : public RelNode<T> {
 public:
  StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> index(IndexID id) const {
    if (this->isEdge()) {
      return this->idxMgr()->getEdgeIndex(this->spaceId(), id);
    } else {
      return this->idxMgr()->getTagIndex(this->spaceId(), id);
    }
  }

  std::unique_ptr<IndexIterator> iterator(std::unique_ptr<kvstore::KVIterator> iter) const {
    if (this->isEdge()) {
      return std::make_unique<EdgeIndexIterator>(std::move(iter), this->vIdLen());
    } else {
      return std::make_unique<VertexIndexIterator>(std::move(iter), this->vIdLen());
    }
  }

  bool isTTLExpired(const folly::StringPiece& val,
                    const std::pair<bool, std::pair<int64_t, std::string>>& ttl,
                    const meta::NebulaSchemaProvider* schema) const {
    if (val.empty() || !ttl.first) {
      return false;
    }
    auto v = IndexKeyUtils::parseIndexTTL(val);
    auto duration = ttl.second.first;
    const auto& col = ttl.second.second;
    return CommonUtils::checkDataExpiredForTTL(schema, std::move(v), col, duration);
  }

 protected:
  IndexNode(RuntimeContext* context, const std::string& name) : RelNode<T>(name, context) {}
};

}  // namespace storage
}  // namespace nebula
