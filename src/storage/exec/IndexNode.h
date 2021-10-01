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
  kvstore::KVStore* kvstore() const { return context_->env()->kvstore_; }
  meta::IndexManager* idxMgr() const { return context_->env()->indexMan_; }

  bool isPlanKilled() const { return context_->isPlanKilled(); }

  const meta::NebulaSchemaProvider* schema() const {
    return context_->isEdge() ? context_->edgeSchema_ : context_->tagSchema_;
  }

  bool isEdge() const { return context_->isEdge(); }

  StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> index(IndexID id) const {
    if (isEdge()) {
      return idxMgr()->getEdgeIndex(context_->spaceId(), id);
    } else {
      return idxMgr()->getTagIndex(context_->spaceId(), id);
    }
  }

  std::unique_ptr<IndexIterator> iterator(std::unique_ptr<kvstore::KVIterator> iter) const {
    if (isEdge()) {
      return std::make_unique<EdgeIndexIterator>(std::move(iter), context_->vIdLen());
    } else {
      return std::make_unique<VertexIndexIterator>(std::move(iter), context_->vIdLen());
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
  IndexNode(RuntimeContext* context, const std::string& name)
      : RelNode<T>(name), context_(DCHECK_NOTNULL(context)) {}

  class Part {
   public:
    Part(RuntimeContext* ctx, PartitionID partId) : ctx_(ctx), partId_(partId) {}

    std::string vertexKey(VertexID vId) const {
      return NebulaKeyUtils::vertexKey(ctx_->vIdLen(), partId_, vId, ctx_->tagId_);
    }

    std::string edgeKey(const std::string& src, const std::string& dst, EdgeRanking rank) const {
      return NebulaKeyUtils::edgeKey(ctx_->vIdLen(), partId_, src, ctx_->edgeType_, rank, dst);
    }

    auto get(const std::string& key, std::string* val) const {
      return kvstore()->get(ctx_->spaceId(), partId_, key, val);
    }

    auto range(const std::string& begin,
               const std::string& end,
               std::unique_ptr<kvstore::KVIterator>* iter) const {
      return kvstore()->range(ctx_->spaceId(), partId_, begin, end, iter);
    }

    auto prefix(const std::string& prefix, std::unique_ptr<kvstore::KVIterator>* iter) const {
      return kvstore()->prefix(ctx_->spaceId(), partId_, prefix, iter);
    }

    kvstore::KVStore* kvstore() const { return ctx_->env()->kvstore_; }

   private:
    RuntimeContext* ctx_;
    PartitionID partId_;
  };

  Part part(PartitionID partId) const { return Part(context_, partId); }

  RuntimeContext* context_;
};

}  // namespace storage
}  // namespace nebula
