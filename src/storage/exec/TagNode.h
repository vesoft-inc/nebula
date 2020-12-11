/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_TAGNODE_H_
#define STORAGE_EXEC_TAGNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

// TagNode will return a DataSet of specified props of tagId
class TagNode final : public IterateNode<VertexID> {
public:
    using RelNode::execute;

    TagNode(PlanContext* planCtx,
            TagContext* ctx,
            TagID tagId,
            const std::vector<PropContext>* props,
            StorageExpressionContext* expCtx = nullptr,
            Expression* exp = nullptr)
        : planContext_(planCtx)
        , tagContext_(ctx)
        , tagId_(tagId)
        , props_(props)
        , expCtx_(expCtx)
        , exp_(exp) {
        UNUSED(expCtx_); UNUSED(exp_);
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        CHECK(schemaIter != tagContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
        ttl_ = QueryUtils::getTagTTLInfo(tagContext_, tagId_);
        tagName_ = tagContext_->tagNames_[tagId_];
    }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        reader_.reset();
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_
                << ", prop size " << props_->size();

        // when update, has already evicted
        if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
            auto cache = tagContext_->vertexCache_->get(std::make_pair(vId, tagId_));
            if (cache.ok()) {
                key_ = NebulaKeyUtils::vertexKey(planContext_->vIdLen_, partId, vId, tagId_, 0L);
                value_ = std::move(cache.value());
                // if data in vertex cache is valid, don't read from kv
                if (resetReader(vId)) {
                    return kvstore::ResultCode::SUCCEEDED;
                }
            }
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId, vId, tagId_);
        ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId, prefix, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            key_ = iter->key().str();
            value_ = iter->val().str();
            resetReader(vId);
            return kvstore::ResultCode::SUCCEEDED;
        }
        return ret;
    }

    kvstore::ResultCode collectTagPropsIfValid(NullHandler nullHandler,
                                               PropHandler valueHandler) {
        if (!valid()) {
            return nullHandler(props_);
        }
        return valueHandler(key_, reader_.get(), props_);
    }

    bool valid() const override {
        return !stopSearching_ && reader_ != nullptr;
    }

    void next() override {
        // tag only has one valid record, so stop iterate
        stopSearching_ = true;
    }

    folly::StringPiece key() const override {
        return key_;
    }

    folly::StringPiece val() const override {
        return value_;
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    const std::string& getTagName() {
        return tagName_;
    }

    TagID getTagId() {
        return tagId_;
    }

private:
    bool resetReader(const VertexID& vId) {
        reader_.reset(*schemas_, value_);
        if (!reader_ || (ttl_.hasValue() && CommonUtils::checkDataExpiredForTTL(
            schemas_->back().get(), reader_.get(), ttl_.value().first, ttl_.value().second))) {
            reader_.reset();
            if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
                tagContext_->vertexCache_->evict(std::make_pair(vId, tagId_));
            }
            return false;
        }
        return true;
    }

    PlanContext                                                          *planContext_;
    TagContext                                                           *tagContext_;
    TagID                                                                 tagId_;
    const std::vector<PropContext>                                       *props_;
    StorageExpressionContext                                             *expCtx_;
    Expression                                                           *exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>> *schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>>                      ttl_;
    std::string                                                           tagName_;

    bool                                                                  stopSearching_ = false;
    std::string                                                           key_;
    std::string                                                           value_;
    RowReaderWrapper                                                      reader_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
