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
            ExpressionContext* expCtx = nullptr,
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
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_
                << ", prop size " << props_->size();

        // when update, has already evicted
        if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
            auto result = tagContext_->vertexCache_->get(std::make_pair(vId, tagId_));
            if (result.ok()) {
                cacheResult_ = std::move(result).value();
                iter_.reset(new SingleTagIterator(planContext_, cacheResult_, schemas_, &ttl_));
                return kvstore::ResultCode::SUCCEEDED;
            }
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId, vId, tagId_);
        ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleTagIterator(planContext_, std::move(iter), tagId_,
                                              schemas_, &ttl_));
        } else {
            iter_.reset();
        }
        return ret;
    }

    kvstore::ResultCode collectTagPropsIfValid(NullHandler nullHandler,
                                               TagPropHandler valueHandler) {
        if (!iter_ || !iter_->valid()) {
            return nullHandler(props_);
        }
        return valueHandler(tagId_, iter_->reader(), props_);
    }

    bool valid() const override {
        return iter_ && iter_->valid();
    }

    void next() override {
        iter_->next();
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    RowReader* reader() const override {
        if (iter_) {
            return iter_->reader();
        }
        return nullptr;
    }

    const std::string& getTagName() {
        return tagName_;
    }

private:
    PlanContext                                                          *planContext_;
    TagContext                                                           *tagContext_;
    TagID                                                                 tagId_;
    const std::vector<PropContext>                                       *props_;
    ExpressionContext                                                    *expCtx_;
    Expression                                                           *exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>>                      ttl_;
    std::string                                                           tagName_;

    std::unique_ptr<StorageIterator>                                      iter_;
    std::string                                                           prefix_;
    std::string                                                           cacheResult_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
