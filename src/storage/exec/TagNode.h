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
class TagNode final : public RelNode<VertexID> {
public:
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

        if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
            auto result = tagContext_->vertexCache_->get(std::make_pair(vId, tagId_), partId);
            if (result.ok()) {
                cacheResult_ = std::move(result).value();
                hitCache_ = true;
                return kvstore::ResultCode::SUCCEEDED;
            } else {
                hitCache_ = false;
                VLOG(1) << "Miss cache for vId " << vId << ", tagId " << tagId_;
            }
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId, vId, tagId_);
        ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleTagIterator(std::move(iter), tagId_, planContext_->vIdLen_));
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode collectTagPropsIfValid(NullHandler nullHandler,
                                               TagPropHandler valueHandler) {
        folly::StringPiece value;
        if (hitCache_) {
            value = cacheResult_;
        } else if (iter_ && iter_->valid()) {
            value = iter_->val();
        } else {
            return nullHandler(props_);
        }

        if (!reader_) {
            reader_ = RowReader::getRowReader(*schemas_, value);
            if (!reader_) {
                VLOG(1) << "Can't get tag reader of " << tagId_;
                return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
            }
        } else if (!reader_->reset(*schemas_, value)) {
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        if (ttl_.hasValue()) {
            auto ttlValue = ttl_.value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return nullHandler(props_);
            }
        }
        if (exp_ != nullptr) {
            // todo(doodle): eval the expression which can be applied to the tag node
            exp_->eval(*expCtx_);
        }
        return valueHandler(tagId_, reader_.get(), props_);
    }

    const std::string& getTagName() {
        return tagName_;
    }

private:
    PlanContext* planContext_;
    TagContext* tagContext_;
    TagID tagId_;
    const std::vector<PropContext>* props_;
    ExpressionContext* expCtx_;
    Expression* exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;
    std::string tagName_;

    std::unique_ptr<RowReader> reader_;
    std::unique_ptr<StorageIterator> iter_;
    std::string prefix_;
    std::string cacheResult_;
    bool hitCache_ = false;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
