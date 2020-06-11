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
    TagNode(TagContext* ctx,
            StorageEnv* env,
            GraphSpaceID spaceId,
            size_t vIdLen,
            TagID tagId,
            const std::vector<PropContext>* props,
            const Expression* exp = nullptr)
        : tagContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , tagId_(tagId)
        , props_(props)
        , exp_(exp) {
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        CHECK(schemaIter != tagContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
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
        prefix_ = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId, tagId_);
        ret = env_->kvstore_->prefix(spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleTagIterator(std::move(iter), tagId_, vIdLen_));
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

        auto reader = RowReader::getRowReader(*schemas_, value);
        if (!reader) {
            VLOG(1) << "Can't get tag reader of " << tagId_;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto ttl = getTagTTLInfo(tagContext_, tagId_);
        if (ttl.hasValue()) {
            auto ttlValue = ttl.value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return nullHandler(props_);
            }
        }
        if (exp_ != nullptr) {
            // todo(doodle): eval the expression which can be applied to the tag node
            // exp_->eval();
        }
        return valueHandler(tagId_, reader.get(), props_);
    }

private:
    TagContext* tagContext_;
    StorageEnv* env_;
    GraphSpaceID spaceId_;
    size_t vIdLen_;
    TagID tagId_;
    const std::vector<PropContext>* props_;
    const Expression* exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;

    std::unique_ptr<StorageIterator> iter_;
    std::string prefix_;
    std::string cacheResult_;
    bool hitCache_ = false;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
