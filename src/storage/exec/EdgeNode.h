/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_EDGENODE_H_
#define STORAGE_EXEC_EDGENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

// EdgeNode will return a StorageIterator which iterates over the specified
// edgeType of given vertexId
template<typename T>
class EdgeNode : public RelNode<T> {
public:
    EdgeIterator* iter() {
        return iter_.get();
    }

    kvstore::ResultCode collectEdgePropsIfValid(NullHandler nullHandler,
                                                EdgePropHandler valueHandler) {
        if (!iter_ || !iter_->valid()) {
            return nullHandler(props_);
        }
        auto key = iter_->key();
        auto val = iter_->val();
        if (!reader_) {
            reader_ = RowReader::getRowReader(*schemas_, val);
            if (!reader_) {
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }
        } else if (!reader_->reset(*schemas_, val)) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        auto ttl = this->getEdgeTTLInfo(edgeContext_, edgeType_);
        if (ttl.hasValue()) {
            auto ttlValue = ttl.value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return nullHandler(props_);
            }
        }
        if (exp_ != nullptr) {
            // todo(doodle): eval the expression which can be applied to the edge node,
            // which means we can only apply to FetchEdgeNode or EdgeTypePrefixScanNode.
            // exp_->eval();
        }
        return valueHandler(edgeType_, key, reader_.get(), props_);
    }

protected:
    EdgeNode(EdgeContext* ctx,
             StorageEnv* env,
             GraphSpaceID spaceId,
             size_t vIdLen,
             EdgeType edgeType,
             const std::vector<PropContext>* props,
             const Expression* exp = nullptr)
        : edgeContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , edgeType_(edgeType)
        , props_(props)
        , exp_(exp) {
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        CHECK(schemaIter != edgeContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
    }

    EdgeNode(EdgeContext* ctx,
             StorageEnv* env,
             GraphSpaceID spaceId,
             size_t vIdLen)
        : edgeContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen) {}

    EdgeContext* edgeContext_;
    StorageEnv* env_;
    GraphSpaceID spaceId_;
    size_t vIdLen_;
    EdgeType edgeType_;
    const std::vector<PropContext>* props_;
    const Expression* exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;

    std::unique_ptr<RowReader> reader_;
    std::unique_ptr<EdgeIterator> iter_;
    std::string prefix_;
};

class FetchEdgeNode final : public EdgeNode<cpp2::EdgeKey> {
public:
    FetchEdgeNode(EdgeContext* ctx,
                  StorageEnv* env,
                  GraphSpaceID spaceId,
                  size_t vIdLen,
                  EdgeType edgeType,
                  const std::vector<PropContext>* props,
                  const Expression* exp = nullptr)
        : EdgeNode(ctx, env, spaceId, vIdLen, edgeType, props, exp) {}

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        if (edgeKey.edge_type == edgeType_) {
            prefix_ = NebulaKeyUtils::edgePrefix(vIdLen_,
                                                 partId,
                                                 edgeKey.src,
                                                 edgeKey.edge_type,
                                                 edgeKey.ranking,
                                                 edgeKey.dst);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto code = env_->kvstore_->prefix(spaceId_, partId, prefix_, &iter);
            if (code == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
                iter_.reset(new SingleEdgeIterator(std::move(iter), edgeType_, vIdLen_));
            }
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

class EdgeTypePrefixScanNode final : public EdgeNode<VertexID> {
public:
    EdgeTypePrefixScanNode(EdgeContext* ctx,
                           StorageEnv* env,
                           GraphSpaceID spaceId,
                           size_t vIdLen,
                           EdgeType edgeType,
                           const std::vector<PropContext>* props,
                           const Expression* exp = nullptr)
        : EdgeNode(ctx, env, spaceId, vIdLen, edgeType, props, exp) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", vId " << vId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(vIdLen_, partId, vId, edgeType_);
        ret = env_->kvstore_->prefix(spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleEdgeIterator(std::move(iter), edgeType_, vIdLen_));
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

class VertexPrefixScanNode final : public EdgeNode<VertexID> {
public:
    VertexPrefixScanNode(EdgeContext* ctx,
                         StorageEnv* env,
                         GraphSpaceID spaceId,
                         size_t vIdLen)
        : EdgeNode(ctx, env, spaceId, vIdLen) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", vId " << vId << ", scan all edges";
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(vIdLen_, partId, vId);
        ret = env_->kvstore_->prefix(spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new AllEdgeIterator(edgeContext_, std::move(iter), vIdLen_));
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
