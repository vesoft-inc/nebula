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
        if (ttl_.hasValue()) {
            auto ttlValue = ttl_.value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return nullHandler(props_);
            }
        }
        if (exp_ != nullptr) {
            // todo(doodle): eval the expression which can be applied to the edge node,
            // which means we can only apply to FetchEdgeNode or SingleEdgeNode.
            exp_->eval(*expCtx_);
        }
        return valueHandler(edgeType_, key, reader_.get(), props_);
    }

    const std::string& getEdgeName() {
        return edgeName_;
    }

protected:
    EdgeNode(PlanContext* planCtx,
             EdgeContext* ctx,
             EdgeType edgeType,
             const std::vector<PropContext>* props,
             ExpressionContext* expCtx,
             Expression* exp)
        : planContext_(planCtx)
        , edgeContext_(ctx)
        , edgeType_(edgeType)
        , props_(props)
        , expCtx_(expCtx)
        , exp_(exp) {
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        CHECK(schemaIter != edgeContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
        ttl_ = QueryUtils::getEdgeTTLInfo(edgeContext_, edgeType_);
        edgeName_ = edgeContext_->edgeNames_[edgeType_];
    }

    EdgeNode(PlanContext* planCtx,
             EdgeContext* ctx)
        : planContext_(planCtx)
        , edgeContext_(ctx) {}

    PlanContext* planContext_;
    EdgeContext* edgeContext_;
    EdgeType edgeType_;
    const std::vector<PropContext>* props_;
    ExpressionContext* expCtx_;
    Expression* exp_;

    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;
    std::string edgeName_;

    std::unique_ptr<RowReader> reader_;
    std::unique_ptr<EdgeIterator> iter_;
    std::string prefix_;
};

// FetchEdgeNode is used to fetch a single edge
class FetchEdgeNode final : public EdgeNode<cpp2::EdgeKey> {
public:
    FetchEdgeNode(PlanContext* planCtx,
                  EdgeContext* ctx,
                  EdgeType edgeType,
                  const std::vector<PropContext>* props,
                  ExpressionContext* expCtx = nullptr,
                  Expression* exp = nullptr)
        : EdgeNode(planCtx, ctx, edgeType, props, expCtx, exp) {}

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        if (edgeKey.edge_type == edgeType_) {
            prefix_ = NebulaKeyUtils::edgePrefix(planContext_->vIdLen_,
                                                 partId,
                                                 edgeKey.src,
                                                 edgeKey.edge_type,
                                                 edgeKey.ranking,
                                                 edgeKey.dst);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto code = planContext_->env_->kvstore_->prefix(
                planContext_->spaceId_, partId, prefix_, &iter);
            if (code == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
                iter_.reset(new SingleEdgeIterator(
                    std::move(iter), edgeType_, planContext_->vIdLen_, schemas_, &ttl_));
            }
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

// SingleEdgeNode is used to scan all edges of a specified edgeType of the same srcId
class SingleEdgeNode final : public EdgeNode<VertexID> {
public:
    SingleEdgeNode(PlanContext* planCtx,
                   EdgeContext* ctx,
                   EdgeType edgeType,
                   const std::vector<PropContext>* props,
                   ExpressionContext* expCtx = nullptr,
                   Expression* exp = nullptr)
        : EdgeNode(planCtx, ctx, edgeType, props, expCtx, exp) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", vId " << vId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(planContext_->vIdLen_, partId, vId, edgeType_);
        ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleEdgeIterator(std::move(iter), edgeType_, planContext_->vIdLen_,
                                               schemas_, &ttl_));
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

// AllEdgeNode is used to scan all edges of a srcId, also can specified all out-edges or in-edges
class AllEdgeNode final : public EdgeNode<VertexID> {
public:
    AllEdgeNode(PlanContext* planCtx,
                EdgeContext* ctx,
                const cpp2::EdgeDirection& dir = cpp2::EdgeDirection::BOTH)
        : EdgeNode(planCtx, ctx)
        , dir_(dir) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", vId " << vId << ", scan all edges";
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(planContext_->vIdLen_, partId, vId);
        ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(
                new AllEdgeIterator(edgeContext_, std::move(iter), planContext_->vIdLen_, dir_));
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    cpp2::EdgeDirection dir_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
