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
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TossEdgeIterator.h"

namespace nebula {
namespace storage {

// EdgeNode will return a StorageIterator which iterates over the specified
// edgeType of given vertexId
template<typename T>
class EdgeNode : public IterateNode<T> {
public:
    nebula::cpp2::ErrorCode
    collectEdgePropsIfValid(NullHandler nullHandler,
                            PropHandler valueHandler) {
        if (!this->valid()) {
            return nullHandler(props_);
        }
        return valueHandler(this->key(), this->reader(), props_);
    }

    const std::string& getEdgeName() {
        return edgeName_;
    }

protected:
    EdgeNode(RunTimeContext* context,
             EdgeContext* edgeContext,
             EdgeType edgeType,
             const std::vector<PropContext>* props,
             StorageExpressionContext* expCtx,
             Expression* exp)
        : context_(context)
        , edgeContext_(edgeContext)
        , edgeType_(edgeType)
        , props_(props)
        , expCtx_(expCtx)
        , exp_(exp) {
        UNUSED(expCtx_); UNUSED(exp_);
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        CHECK(schemaIter != edgeContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
        ttl_ = QueryUtils::getEdgeTTLInfo(edgeContext_, std::abs(edgeType_));
        edgeName_ = edgeContext_->edgeNames_[edgeType_];
    }

    EdgeNode(RunTimeContext* context,
             EdgeContext* ctx)
        : context_(context)
        , edgeContext_(ctx) {}

    RunTimeContext* context_;
    EdgeContext* edgeContext_;
    EdgeType edgeType_;
    const std::vector<PropContext>* props_;
    StorageExpressionContext* expCtx_;
    Expression* exp_;

    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;
    std::string edgeName_;
};

// FetchEdgeNode is used to fetch a single edge
class FetchEdgeNode final : public EdgeNode<cpp2::EdgeKey> {
public:
    using RelNode::execute;

    FetchEdgeNode(RunTimeContext* context,
                  EdgeContext* edgeContext,
                  EdgeType edgeType,
                  const std::vector<PropContext>* props,
                  StorageExpressionContext* expCtx = nullptr,
                  Expression* exp = nullptr)
        : EdgeNode(context, edgeContext, edgeType, props, expCtx, exp) {}

    bool valid() const override {
        return valid_;
    }

    void next() override {
        valid_ = false;
    }

    folly::StringPiece key() const override {
        return key_;
    }

    folly::StringPiece val() const override {
        return val_;
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    nebula::cpp2::ErrorCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        valid_ = false;
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        if (edgeType_ !=  *edgeKey.edge_type_ref()) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        key_ = NebulaKeyUtils::edgeKey(context_->vIdLen(),
                                       partId,
                                       (*edgeKey.src_ref()).getStr(),
                                       *edgeKey.edge_type_ref(),
                                       *edgeKey.ranking_ref(),
                                       (*edgeKey.dst_ref()).getStr());
        ret = context_->env()->kvstore_->get(context_->spaceId(), partId, key_, &val_);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            resetReader();
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            // regard key not found as succeed as well, upper node will handle it
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        return ret;
    }

private:
    void resetReader() {
        reader_.reset(*schemas_, val_);
        if (!reader_ ||
            (ttl_.hasValue() && CommonUtils::checkDataExpiredForTTL(schemas_->back().get(),
                                                                    reader_.get(),
                                                                    ttl_.value().first,
                                                                    ttl_.value().second))) {
            reader_.reset();
            return;
        }
        valid_ = true;
    }


    bool                valid_ = false;
    std::string         key_;
    std::string         val_;
    RowReaderWrapper    reader_;
};

// SingleEdgeNode is used to scan all edges of a specified edgeType of the same srcId
class SingleEdgeNode final : public EdgeNode<VertexID> {
public:
    using RelNode::execute;
    SingleEdgeNode(RunTimeContext* context,
                   EdgeContext* edgeContext,
                   EdgeType edgeType,
                   const std::vector<PropContext>* props,
                   StorageExpressionContext* expCtx = nullptr,
                   Expression* exp = nullptr)
        : EdgeNode(context, edgeContext, edgeType, props, expCtx, exp) {}

    SingleEdgeIterator* iter() {
        return iter_.get();
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
        return iter_->reader();
    }

    nebula::cpp2::ErrorCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", vId " << vId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(context_->vIdLen(), partId, vId, edgeType_);
        ret = context_->env()->kvstore_->prefix(context_->spaceId(), partId, prefix_, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(
                new SingleEdgeIterator(context_, std::move(iter), edgeType_, schemas_, &ttl_));
        } else {
            iter_.reset();
        }
        return ret;
    }

private:
    std::unique_ptr<SingleEdgeIterator> iter_;
    std::string prefix_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
