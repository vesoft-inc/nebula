/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_HASHJOINNODE_H_
#define STORAGE_EXEC_HASHJOINNODE_H_

#include "common/base/Base.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

// HashJoinNode has input of serveral TagNode and EdgeNode, the EdgeNode is several
// SingleEdgeNode of different edge types all edges of a vertex.
// The output would be the result of tag, it is a List, each cell save a list of property values,
// if tag not found, it will be a empty value.
// Also it will return a iterator of edges which can pass ttl check and ready to be read.
class HashJoinNode : public IterateNode<VertexID> {
public:
    using RelNode::execute;

    HashJoinNode(RunTimeContext* context,
                 const std::vector<TagNode*>& tagNodes,
                 const std::vector<SingleEdgeNode*>& edgeNodes,
                 TagContext* tagContext,
                 EdgeContext* edgeContext,
                 StorageExpressionContext* expCtx)
        : context_(context)
        , tagNodes_(tagNodes)
        , edgeNodes_(edgeNodes)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext)
        , expCtx_(expCtx) {
        UNUSED(tagContext_);
    }

    nebula::cpp2::ErrorCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        if (expCtx_ != nullptr) {
            expCtx_->clear();
        }
        result_.setList(nebula::List());
        auto& result = result_.mutableList();
        if (context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
        }

        // add result of each tag node to tagResult
        for (auto* tagNode : tagNodes_) {
            ret = tagNode->collectTagPropsIfValid(
                [&result] (const std::vector<PropContext>*) -> nebula::cpp2::ErrorCode {
                    result.values.emplace_back(Value());
                    return nebula::cpp2::ErrorCode::SUCCEEDED;
                },
                [this, &result, tagNode] (folly::StringPiece key,
                                          RowReader* reader,
                                          const std::vector<PropContext>* props)
                -> nebula::cpp2::ErrorCode {
                    nebula::List list;
                    list.reserve(props->size());
                    const auto& tagName = tagNode->getTagName();
                    for (const auto& prop : *props) {
                        VLOG(2) << "Collect prop " << prop.name_;
                        auto value = QueryUtils::readVertexProp(
                            key, context_->vIdLen(), context_->isIntId(), reader, prop);
                        if (!value.ok()) {
                            return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                        }
                        if (prop.filtered_ && expCtx_ != nullptr) {
                            expCtx_->setTagProp(tagName, prop.name_, value.value());
                        }
                        if (prop.returned_) {
                            list.emplace_back(std::move(value).value());
                        }
                    }
                    result.values.emplace_back(std::move(list));
                    return nebula::cpp2::ErrorCode::SUCCEEDED;
                });
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
        }

        std::vector<SingleEdgeIterator*> iters;
        for (auto* edgeNode : edgeNodes_) {
            iters.emplace_back(edgeNode->iter());
        }
        iter_.reset(new MultiEdgeIterator(std::move(iters)));
        if (iter_->valid()) {
            setCurrentEdgeInfo();
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    bool valid() const override {
        return iter_->valid();
    }

    void next() override {
        iter_->next();
        if (iter_->valid()) {
            setCurrentEdgeInfo();
        }
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    // return the edge row reader which could pass filter
    RowReader* reader() const override {
        return iter_->reader();
    }

private:
    // return true when the value iter points to a value which can pass ttl and filter
    void setCurrentEdgeInfo() {
        EdgeType type = iter_->edgeType();
        // update info when edgeType changes while iterating over different edgeTypes
        if (type != context_->edgeType_) {
            auto idxIter = edgeContext_->indexMap_.find(type);
            CHECK(idxIter != edgeContext_->indexMap_.end());
            auto schemaIter = edgeContext_->schemas_.find(std::abs(type));
            CHECK(schemaIter != edgeContext_->schemas_.end());
            CHECK(!schemaIter->second.empty());

            context_->edgeSchema_ = schemaIter->second.back().get();
            // idx is the index in all edges need to return
            auto idx = idxIter->second;
            context_->edgeType_ = type;
            context_->edgeName_ = edgeNodes_[iter_->getIdx()]->getEdgeName();
            // the columnIdx_ would be the column index in a response row, so need to add
            // the offset of tags and other fields
            context_->columnIdx_ = edgeContext_->offset_ + idx;
            context_->props_ = &(edgeContext_->propContexts_[idx].second);

            expCtx_->resetSchema(context_->edgeName_, context_->edgeSchema_, true);
        }
    }

private:
    RunTimeContext* context_;
    std::vector<TagNode*> tagNodes_;
    std::vector<SingleEdgeNode*> edgeNodes_;
    TagContext* tagContext_;
    EdgeContext* edgeContext_;
    StorageExpressionContext* expCtx_;

    std::unique_ptr<MultiEdgeIterator> iter_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_HASHJOINNODE_H_
