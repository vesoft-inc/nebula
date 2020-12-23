/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETPROPNODE_H_
#define STORAGE_EXEC_GETPROPNODE_H_

#include "common/base/Base.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"

namespace nebula {
namespace storage {

class GetTagPropNode : public QueryNode<VertexID> {
public:
    using RelNode<VertexID>::execute;

    explicit GetTagPropNode(PlanContext *planCtx,
                            std::vector<TagNode*> tagNodes,
                            nebula::DataSet* resultDataSet,
                            VertexCache* vertexCache)
        : planContext_(planCtx)
        , tagNodes_(std::move(tagNodes))
        , resultDataSet_(resultDataSet)
        , vertexCache_(vertexCache) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        // todo(doodle): revert to previous behavior, here is an extra io, will be deleted asap
        // check is vertex exists
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId, vId);
        auto ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_,
                                                        partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        if (iter == nullptr || !iter->valid()) {
            // not emplace row when vertex not exists
            return kvstore::ResultCode::SUCCEEDED;
        }

        ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        List row;
        // vertexId is the first column
        if (planContext_->isIntId_) {
            row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
            row.emplace_back(vId);
        }
        auto vIdLen = planContext_->vIdLen_;
        auto isIntId = planContext_->isIntId_;
        for (auto* tagNode : tagNodes_) {
            ret = tagNode->collectTagPropsIfValid(
                [&row](const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    for (const auto& prop : *props) {
                        if (prop.returned_) {
                            row.emplace_back(Value());
                        }
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &row, vIdLen, isIntId, &vId, tagNode] (folly::StringPiece key,
                                                              RowReader* reader,
                                                              const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    if (!QueryUtils::collectVertexProps(key, vIdLen, isIntId,
                                                        reader, props, row).ok()) {
                        return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                    }
                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        auto tagId = tagNode->getTagId();
                        vertexCache_->insert(std::make_pair(vId, tagId), reader->getData());
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        resultDataSet_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    PlanContext*                planContext_;
    std::vector<TagNode*>       tagNodes_;
    nebula::DataSet*            resultDataSet_;
    VertexCache*                vertexCache_;
};

class GetEdgePropNode : public QueryNode<cpp2::EdgeKey> {
public:
    using RelNode::execute;

    GetEdgePropNode(PlanContext *planCtx,
                    std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                    nebula::DataSet* resultDataSet)
        : planContext_(planCtx)
        , edgeNodes_(std::move(edgeNodes))
        , resultDataSet_(resultDataSet) {}

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        List row;
        auto vIdLen = planContext_->vIdLen_;
        auto isIntId = planContext_->isIntId_;
        for (auto* edgeNode : edgeNodes_) {
            ret = edgeNode->collectEdgePropsIfValid(
                [&row] (const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    for (const auto& prop : *props) {
                        if (prop.returned_) {
                            row.emplace_back(Value());
                        }
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [&row, vIdLen, isIntId] (folly::StringPiece key,
                                         RowReader* reader,
                                         const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    if (!QueryUtils::collectEdgeProps(key, vIdLen, isIntId,
                                                      reader, props, row).ok()) {
                        return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        resultDataSet_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    PlanContext*                            planContext_;
    std::vector<EdgeNode<cpp2::EdgeKey>*>   edgeNodes_;
    nebula::DataSet*                        resultDataSet_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETPROPNODE_H_
