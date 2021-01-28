/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXVERTEXNODE_H_
#define STORAGE_EXEC_INDEXVERTEXNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/IndexScanNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexVertexNode final : public RelNode<T> {
public:
    using RelNode<T>::execute;

    IndexVertexNode(PlanContext* planCtx,
                    VertexCache* vertexCache,
                    IndexScanNode<T>* indexScanNode,
                    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
                    const std::string& schemaName)
        : planContext_(planCtx)
        , vertexCache_(vertexCache)
        , indexScanNode_(indexScanNode)
        , schemas_(schemas)
        , schemaName_(schemaName) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        data_.clear();
        std::vector<VertexID> vids;
        auto* iter = static_cast<VertexIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            vids.emplace_back(iter->vId());
            iter->next();
        }
        for (const auto& vId : vids) {
            VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << planContext_->tagId_;
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                auto result = vertexCache_->get(std::make_pair(vId, planContext_->tagId_));
                if (result.ok()) {
                    auto vertexKey = NebulaKeyUtils::vertexKey(planContext_->vIdLen_,
                                                               partId,
                                                               vId,
                                                               planContext_->tagId_);
                    data_.emplace_back(std::move(vertexKey), std::move(result).value());
                    continue;
                } else {
                    VLOG(1) << "Miss cache for vId " << vId << ", tagId " << planContext_->tagId_;
                }
            }

            std::unique_ptr<kvstore::KVIterator> vIter;
            auto prefix = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId,
                                                       vId, planContext_->tagId_);
            ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_,
                                                       partId, prefix, &vIter);
            if (ret == kvstore::ResultCode::SUCCEEDED && vIter && vIter->valid()) {
                data_.emplace_back(vIter->key(), vIter->val());
            } else {
                return ret;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    std::vector<kvstore::KV> moveData() {
        return std::move(data_);
    }

    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& getSchemas() {
        return schemas_;
    }

    const std::string& getSchemaName() {
        return schemaName_;
    }

private:
    PlanContext*                                                          planContext_;
    VertexCache*                                                          vertexCache_;
    IndexScanNode<T>*                                                     indexScanNode_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas_;
    const std::string&                                                    schemaName_;
    std::vector<kvstore::KV>                                              data_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXVERTEXNODE_H_
