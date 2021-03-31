/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXEDGENODE_H_
#define STORAGE_EXEC_INDEXEDGENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexEdgeNode final : public RelNode<T> {
public:
    using RelNode<T>::execute;

    IndexEdgeNode(PlanContext* planCtx,
                  IndexScanNode<T>* indexScanNode,
                  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
                  const std::string& schemaName)
        : planContext_(planCtx)
        , indexScanNode_(indexScanNode)
        , schemas_(schemas)
        , schemaName_(schemaName) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        data_.clear();
        std::vector<storage::cpp2::EdgeKey> edges;
        auto* iter = static_cast<EdgeIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            storage::cpp2::EdgeKey edge;
            edge.set_src(iter->srcId());
            edge.set_edge_type(planContext_->edgeType_);
            edge.set_ranking(iter->ranking());
            edge.set_dst(iter->dstId());
            edges.emplace_back(std::move(edge));
            iter->next();
        }
        for (const auto& edge : edges) {
            auto prefix = NebulaKeyUtils::edgePrefix(planContext_->vIdLen_,
                                                     partId,
                                                     (*edge.src_ref()).getStr(),
                                                     planContext_->edgeType_,
                                                     edge.get_ranking(),
                                                     (*edge.dst_ref()).getStr());
            std::unique_ptr<kvstore::KVIterator> eIter;
            ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_,
                                                       partId, prefix, &eIter);
            if (ret == kvstore::ResultCode::SUCCEEDED && eIter && eIter->valid()) {
                data_.emplace_back(eIter->key(), eIter->val());
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
    IndexScanNode<T>*                                                     indexScanNode_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas_;
    const std::string&                                                    schemaName_;
    std::vector<kvstore::KV>                                              data_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXEDGENODE_H_
