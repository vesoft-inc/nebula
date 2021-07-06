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

    IndexEdgeNode(RunTimeContext* context,
                  IndexScanNode<T>* indexScanNode,
                  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas,
                  const std::string& schemaName)
        : context_(context)
        , indexScanNode_(indexScanNode)
        , schemas_(schemas)
        , schemaName_(schemaName) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        auto ttlProp = CommonUtils::ttlProps(context_->edgeSchema_);

        data_.clear();
        std::vector<storage::cpp2::EdgeKey> edges;
        auto* iter = static_cast<EdgeIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            if (!iter->val().empty() && ttlProp.first) {
                auto v = IndexKeyUtils::parseIndexTTL(iter->val());
                if (CommonUtils::checkDataExpiredForTTL(context_->edgeSchema_,
                                                        std::move(v),
                                                        ttlProp.second.second,
                                                        ttlProp.second.first)) {
                    iter->next();
                    continue;
                }
            }
            storage::cpp2::EdgeKey edge;
            edge.set_src(iter->srcId());
            edge.set_edge_type(context_->edgeType_);
            edge.set_ranking(iter->ranking());
            edge.set_dst(iter->dstId());
            edges.emplace_back(std::move(edge));
            iter->next();
        }
        for (const auto& edge : edges) {
            auto prefix = NebulaKeyUtils::edgePrefix(context_->vIdLen(),
                                                     partId,
                                                     (*edge.src_ref()).getStr(),
                                                     context_->edgeType_,
                                                     edge.get_ranking(),
                                                     (*edge.dst_ref()).getStr());
            std::unique_ptr<kvstore::KVIterator> eIter;
            ret = context_->env()->kvstore_->prefix(context_->spaceId(),
                                                       partId, prefix, &eIter);
            if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && eIter && eIter->valid()) {
                data_.emplace_back(eIter->key(), eIter->val());
            } else {
                return ret;
            }
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
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
    RunTimeContext*                                                       context_;
    IndexScanNode<T>*                                                     indexScanNode_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& schemas_;
    const std::string&                                                    schemaName_;
    std::vector<kvstore::KV>                                              data_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXEDGENODE_H_
