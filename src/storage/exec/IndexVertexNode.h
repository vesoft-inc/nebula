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

    IndexVertexNode(RunTimeContext* context,
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

        auto ttlProp = CommonUtils::ttlProps(context_->tagSchema_);

        data_.clear();
        std::vector<VertexID> vids;
        auto* iter = static_cast<VertexIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            if (!iter->val().empty() && ttlProp.first) {
                auto v = IndexKeyUtils::parseIndexTTL(iter->val());
                if (CommonUtils::checkDataExpiredForTTL(context_->tagSchema_,
                                                        std::move(v),
                                                        ttlProp.second.second,
                                                        ttlProp.second.first)) {
                    iter->next();
                    continue;
                }
            }
            vids.emplace_back(iter->vId());
            iter->next();
        }
        for (const auto& vId : vids) {
            VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << context_->tagId_;
            auto key = NebulaKeyUtils::vertexKey(context_->vIdLen(), partId, vId, context_->tagId_);
            std::string val;
            ret = context_->env()->kvstore_->get(context_->spaceId(), partId, key, &val);
            if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
                data_.emplace_back(std::move(key), std::move(val));
            } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
                continue;
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
#endif   // STORAGE_EXEC_INDEXVERTEXNODE_H_
