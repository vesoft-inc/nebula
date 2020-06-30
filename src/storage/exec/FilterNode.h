/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/exec/HashJoinNode.h"

namespace nebula {
namespace storage {

// FilterNode will receive the result from a HashJoinNode, check whether tag data and edge data
// could pass the expression filter
class FilterNode : public IterateEdgeNode<VertexID> {
public:
    FilterNode(IterateEdgeNode* hashJoinNode,
               StorageExpressionContext* expCtx = nullptr,
               Expression* exp = nullptr)
        : IterateEdgeNode(hashJoinNode)
        , expCtx_(expCtx)
        , exp_(exp) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        while (upstream_->valid() && !check()) {
            upstream_->next();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    // return true when the value iter points to a value which can filter
    bool check() override {
        if (exp_ != nullptr) {
            expCtx_->reset(upstream_->reader(), upstream_->key(), upstream_->edgeName());
            auto result = exp_->eval(*expCtx_);
            if (result.type() == Value::Type::BOOL) {
                return result.getBool();
            } else {
                return false;
            }
        }
        return true;
    }

private:
    StorageExpressionContext* expCtx_;
    Expression* exp_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
