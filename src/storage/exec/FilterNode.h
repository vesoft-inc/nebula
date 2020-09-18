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
#include "storage/context/StorageExpressionContext.h"

namespace nebula {
namespace storage {

/*
FilterNode will receive the result from upstream, check whether tag data or edge data
could pass the expression filter. FilterNode can only accept one upstream node, user
must make sure that the upstream only output only tag data or edge data, but not both.

As for GetNeighbors, it will have filter that involves both tag and edge expression. In
that case, FilterNode has a upstream of HashJoinNode, which will keeps poping out edge
data. All tage data has been put into ExpressionContext before FilterNode is executed.
By that means, it can check the filter of tag + edge.
*/
template<typename T>
class FilterNode : public IterateNode<T> {
public:
    using RelNode<T>::execute;

    FilterNode(PlanContext* planCtx,
               IterateNode<T>* upstream,
               StorageExpressionContext* expCtx = nullptr,
               Expression* exp = nullptr)
        : IterateNode<T>(upstream)
        , planContext_(planCtx)
        , expCtx_(expCtx)
        , filterExp_(exp) {}

    kvstore::ResultCode execute(PartitionID partId, const T& vId) override {
        auto ret = RelNode<T>::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        do {
            if (planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                break;
            }
            if (this->valid() && !check()) {
                planContext_->resultStat_ = ResultStatus::FILTER_OUT;
                this->next();
                continue;
            }
            break;
        } while (true);
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    // return true when the value iter points to a value which can filter
    bool check() override {
        if (filterExp_ != nullptr) {
            expCtx_->reset(this->reader(), this->key().str());
            // result is false when filter out
            auto result = filterExp_->eval(*expCtx_);
            // NULL is always false
            auto ret = result.toBool();
            if (ret.ok() && ret.value()) {
                return true;
            }
            return false;
        }
        return true;
    }

private:
    PlanContext                      *planContext_;
    StorageExpressionContext         *expCtx_;
    Expression                       *filterExp_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
