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
that case, FilterNode has a upstream of HashJoinNode, which will keep popping out edge
data. All tage data has been put into ExpressionContext before FilterNode is executed.
By that means, it can check the filter of tag + edge.
*/
template<typename T>
class FilterNode : public IterateNode<T> {
public:
    using RelNode<T>::execute;

    FilterNode(RunTimeContext* context,
               IterateNode<T>* upstream,
               StorageExpressionContext* expCtx = nullptr,
               Expression* exp = nullptr)
        : IterateNode<T>(upstream)
        , context_(context)
        , expCtx_(expCtx)
        , filterExp_(exp) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId, const T& vId) override {
        auto ret = RelNode<T>::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        do {
            if (context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                break;
            }
            if (this->valid() && !check()) {
                context_->resultStat_ = ResultStatus::FILTER_OUT;
                this->next();
                continue;
            }
            break;
        } while (true);
        return nebula::cpp2::ErrorCode::SUCCEEDED;
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
            if (ret.isBool() && ret.getBool()) {
                return true;
            }
            return false;
        }
        return true;
    }

private:
    RunTimeContext                   *context_;
    StorageExpressionContext         *expCtx_;
    Expression                       *filterExp_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
