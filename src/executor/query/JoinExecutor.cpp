/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/JoinExecutor.h"

#include "planner/Query.h"
#include "context/QueryExpressionContext.h"
#include "context/Iterator.h"

namespace nebula {
namespace graph {

Status JoinExecutor::checkInputDataSets() {
    auto* join = asNode<Join>(node());
    auto lhsIter = ectx_->getVersionedResult(join->leftVar().first, join->leftVar().second).iter();
    DCHECK(!!lhsIter);
    VLOG(1) << "lhs: " << join->leftVar().first << " " << lhsIter->size();
    if (lhsIter->isGetNeighborsIter() || lhsIter->isDefaultIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return Status::Error(ss.str());
    }
    auto rhsIter =
        ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second).iter();
    DCHECK(!!rhsIter);
    VLOG(1) << "rhs: " << join->rightVar().first << " " << rhsIter->size();
    if (rhsIter->isGetNeighborsIter() || rhsIter->isDefaultIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << rhsIter->kind();
        return Status::Error(ss.str());
    }
    return Status::OK();
}

void JoinExecutor::buildHashTable(const std::vector<Expression*>& hashKeys, Iterator* iter) {
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx(iter));
            list.values.emplace_back(std::move(val));
        }

        auto& vals = hashTable_[list];
        vals.emplace_back(iter->row());
    }
}

}  // namespace graph
}  // namespace nebula
