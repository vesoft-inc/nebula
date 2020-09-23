/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/SortExecutor.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SortExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* sort = asNode<Sort>(node());
    auto iter = ectx_->getResult(sort->inputVar()).iter();
    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in sort executor");
    }
    if (UNLIKELY(iter->isDefaultIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported DefaultIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (UNLIKELY(iter->isGetNeighborsIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported GetNeighborsIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }

    auto &factors = sort->factors();
    auto comparator = [&factors] (const LogicalRow &lhs, const LogicalRow &rhs) {
        for (auto &item : factors) {
            auto index = item.first;
            auto orderType = item.second;
            if (lhs[index] == rhs[index]) {
                continue;
            }

            if (orderType == OrderFactor::OrderType::ASCEND) {
                return lhs[index] < rhs[index];
            } else if (orderType == OrderFactor::OrderType::DESCEND) {
                return lhs[index] > rhs[index];
            }
        }
        return false;
    };

    if (iter->isSequentialIter()) {
        auto seqIter = static_cast<SequentialIter*>(iter.get());
        std::sort(seqIter->begin(), seqIter->end(), comparator);
    } else if (iter->isJoinIter()) {
        auto joinIter = static_cast<JoinIter*>(iter.get());
        std::sort(joinIter->begin(), joinIter->end(), comparator);
    } else if (iter->isPropIter()) {
        auto propIter = static_cast<PropIter*>(iter.get());
        std::sort(propIter->begin(), propIter->end(), comparator);
    }
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
