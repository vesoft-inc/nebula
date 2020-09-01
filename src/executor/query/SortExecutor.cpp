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
    if (UNLIKELY(iter->isGetNeighborsIter())) {
        std::string errMsg = "Internal error: Sort executor does not supported GetNeighborsIter";
        LOG(ERROR) << errMsg;
        return Status::Error(errMsg);
    }
    if (iter->isSequentialIter()) {
        auto seqIter = static_cast<SequentialIter*>(iter.get());
        auto &factors = sort->factors();
        auto &colIndices = seqIter->getColIndices();
        std::vector<std::pair<size_t, OrderFactor::OrderType>> indexes;
        for (auto &factor : factors) {
            auto indexFind = colIndices.find(factor.first);
            if (indexFind == colIndices.end()) {
                LOG(ERROR) << "Column name `" << factor.first
                           << "' does not exist.";
                return Status::Error("Column name `%s' does not exist.",
                                     factor.first.c_str());
            }
            indexes.emplace_back(std::make_pair(indexFind->second, factor.second));
        }
        auto comparator = [&indexes] (const LogicalRow &lhs, const LogicalRow &rhs) {
            for (auto &item : indexes) {
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
        std::sort(seqIter->begin(), seqIter->end(), comparator);
    }
    // TODO: Sort the join iter.
    return finish(ResultBuilder().value(iter->valuePtr()).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
